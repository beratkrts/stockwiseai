#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rolling backtest + model selection for weekly material forecasts.

Models:
  - Intermittent (TSB)
  - ETS
  - Moving average (MA4, MA13, MA26)

Backtest:
  - last 52 weeks
  - 1-week-ahead rolling origin
Inactive rule:
  - if last 26 weeks sum = 0, force forecast = 0

Outputs:
  - material_level_backtest.csv
  - category_unit_backtest.csv
  - overall_backtest.csv
"""

import argparse
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd
import psycopg2
from statsmodels.tsa.holtwinters import ExponentialSmoothing, SimpleExpSmoothing

HISTORY_MIN = 4
BACKTEST_WEEKS = 52
FORECAST_H = 12
ETS_ZERO_RATIO_MAX = 0.4
INACTIVE_WEEKS = 26

WEEKDAY_MAP = {
    0: "MON",
    1: "TUE",
    2: "WED",
    3: "THU",
    4: "FRI",
    5: "SAT",
    6: "SUN",
}


def pg_conn(conn_str: str):
    return psycopg2.connect(conn_str)


def load_weekly(conn_str: str) -> pd.DataFrame:
    conn = pg_conn(conn_str)
    df = pd.read_sql("""
        SELECT
            bom_material_name,
            bom_material_category,
            bom_unit_of_measure,
            week_start,
            qty::float
        FROM core.weekly_consumption
        ORDER BY bom_material_name, week_start
    """, conn)
    conn.close()

    df["week_start"] = pd.to_datetime(df["week_start"])
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0.0)
    df["bom_material_name"] = df["bom_material_name"].astype(str).fillna("").str.strip()
    df = df[df["bom_material_name"] != ""]
    return df


def to_weekly_series(series: pd.Series) -> pd.Series:
    s = series.groupby(series.index).sum().sort_index()
    if s.empty:
        return s
    freq = pd.infer_freq(s.index)
    if freq is None or not str(freq).startswith("W-"):
        mode_dow = int(pd.Series(s.index.dayofweek).mode().iloc[0])
        freq = f"W-{WEEKDAY_MAP[mode_dow]}"
    full_idx = pd.date_range(s.index.min(), s.index.max(), freq=freq)
    return s.reindex(full_idx).fillna(0.0)


def tsb_constant(series: pd.Series, alpha: float, beta: float) -> float:
    y = np.asarray(series, dtype=float)
    if len(y) == 0:
        return 0.0
    p = 0.5
    z = y[y > 0].mean() if (y > 0).any() else 0.0
    for v in y:
        occ = 1.0 if v > 0 else 0.0
        p = p + alpha * (occ - p)
        if v > 0:
            z = z + beta * (v - z)
    return float(p * z)


def tsb_forecast(series: pd.Series) -> float:
    grid = [(0.1, 0.1), (0.2, 0.2), (0.3, 0.2), (0.2, 0.3)]
    best = 0.0
    for a, b in grid:
        c = tsb_constant(series, a, b)
        if c > best:
            best = c
    return float(best)


def tsb_forecast_array(series: pd.Series, h: int) -> np.ndarray:
    const = tsb_forecast(series)
    return np.repeat(const, h)


def ets_forecast(series: pd.Series) -> float:
    try:
        m = ExponentialSmoothing(series, trend="add").fit(optimized=True)
        return float(m.forecast(1)[0])
    except Exception:
        try:
            m = SimpleExpSmoothing(series).fit()
            return float(m.forecast(1)[0])
        except Exception:
            return 0.0


def ets_forecast_array(series: pd.Series, h: int) -> np.ndarray:
    try:
        m = ExponentialSmoothing(series, trend="add").fit(optimized=True)
        return np.asarray(m.forecast(h), dtype=float)
    except Exception:
        try:
            m = SimpleExpSmoothing(series).fit()
            return np.asarray(m.forecast(h), dtype=float)
        except Exception:
            return np.zeros(h, dtype=float)


def ma_forecast_array(series: pd.Series, k: int, h: int) -> np.ndarray:
    if len(series) == 0:
        return np.zeros(h, dtype=float)
    return np.repeat(float(series.tail(k).mean()), h)


@dataclass
class MetricSums:
    abs_err: float = 0.0
    actual_sum: float = 0.0
    count: int = 0

    def add(self, actual: float, forecast: float) -> None:
        self.abs_err += abs(actual - forecast)
        self.actual_sum += float(actual)
        self.count += 1

    def wape(self) -> float:
        if self.actual_sum == 0:
            return float("inf") if self.abs_err > 0 else 0.0
        return (self.abs_err / self.actual_sum) * 100.0

    def mae(self) -> float:
        if self.count == 0:
            return float("inf")
        return self.abs_err / self.count


def backtest_material(series: pd.Series) -> Dict[str, MetricSums]:
    s = to_weekly_series(series)
    zero_ratio = float((s.tail(BACKTEST_WEEKS) == 0).mean()) if len(s) else 1.0
    methods = {
        "TSB": lambda x: tsb_forecast_array(x, FORECAST_H),
        "MA4": lambda x: ma_forecast_array(x, 4, FORECAST_H),
        "MA13": lambda x: ma_forecast_array(x, 13, FORECAST_H),
        "MA26": lambda x: ma_forecast_array(x, 26, FORECAST_H),
    }
    if zero_ratio < ETS_ZERO_RATIO_MAX:
        methods["ETS"] = lambda x: ets_forecast_array(x, FORECAST_H)
    sums = {m: MetricSums() for m in methods}

    last_date = s.index.max()
    if pd.isna(last_date):
        return sums
    end_asof = last_date - pd.Timedelta(weeks=FORECAST_H)
    if end_asof < s.index.min():
        return sums

    start_asof = end_asof - pd.Timedelta(weeks=BACKTEST_WEEKS - 1)
    target_asofs = s.loc[(s.index >= start_asof) & (s.index <= end_asof)].index

    for as_of in target_asofs:
        hist = s.loc[s.index <= as_of]
        if len(hist) < HISTORY_MIN:
            continue
        actual = float(s.loc[as_of + pd.Timedelta(weeks=1) : as_of + pd.Timedelta(weeks=FORECAST_H)].sum())
        for name, fn in methods.items():
            fc = np.asarray(fn(hist), dtype=float)
            fc_sum = float(np.maximum(0.0, fc).sum())
            sums[name].add(actual, fc_sum)

    return sums


def choose_best_method(sums: Dict[str, MetricSums]) -> str:
    best_method = None
    best_wape = float("inf")
    for m, s in sums.items():
        w = s.wape()
        if w < best_wape:
            best_method = m
            best_wape = w
    return best_method or "NO_DATA"


def material_inactive(series: pd.Series) -> bool:
    s = to_weekly_series(series)
    last_date = s.index.max()
    if pd.isna(last_date):
        return True
    recent = s.loc[s.index >= last_date - pd.Timedelta(weeks=INACTIVE_WEEKS - 1)]
    return float(recent.sum()) == 0.0


def zero_forecast_sums(series: pd.Series) -> MetricSums:
    s = to_weekly_series(series)
    last_date = s.index.max()
    if pd.isna(last_date):
        return MetricSums()
    end_asof = last_date - pd.Timedelta(weeks=FORECAST_H)
    if end_asof < s.index.min():
        return MetricSums()
    start_asof = end_asof - pd.Timedelta(weeks=BACKTEST_WEEKS - 1)
    target_asofs = s.loc[(s.index >= start_asof) & (s.index <= end_asof)].index
    sums = MetricSums()
    for as_of in target_asofs:
        actual = float(s.loc[as_of + pd.Timedelta(weeks=1) : as_of + pd.Timedelta(weeks=FORECAST_H)].sum())
        sums.add(actual, 0.0)
    return sums


def forecast_next_12w(series: pd.Series, method: str) -> np.ndarray:
    s = to_weekly_series(series)
    if method == "INACTIVE_ZERO":
        return np.zeros(FORECAST_H, dtype=float)
    if method == "TSB":
        return tsb_forecast_array(s, FORECAST_H)
    if method == "ETS":
        return ets_forecast_array(s, FORECAST_H)
    if method == "MA4":
        return ma_forecast_array(s, 4, FORECAST_H)
    if method == "MA13":
        return ma_forecast_array(s, 13, FORECAST_H)
    if method == "MA26":
        return ma_forecast_array(s, 26, FORECAST_H)
    return np.zeros(FORECAST_H, dtype=float)


def write_results_to_db(
    conn_str: str,
    forecast_rows: List[Dict[str, object]],
    summary_rows: List[Dict[str, object]],
    material_metrics: pd.DataFrame,
    category_metrics: pd.DataFrame,
    overall_metrics: pd.DataFrame,
) -> None:
    conn = pg_conn(conn_str)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS core.final_forecast;")
    cur.execute("DROP TABLE IF EXISTS core.final_forecast_summary;")
    cur.execute("DROP TABLE IF EXISTS core.final_forecast_material_metrics;")
    cur.execute("DROP TABLE IF EXISTS core.final_forecast_category_unit_metrics;")
    cur.execute("DROP TABLE IF EXISTS core.final_forecast_overall_metrics;")

    cur.execute("""
        CREATE TABLE core.final_forecast_summary (
            bom_material_name TEXT PRIMARY KEY,
            chosen_method TEXT,
            wape_12w NUMERIC,
            forecast_12w NUMERIC
        );
    """)

    cur.execute("""
        CREATE TABLE core.final_forecast (
            bom_material_name TEXT,
            week_start DATE,
            forecast_qty NUMERIC,
            chosen_method TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE core.final_forecast_material_metrics (
            bom_material_name TEXT,
            bom_material_category TEXT,
            bom_unit_of_measure TEXT,
            method TEXT,
            wape NUMERIC,
            mae NUMERIC,
            actual_sum NUMERIC,
            n_points INTEGER,
            inactive_flag INTEGER,
            best_method TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE core.final_forecast_category_unit_metrics (
            bom_material_category TEXT,
            bom_unit_of_measure TEXT,
            wape NUMERIC,
            mae NUMERIC,
            actual_sum NUMERIC,
            n_points INTEGER
        );
    """)

    cur.execute("""
        CREATE TABLE core.final_forecast_overall_metrics (
            scope TEXT,
            wape NUMERIC,
            mae NUMERIC,
            actual_sum NUMERIC,
            n_points INTEGER
        );
    """)

    for r in summary_rows:
        cur.execute("""
            INSERT INTO core.final_forecast_summary
            (bom_material_name, chosen_method, wape_12w, forecast_12w)
            VALUES (%s, %s, %s, %s)
        """, (
            r["bom_material_name"], r["chosen_method"],
            r["wape_12w"], r["forecast_12w"]
        ))

    for r in forecast_rows:
        cur.execute("""
            INSERT INTO core.final_forecast
            (bom_material_name, week_start, forecast_qty, chosen_method)
            VALUES (%s, %s, %s, %s)
        """, (
            r["bom_material_name"], r["week_start"],
            r["forecast_qty"], r["chosen_method"]
        ))

    for _, r in material_metrics.iterrows():
        cur.execute("""
            INSERT INTO core.final_forecast_material_metrics
            (bom_material_name, bom_material_category, bom_unit_of_measure, method, wape, mae, actual_sum, n_points, inactive_flag, best_method)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            r["bom_material_name"], r["bom_material_category"], r["bom_unit_of_measure"],
            r["method"], r["wape"], r["mae"], r["actual_sum"], int(r["n_points"]),
            int(r["inactive_flag"]), r["best_method"]
        ))

    for _, r in category_metrics.iterrows():
        cur.execute("""
            INSERT INTO core.final_forecast_category_unit_metrics
            (bom_material_category, bom_unit_of_measure, wape, mae, actual_sum, n_points)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            r["bom_material_category"], r["bom_unit_of_measure"],
            r["wape"], r["mae"], r["actual_sum"], int(r["n_points"])
        ))

    for _, r in overall_metrics.iterrows():
        cur.execute("""
            INSERT INTO core.final_forecast_overall_metrics
            (scope, wape, mae, actual_sum, n_points)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            r["scope"], r["wape"], r["mae"], r["actual_sum"], int(r["n_points"])
        ))

    conn.commit()
    conn.close()


def main(conn_str: str, out_material: str, out_category: str, out_overall: str) -> None:
    df = load_weekly(conn_str)
    if df.empty:
        raise SystemExit("No data in core.weekly_consumption")

    material_info = (
        df.groupby("bom_material_name")[["bom_material_category", "bom_unit_of_measure"]]
          .first()
          .reset_index()
    )

    material_rows: List[Dict[str, object]] = []
    category_sums: Dict[Tuple[str, str], MetricSums] = {}
    overall_sums = MetricSums()
    forecast_rows: List[Dict[str, object]] = []
    summary_rows: List[Dict[str, object]] = []

    for mat, g in df.groupby("bom_material_name"):
        s = g.set_index("week_start")["qty"].sort_index()
        sums = backtest_material(s)
        inactive = material_inactive(s)
        best_method = "INACTIVE_ZERO" if inactive else choose_best_method(sums)

        if inactive:
            best_sums = zero_forecast_sums(s)
        else:
            best_sums = sums.get(best_method, MetricSums())

        info = material_info[material_info["bom_material_name"] == mat].iloc[0]
        cat = str(info["bom_material_category"])
        unit = str(info["bom_unit_of_measure"])

        key = (cat, unit)
        category_sums.setdefault(key, MetricSums())
        category_sums[key].abs_err += best_sums.abs_err
        category_sums[key].actual_sum += best_sums.actual_sum
        category_sums[key].count += best_sums.count

        overall_sums.abs_err += best_sums.abs_err
        overall_sums.actual_sum += best_sums.actual_sum
        overall_sums.count += best_sums.count

        for m, ms in sums.items():
            material_rows.append({
                "bom_material_name": mat,
                "bom_material_category": cat,
                "bom_unit_of_measure": unit,
                "method": m,
                "wape": ms.wape(),
                "mae": ms.mae(),
                "actual_sum": ms.actual_sum,
                "n_points": ms.count,
                "inactive_flag": int(inactive),
                "best_method": best_method,
            })

        # Add chosen method row for easy filtering
        material_rows.append({
            "bom_material_name": mat,
            "bom_material_category": cat,
            "bom_unit_of_measure": unit,
            "method": "BEST",
            "wape": best_sums.wape(),
            "mae": best_sums.mae(),
            "actual_sum": best_sums.actual_sum,
            "n_points": best_sums.count,
            "inactive_flag": int(inactive),
            "best_method": best_method,
        })

        fc = forecast_next_12w(s, best_method)
        fc = np.maximum(0.0, np.asarray(fc, dtype=float))
        last_date = to_weekly_series(s).index.max()
        if pd.notna(last_date):
            future_idx = pd.date_range(last_date + pd.Timedelta(weeks=1), periods=FORECAST_H, freq=pd.infer_freq(to_weekly_series(s).index) or "W-TUE")
            for dt, qty in zip(future_idx, fc):
                forecast_rows.append({
                    "bom_material_name": mat,
                    "week_start": dt.date(),
                    "forecast_qty": float(qty),
                    "chosen_method": best_method,
                })

        summary_rows.append({
            "bom_material_name": mat,
            "chosen_method": best_method,
            "wape_12w": best_sums.wape(),
            "forecast_12w": float(np.sum(fc)),
        })

    material_df = pd.DataFrame(material_rows)
    material_df.to_csv(out_material, index=False)

    category_rows = []
    for (cat, unit), sums in category_sums.items():
        category_rows.append({
            "bom_material_category": cat,
            "bom_unit_of_measure": unit,
            "wape": sums.wape(),
            "mae": sums.mae(),
            "actual_sum": sums.actual_sum,
            "n_points": sums.count,
        })
    category_df = pd.DataFrame(category_rows)
    category_df.to_csv(out_category, index=False)

    overall_df = pd.DataFrame([{
        "scope": "ALL_MATERIALS_BEST",
        "wape": overall_sums.wape(),
        "mae": overall_sums.mae(),
        "actual_sum": overall_sums.actual_sum,
        "n_points": overall_sums.count,
    }])
    overall_df.to_csv(out_overall, index=False)

    write_results_to_db(
        conn_str=conn_str,
        forecast_rows=forecast_rows,
        summary_rows=summary_rows,
        material_metrics=material_df,
        category_metrics=category_df,
        overall_metrics=overall_df,
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg-conn", required=True)
    ap.add_argument("--out-material", default="material_level_backtest.csv")
    ap.add_argument("--out-category", default="category_unit_backtest.csv")
    ap.add_argument("--out-overall", default="overall_backtest.csv")
    args = ap.parse_args()

    main(args.pg_conn, args.out_material, args.out_category, args.out_overall)
