import os
from forecast_backtest import main


def build_conn_str() -> str:
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB", "tkis_stockwise")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "postgres")
    return f"dbname={db} user={user} password={password} host={host} port={port}"


if __name__ == "__main__":
    conn_str = build_conn_str()
    main(
        conn_str=conn_str,
        out_material=os.getenv("FORECAST_OUT_MATERIAL", "material_level_backtest.csv"),
        out_category=os.getenv("FORECAST_OUT_CATEGORY", "category_unit_backtest.csv"),
        out_overall=os.getenv("FORECAST_OUT_OVERALL", "overall_backtest.csv"),
    )
