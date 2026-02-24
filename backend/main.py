from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
import psycopg
from psycopg_pool import ConnectionPool
import os
from typing import Optional
import io
from datetime import datetime
import xlsxwriter

from dotenv import load_dotenv


load_dotenv()


PG_CONNINFO = (
    f"dbname={os.getenv('PG_DBNAME', 'tkis_stockwise')} "
    f"user={os.getenv('PG_USER', 'postgres')} "
    f"password={os.getenv('PG_PASSWORD', '1234')} "
    f"host={os.getenv('PG_HOST', 'localhost')} "
    f"port={os.getenv('PG_PORT', '5432')} "
    "options='-c client_encoding=UTF8'"
)

pool: Optional[ConnectionPool] = None


def get_pool() -> ConnectionPool:
    global pool
    if pool is None:
        pool = ConnectionPool(conninfo=PG_CONNINFO, min_size=1, max_size=5, open=True)
    return pool


def run_query(sql: str, params=None):
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

def build_materials_query(
    category: str,
    q: str | None,
    status: str | None,
    supplier: str | None,
    item_no: str | None,
    sort_by: str | None,
    sort_dir: str | None,
):
    tr_norm_expr = "lower(translate({col}, 'İIıiŞşĞğÜüÖöÇç', 'iiiissgguuoocc'))"
    tr_norm_param = "lower(translate(%s, 'İIıiŞşĞğÜüÖöÇç', 'iiiissgguuoocc'))"
    sort_map = {
        "bom_material_name": "o.bom_material_name",
        "unit_of_measure": "o.unit_of_measure",
        "material_category": "o.material_category",
        "forecast_12w": "o.forecast_12w",
        "wape": "m.wape",
        "current_stock": "o.current_stock",
        "safety_status": "o.safety_status",
    }
    sort_col = sort_map.get(sort_by or "")
    sort_dir_sql = None
    if sort_dir:
        if sort_dir.lower() == "asc":
            sort_dir_sql = "ASC"
        elif sort_dir.lower() == "desc":
            sort_dir_sql = "DESC"
    if sort_col:
        sort_dir_sql = sort_dir_sql or "ASC"
        order_sql = f"ORDER BY {sort_col} {sort_dir_sql} NULLS LAST"
    else:
        order_sql = "ORDER BY o.safety_status, o.bom_material_name"

    params = []
    if category.upper() == "OTHER":
        where = ["o.material_category NOT IN (%s,%s,%s)"]
        params.extend(["KUMAŞ", "SLAT", "Profil"])
    else:
        where = ["o.material_category = %s"]
        params.append(category)

    if q:
        where.append(f"{tr_norm_expr.format(col='o.bom_material_name')} LIKE {tr_norm_param}")
        params.append(f"%{q}%")

    if status:
        where.append("o.safety_status = %s")
        params.append(status)

    if supplier:
        where.append(
            """
            EXISTS (
                SELECT 1
                FROM core.dashboard_material_variants v
                LEFT JOIN raw.stock_master sm
                  ON sm.adi = v.stock_adi
                WHERE v.bom_material_name = o.bom_material_name
                  AND %s = ANY (
                      ARRAY[
                          sm.tedarikci_1,
                          sm.tedarikci_2,
                          sm.tedarikci_3,
                          sm.tedarikci_4,
                          sm.tedarikci_5
                      ]
                  )
            )
            """
        )
        params.append(supplier)

    if item_no:
        where.append(
            """
            (
                EXISTS (
                    SELECT 1
                    FROM core.bom_unique_materials bu1
                    WHERE bu1.material_name = o.bom_material_name
                      AND CAST(bu1.item_no AS TEXT) ILIKE %s
                )
                OR EXISTS (
                    SELECT 1
                    FROM core.dashboard_material_variants v
                    LEFT JOIN core.bom_unique_materials bu2
                      ON bu2.material_name = v.stock_adi
                    LEFT JOIN raw.stock_master sm2
                      ON sm2.adi = v.stock_adi
                    WHERE v.bom_material_name = o.bom_material_name
                      AND COALESCE(CAST(bu2.item_no AS TEXT), CAST(sm2.turu3 AS TEXT)) ILIKE %s
                )
            )
            """
        )
        item_no_like = f"%{item_no}%"
        params.extend([item_no_like, item_no_like])

    where_sql = " AND ".join(where)
    return where_sql, params, order_sql


app = FastAPI(title="TKIS Materials API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/materials")
def list_materials(
    category: str,
    q: str | None = Query(None, description="Search bom_material_name"),
    status: str | None = Query(None, description="safety_status filter"),
    supplier: str | None = Query(None, description="Filter by supplier"),
    item_no: str | None = Query(None, description="Filter by item_no"),
    sort_by: str | None = Query(None, description="Sort column"),
    sort_dir: str | None = Query(None, description="Sort direction (asc/desc)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    where_sql, params, order_sql = build_materials_query(
        category=category,
        q=q,
        status=status,
        supplier=supplier,
        item_no=item_no,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )

    total = run_query(
        f"SELECT COUNT(*) AS cnt FROM core.dashboard_material_overview o WHERE {where_sql}",
        params,
    )[0]["cnt"]

    offset = (page - 1) * page_size
    rows = run_query(
        f"""
        SELECT o.bom_material_name,
               o.unit_of_measure,
               o.material_category,
               o.forecast_12w,
               m.wape,
               o.current_stock,
               o.safety_status,
               bu.item_no
        FROM core.dashboard_material_overview o
        LEFT JOIN core.final_forecast_material_metrics m
          ON m.bom_material_name = o.bom_material_name
         AND m.method = 'BEST'
        LEFT JOIN core.bom_unique_materials bu
          ON bu.material_name = o.bom_material_name
        WHERE {where_sql}
        {order_sql}
        LIMIT %s OFFSET %s
        """,
        params + [page_size, offset],
    )

    return {
        "items": rows,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total - 1) // page_size + 1 if total else 0,
    }


@app.get("/materials-export")
def export_materials(
    category: str,
    q: str | None = Query(None),
    status: str | None = Query(None),
    supplier: str | None = Query(None),
    item_no: str | None = Query(None),
    sort_by: str | None = Query(None),
    sort_dir: str | None = Query(None),
):
    where_sql, params, order_sql = build_materials_query(
        category=category,
        q=q,
        status=status,
        supplier=supplier,
        item_no=item_no,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    rows = run_query(
        f"""
        SELECT o.bom_material_name,
               bu.item_no,
               o.unit_of_measure,
               o.material_category,
               o.forecast_12w,
               m.wape,
               o.current_stock,
               o.safety_status
        FROM core.dashboard_material_overview o
        LEFT JOIN core.final_forecast_material_metrics m
          ON m.bom_material_name = o.bom_material_name
         AND m.method = 'BEST'
        LEFT JOIN core.bom_unique_materials bu
          ON bu.material_name = o.bom_material_name
        WHERE {where_sql}
        {order_sql}
        """,
        params,
    )

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    worksheet = workbook.add_worksheet("Material List")

    header_fmt = workbook.add_format({"bold": True, "bg_color": "#111827", "font_color": "#FFFFFF"})
    num_fmt = workbook.add_format({"num_format": "0.00"})
    status_formats = {
        "CRITICAL": workbook.add_format({"bg_color": "#FCA5A5", "font_color": "#7F1D1D"}),
        "MEDIUM": workbook.add_format({"bg_color": "#FDE68A", "font_color": "#78350F"}),
        "SAFE": workbook.add_format({"bg_color": "#BBF7D0", "font_color": "#065F46"}),
        "EN_BILGISI_EKSIK": workbook.add_format({"bg_color": "#BFDBFE", "font_color": "#1D4ED8"}),
    }
    status_num_formats = {
        "CRITICAL": workbook.add_format({"bg_color": "#FCA5A5", "font_color": "#7F1D1D", "num_format": "0.00"}),
        "MEDIUM": workbook.add_format({"bg_color": "#FDE68A", "font_color": "#78350F", "num_format": "0.00"}),
        "SAFE": workbook.add_format({"bg_color": "#BBF7D0", "font_color": "#065F46", "num_format": "0.00"}),
        "EN_BILGISI_EKSIK": workbook.add_format({"bg_color": "#BFDBFE", "font_color": "#1D4ED8", "num_format": "0.00"}),
    }

    headers = [
        "Material",
        "Item No",
        "Unit",
        "Category",
        "12W Forecast",
        "Error Rate (1Y)",
        "Stock",
        "Status",
    ]
    worksheet.write_row(0, 0, headers, header_fmt)

    for idx, row in enumerate(rows, start=1):
        status = row.get("safety_status") or ""
        row_fmt = status_formats.get(status)
        row_num_fmt = status_num_formats.get(status, num_fmt)

        worksheet.write(idx, 0, row["bom_material_name"] or "", row_fmt)
        worksheet.write(idx, 1, row.get("item_no") or "", row_fmt)
        worksheet.write(idx, 2, row.get("unit_of_measure") or "", row_fmt)
        worksheet.write(idx, 3, row.get("material_category") or "", row_fmt)
        if row.get("forecast_12w") is None:
            worksheet.write(idx, 4, "", row_fmt)
        else:
            worksheet.write_number(idx, 4, float(row["forecast_12w"]), row_num_fmt)
        if row.get("wape") is None:
            worksheet.write(idx, 5, "", row_fmt)
        else:
            worksheet.write_number(idx, 5, float(row["wape"]), row_num_fmt)
        if row.get("current_stock") is None:
            worksheet.write(idx, 6, "", row_fmt)
        else:
            worksheet.write_number(idx, 6, float(row["current_stock"]), row_num_fmt)
        if row_fmt:
            worksheet.write(idx, 7, status, row_fmt)
        else:
            worksheet.write(idx, 7, status)

    worksheet.set_column(0, 0, 40)
    worksheet.set_column(1, 1, 14)
    worksheet.set_column(2, 2, 10)
    worksheet.set_column(3, 3, 12)
    worksheet.set_column(4, 6, 16)
    worksheet.set_column(7, 7, 14)

    workbook.close()
    output.seek(0)
    stamp = datetime.now().strftime("%Y-%m-%d")
    filename = f"material_list_{stamp}.xlsx"
    headers = {"Content-Disposition": f'attachment; filename=\"{filename}\"'}
    return Response(
        content=output.read(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@app.get("/materials/{material_name:path}/variants")
def material_variants(material_name: str):
    rows = run_query(
        """
        SELECT
            v.stock_adi,
            v.warehouse,
            v.current_stock,
            v.stock_uom,
            v.open_order_in_transit,
            COALESCE(CAST(bu.item_no AS TEXT), CAST(sm.turu3 AS TEXT)) AS item_no,
            sm.tedarikci_1,
            sm.tedarikci_2,
            sm.tedarikci_3,
            sm.tedarikci_4,
            sm.tedarikci_5
        FROM core.dashboard_material_variants v
        LEFT JOIN raw.stock_master sm
          ON sm.adi = v.stock_adi
        LEFT JOIN core.bom_unique_materials bu
          ON bu.material_name = v.stock_adi
        WHERE v.bom_material_name = %s
        ORDER BY v.stock_adi, v.warehouse
        """,
        (material_name,),
    )
    if rows is None:
        raise HTTPException(status_code=404, detail="Material not found")
    return {"items": rows}


@app.get("/materials/{material_name:path}/suppliers")
def material_suppliers(material_name: str):
    rows = run_query(
        """
        SELECT DISTINCT supplier
        FROM (
            SELECT UNNEST(
                ARRAY[
                    sm.tedarikci_1,
                    sm.tedarikci_2,
                    sm.tedarikci_3,
                    sm.tedarikci_4,
                    sm.tedarikci_5
                ]
            ) AS supplier
            FROM core.dashboard_material_variants v
            LEFT JOIN raw.stock_master sm
              ON sm.adi = v.stock_adi
            WHERE v.bom_material_name = %s
        ) s
        WHERE supplier IS NOT NULL
          AND supplier <> ''
        ORDER BY supplier
        """,
        (material_name,),
    )
    return {"items": [row["supplier"] for row in (rows or [])]}


@app.get("/suppliers")
def list_suppliers():
    rows = run_query(
        """
        SELECT DISTINCT supplier
        FROM (
            SELECT UNNEST(
                ARRAY[
                    tedarikci_1,
                    tedarikci_2,
                    tedarikci_3,
                    tedarikci_4,
                    tedarikci_5
                ]
            ) AS supplier
            FROM raw.stock_master
        ) s
        WHERE supplier IS NOT NULL
          AND supplier <> ''
        ORDER BY supplier
        """
    )
    return {"items": [row["supplier"] for row in (rows or [])]}


@app.get("/materials/{material_name:path}/flow-observation")
def material_flow_observation(material_name: str):
    rows = run_query(
        """
        SELECT
            bom_material_name,
            stock_adi,
            warehouse,
            w22_out_qty,
            w22_out_uom,
            seat_consumed_qty,
            seat_consumed_uom,
            count_end_date
        FROM core.dashboard_material_flow_observation
        WHERE bom_material_name = %s
        ORDER BY warehouse, stock_adi
        """,
        (material_name,),
    )
    return {"items": rows or []}


@app.get("/forecast-details")
def list_forecast_details(
    category: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    params = []
    where = []
    if category.upper() == "OTHER":
        where.append("bom_material_category NOT IN (%s,%s,%s)")
        params.extend(["KUMA\u015e", "SLAT", "Profil"])
    else:
        where.append("bom_material_category = %s")
        params.append(category)

    where_sql = " AND ".join(where)

    total = run_query(
        f"""
        SELECT COUNT(*) AS cnt
        FROM core.final_forecast_category_unit_metrics
        WHERE {where_sql}
        """,
        params,
    )[0]["cnt"]

    offset = (page - 1) * page_size
    category_unit_rows = run_query(
        f"""
        SELECT
            bom_material_category,
            bom_unit_of_measure,
            wape,
            mae,
            actual_sum,
            n_points
        FROM core.final_forecast_category_unit_metrics
        WHERE {where_sql}
        ORDER BY wape NULLS LAST, bom_unit_of_measure
        LIMIT %s OFFSET %s
        """,
        params + [page_size, offset],
    )
    overall_rows = run_query(
        """
        SELECT scope, wape, mae, actual_sum, n_points
        FROM core.final_forecast_overall_metrics
        ORDER BY scope
        """
    )

    return {
        "category_unit": category_unit_rows,
        "overall": overall_rows,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total - 1) // page_size + 1 if total else 0,
    }


@app.get("/forecast-meta")
def forecast_meta():
    rows = run_query(
        """
        SELECT MIN(week_start) AS first_forecast_date,
               MAX(week_start) AS last_forecast_date
        FROM core.final_forecast
        """
    )
    first_date = rows[0]["first_forecast_date"] if rows else None
    last_date = rows[0]["last_forecast_date"] if rows else None
    return {"first_forecast_date": first_date, "last_forecast_date": last_date}


@app.get("/open-orders")
def list_open_orders(
    q: str | None = Query(None, description="Search material name/label"),
    h_id: str | None = Query(None, description="Search by h_id"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    tr_norm_expr = "lower(translate({col}, 'İIıiŞşĞğÜüÖöÇç', 'iiiissgguuoocc'))"
    tr_norm_param = "lower(translate(%s, 'İIıiŞşĞğÜüÖöÇç', 'iiiissgguuoocc'))"
    params = []
    where = []
    if q:
        where.append(
            f"({tr_norm_expr.format(col='o.material_name')} LIKE {tr_norm_param}"
            f" OR {tr_norm_expr.format(col='o.material_label')} LIKE {tr_norm_param})"
        )
        q_like = f"%{q}%"
        params.extend([q_like, q_like])
    if h_id:
        where.append("CAST(o.h_id AS TEXT) LIKE %s")
        params.append(f"%{h_id}%")

    where_sql = " AND ".join(where)
    where_clause = f"WHERE {where_sql}" if where_sql else ""

    total = run_query(
        f"""
        WITH open_orders AS (
            SELECT
                h_id,
                hs_id,
                transaction_date,
                company_code,
                document_type,
                movement_status,
                material_name,
                material_label,
                material_category,
                item_no,
                unit_of_measure,
                SUM(quantity) AS open_qty
            FROM raw.raw_open_order_movements
            GROUP BY
                h_id, hs_id, transaction_date, company_code, document_type, movement_status,
                material_name, material_label, material_category, item_no, unit_of_measure
        )
        SELECT COUNT(*) AS cnt
        FROM open_orders o
        {where_clause}
        """,
        params,
    )[0]["cnt"]

    offset = (page - 1) * page_size
    rows = run_query(
        f"""
        WITH open_orders AS (
            SELECT
                h_id,
                hs_id,
                transaction_date,
                company_code,
                document_type,
                movement_status,
                material_name,
                material_label,
                material_category,
                item_no,
                unit_of_measure,
                CASE
                    WHEN unit_of_measure = 'PktAdtMt' THEN 'Mt'
                    WHEN unit_of_measure = 'PktAdt' THEN 'Adet'
                    ELSE unit_of_measure
                END AS unit_norm,
                SUM(quantity) AS open_qty
            FROM raw.raw_open_order_movements
            GROUP BY
                h_id, hs_id, transaction_date, company_code, document_type, movement_status,
                material_name, material_label, material_category, item_no, unit_of_measure, unit_norm
        ),
        open_orders_with_next AS (
            SELECT
                o.*,
                LEAD(o.transaction_date) OVER (
                    PARTITION BY o.material_name, o.unit_norm
                    ORDER BY o.transaction_date, o.h_id
                ) AS next_order_date
            FROM open_orders o
        ),
        matched_receipts AS (
            SELECT
                r.ref_hid AS h_id,
                r.material_name,
                CASE
                    WHEN r.unit_of_measure = 'PktAdtMt' THEN 'Mt'
                    WHEN r.unit_of_measure = 'PktAdt' THEN 'Adet'
                    ELSE r.unit_of_measure
                END AS unit_norm,
                SUM(r.quantity) AS matched_qty,
                ARRAY_AGG(DISTINCT r.h_id) AS received_hids
            FROM raw.raw_stock_movements r
            JOIN open_orders_with_next o
              ON o.h_id = r.ref_hid
             AND o.material_name = r.material_name
             AND o.unit_norm = CASE
                    WHEN r.unit_of_measure = 'PktAdtMt' THEN 'Mt'
                    WHEN r.unit_of_measure = 'PktAdt' THEN 'Adet'
                    ELSE r.unit_of_measure
                END
             AND r.transaction_date >= o.transaction_date
            WHERE r.ref_hid IS NOT NULL
              AND r.document_type LIKE 'Depo Giri%%'
              AND r.company_code = 'WAREHOUSE22'
            GROUP BY
                r.ref_hid,
                r.material_name,
                CASE
                    WHEN r.unit_of_measure = 'PktAdtMt' THEN 'Mt'
                    WHEN r.unit_of_measure = 'PktAdt' THEN 'Adet'
                    ELSE r.unit_of_measure
                END
        ),
        open_orders_enriched AS (
            SELECT
                o.*,
                COALESCE(m.matched_qty, 0) AS matched_qty,
                m.received_hids,
                GREATEST(o.open_qty - COALESCE(m.matched_qty, 0), 0) AS residual_open
            FROM open_orders_with_next o
            LEFT JOIN matched_receipts m
              ON m.h_id = o.h_id
             AND m.material_name = o.material_name
             AND m.unit_norm = o.unit_norm
        )
        SELECT
            o.h_id,
            o.hs_id,
            o.transaction_date,
            o.company_code,
            o.document_type,
            o.movement_status,
            o.material_name,
            o.material_label,
            o.material_category,
            o.item_no,
            o.unit_of_measure,
            o.open_qty,
            o.matched_qty AS received_qty,
            o.residual_open AS remaining_qty,
            o.received_hids
        FROM open_orders_enriched o
        {where_clause}
        ORDER BY o.transaction_date DESC, o.h_id
        LIMIT %s OFFSET %s
        """,
        params + [page_size, offset],
    )

    return {
        "items": rows,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total - 1) // page_size + 1 if total else 0,
    }
