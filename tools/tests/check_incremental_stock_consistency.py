import os
import os
import sys

from dotenv import load_dotenv
import psycopg2


TOLERANCE = 0.0001


def safe_print(value) -> None:
    text = str(value)
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="backslashreplace"))


def get_conn():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "127.0.0.1"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "tkis_stockwise"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
    )


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def scalar(cur, sql: str, params=()):
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None


def main() -> int:
    conn = get_conn()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()

    required = [
        ("raw", "raw_stock_movements"),
        ("core", "seat_warehouses"),
        ("core", "current_stock_seat_warehouses"),
        ("core", "raw_current_stock"),
        ("core", "bom_to_stock_map"),
        ("core", "current_stock_by_variant"),
    ]
    missing = [f"{s}.{t}" for s, t in required if not table_exists(cur, s, t)]
    if missing:
        print("FAIL missing tables:", ", ".join(missing))
        cur.close()
        conn.close()
        return 2

    raw_count = scalar(cur, "SELECT COUNT(*) FROM core.raw_current_stock")
    variant_count = scalar(cur, "SELECT COUNT(*) FROM core.current_stock_by_variant")
    print(f"core.raw_current_stock rows: {raw_count}")
    print(f"core.current_stock_by_variant rows: {variant_count}")

    expected_stock_cte = """
        WITH expected_non_seat AS (
            SELECT
                material_name AS stock_adi,
                company_code AS warehouse,
                unit_of_measure AS stock_uom,
                SUM(
                    CASE
                        WHEN document_type = 'Depo Çıkış' THEN -quantity
                        ELSE quantity
                    END
                )::numeric AS current_stock
            FROM raw.raw_stock_movements
            WHERE company_code NOT IN (SELECT warehouse FROM core.seat_warehouses)
            GROUP BY 1, 2, 3
        ),
        expected_seat AS (
            SELECT
                stock_adi,
                warehouse,
                stock_uom,
                SUM(current_stock)::numeric AS current_stock
            FROM core.current_stock_seat_warehouses
            GROUP BY 1, 2, 3
        ),
        expected AS (
            SELECT * FROM expected_non_seat
            UNION ALL
            SELECT * FROM expected_seat
        ),
        actual AS (
            SELECT
                stock_adi,
                warehouse,
                stock_uom,
                SUM(current_stock)::numeric AS current_stock,
                COUNT(*) AS row_count
            FROM core.raw_current_stock
            GROUP BY 1, 2, 3
        )
    """

    raw_mismatch_sql = f"""
        {expected_stock_cte}
        SELECT COUNT(*)
        FROM expected e
        FULL OUTER JOIN actual a
          ON a.stock_adi = e.stock_adi
         AND a.warehouse = e.warehouse
         AND a.stock_uom = e.stock_uom
        WHERE
            e.stock_adi IS NULL
            OR a.stock_adi IS NULL
            OR ABS(COALESCE(a.current_stock, 0) - COALESCE(e.current_stock, 0)) > %s
            OR COALESCE(a.row_count, 0) <> 1
    """
    raw_mismatch = scalar(cur, raw_mismatch_sql, (TOLERANCE,))
    print(f"raw_current_stock mismatch groups: {raw_mismatch}")
    if raw_mismatch:
        cur.execute(
            f"""
            {expected_stock_cte}
            SELECT
                COALESCE(e.stock_adi, a.stock_adi) AS stock_adi,
                COALESCE(e.warehouse, a.warehouse) AS warehouse,
                COALESCE(e.stock_uom, a.stock_uom) AS stock_uom,
                COALESCE(e.current_stock, 0) AS expected_stock,
                COALESCE(a.current_stock, 0) AS actual_stock,
                COALESCE(a.row_count, 0) AS actual_rows
            FROM expected e
            FULL OUTER JOIN actual a
              ON a.stock_adi = e.stock_adi
             AND a.warehouse = e.warehouse
             AND a.stock_uom = e.stock_uom
            WHERE
                e.stock_adi IS NULL
                OR a.stock_adi IS NULL
                OR ABS(COALESCE(a.current_stock, 0) - COALESCE(e.current_stock, 0)) > %s
                OR COALESCE(a.row_count, 0) <> 1
            ORDER BY ABS(COALESCE(a.current_stock, 0) - COALESCE(e.current_stock, 0)) DESC
            LIMIT 10
            """,
            (TOLERANCE,),
        )
        print("raw_current_stock mismatch samples:")
        for row in cur.fetchall():
            safe_print(f"  {row}")
        cur.execute(
            f"""
            {expected_stock_cte}
            SELECT
                COALESCE(e.warehouse, a.warehouse) AS warehouse,
                COUNT(*) AS mismatch_groups
            FROM expected e
            FULL OUTER JOIN actual a
              ON a.stock_adi = e.stock_adi
             AND a.warehouse = e.warehouse
             AND a.stock_uom = e.stock_uom
            WHERE
                e.stock_adi IS NULL
                OR a.stock_adi IS NULL
                OR ABS(COALESCE(a.current_stock, 0) - COALESCE(e.current_stock, 0)) > %s
                OR COALESCE(a.row_count, 0) <> 1
            GROUP BY COALESCE(e.warehouse, a.warehouse)
            ORDER BY mismatch_groups DESC, warehouse
            """
            ,
            (TOLERANCE,),
        )
        print("raw_current_stock mismatch by warehouse:")
        for row in cur.fetchall():
            safe_print(f"  {row}")

    variant_mismatch_sql = f"""
        WITH expected_base AS (
            SELECT
                b.bom_material_name,
                b.bom_uom,
                b.bom_type,
                r.stock_adi,
                r.stock_uom,
                r.warehouse,
                CASE
                    WHEN b.bom_type LIKE 'KUMA%%'
                     AND b.bom_uom = 'Mt2'
                     AND (r.stock_uom ILIKE '%%mt%%' AND r.stock_uom NOT ILIKE '%%mt2%%')
                    THEN
                        r.current_stock
                        * (
                            NULLIF(
                                REGEXP_REPLACE(b.ek_2, '[^0-9\\.]', '', 'g'),
                                ''
                            )::numeric
                            / 100.0
                        )
                    ELSE
                        r.current_stock
                END::numeric AS current_stock
            FROM core.bom_to_stock_map b
            JOIN core.raw_current_stock r
              ON b.stock_adi = r.stock_adi
        ),
        expected AS (
            SELECT
                bom_material_name,
                bom_uom,
                bom_type,
                stock_adi,
                stock_uom,
                warehouse,
                SUM(current_stock)::numeric AS current_stock,
                COUNT(*) AS row_count
            FROM expected_base
            GROUP BY 1, 2, 3, 4, 5, 6
        ),
        actual AS (
            SELECT
                bom_material_name,
                bom_uom,
                bom_type,
                stock_adi,
                stock_uom,
                warehouse,
                SUM(current_stock)::numeric AS current_stock,
                COUNT(*) AS row_count
            FROM core.current_stock_by_variant
            GROUP BY 1, 2, 3, 4, 5, 6
        )
        SELECT COUNT(*)
        FROM expected e
        FULL OUTER JOIN actual a
          ON a.bom_material_name = e.bom_material_name
         AND a.bom_uom = e.bom_uom
         AND a.bom_type = e.bom_type
         AND a.stock_adi = e.stock_adi
         AND a.stock_uom = e.stock_uom
         AND a.warehouse = e.warehouse
        WHERE
            e.bom_material_name IS NULL
            OR a.bom_material_name IS NULL
            OR ABS(COALESCE(a.current_stock, 0) - COALESCE(e.current_stock, 0)) > %s
            OR COALESCE(a.row_count, 0) <> COALESCE(e.row_count, 0)
    """
    variant_mismatch = scalar(cur, variant_mismatch_sql, (TOLERANCE,))
    print(f"current_stock_by_variant mismatch groups: {variant_mismatch}")
    if variant_mismatch:
        cur.execute(
            """
            WITH expected_base AS (
                SELECT
                    b.bom_material_name,
                    b.bom_uom,
                    b.bom_type,
                    r.stock_adi,
                    r.stock_uom,
                    r.warehouse,
                    CASE
                        WHEN b.bom_type LIKE 'KUMA%%'
                         AND b.bom_uom = 'Mt2'
                         AND (r.stock_uom ILIKE '%%mt%%' AND r.stock_uom NOT ILIKE '%%mt2%%')
                        THEN
                            r.current_stock
                            * (
                                NULLIF(
                                    REGEXP_REPLACE(b.ek_2, '[^0-9\\.]', '', 'g'),
                                    ''
                                )::numeric
                                / 100.0
                            )
                        ELSE
                            r.current_stock
                    END::numeric AS current_stock
                FROM core.bom_to_stock_map b
                JOIN core.raw_current_stock r
                  ON b.stock_adi = r.stock_adi
            ),
            expected AS (
                SELECT
                    bom_material_name,
                    bom_uom,
                    bom_type,
                    stock_adi,
                    stock_uom,
                    warehouse,
                    SUM(current_stock)::numeric AS current_stock,
                    COUNT(*) AS row_count
                FROM expected_base
                GROUP BY 1, 2, 3, 4, 5, 6
            ),
            actual AS (
                SELECT
                    bom_material_name,
                    bom_uom,
                    bom_type,
                    stock_adi,
                    stock_uom,
                    warehouse,
                    SUM(current_stock)::numeric AS current_stock,
                    COUNT(*) AS row_count
                FROM core.current_stock_by_variant
                GROUP BY 1, 2, 3, 4, 5, 6
            )
            SELECT
                COALESCE(e.bom_material_name, a.bom_material_name) AS bom_material_name,
                COALESCE(e.stock_adi, a.stock_adi) AS stock_adi,
                COALESCE(e.warehouse, a.warehouse) AS warehouse,
                COALESCE(e.current_stock, 0) AS expected_stock,
                COALESCE(a.current_stock, 0) AS actual_stock,
                COALESCE(e.row_count, 0) AS expected_rows,
                COALESCE(a.row_count, 0) AS actual_rows
            FROM expected e
            FULL OUTER JOIN actual a
              ON a.bom_material_name = e.bom_material_name
             AND a.bom_uom = e.bom_uom
             AND a.bom_type = e.bom_type
             AND a.stock_adi = e.stock_adi
             AND a.stock_uom = e.stock_uom
             AND a.warehouse = e.warehouse
            WHERE
                e.bom_material_name IS NULL
                OR a.bom_material_name IS NULL
                OR ABS(COALESCE(a.current_stock, 0) - COALESCE(e.current_stock, 0)) > %s
                OR COALESCE(a.row_count, 0) <> COALESCE(e.row_count, 0)
            ORDER BY ABS(COALESCE(a.current_stock, 0) - COALESCE(e.current_stock, 0)) DESC
            LIMIT 10
            """,
            (TOLERANCE,),
        )
        print("current_stock_by_variant mismatch samples:")
        for row in cur.fetchall():
            safe_print(f"  {row}")
        cur.execute(
            """
            WITH expected_base AS (
                SELECT
                    b.bom_material_name,
                    b.bom_uom,
                    b.bom_type,
                    r.stock_adi,
                    r.stock_uom,
                    r.warehouse,
                    CASE
                        WHEN b.bom_type LIKE 'KUMA%%'
                         AND b.bom_uom = 'Mt2'
                         AND (r.stock_uom ILIKE '%%mt%%' AND r.stock_uom NOT ILIKE '%%mt2%%')
                        THEN
                            r.current_stock
                            * (
                                NULLIF(
                                    REGEXP_REPLACE(b.ek_2, '[^0-9\\.]', '', 'g'),
                                    ''
                                )::numeric
                                / 100.0
                            )
                        ELSE
                            r.current_stock
                    END::numeric AS current_stock
                FROM core.bom_to_stock_map b
                JOIN core.raw_current_stock r
                  ON b.stock_adi = r.stock_adi
            ),
            expected AS (
                SELECT
                    bom_material_name,
                    bom_uom,
                    bom_type,
                    stock_adi,
                    stock_uom,
                    warehouse,
                    SUM(current_stock)::numeric AS current_stock,
                    COUNT(*) AS row_count
                FROM expected_base
                GROUP BY 1, 2, 3, 4, 5, 6
            ),
            actual AS (
                SELECT
                    bom_material_name,
                    bom_uom,
                    bom_type,
                    stock_adi,
                    stock_uom,
                    warehouse,
                    SUM(current_stock)::numeric AS current_stock,
                    COUNT(*) AS row_count
                FROM core.current_stock_by_variant
                GROUP BY 1, 2, 3, 4, 5, 6
            )
            SELECT
                COALESCE(e.warehouse, a.warehouse) AS warehouse,
                COUNT(*) AS mismatch_groups
            FROM expected e
            FULL OUTER JOIN actual a
              ON a.bom_material_name = e.bom_material_name
             AND a.bom_uom = e.bom_uom
             AND a.bom_type = e.bom_type
             AND a.stock_adi = e.stock_adi
             AND a.stock_uom = e.stock_uom
             AND a.warehouse = e.warehouse
            WHERE
                e.bom_material_name IS NULL
                OR a.bom_material_name IS NULL
                OR ABS(COALESCE(a.current_stock, 0) - COALESCE(e.current_stock, 0)) > %s
                OR COALESCE(a.row_count, 0) <> COALESCE(e.row_count, 0)
            GROUP BY COALESCE(e.warehouse, a.warehouse)
            ORDER BY mismatch_groups DESC, warehouse
            """,
            (TOLERANCE,),
        )
        print("current_stock_by_variant mismatch by warehouse:")
        for row in cur.fetchall():
            safe_print(f"  {row}")

    cur.execute(
        """
        SELECT name, last_hid
        FROM core.sync_hid_state
        WHERE name IN ('raw_current_stock', 'current_stock_by_variant', 'bom_unique_materials')
        ORDER BY name
        """
    )
    state_rows = cur.fetchall()
    print("sync_hid_state:", state_rows)

    cur.close()
    conn.close()

    if raw_mismatch or variant_mismatch:
        print("RESULT: FAIL")
        return 1
    print("RESULT: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
