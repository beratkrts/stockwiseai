import argparse
import csv
import os
from datetime import datetime

import psycopg2


def connect_pg():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "127.0.0.1"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "tkis_stockwise"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
    )


def parse_ymd(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


def export_raw_bom(start_date, end_date, out_path):
    sql = """
        SELECT
            id,
            h_id,
            transaction_date,
            company_code,
            document_type,
            material_category,
            material_name,
            material_color,
            unit_of_measure,
            quantity,
            item_no,
            created_at
        FROM raw.raw_bom_consumption
        WHERE transaction_date BETWEEN %s AND %s
        ORDER BY transaction_date, id
    """
    with connect_pg() as conn:
        with conn.cursor() as cur, open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "id",
                    "h_id",
                    "transaction_date",
                    "company_code",
                    "document_type",
                    "material_category",
                    "material_name",
                    "material_color",
                    "unit_of_measure",
                    "quantity",
                    "item_no",
                    "created_at",
                ]
            )
            cur.execute(sql, (start_date, end_date))
            while True:
                rows = cur.fetchmany(5000)
                if not rows:
                    break
                writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--out", required=True, help="output csv path")
    args = parser.parse_args()

    start_date = parse_ymd(args.start)
    end_date = parse_ymd(args.end)
    export_raw_bom(start_date, end_date, args.out)
    print(f"Wrote CSV: {args.out}")


if __name__ == "__main__":
    main()
