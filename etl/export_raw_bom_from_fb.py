import argparse
import csv
from datetime import datetime

import firebirdsql


FB_CFG = {
    "host": "ts1004",
    "port": 3050,
    "database": r"d:\etakip\ticari\data\TKIS\tkis.etk",
    "user": "OWNER",
    "password": "4844gunuf",
    "charset": "WIN1254",
}


def connect_fb():
    return firebirdsql.connect(
        host=FB_CFG["host"],
        port=FB_CFG["port"],
        database=FB_CFG["database"],
        user=FB_CFG["user"],
        password=FB_CFG["password"],
        charset=FB_CFG["charset"],
    )


def fetch_bom_hids(cur, start_date: str, end_date: str):
    q = """
        SELECT H_ID
        FROM HAREKETLER
        WHERE TARIH > '2021-12-31'
          AND TARIH BETWEEN ? AND ?
          AND DURUM in ('Aktif', 'Sipar', 'Son')
          AND HTIPI IN (21, 22)
        ORDER BY H_ID
    """
    cur.execute(q, (start_date, end_date))
    return [r[0] for r in cur.fetchall()]


def fetch_bom_rows_by_hid(cur, hid: int):
    q = """
        SELECT
            h.H_ID AS H_ID,
            h.TARIH AS TRANSACTION_DATE,
            h.FIRMA AS COMPANY_CODE,
            h.TIPI AS DOCUMENT_TYPE,
            r.URUN AS MATERIAL_CATEGORY,
            r.TURU AS MATERIAL_NAME,
            r.BIRIM AS UNIT_OF_MEASURE,
            CASE
                WHEN r.RMEK IN ('5019','Z5004','Z5005','Z5016','Z5017','Z5018','Z5019')
                 AND r.URUN = 'KUMAŞ'
                THEN r.MIKTAR * 2
                ELSE r.MIKTAR
            END AS QUANTITY,
            r.RITEM AS ITEM_NO
        FROM HAREKETLER h
        JOIN (
            SELECT
                ANAGRUP,
                URUN,
                TURU,
                SBUP_ADI     AS SBUP,
                SUM(MIKTAR)  AS MIKTAR,
                BIRIM,
                MAX(RMEK)    AS RMEK,
                MAX(RITEM)   AS RITEM,
                HSID
            FROM RECETE_STORSCREEN(?)
            GROUP BY ANAGRUP, URUN, TURU, SBUP_ADI, BIRIM, HSID
        ) r ON 1 = 1
        WHERE h.H_ID = ?
    """
    cur.execute(q, (hid, hid))
    return cur.fetchall()


def parse_ymd(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").date().isoformat()


def export_csv(start_date: str, end_date: str, out_path: str):
    with connect_fb() as con, con.cursor() as cur, open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "h_id",
                "transaction_date",
                "company_code",
                "document_type",
                "material_category",
                "material_name",
                "unit_of_measure",
                "quantity",
                "item_no",
            ]
        )
        hids = fetch_bom_hids(cur, start_date, end_date)
        for hid in hids:
            rows = fetch_bom_rows_by_hid(cur, hid)
            if rows:
                writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--out", required=True, help="output csv path")
    args = parser.parse_args()

    start_date = parse_ymd(args.start)
    end_date = parse_ymd(args.end)
    export_csv(start_date, end_date, args.out)
    print(f"Wrote CSV: {args.out}")


if __name__ == "__main__":
    main()
