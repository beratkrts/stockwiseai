import os
import os
from typing import Dict, Iterable, List, Tuple

import firebirdsql
import psycopg2
import psycopg2.extras


FB_CFG = {
    "host": os.getenv("FB_HOST", "127.0.0.1"),
    "port": int(os.getenv("FB_PORT", "3050")),
    "database": os.getenv("FB_DB"),
    "user": os.getenv("FB_USER", "sysdba"),
    "password": os.getenv("FB_PASSWORD", "masterkey"),
    "charset": os.getenv("FB_CHARSET", "UTF8"),
}

PG_CFG = {
    "host": os.getenv("PG_HOST", "127.0.0.1"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "db": os.getenv("PG_DB", "tkis_stockwise"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

RAW_SCHEMA = os.getenv("PG_RAW_SCHEMA", "raw")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
FB_IN_LIMIT = 1000


def connect_fb() -> firebirdsql.Connection:
    return firebirdsql.connect(
        host=FB_CFG["host"],
        port=FB_CFG["port"],
        database=FB_CFG["database"],
        user=FB_CFG["user"],
        password=FB_CFG["password"],
        charset=FB_CFG["charset"],
    )


def connect_pg() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=PG_CFG["host"],
        port=PG_CFG["port"],
        dbname=PG_CFG["db"],
        user=PG_CFG["user"],
        password=PG_CFG["password"],
    )


def chunked(values: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def fetch_ref_hid_map(fb_cur, hids: List[int]) -> Dict[int, int]:
    if not hids:
        return {}
    placeholders = ",".join(["?"] * len(hids))
    q = f"""
        SELECT H_ID, REF_HID
        FROM HAREKETLER
        WHERE H_ID IN ({placeholders})
    """
    fb_cur.execute(q, hids)
    mapping = {}
    for row in fb_cur.fetchall():
        if row[1] is None:
            continue
        ref_hid = int(row[1])
        if ref_hid <= 0:
            continue
        mapping[int(row[0])] = ref_hid
    return mapping


def ensure_ref_hid_column(pg) -> None:
    with pg.cursor() as cur:
        cur.execute(
            f"ALTER TABLE {RAW_SCHEMA}.raw_stock_movements "
            "ADD COLUMN IF NOT EXISTS ref_hid INTEGER"
        )
    pg.commit()


def load_pg_hids(pg) -> List[int]:
    with pg.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT h_id
            FROM {RAW_SCHEMA}.raw_stock_movements
            WHERE ref_hid IS NULL
              AND document_type LIKE %s
            """,
            ('Depo Giri%',),
        )
        return [int(r[0]) for r in cur.fetchall() if r and r[0] is not None]


def apply_updates(pg, pairs: List[Tuple[int, int]]) -> None:
    if not pairs:
        return
    with pg.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"""
            UPDATE {RAW_SCHEMA}.raw_stock_movements AS t
            SET ref_hid = v.ref_hid
            FROM (VALUES %s) AS v(h_id, ref_hid)
            WHERE t.h_id = v.h_id
              AND t.ref_hid IS NULL
            """,
            pairs,
            page_size=BATCH_SIZE,
        )
    pg.commit()


def main() -> None:
    pg = connect_pg()
    ensure_ref_hid_column(pg)
    hids = load_pg_hids(pg)
    if not hids:
        print("No h_id values found in raw_stock_movements.")
        return

    updated = 0
    with connect_fb() as fb, fb.cursor() as fb_cur:
        for batch in chunked(hids, min(BATCH_SIZE, FB_IN_LIMIT)):
            mapping = fetch_ref_hid_map(fb_cur, batch)
            pairs = [(hid, mapping[hid]) for hid in batch if hid in mapping]
            apply_updates(pg, pairs)
            updated += len(pairs)

    print(f"Updated ref_hid for {updated} rows.")


if __name__ == "__main__":
    main()
