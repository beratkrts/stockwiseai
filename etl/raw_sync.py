import os
import time
import calendar
import logging
import subprocess
import gzip
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler
from datetime import datetime, date, timedelta
from typing import Iterable, List, Optional, Sequence, Tuple

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import pyodbc


LOG = logging.getLogger("raw_sync")


def setup_logger(level=logging.INFO, log_file="raw_sync.log") -> logging.Logger:
    logger = logging.getLogger("raw_sync")
    logger.setLevel(level)
    logger.propagate = False

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")

    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(fmt)

    fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)

    logger.handlers.clear()
    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger


setup_logger()
load_dotenv()


FB_CFG = {
    "host": os.getenv("FB_HOST", "127.0.0.1"),
    "port": int(os.getenv("FB_PORT", "3050")),
    "database": os.getenv("FB_DB"),
    "charset": os.getenv("FB_CHARSET", "UTF8"),
}

FB_DSN_LIVE = os.getenv("FB_ODBC_DSN_LIVE", os.getenv("FB_ODBC_DSN", "live"))
FB_DSN_FULL = os.getenv("FB_ODBC_DSN_FULL", os.getenv("FB_ODBC_DSN", "test"))
FB_ACTIVE_DSN = FB_DSN_LIVE

PG_CFG = {
    "host": os.getenv("PG_HOST", "127.0.0.1"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "db": os.getenv("PG_DB", "tkis_stockwise"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

RAW_SCHEMA = os.getenv("PG_RAW_SCHEMA", "raw")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "300"))
FULL_START = os.getenv("FULL_START", "2019-01-01")
FULL_END = os.getenv("FULL_END", datetime.now().strftime("%Y-%m-%d"))
FULL_WINDOW_MONTHS = int(os.getenv("FULL_WINDOW_MONTHS", "6"))
CORE_5MIN_SQL = os.getenv("CORE_5MIN_SQL", "")
CORE_5MIN_SECONDS = int(os.getenv("CORE_5MIN_SECONDS", "3600"))
CORE_WEEKLY_PRE_SQL = os.getenv("CORE_WEEKLY_PRE_SQL", "etl/sql/core_weekly_pre_forecast.sql")
CORE_WEEKLY_POST_SQL = os.getenv("CORE_WEEKLY_POST_SQL", "etl/sql/core_weekly_post_forecast.sql")
CORE_MAPPING_SQL = os.getenv("CORE_MAPPING_SQL", "")
CORE_DASHBOARD_SQL = os.getenv("CORE_DASHBOARD_SQL", "etl/sql/core_dashboard_refresh.sql")
CORE_DASHBOARD_SECONDS = int(os.getenv("CORE_DASHBOARD_SECONDS", "1800"))
MONTHLY_SEAT_SQL = os.getenv("MONTHLY_SEAT_SQL", "etl/sql/core_monthly_seat.sql")
MONTHLY_ENABLED = os.getenv("MONTHLY_ENABLED", "true").lower() in ("1", "true", "yes")
MONTHLY_DAY = int(os.getenv("MONTHLY_DAY", "2"))
MONTHLY_TIME = os.getenv("MONTHLY_TIME", "02:00")
MONTHLY_WINDOW_MINUTES = int(os.getenv("MONTHLY_WINDOW_MINUTES", "120"))
OPEN_ORDER_SECONDS = int(os.getenv("OPEN_ORDER_SECONDS", "1800"))
FORECAST_COMMAND = os.getenv("FORECAST_COMMAND", "")
WEEKLY_ENABLED = os.getenv("WEEKLY_ENABLED", "true").lower() in ("1", "true", "yes")
WEEKLY_DAY = int(os.getenv("WEEKLY_DAY", "0"))  # 0=Monday
WEEKLY_TIME = os.getenv("WEEKLY_TIME", "02:00")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, os.pardir))
WEEKLY_WINDOW_MINUTES = int(os.getenv("WEEKLY_WINDOW_MINUTES", "120"))
WEEKLY_MARKER_FILE = os.getenv(
    "WEEKLY_MARKER_FILE",
    os.path.join(PROJECT_ROOT, "logs", "weekly_in_progress.flag"),
)

con_fb = None
con_fb_dsn = None
LAST_CORE_RUN = None
LAST_DASHBOARD_RUN = None
LAST_WEEKLY_RUN = None
LAST_MONTHLY_RUN = None
LAST_OPEN_ORDER_RUN = None


# ---------------------------
# Connection helpers
# ---------------------------

def normalize_fb_path(path: str) -> str:
    return path.replace("\\", "/")


def connect_fb(dsn: Optional[str] = None) -> pyodbc.Connection:
    dsn = dsn or FB_ACTIVE_DSN
    LOG.info("Connecting to Firebird via ODBC %s:%s/%s", FB_CFG["host"], FB_CFG["port"], FB_CFG["database"])
    pyodbc.pooling = False

    if os.getenv("ODBCINI"):
        os.environ["ODBCINI"] = os.getenv("ODBCINI")
    if os.getenv("ODBCSYSINI"):
        os.environ["ODBCSYSINI"] = os.getenv("ODBCSYSINI")
    if os.getenv("ODBCINSTINI"):
        os.environ["ODBCINSTINI"] = os.getenv("ODBCINSTINI")

    db_path = normalize_fb_path(FB_CFG["database"] or "")
    charset = FB_CFG["charset"]
    host = FB_CFG["host"]
    port = FB_CFG["port"]

    conn_strings = [
        f"DSN={dsn};CHARSET={charset};",
        f"DSN={dsn};",
        f"DRIVER=FirebirdODBC;DBNAME={host}/{port}:{db_path};CHARSET={charset};",
        f"DRIVER=/usr/lib/libOdbcFb.so;DBNAME={host}/{port}:{db_path};CHARSET={charset};",
    ]

    last_exc = None
    for conn_str in conn_strings:
        try:
            LOG.info("ODBC connect trying: %s", conn_str)
            conn = pyodbc.connect(conn_str, autocommit=True)
            if charset:
                try:
                    conn.setdecoding(pyodbc.SQL_CHAR, encoding="cp1254")
                    conn.setencoding(encoding="cp1254")
                except Exception:
                    pass
            LOG.info("ODBC connect OK: %s", conn_str)
            return conn
        except pyodbc.Error as exc:
            last_exc = exc
            LOG.warning("ODBC connect failed: %s (%s)", conn_str, exc)
            continue

    if last_exc:
        raise last_exc
    raise RuntimeError("ODBC connect failed without exception")


def ensure_fb() -> pyodbc.Connection:
    global con_fb, con_fb_dsn
    if con_fb is None or con_fb_dsn != FB_ACTIVE_DSN:
        con_fb = connect_fb(FB_ACTIVE_DSN)
        con_fb_dsn = FB_ACTIVE_DSN
    return con_fb


def close_fb() -> None:
    global con_fb, con_fb_dsn
    if con_fb:
        try:
            con_fb.close()
        except Exception:
            pass
        con_fb = None
        con_fb_dsn = None


def set_fb_dsn(dsn: str) -> None:
    global FB_ACTIVE_DSN
    if dsn != FB_ACTIVE_DSN:
        FB_ACTIVE_DSN = dsn
        close_fb()


@contextmanager
def use_fb_dsn(dsn: str):
    prev = FB_ACTIVE_DSN
    set_fb_dsn(dsn)
    try:
        yield
    finally:
        set_fb_dsn(prev)


def connect_pg() -> psycopg2.extensions.connection:
    retries = int(os.getenv("PG_CONNECT_RETRIES", "30"))
    wait_seconds = float(os.getenv("PG_CONNECT_WAIT_SECONDS", "5"))
    for attempt in range(1, retries + 1):
        try:
            LOG.info("Connecting to Postgres %s:%s/%s (attempt %d/%d)", PG_CFG["host"], PG_CFG["port"], PG_CFG["db"], attempt, retries)
            con = psycopg2.connect(
                host=PG_CFG["host"],
                port=PG_CFG["port"],
                dbname=PG_CFG["db"],
                user=PG_CFG["user"],
                password=PG_CFG["password"],
            )
            con.set_client_encoding("UTF8")
            return con
        except psycopg2.OperationalError as exc:
            if attempt >= retries:
                raise
            LOG.warning("Postgres not ready yet (%s). Retrying in %ss...", exc, wait_seconds)
            time.sleep(wait_seconds)
    raise RuntimeError("Postgres connection retries exhausted")


# ---------------------------
# PG schema helpers
# ---------------------------

def ensure_pg_schema(pg) -> None:
    with pg.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")
        cur.execute("CREATE SCHEMA IF NOT EXISTS core;")

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.raw_bom_consumption (
            id SERIAL PRIMARY KEY,
            h_id INTEGER,
            transaction_date DATE,
            company_code TEXT,
            document_type TEXT,
            material_category TEXT,
            material_name TEXT,
            unit_of_measure TEXT,
            quantity NUMERIC,
            item_no TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)
        cur.execute(f"""
        ALTER TABLE {RAW_SCHEMA}.raw_bom_consumption
        ADD COLUMN IF NOT EXISTS material_color TEXT;
        """)

        cur.execute(f"""
        CREATE INDEX IF NOT EXISTS ix_raw_bom_consumption_hid
        ON {RAW_SCHEMA}.raw_bom_consumption (h_id);
        """)

        cur.execute(f"""
        CREATE INDEX IF NOT EXISTS ix_raw_bom_consumption_item
        ON {RAW_SCHEMA}.raw_bom_consumption (item_no);
        """)

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.raw_stock_movements (
            id SERIAL PRIMARY KEY,
            h_id INTEGER,
            ref_hid INTEGER,
            hs_id INTEGER,
            transaction_date DATE,
            company_code TEXT,
            document_type TEXT,
            movement_status TEXT,
            material_name TEXT,
            material_label TEXT,
            material_category TEXT,
            item_no TEXT,
            unit_of_measure TEXT,
            quantity NUMERIC,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)
        cur.execute(f"""
        ALTER TABLE {RAW_SCHEMA}.raw_stock_movements
        ADD COLUMN IF NOT EXISTS ref_hid INTEGER;
        """)
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.raw_open_order_movements (
            id SERIAL PRIMARY KEY,
            h_id INTEGER,
            hs_id INTEGER,
            transaction_date DATE,
            company_code TEXT,
            document_type TEXT,
            movement_status TEXT,
            material_name TEXT,
            material_label TEXT,
            material_category TEXT,
            item_no TEXT,
            unit_of_measure TEXT,
            quantity NUMERIC,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        cur.execute(f"""
        CREATE INDEX IF NOT EXISTS ix_raw_stock_mov_hid
          ON {RAW_SCHEMA}.raw_stock_movements (h_id);
        """)
        cur.execute(f"""
        CREATE INDEX IF NOT EXISTS ix_raw_stock_mov_hsid
          ON {RAW_SCHEMA}.raw_stock_movements (hs_id);
        """)

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.stock_master (
            id SERIAL PRIMARY KEY,
            s_id INTEGER,
            adi TEXT,
            renk_id INTEGER,
            aciklama TEXT,
            eni NUMERIC,
            boyu NUMERIC,
            agirlik NUMERIC,
            ana_tur TEXT,
            tedarikci_1 TEXT,
            tedarikci_2 TEXT,
            tedarikci_3 TEXT,
            tedarikci_4 TEXT,
            tedarikci_5 TEXT,
            recete_1 TEXT,
            recete_2 TEXT,
            recete_3 TEXT,
            recete_4 TEXT,
            recete_5 TEXT,
            recete_6 TEXT,
            recete_7 TEXT,
            katolog TEXT,
            kumas_en NUMERIC,
            kumas_boy NUMERIC,
            sure_1 NUMERIC,
            sure_2 NUMERIC,
            ek_1 TEXT,
            ek_2 TEXT,
            ek_3 TEXT,
            tam_adi TEXT,
            ana_grup TEXT,
            alt_grup TEXT,
            birim TEXT,
            turu TEXT,
            turu3 TEXT
        );
        """)

        cur.execute(f"""
        CREATE INDEX IF NOT EXISTS ix_stock_master_adi
          ON {RAW_SCHEMA}.stock_master (adi);
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS core.sync_hid_state (
            name TEXT PRIMARY KEY,
            last_hid BIGINT NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS core.seat_warehouses (
            warehouse TEXT PRIMARY KEY
        );
        """)
        cur.execute("""
        INSERT INTO core.seat_warehouses (warehouse) VALUES
            ('JALUZİ KOLTUK DEPO'),
            ('KATLAMALI KOLTUK DEPO'),
            ('STOR KOLTUK DEPO')
        ON CONFLICT DO NOTHING;
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS core.raw_current_stock (
            stock_adi TEXT,
            warehouse TEXT,
            stock_uom TEXT,
            current_stock NUMERIC
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS core.bom_unique_materials (
            material_name     TEXT PRIMARY KEY,
            material_color    TEXT,
            item_no           TEXT,
            unit_of_measure   TEXT,
            material_category TEXT
        );
        """)

    pg.commit()


def truncate_table(pg, table_name: str) -> None:
    with pg.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {RAW_SCHEMA}.{table_name}")
    pg.commit()

def write_weekly_marker() -> None:
    marker_dir = os.path.dirname(WEEKLY_MARKER_FILE)
    if marker_dir:
        os.makedirs(marker_dir, exist_ok=True)
    payload = f"started_at={datetime.now().isoformat()}\n"
    with open(WEEKLY_MARKER_FILE, "w", encoding="utf-8") as f:
        f.write(payload)


def clear_weekly_marker() -> None:
    try:
        os.remove(WEEKLY_MARKER_FILE)
    except FileNotFoundError:
        pass

def backup_raw_bom_consumption(pg) -> Optional[str]:
    backup_dir = os.getenv("BOM_BACKUP_DIR", "").strip()
    if not backup_dir:
        LOG.info("BOM backup disabled; BOM_BACKUP_DIR not set")
        return None
    try:
        keep = int(os.getenv("BOM_BACKUP_KEEP", "1"))
    except ValueError:
        keep = 1
    keep = max(1, keep)
    LOG.info("BOM backup retention keep=%d", keep)
    os.makedirs(backup_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(backup_dir, f"raw_bom_consumption_{ts}.csv.gz")
    LOG.info("Backing up raw_bom_consumption to %s", path)
    with pg.cursor() as cur, gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        cur.copy_expert(
            f"""
            COPY {RAW_SCHEMA}.raw_bom_consumption
                (h_id, transaction_date, company_code, document_type,
                 material_category, material_name, unit_of_measure,
                 quantity, item_no, material_color)
            TO STDOUT WITH (FORMAT CSV)
            """,
            f,
        )
    pg.commit()
    cleanup_bom_backups(backup_dir, keep)
    return path


def restore_raw_bom_consumption(pg, path: str) -> None:
    LOG.warning("Restoring raw_bom_consumption from %s", path)
    truncate_table(pg, "raw_bom_consumption")
    with pg.cursor() as cur, gzip.open(path, "rt", encoding="utf-8") as f:
        cur.copy_expert(
            f"""
            COPY {RAW_SCHEMA}.raw_bom_consumption
                (h_id, transaction_date, company_code, document_type,
                 material_category, material_name, unit_of_measure,
                 quantity, item_no, material_color)
            FROM STDIN WITH (FORMAT CSV)
            """,
            f,
        )
    pg.commit()
    LOG.warning("Restore completed for raw_bom_consumption")


def cleanup_bom_backups(backup_dir: str, keep: int) -> None:
    try:
        entries = []
        for name in os.listdir(backup_dir):
            if not name.startswith("raw_bom_consumption_") or not name.endswith(".csv.gz"):
                continue
            full_path = os.path.join(backup_dir, name)
            try:
                mtime = os.path.getmtime(full_path)
            except OSError:
                continue
            entries.append((mtime, full_path))
        entries.sort(reverse=True)
        LOG.info("BOM backups found=%d keep=%d", len(entries), keep)
        for _, path in entries[keep:]:
            try:
                os.remove(path)
                LOG.info("Removed old BOM backup %s", path)
            except OSError as exc:
                LOG.warning("Failed to remove old BOM backup %s: %s", path, exc)
    except OSError as exc:
        LOG.warning("Backup cleanup skipped: %s", exc)

def get_core_state_hid(pg, name: str) -> Optional[int]:
    with pg.cursor() as cur:
        cur.execute(
            "SELECT last_hid FROM core.sync_hid_state WHERE name = %s",
            (name,),
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def set_core_state_hid(pg, name: str, last_hid: int) -> None:
    with pg.cursor() as cur:
        cur.execute(
            """
            INSERT INTO core.sync_hid_state (name, last_hid, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (name) DO UPDATE
            SET last_hid = EXCLUDED.last_hid,
                updated_at = NOW()
            """,
            (name, last_hid),
        )
    pg.commit()

def pg_table_exists(pg, schema: str, table: str) -> bool:
    with pg.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


def split_sql_statements(sql_text: str) -> List[str]:
    statements = []
    buf = []
    i = 0
    in_dollar = False
    dollar_tag = ""
    while i < len(sql_text):
        ch = sql_text[i]
        if ch == "$":
            # detect start/end of dollar-quoted block
            j = i + 1
            while j < len(sql_text) and sql_text[j] != "$":
                j += 1
            if j < len(sql_text):
                tag = sql_text[i : j + 1]
                if not in_dollar:
                    in_dollar = True
                    dollar_tag = tag
                elif tag == dollar_tag:
                    in_dollar = False
                    dollar_tag = ""
                buf.append(tag)
                i = j + 1
                continue
        if ch == ";" and not in_dollar:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def resolve_sql_path(path: str) -> str:
    if not path:
        return path
    if os.path.isabs(path):
        return path
    return os.path.join(PROJECT_ROOT, path)


def execute_sql_file(pg, path: str) -> None:
    sql_path = resolve_sql_path(path)
    sql_text = open(sql_path, "r", encoding="utf-8").read()
    for stmt in split_sql_statements(sql_text):
        stmt_upper = stmt.strip().upper()
        if stmt_upper.startswith("REFRESH MATERIALIZED VIEW CONCURRENTLY"):
            pg.commit()
            pg.autocommit = True
            try:
                with pg.cursor() as cur:
                    cur.execute(stmt)
            finally:
                pg.autocommit = False
        else:
            with pg.cursor() as cur:
                cur.execute(stmt)
    pg.commit()





# ---------------------------
# Firebird query helpers
# ---------------------------

def fb_select_all(sql: str, params=(), retries=3, pause=0.5):
    global con_fb
    for attempt in range(1, retries + 1):
        try:
            ensure_fb()
            cur = con_fb.cursor()
            try:
                cur.execute(sql, params)
                return cur.fetchall()
            finally:
                cur.close()
        except Exception as exc:
            LOG.warning("Firebird select failed try=%s/%s: %s", attempt, retries, repr(exc))
            close_fb()
            time.sleep(pause * attempt)
    raise RuntimeError("Firebird select failed")


def fetch_bom_hids(d1: str, d2: str) -> List[int]:
    q = """
        SELECT H_ID
        FROM HAREKETLER
        WHERE TARIH > '2021-12-31'
          AND TARIH BETWEEN ? AND ?
          AND DURUM in ('Aktif', 'Sipar', 'Son')
          AND HTIPI IN (21, 22)
        ORDER BY H_ID
    """
    rows = fb_select_all(q, (d1, d2))
    return [r[0] for r in rows]


def fetch_bom_hids_since(last_hid: int) -> List[int]:
    q = """
        SELECT H_ID
        FROM HAREKETLER
        WHERE TARIH > '2021-12-31'
          AND DURUM in ('Aktif', 'Sipar', 'Son')
          AND HTIPI IN (21, 22)
          AND H_ID > ?
        ORDER BY H_ID
    """
    rows = fb_select_all(q, (last_hid,))
    return [r[0] for r in rows]


def fetch_bom_rows_by_hid(hid: int) -> List[Tuple]:
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
    return fb_select_all(q, (hid, hid))


def fetch_stock_headers(d1: str, d2: str) -> List[Tuple[int, date, str, str, str, int]]:
    q = """
        SELECT H_ID, TARIH, TIPI, DURUM, FIRMA, REF_HID
        FROM HAREKETLER
        WHERE HTIPI IN (50, 51)
          AND DURUM IN ('Aktif', 'Sipar', 'Son')
          AND TARIH BETWEEN ? AND ?
        ORDER BY TARIH, H_ID
    """
    return fb_select_all(q, (d1, d2))


def fetch_stock_header_by_id(hid: int) -> Optional[Tuple[int, date, str, str, str, int]]:
    q = """
        SELECT H_ID, TARIH, TIPI, DURUM, FIRMA, REF_HID
        FROM HAREKETLER
        WHERE H_ID = ?
          AND DURUM IN ('Aktif', 'Sipar', 'Son')
    """
    rows = fb_select_all(q, (hid,))
    return rows[0] if rows else None


def fetch_stock_lines(h_id: int, retries=5):
    q = """
        SELECT
            hs.HS_ID,
            hs.H_ID,
            hs.URUN_TURU,
            hs.URUN_KODU,
            hs.BIRIM,
            hs.TOPLAM_MIKTAR,
            sk.TURU2 AS CAT,
            sk.TURU3 AS ITEMNO
        FROM HAREKET_SATIR hs
        LEFT JOIN STOK_KARTI sk ON sk.ADI = hs.URUN_KODU
        WHERE hs.H_ID = ?
    """
    global con_fb
    for attempt in range(1, retries + 1):
        try:
            ensure_fb()
            cur = con_fb.cursor()
            cur.execute(q, (h_id,))
            while True:
                rows = cur.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    yield row
            cur.close()
            return
        except Exception as exc:
            LOG.warning("Stock line fetch failed H_ID=%s try=%s/%s: %s", h_id, attempt, retries, repr(exc))
            close_fb()
            time.sleep(attempt * 0.5)

    raise RuntimeError(f"Stock line fetch failed for H_ID={h_id}")


def fetch_open_order_rows() -> Iterable[Tuple]:
    q = """
        SELECT
            h.H_ID AS H_ID,
            hs.HS_ID AS HS_ID,
            h.TARIH AS TRANSACTION_DATE,
            h.FIRMA AS COMPANY_CODE,
            h.TIPI AS DOCUMENT_TYPE,
            h.DURUM AS MOVEMENT_STATUS,
            hs.URUN_TURU AS MATERIAL_NAME,
            hs.URUN_KODU AS MATERIAL_LABEL,
            s.TURU2 AS MATERIAL_CATEGORY,
            s.TURU3 AS ITEM_NO,
            hs.BIRIM AS UNIT_OF_MEASURE,
            hs.TOPLAM_MIKTAR AS QUANTITY
        FROM HAREKETLER h
        JOIN HAREKET_SATIR hs ON hs.H_ID = h.H_ID
        JOIN STOK_TURLER t ON t.ADI = hs.URUN_TURU
        JOIN STOK_KARTI s ON t.S_ID = s.S_ID
        WHERE h.HTIPI = 10
          AND h.HDURUM = 34
    """
    rows = fb_select_all(q)
    for row in rows:
        yield row


def fetch_stock_master_rows() -> List[Tuple]:
    base = """
        SELECT
            sk.s_id,
            t.adi,
            t.renk_id,
            t.aciklama,
            t.eni,
            t.boyu,
            t.agirlik,
            t.ana_tur,
            t.tedarikci_1,
            t.tedarikci_2,
            t.tedarikci_3,
            t.tedarikci_4,
            t.tedarikci_5,
            t.recete_1,
            t.recete_2,
            t.recete_3,
            t.recete_4,
            t.recete_5,
            t.recete_6,
            t.recete_7,
            t.katolog,
            t.kumas_en,
            t.kumas_boy,
            t.sure_1,
            t.sure_2,
            t.ek_1,
            t.ek_2,
            t.ek_3,
            sk.tam_adi,
            sk.ana_grup,
            sk.alt_grup,
            sk.birim,
            sk.turu,
            sk.turu3
        FROM stok_karti sk
        JOIN stok_turler t ON sk.s_id = t.s_id
    """
    return fb_select_all(base)


# ---------------------------
# PG insert helpers
# ---------------------------

def pg_insert_bom_batch(pg, batch: Sequence[Sequence[object]]) -> int:
    if not batch:
        return 0
    with pg.cursor() as cur:
        psycopg2.extras.execute_batch(cur, f"""
            INSERT INTO {RAW_SCHEMA}.raw_bom_consumption
            (h_id, transaction_date, company_code, document_type,
             material_category, material_name, unit_of_measure,
             quantity, item_no)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, batch, page_size=BATCH_SIZE)
    pg.commit()
    return len(batch)


def pg_insert_stock_batch(pg, batch: Sequence[Sequence[object]]) -> int:
    if not batch:
        return 0
    with pg.cursor() as cur:
        psycopg2.extras.execute_batch(cur, f"""
            INSERT INTO {RAW_SCHEMA}.raw_stock_movements
            (h_id, ref_hid, hs_id, transaction_date, company_code, document_type,
             movement_status, material_name, material_label, material_category,
             item_no, unit_of_measure, quantity)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, batch, page_size=BATCH_SIZE)
    pg.commit()
    return len(batch)


def pg_insert_stock_master_batch(pg, batch: Sequence[Sequence[object]]) -> int:
    if not batch:
        return 0
    filtered = [row for row in batch if row and row[0] is not None]
    if not filtered:
        return 0
    with pg.cursor() as cur:
        psycopg2.extras.execute_batch(cur, f"""
            INSERT INTO {RAW_SCHEMA}.stock_master
            (s_id, adi, renk_id, aciklama, eni, boyu, agirlik, ana_tur,
             tedarikci_1, tedarikci_2, tedarikci_3, tedarikci_4, tedarikci_5,
             recete_1, recete_2, recete_3, recete_4, recete_5, recete_6, recete_7,
             katolog, kumas_en, kumas_boy, sure_1, sure_2,
             ek_1, ek_2, ek_3, tam_adi, ana_grup, alt_grup, birim, turu, turu3)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, filtered, page_size=BATCH_SIZE)
    pg.commit()
    return len(filtered)


def pg_insert_open_order_batch(pg, batch: Sequence[Sequence[object]]) -> int:
    if not batch:
        return 0
    with pg.cursor() as cur:
        psycopg2.extras.execute_batch(cur, f"""
            INSERT INTO {RAW_SCHEMA}.raw_open_order_movements
            (h_id, hs_id, transaction_date, company_code, document_type,
             movement_status, material_name, material_label, material_category,
             item_no, unit_of_measure, quantity)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, batch, page_size=BATCH_SIZE)
    pg.commit()
    return len(batch)


# ---------------------------
# Date utilities
# ---------------------------

def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def add_months(d: date, months: int) -> date:
    m = d.month - 1 + months
    y = d.year + m // 12
    m = m % 12 + 1
    last_day = calendar.monthrange(y, m)[1]
    return date(y, m, min(d.day, last_day))


def iter_windows(d1_str: str, d2_str: str, months: int) -> Iterable[Tuple[str, str]]:
    start = parse_ymd(d1_str)
    end = parse_ymd(d2_str)
    cur = start
    while cur <= end:
        cur_end = add_months(cur, months) - timedelta(days=1)
        if cur_end > end:
            cur_end = end
        yield cur.strftime("%Y-%m-%d"), cur_end.strftime("%Y-%m-%d")
        cur = cur_end + timedelta(days=1)


# ---------------------------
# Full load
# ---------------------------

def full_load_bom(pg, start: str, end: str, months: int) -> None:
    LOG.info("Full load BOM %s -> %s", start, end)
    backup_path = backup_raw_bom_consumption(pg)
    try:
        truncate_table(pg, "raw_bom_consumption")

        total = 0
        for ws, we in iter_windows(start, end, months):
            close_fb()
            ensure_fb()
            LOG.info("BOM window %s -> %s", ws, we)
            hids = fetch_bom_hids(ws, we)
            LOG.info("BOM H_ID count=%d", len(hids))
            batch = []
            window_written = 0
            for hid in hids:
                try:
                    rows = fetch_bom_rows_by_hid(hid)
                    for row in rows:
                        batch.append(row)
                        if len(batch) >= BATCH_SIZE:
                            written = pg_insert_bom_batch(pg, batch)
                            total += written
                            window_written += written
                            batch.clear()
                except Exception as exc:
                    LOG.error("BOM H_ID=%s failed: %s", hid, repr(exc))
                    continue

            if batch:
                written = pg_insert_bom_batch(pg, batch)
                total += written
                window_written += written
                batch.clear()
            LOG.info("BOM window complete: %s -> %s rows=%d", ws, we, window_written)

        LOG.info("Full load BOM complete: %d rows", total)
    except Exception as exc:
        LOG.error("Full load BOM failed: %s", repr(exc))
        if backup_path:
            try:
                restore_raw_bom_consumption(pg, backup_path)
            except Exception as restore_exc:
                LOG.error("BOM restore failed: %s", repr(restore_exc))
        raise



def full_load_stock(pg, start: str, end: str, months: int) -> None:
    LOG.info("Full load stock %s -> %s", start, end)
    truncate_table(pg, "raw_stock_movements")

    total = 0
    for ws, we in iter_windows(start, end, months):
        close_fb()
        ensure_fb()
        LOG.info("Stock window %s -> %s", ws, we)
        headers = fetch_stock_headers(ws, we)
        LOG.info("Stock headers=%d", len(headers))
        batch = []
        window_written = 0
        for h_id, tarih, tipi, durum, firma, ref_hid in headers:
            try:
                for hs_id, hid2, urun_turu, urun_kodu, birim, toplam_miktar, cat, itemno in fetch_stock_lines(h_id):
                    batch.append([
                        h_id,
                        ref_hid,
                        hs_id,
                        tarih,
                        firma,
                        str(tipi),
                        str(durum),
                        urun_turu,
                        urun_kodu,
                        cat,
                        itemno,
                        birim,
                        float(toplam_miktar or 0),
                    ])
                    if len(batch) >= BATCH_SIZE:
                        written = pg_insert_stock_batch(pg, batch)
                        total += written
                        window_written += written
                        batch.clear()
            except Exception as exc:
                LOG.error("Stock H_ID=%s failed: %s", h_id, repr(exc))
                continue

        if batch:
            written = pg_insert_stock_batch(pg, batch)
            total += written
            window_written += written
            batch.clear()
        LOG.info("Stock window complete: %s -> %s rows=%d", ws, we, window_written)

    LOG.info("Full load stock complete: %d rows", total)


def full_load_stock_master(pg) -> None:
    LOG.info("Full load stock_master")
    truncate_table(pg, "stock_master")
    rows = fetch_stock_master_rows()
    batch: List[Tuple] = []
    total = 0
    for row in rows:
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            total += pg_insert_stock_master_batch(pg, batch)
            batch.clear()
    if batch:
        total += pg_insert_stock_master_batch(pg, batch)
    LOG.info("Full load stock_master complete: %d rows", total)


def rebuild_bom_unique_materials(pg) -> None:
    LOG.info("Rebuilding core.bom_unique_materials from raw_bom_consumption")
    with pg.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS core.bom_unique_materials_new;")
        cur.execute("CREATE TABLE core.bom_unique_materials_new (LIKE core.bom_unique_materials INCLUDING ALL);")
        cur.execute(
            """
            INSERT INTO core.bom_unique_materials_new
                (material_name, material_color, item_no, unit_of_measure, material_category)
            SELECT DISTINCT ON (material_name)
                material_name,
                material_color,
                item_no,
                unit_of_measure,
                material_category
            FROM raw.raw_bom_consumption
            WHERE material_name IS NOT NULL
              AND material_name <> ''
            ORDER BY material_name, transaction_date DESC, h_id DESC
            """
        )
    pg.commit()

    with pg.cursor() as cur:
        cur.execute(
            """
            DO $$
            BEGIN
                EXECUTE 'DROP TABLE IF EXISTS core.bom_unique_materials_old';
                IF to_regclass('core.bom_unique_materials') IS NOT NULL THEN
                    EXECUTE 'ALTER TABLE core.bom_unique_materials RENAME TO bom_unique_materials_old';
                END IF;
                EXECUTE 'ALTER TABLE core.bom_unique_materials_new RENAME TO bom_unique_materials';
                EXECUTE 'DROP TABLE IF EXISTS core.bom_unique_materials_old';
            END $$;
            """
        )
    pg.commit()

    set_core_state_hid(pg, "bom_unique_materials", get_max_bom_hid_pg(pg))
    LOG.info("Rebuild core.bom_unique_materials complete")

# ---------------------------
# Incremental helpers
# ---------------------------

def fetch_stock_changed_hids(last_hid: int) -> List[int]:
    hids = set()

    q1 = """
        SELECT DISTINCT H_ID
        FROM HAREKETLER
        WHERE HTIPI IN (50, 51)
          AND DURUM IN ('Aktif', 'Sipar', 'Son')
          AND H_ID > ?
    """
    for r in fb_select_all(q1, (last_hid,)):
        hids.add(r[0])

    return sorted(hids)


def get_max_bom_hid_pg(pg) -> int:
    with pg.cursor() as cur:
        cur.execute(f"SELECT COALESCE(MAX(h_id), 0) FROM {RAW_SCHEMA}.raw_bom_consumption")
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def get_max_stock_hid_pg(pg) -> int:
    with pg.cursor() as cur:
        cur.execute(f"SELECT COALESCE(MAX(h_id), 0) FROM {RAW_SCHEMA}.raw_stock_movements")
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def incremental_bom(pg, last_hid: int) -> int:
    hids = fetch_bom_hids_since(last_hid)
    if not hids:
        return last_hid

    LOG.info("BOM incremental (append-only) H_ID count=%d", len(hids))

    batch = []
    total = 0
    max_hid = last_hid
    for hid in hids:
        try:
            rows = fetch_bom_rows_by_hid(hid)
            for row in rows:
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    total += pg_insert_bom_batch(pg, batch)
                    batch.clear()
            max_hid = max(max_hid, int(hid))
        except Exception as exc:
            LOG.error("BOM incremental H_ID=%s failed: %s", hid, repr(exc))
            continue

    if batch:
        total += pg_insert_bom_batch(pg, batch)
        batch.clear()

    LOG.info("BOM incremental rows=%d", total)
    return max_hid


def incremental_stock(pg, last_hid: int) -> int:
    hids = fetch_stock_changed_hids(last_hid)
    if not hids:
        return last_hid

    LOG.info("Stock incremental (append-only) H_ID count=%d", len(hids))

    batch = []
    total = 0
    for hid in hids:
        header = fetch_stock_header_by_id(hid)
        if not header:
            continue
        _, tarih, tipi, durum, firma, ref_hid = header
        try:
            for hs_id, hid2, urun_turu, urun_kodu, birim, toplam_miktar, cat, itemno in fetch_stock_lines(hid):
                batch.append([
                    hid,
                    ref_hid,
                    hs_id,
                    tarih,
                    firma,
                    str(tipi),
                    str(durum),
                    urun_turu,
                    urun_kodu,
                    cat,
                    itemno,
                    birim,
                    float(toplam_miktar or 0),
                ])
                if len(batch) >= BATCH_SIZE:
                    total += pg_insert_stock_batch(pg, batch)
                    batch.clear()
        except Exception as exc:
            LOG.error("Stock incremental H_ID=%s failed: %s", hid, repr(exc))
            continue

    if batch:
        total += pg_insert_stock_batch(pg, batch)
        batch.clear()

    LOG.info("Stock incremental rows=%d", total)
    return get_max_stock_hid_pg(pg)


def incremental_stock_master(pg) -> bool:
    LOG.info("Stock master incremental disabled; no-op")
    return False


def refresh_open_orders(pg) -> int:
    if not pg_table_exists(pg, RAW_SCHEMA, "raw_open_order_movements"):
        LOG.warning("Skipping open order refresh; raw_open_order_movements missing")
        return 0
    truncate_table(pg, "raw_open_order_movements")
    batch = []
    total = 0
    for row in fetch_open_order_rows():
        batch.append(list(row))
        if len(batch) >= BATCH_SIZE:
            total += pg_insert_open_order_batch(pg, batch)
            batch.clear()
    if batch:
        total += pg_insert_open_order_batch(pg, batch)
        batch.clear()
    LOG.info("Open order refresh rows=%d", total)
    return total


def incremental_bom_unique_materials(pg) -> bool:
    if not pg_table_exists(pg, "core", "bom_unique_materials"):
        LOG.warning("Skipping bom_unique_materials incremental; core table missing")
        return False

    last_hid = get_core_state_hid(pg, "bom_unique_materials")
    if last_hid is None:
        with pg.cursor() as cur:
            cur.execute("SELECT 1 FROM core.bom_unique_materials LIMIT 1")
            has_rows = cur.fetchone() is not None
        if has_rows:
            max_hid = get_max_bom_hid_pg(pg)
            set_core_state_hid(pg, "bom_unique_materials", max_hid)
            return False
        last_hid = 0
    with pg.cursor() as cur:
        cur.execute(
            f"SELECT COALESCE(MAX(h_id), 0) FROM {RAW_SCHEMA}.raw_bom_consumption WHERE h_id > %s",
            (last_hid,),
        )
        row = cur.fetchone()
        max_hid = int(row[0]) if row and row[0] is not None else 0

    if max_hid <= last_hid:
        return False

    with pg.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO core.bom_unique_materials AS t
                (material_name, material_color, item_no, unit_of_measure, material_category)
            SELECT DISTINCT
                material_name,
                material_color,
                item_no,
                unit_of_measure,
                material_category
            FROM {RAW_SCHEMA}.raw_bom_consumption
            WHERE h_id > %s
              AND material_name IS NOT NULL
              AND material_name <> ''
            ON CONFLICT (material_name) DO NOTHING
            """,
            (last_hid,),
        )
    pg.commit()
    set_core_state_hid(pg, "bom_unique_materials", max_hid)
    return True


def incremental_raw_current_stock(pg) -> bool:
    if not pg_table_exists(pg, "core", "raw_current_stock"):
        LOG.warning("Skipping raw_current_stock incremental; core table missing")
        return False

    last_hid = get_core_state_hid(pg, "raw_current_stock")
    if last_hid is None:
        with pg.cursor() as cur:
            cur.execute("SELECT 1 FROM core.raw_current_stock LIMIT 1")
            has_rows = cur.fetchone() is not None
        if has_rows:
            max_hid = get_max_stock_hid_pg(pg)
            set_core_state_hid(pg, "raw_current_stock", max_hid)
            return False
        last_hid = 0
    with pg.cursor() as cur:
        cur.execute(
            f"SELECT COALESCE(MAX(h_id), 0) FROM {RAW_SCHEMA}.raw_stock_movements WHERE h_id > %s",
            (last_hid,),
        )
        row = cur.fetchone()
        max_hid = int(row[0]) if row and row[0] is not None else 0

    if max_hid <= last_hid:
        return False

    delta_sql = f"""
        SELECT
            material_name   AS stock_adi,
            company_code    AS warehouse,
            unit_of_measure AS stock_uom,
            SUM(
                CASE
                    WHEN document_type = 'Depo Çıkış' THEN -quantity
                    ELSE quantity
                END
            ) AS delta
        FROM {RAW_SCHEMA}.raw_stock_movements
        WHERE h_id > %s
          AND company_code NOT IN (
              SELECT warehouse FROM core.seat_warehouses
          )
        GROUP BY material_name, company_code, unit_of_measure
    """

    with pg.cursor() as cur:
        cur.execute(
            f"""
            WITH delta AS (
                {delta_sql}
            )
            UPDATE core.raw_current_stock r
            SET current_stock = r.current_stock + d.delta
            FROM delta d
            WHERE r.stock_adi = d.stock_adi
              AND r.warehouse = d.warehouse
              AND r.stock_uom = d.stock_uom
            """,
            (last_hid,),
        )
        cur.execute(
            f"""
            WITH delta AS (
                {delta_sql}
            )
            INSERT INTO core.raw_current_stock (stock_adi, warehouse, stock_uom, current_stock)
            SELECT d.stock_adi, d.warehouse, d.stock_uom, d.delta
            FROM delta d
            LEFT JOIN core.raw_current_stock r
              ON r.stock_adi = d.stock_adi
             AND r.warehouse = d.warehouse
             AND r.stock_uom = d.stock_uom
            WHERE r.stock_adi IS NULL
            """,
            (last_hid,),
        )
    pg.commit()
    set_core_state_hid(pg, "raw_current_stock", max_hid)
    return True


def incremental_current_stock_by_variant(pg) -> bool:
    if not pg_table_exists(pg, "core", "current_stock_by_variant"):
        LOG.warning("Skipping current_stock_by_variant incremental; core table missing")
        return False
    if not pg_table_exists(pg, "core", "bom_to_stock_map"):
        LOG.warning("Skipping current_stock_by_variant incremental; bom_to_stock_map missing")
        return False

    last_hid = get_core_state_hid(pg, "current_stock_by_variant")
    if last_hid is None:
        with pg.cursor() as cur:
            cur.execute("SELECT 1 FROM core.current_stock_by_variant LIMIT 1")
            has_rows = cur.fetchone() is not None
        if has_rows:
            max_hid = get_max_stock_hid_pg(pg)
            set_core_state_hid(pg, "current_stock_by_variant", max_hid)
            return False
        last_hid = 0

    with pg.cursor() as cur:
        cur.execute(
            f"""
            SELECT COALESCE(MAX(h_id), 0)
            FROM {RAW_SCHEMA}.raw_stock_movements
            WHERE h_id > %s
              AND company_code NOT IN (SELECT warehouse FROM core.seat_warehouses)
            """,
            (last_hid,),
        )
        row = cur.fetchone()
        max_hid = int(row[0]) if row and row[0] is not None else 0

    if max_hid <= last_hid:
        return False

    with pg.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT material_name
            FROM {RAW_SCHEMA}.raw_stock_movements
            WHERE h_id > %s
              AND company_code NOT IN (SELECT warehouse FROM core.seat_warehouses)
            """,
            (last_hid,),
        )
        stock_names = [r[0] for r in cur.fetchall() if r and r[0]]

    if not stock_names:
        set_core_state_hid(pg, "current_stock_by_variant", max_hid)
        return False

    LOG.info("current_stock_by_variant incremental stock_adi count=%d", len(stock_names))

    with pg.cursor() as cur:
        cur.execute(
            """
            DELETE FROM core.current_stock_by_variant
            WHERE stock_adi = ANY(%s)
            """,
            (stock_names,),
        )
        cur.execute(
            """
            INSERT INTO core.current_stock_by_variant
            SELECT
                b.bom_material_name,
                b.bom_uom,
                b.bom_type,
                r.stock_adi,
                r.stock_uom,
                r.warehouse,
                CASE
                    WHEN b.bom_type LIKE 'KUMA%%'
                     AND b.bom_uom  = 'Mt2'
                     AND (r.stock_uom ILIKE '%%mt%%' AND r.stock_uom NOT ILIKE '%%mt2%%')
                    THEN
                        r.current_stock
                        * (
                            NULLIF(
                                REGEXP_REPLACE(b.ek_2, '[^0-9\.]', '', 'g'),
                                ''
                            )::NUMERIC
                          / 100.0
                          )
                    ELSE
                        r.current_stock
                END AS current_stock
            FROM core.bom_to_stock_map b
            JOIN core.raw_current_stock r
                  ON b.stock_adi = r.stock_adi
            WHERE r.stock_adi = ANY(%s)
            """,
            (stock_names,),
        )
    pg.commit()
    set_core_state_hid(pg, "current_stock_by_variant", max_hid)
    return True


# ---------------------------
# Main loop
# ---------------------------

def run_full(pg, include_monthly_refresh: bool = True) -> None:
    with use_fb_dsn(FB_DSN_FULL):
        full_load_bom(pg, FULL_START, FULL_END, FULL_WINDOW_MONTHS)
        full_load_stock(pg, FULL_START, FULL_END, FULL_WINDOW_MONTHS)
        full_load_stock_master(pg)
    LOG.info("Full load complete; building core incrementals")
    incremental_bom_unique_materials(pg)
    incremental_raw_current_stock(pg)
    incremental_current_stock_by_variant(pg)
    if include_monthly_refresh:
        LOG.info("Core incrementals complete; running monthly seat refresh")
        run_monthly_seat(pg)

    if CORE_MAPPING_SQL:
        LOG.info("Running mapping refresh: %s", CORE_MAPPING_SQL)
        execute_sql_file(pg, CORE_MAPPING_SQL)


def run_full_stock_only(pg) -> None:
    with use_fb_dsn(FB_DSN_FULL):
        full_load_stock(pg, FULL_START, FULL_END, FULL_WINDOW_MONTHS)
        full_load_stock_master(pg)
    set_core_state_hid(pg, "raw_current_stock", get_max_stock_hid_pg(pg))
    LOG.info("Full load stock complete; building core current stock")
    incremental_raw_current_stock(pg)
    incremental_current_stock_by_variant(pg)
    LOG.info("Core current stock complete; running monthly seat refresh")
    run_monthly_seat(pg)

    if CORE_MAPPING_SQL and pg_table_exists(pg, "core", "bom_unique_materials"):
        LOG.info("Running mapping refresh: %s", CORE_MAPPING_SQL)
        execute_sql_file(pg, CORE_MAPPING_SQL)
    elif CORE_MAPPING_SQL:
        LOG.warning("Skipping mapping refresh; core.bom_unique_materials missing")


def run_incremental(pg, run_refresh_jobs: bool = True) -> None:
    global LAST_CORE_RUN, LAST_DASHBOARD_RUN, LAST_OPEN_ORDER_RUN
    bom_last_hid = get_max_bom_hid_pg(pg)
    stock_last_hid = get_max_stock_hid_pg(pg)

    bom_new_hid = incremental_bom(pg, bom_last_hid)
    stock_new_hid = incremental_stock(pg, stock_last_hid)
    _ = stock_new_hid
    master_changed = incremental_stock_master(pg)

    if (bom_new_hid != bom_last_hid) or master_changed:
        if CORE_MAPPING_SQL:
            LOG.info("Running mapping refresh: %s", CORE_MAPPING_SQL)
            execute_sql_file(pg, CORE_MAPPING_SQL)

    incremental_bom_unique_materials(pg)
    incremental_raw_current_stock(pg)
    incremental_current_stock_by_variant(pg)

    if run_refresh_jobs and OPEN_ORDER_SECONDS > 0:
        now = datetime.now()
        if LAST_OPEN_ORDER_RUN is None or (now - LAST_OPEN_ORDER_RUN).total_seconds() >= OPEN_ORDER_SECONDS:
            refresh_open_orders(pg)
            LAST_OPEN_ORDER_RUN = now

    if run_refresh_jobs and CORE_5MIN_SQL and CORE_5MIN_SECONDS > 0:
        now = datetime.now()
        if LAST_CORE_RUN is None or (now - LAST_CORE_RUN).total_seconds() >= CORE_5MIN_SECONDS:
            LOG.info("Running core refresh: %s", CORE_5MIN_SQL)
            execute_sql_file(pg, CORE_5MIN_SQL)
            LAST_CORE_RUN = now

    if run_refresh_jobs and CORE_DASHBOARD_SQL and CORE_DASHBOARD_SECONDS > 0:
        now = datetime.now()
        if LAST_DASHBOARD_RUN is None or (now - LAST_DASHBOARD_RUN).total_seconds() >= CORE_DASHBOARD_SECONDS:
            LOG.info("Running dashboard refresh: %s", CORE_DASHBOARD_SQL)
            execute_sql_file(pg, CORE_DASHBOARD_SQL)
            LAST_DASHBOARD_RUN = now


def run_weekly(pg) -> None:
    global LAST_WEEKLY_RUN
    LOG.info("Weekly refresh starting")
    write_weekly_marker()
    try:
        with use_fb_dsn(FB_DSN_FULL):
            full_load_bom(pg, FULL_START, FULL_END, FULL_WINDOW_MONTHS)
        with use_fb_dsn(FB_DSN_LIVE):
            last_hid = get_max_bom_hid_pg(pg)
            LOG.info("BOM live incremental after full load (last_hid=%d)", last_hid)
            incremental_bom(pg, last_hid)
            full_load_stock_master(pg)
        rebuild_bom_unique_materials(pg)
        if CORE_WEEKLY_PRE_SQL:
            LOG.info("Running weekly pre-forecast SQL: %s", CORE_WEEKLY_PRE_SQL)
            execute_sql_file(pg, CORE_WEEKLY_PRE_SQL)

        if FORECAST_COMMAND:
            LOG.info("Running forecast command")
            result = subprocess.run(FORECAST_COMMAND, shell=True)
            if result.returncode != 0:
                raise RuntimeError(f"Forecast command failed with code {result.returncode}")
        else:
            LOG.warning("FORECAST_COMMAND not set; skipping forecast run")

        if CORE_WEEKLY_POST_SQL:
            LOG.info("Running weekly post-forecast SQL: %s", CORE_WEEKLY_POST_SQL)
            execute_sql_file(pg, CORE_WEEKLY_POST_SQL)
    except Exception:
        LOG.error("Weekly refresh failed; leaving marker for retry")
        raise
    clear_weekly_marker()
    LAST_WEEKLY_RUN = datetime.now()
    LOG.info("Weekly refresh complete")


def run_weekly_forecast_pipeline(pg) -> None:
    global LAST_WEEKLY_RUN
    if CORE_WEEKLY_PRE_SQL:
        LOG.info("Running weekly pre-forecast SQL: %s", CORE_WEEKLY_PRE_SQL)
        execute_sql_file(pg, CORE_WEEKLY_PRE_SQL)

    if FORECAST_COMMAND:
        LOG.info("Running forecast command")
        result = subprocess.run(FORECAST_COMMAND, shell=True)
        if result.returncode != 0:
            raise RuntimeError(f"Forecast command failed with code {result.returncode}")
    else:
        LOG.warning("FORECAST_COMMAND not set; skipping forecast run")

    if CORE_WEEKLY_POST_SQL:
        LOG.info("Running weekly post-forecast SQL: %s", CORE_WEEKLY_POST_SQL)
        execute_sql_file(pg, CORE_WEEKLY_POST_SQL)

    LAST_WEEKLY_RUN = datetime.now()


def run_post_weekly_refreshes(pg) -> None:
    if OPEN_ORDER_SECONDS > 0:
        refresh_open_orders(pg)

    if CORE_5MIN_SQL:
        LOG.info("Running core refresh: %s", CORE_5MIN_SQL)
        execute_sql_file(pg, CORE_5MIN_SQL)

    if CORE_DASHBOARD_SQL:
        LOG.info("Running dashboard refresh: %s", CORE_DASHBOARD_SQL)
        execute_sql_file(pg, CORE_DASHBOARD_SQL)


def run_bootstrap(pg) -> None:
    LOG.info("Bootstrap starting")
    run_full(pg, include_monthly_refresh=False)
    LOG.info("Bootstrap full load complete; running one live incremental catch-up")
    with use_fb_dsn(FB_DSN_LIVE):
        run_incremental(pg, run_refresh_jobs=False)
    LOG.info("Live incremental catch-up complete; running monthly seat refresh")
    run_monthly_seat(pg)
    run_weekly_forecast_pipeline(pg)
    run_post_weekly_refreshes(pg)
    LOG.info("Bootstrap complete")


def run_bootstrap_continue(pg) -> None:
    """
    Continue bootstrap from live catch-up stage after a completed full load.
    Runs live incremental catch-up, monthly seat, then weekly forecast pipeline.
    """
    LOG.info("Bootstrap continue starting (skip full load)")
    with use_fb_dsn(FB_DSN_LIVE):
        run_incremental(pg, run_refresh_jobs=False)
    LOG.info("Live incremental catch-up complete; running monthly seat refresh")
    run_monthly_seat(pg)
    run_weekly_forecast_pipeline(pg)
    run_post_weekly_refreshes(pg)
    LOG.info("Bootstrap continue complete")


def run_bootstrap_stock_only(pg) -> None:
    LOG.info("Bootstrap (stock-only) starting")
    run_full_stock_only(pg)
    run_weekly_forecast_pipeline(pg)
    run_post_weekly_refreshes(pg)
    LOG.info("Bootstrap (stock-only) complete")


def run_complete_with_live_incremental(pg) -> None:
    """
    Complete full-load flow using live incremental catch-up only (no delete/reload),
    then run monthly seat and weekly forecast pipeline.
    """
    LOG.info("Live completion starting (incremental-only)")

    with use_fb_dsn(FB_DSN_LIVE):
        run_incremental(pg, run_refresh_jobs=False)
        full_load_stock_master(pg)

    LOG.info("Live incremental catch-up complete; running monthly seat refresh")
    run_monthly_seat(pg)
    run_weekly_forecast_pipeline(pg)
    run_post_weekly_refreshes(pg)
    LOG.info("Live completion complete")


def run_monthly_seat(pg) -> None:
    global LAST_MONTHLY_RUN
    if not MONTHLY_SEAT_SQL:
        LOG.warning("MONTHLY_SEAT_SQL not set; skipping monthly seat refresh")
        return
    LOG.info("Monthly seat refresh starting: %s", MONTHLY_SEAT_SQL)
    execute_sql_file(pg, MONTHLY_SEAT_SQL)
    LAST_MONTHLY_RUN = datetime.now()
    LOG.info("Monthly seat refresh complete")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="run full load and exit")
    parser.add_argument("--stock-only", action="store_true", help="run full stock load (movements + stock_master) and exit")
    parser.add_argument("--once", action="store_true", help="run one incremental pass and exit")
    parser.add_argument("--weekly", action="store_true", help="run weekly pipeline and exit")
    parser.add_argument("--bootstrap", action="store_true", help="run first-time bootstrap and exit")
    parser.add_argument("--bootstrap-continue", action="store_true", help="continue bootstrap after full load using live catch-up")
    parser.add_argument("--bootstrap-stock-only", action="store_true", help="bootstrap without raw_bom_consumption")
    parser.add_argument("--complete-live", action="store_true", help="run live incremental catch-up, then monthly+weekly")
    args = parser.parse_args()

    pg = connect_pg()
    ensure_pg_schema(pg)

    if args.full:
        run_full(pg)
        pg.close()
        close_fb()
        return
    
    if args.stock_only:
        run_full_stock_only(pg)
        pg.close()
        close_fb()
        return

    if args.once:
        run_incremental(pg)
        pg.close()
        close_fb()
        return
    
    if args.weekly:
        run_weekly(pg)
        pg.close()
        close_fb()
        return

    if args.bootstrap:
        run_bootstrap(pg)
        pg.close()
        close_fb()
        return

    if args.bootstrap_continue:
        run_bootstrap_continue(pg)
        pg.close()
        close_fb()
        return

    if args.bootstrap_stock_only:
        run_bootstrap_stock_only(pg)
        pg.close()
        close_fb()
        return

    if args.complete_live:
        run_complete_with_live_incremental(pg)
        pg.close()
        close_fb()
        return

    if WEEKLY_ENABLED and os.path.exists(WEEKLY_MARKER_FILE):
        LOG.warning("Weekly marker found; retrying weekly refresh")
        try:
            run_weekly(pg)
        except Exception as exc:
            LOG.error("Weekly retry failed: %s", repr(exc))

    while True:
        try:
            run_incremental(pg)
            if WEEKLY_ENABLED and WEEKLY_DAY >= 0:
                now = datetime.now()
                try:
                    hour, minute = [int(x) for x in WEEKLY_TIME.split(":")]
                except ValueError:
                    hour, minute = 2, 0

                days_ago = (now.weekday() - WEEKLY_DAY) % 7
                scheduled = datetime(
                    year=now.year,
                    month=now.month,
                    day=now.day,
                    hour=hour,
                    minute=minute,
                    second=0,
                    microsecond=0,
                ) - timedelta(days=days_ago)
                if scheduled > now:
                    scheduled -= timedelta(days=7)

                window = timedelta(minutes=WEEKLY_WINDOW_MINUTES)
                if now >= scheduled and now - scheduled <= window:
                    if LAST_WEEKLY_RUN is None or LAST_WEEKLY_RUN < scheduled:
                        run_weekly(pg)
            if MONTHLY_ENABLED and MONTHLY_DAY >= 1:
                now = datetime.now()
                try:
                    hour, minute = [int(x) for x in MONTHLY_TIME.split(":")]
                except ValueError:
                    hour, minute = 2, 0
                scheduled = datetime(
                    year=now.year,
                    month=now.month,
                    day=MONTHLY_DAY,
                    hour=hour,
                    minute=minute,
                    second=0,
                    microsecond=0,
                )
                if scheduled > now:
                    year = now.year - 1 if now.month == 1 else now.year
                    month = 12 if now.month == 1 else now.month - 1
                    scheduled = scheduled.replace(year=year, month=month)
                window = timedelta(minutes=MONTHLY_WINDOW_MINUTES)
                if now >= scheduled and now - scheduled <= window:
                    if LAST_MONTHLY_RUN is None or LAST_MONTHLY_RUN < scheduled:
                        run_monthly_seat(pg)
        except Exception as exc:
            LOG.error("Incremental cycle failed: %s", repr(exc))
            close_fb()
        time.sleep(SYNC_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
