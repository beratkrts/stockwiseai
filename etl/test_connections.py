import os
import sys

from dotenv import load_dotenv
import psycopg2
import pyodbc


def normalize_fb_path(path: str) -> str:
    return path.replace("\\", "/")


def connect_pg():
    host = os.getenv("PG_HOST", "127.0.0.1")
    port = int(os.getenv("PG_PORT", "5432"))
    db = os.getenv("PG_DB", "tkis_stockwise")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "postgres")
    print(f"PG connect -> {host}:{port}/{db}")
    con = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    con.set_client_encoding("UTF8")
    return con


def connect_fb():
    pyodbc.pooling = False

    if os.getenv("ODBCINI"):
        os.environ["ODBCINI"] = os.getenv("ODBCINI")
    if os.getenv("ODBCSYSINI"):
        os.environ["ODBCSYSINI"] = os.getenv("ODBCSYSINI")
    if os.getenv("ODBCINSTINI"):
        os.environ["ODBCINSTINI"] = os.getenv("ODBCINSTINI")

    dsn = os.getenv("FB_ODBC_DSN", "test")
    charset = os.getenv("FB_CHARSET", "WIN1254")
    host = os.getenv("FB_HOST", "127.0.0.1")
    port = os.getenv("FB_PORT", "3050")
    db_path = normalize_fb_path(os.getenv("FB_DB", ""))

    conn_strings = [
        f"DSN={dsn};",
        f"DSN={dsn};CHARSET={charset};",
        f"DRIVER=FirebirdODBC;DBNAME={host}/{port}:{db_path};CHARSET={charset};",
        f"DRIVER=/usr/lib/libOdbcFb.so;DBNAME={host}/{port}:{db_path};CHARSET={charset};",
    ]

    last_exc = None
    for conn_str in conn_strings:
        try:
            print("FB ODBC connect trying:", conn_str)
            conn = pyodbc.connect(conn_str, autocommit=True)
            print("FB ODBC connect OK:", conn_str)
            return conn
        except pyodbc.Error as exc:
            last_exc = exc
            print("FB ODBC connect failed:", conn_str, exc)
            continue

    if last_exc:
        raise last_exc
    raise RuntimeError("FB ODBC connect failed without exception")


def main() -> int:
    load_dotenv()

    try:
        pg = connect_pg()
        with pg.cursor() as cur:
            cur.execute("SELECT 1")
            print("PG OK:", cur.fetchone()[0])
        pg.close()
    except Exception as exc:
        print("PG FAIL:", repr(exc))
        return 1

    try:
        fb = connect_fb()
        cur = fb.cursor()
        cur.execute("SELECT 1 FROM RDB$DATABASE")
        row = cur.fetchone()
        print("FB OK:", row[0] if row else None)
        cur.close()
        fb.close()
    except Exception as exc:
        print("FB FAIL:", repr(exc))
        return 2

    print("All connections OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
