import os


def normalize_fb_path(path: str) -> str:
    return path.replace("\\", "/")


def main() -> None:
    dsn = os.getenv("FB_ODBC_DSN", "test")
    os.environ["ODBCINI"] = "/etc/odbc.ini"
    os.environ["ODBCSYSINI"] = "/etc"
    os.environ["ODBCINSTINI"] = "/etc/odbcinst.ini"
    user = os.getenv("FB_USER", "OWNER")
    password = os.getenv("FB_PASSWORD", "")
    charset = os.getenv("FB_CHARSET", "WIN1254")
    host = os.getenv("FB_HOST", "127.0.0.1")
    port = os.getenv("FB_PORT", "3050")
    db_path = normalize_fb_path(os.getenv("FB_DB", ""))

    import pyodbc
    pyodbc.pooling = False
    print("ODBCINI:", os.environ.get("ODBCINI"))
    print("ODBCSYSINI:", os.environ.get("ODBCSYSINI"))
    print("ODBCINSTINI:", os.environ.get("ODBCINSTINI"))
    print("drivers:", pyodbc.drivers())
    print("datasources:", pyodbc.dataSources())

    conn_strings = [
        f"DSN={dsn};",
        f"DSN={dsn};USER={user};PASSWORD={password};CHARSET={charset};",
        f"DSN={dsn};UID={user};PWD={password};CHARSET={charset};",
        f"DRIVER=FirebirdODBC;DBNAME={host}/{port}:{db_path};USER={user};PASSWORD={password};CHARSET={charset};",
        f"DRIVER=/usr/lib/libOdbcFb.so;DBNAME={host}/{port}:{db_path};USER={user};PASSWORD={password};CHARSET={charset};",
    ]

    last_exc = None
    conn = None
    for conn_str in conn_strings:
        try:
            print("ODBC connect trying:", conn_str)
            conn = pyodbc.connect(conn_str, autocommit=True)
            print("ODBC connect OK:", conn_str)
            break
        except pyodbc.Error as exc:
            last_exc = exc
            print("ODBC connect failed:", conn_str, exc)
            continue
    if conn is None:
        raise last_exc
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM RDB$DATABASE")
    row = cur.fetchone()
    print("ODBC OK:", row[0] if row else None)
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
