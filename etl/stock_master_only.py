import sys


def main() -> None:
    sys.path.append("/app/etl")
    import raw_sync as r

    pg = r.connect_pg()
    r.ensure_pg_schema(pg)
    r.full_load_stock_master(pg)
    pg.close()


if __name__ == "__main__":
    main()
