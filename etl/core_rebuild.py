import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pre", action="store_true", help="run core weekly pre-forecast SQL")
    parser.add_argument("--post", action="store_true", help="run core weekly post-forecast SQL")
    parser.add_argument("--dashboard", action="store_true", help="run dashboard refresh SQL")
    args = parser.parse_args()

    if not (args.pre or args.post or args.dashboard):
        parser.error("Select at least one: --pre, --post, --dashboard")

    sys.path.append("/app/etl")
    import raw_sync as r

    pg = r.connect_pg()
    r.ensure_pg_schema(pg)

    if args.pre:
        r.execute_sql_file(pg, r.CORE_WEEKLY_PRE_SQL)
    if args.post:
        r.execute_sql_file(pg, r.CORE_WEEKLY_POST_SQL)
    if args.dashboard:
        r.execute_sql_file(pg, r.CORE_DASHBOARD_SQL)

    pg.close()


if __name__ == "__main__":
    main()
