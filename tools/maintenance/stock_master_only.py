import sys
import sys
from pathlib import Path


def main() -> None:
    etl_dir = Path(__file__).resolve().parents[2] / "etl"
    sys.path.append(str(etl_dir))
    import raw_sync as r

    pg = r.connect_pg()
    r.ensure_pg_schema(pg)
    r.full_load_stock_master(pg)
    pg.close()


if __name__ == "__main__":
    main()
