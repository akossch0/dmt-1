import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

# ────────────────────────────────────────────────────────────────────────────────
# Configuration 
# ────────────────────────────────────────────────────────────────────────────────
DB_PARAMS: dict[str, str | int] = {
    "host": "dtim.essi.upc.edu",
    "port": 5432,
    "dbname": "dbakosschneider",
    "user": "akosschneider",
    "password": "DMT2025!",  
}

BASE_DIR: Path | str = (
    r"C:\Users\andre\Documents\Data Science\Master in Data Science\Second Year\Second Semester\Subjects\Data Management for Transportation\Projects\Project 2\dmt-1"
)

POP_RAW_TABLE   = "population_raw"
POP_CLEAN_TABLE = "population_clean"

POP_COLUMNS = ["data_referencia", "seccio_censal", "valor"]

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def get_engine():
    url = (
        f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@"
        f"{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
    )
    return create_engine(url)


def missing_summary(df: pd.DataFrame, name: str = "") -> None:
    nan = df.isna().sum()
    pct = (nan / len(df) * 100).round(2)
    header = f"Missing values — {name}" if name else "Missing values"
    print("\n" + header)
    print("=" * len(header))
    for col in df.columns:
        print(f"{col:15}: {nan[col]:4}  ({pct[col]:5.2f}%)")


def upload_population_clean(engine, if_exists: str = "replace") -> None:
    logger.info("Reading raw population table: %s", POP_RAW_TABLE)
    df = pd.read_sql(f"SELECT * FROM {POP_RAW_TABLE}", con=engine)

    # keep just the requested columns, lower-case them
    df = df[POP_COLUMNS]
    df.columns = df.columns.str.lower()

    missing_summary(df, POP_CLEAN_TABLE)

    logger.info("→ Loading %s (%d rows) into PostGIS", POP_CLEAN_TABLE, len(df))
    df.to_sql(POP_CLEAN_TABLE, engine, if_exists=if_exists, index=False)
    logger.info("   Done.")

# ────────────────────────────────────────────────────────────────────────────────
# Main routine
# ────────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("admin_units_clean_to_postgis")
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(levelname)s: %(message)s")


def main(base_dir: Path | str = BASE_DIR, if_exists: str = "replace") -> None:
    base_dir = Path(base_dir).expanduser()
    os.chdir(base_dir)
    logger.info("Working directory: %s", Path.cwd())

    engine = get_engine()
    upload_population_clean(engine, if_exists=if_exists)

if __name__ == "__main__":
    main()