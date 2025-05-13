
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
    r"C:\Users\bruna\OneDrive\Documents\GitHub\dmt-1"
)

CSV_PATH = Path("data/income/raw/income.csv")
TARGET_TABLE = "income_raw"

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def get_connection_string() -> str:
    return (
        f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}"
        f"@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
    )

def prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=lambda c: c.lower())
    return df

# ────────────────────────────────────────────────────────────────────────────────
# Main routine
# ────────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("upload_income_raw")
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(levelname)s: %(message)s")

def main(base_dir: Path | str = BASE_DIR, if_exists: str = "replace") -> None:
    base_dir = Path(base_dir).expanduser()
    os.chdir(base_dir)
    logger.info("Working directory: %s", Path.cwd())

    logger.info("Reading CSV: %s", CSV_PATH)
    df = pd.read_csv(CSV_PATH)
    df = prepare_df(df)

    logger.info("Uploading to database table: %s", TARGET_TABLE)
    engine = create_engine(get_connection_string())
    df.to_sql(TARGET_TABLE, engine, if_exists=if_exists, index=False)
    logger.info("✅ Uploaded %d rows to %s", len(df), TARGET_TABLE)

if __name__ == "__main__":
    main()
