
import logging
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

# ─────────── Config ───────────
DB_PARAMS = {
    "host": "dtim.essi.upc.edu",
    "port": 5432,
    "dbname": "dbakosschneider",
    "user": "akosschneider",
    "password": "DMT2025!"
}

BASE_DIR = Path(r"C:\Users\andre\Documents\Data Science\Master in Data Science\Second Year\Second Semester\Subjects\Data Management for Transportation\Projects\Project 2\dmt-1")
DATA_DIR = BASE_DIR / "data/income/raw"
TARGET_TABLE = "income_raw"

# ─────────── Helpers ───────────

def get_connection_string() -> str:
    return (
        f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}"
        f"@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
    )

def load_and_combine_csvs(data_dir: Path) -> pd.DataFrame:
    all_csvs = list(data_dir.glob("income_*.csv"))
    frames = []
    for file in all_csvs:
        df = pd.read_csv(file, sep=",", encoding="utf-8")
        df.columns = df.columns.str.lower()

        if "any" in df.columns:
            df["any"] = pd.to_datetime(df["any"]).dt.strftime("%Y-%m-%d")

        frames.append(df)

    return pd.concat(frames, ignore_index=True)

# ─────────── Main ───────────

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("upload_income_raw_all")

def main():
    os.chdir(BASE_DIR)
    log.info("Working directory: %s", os.getcwd())

    log.info("Loading CSV files from %s", DATA_DIR)
    df = load_and_combine_csvs(DATA_DIR)
    log.info("Total rows loaded: %d", len(df))

    engine = create_engine(get_connection_string())
    log.info("Uploading to database table: %s", TARGET_TABLE)
    df.to_sql(TARGET_TABLE, engine, if_exists="replace", index=False)
    log.info("✅ Upload complete.")

if __name__ == "__main__":
    main()
