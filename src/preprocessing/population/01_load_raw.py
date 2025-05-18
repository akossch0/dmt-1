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

POPULATION_DIR = Path("data/population/raw")   
POPULATION_TABLE = "population_raw" 

BASE_DIR: Path | str = (
    r"C:\Users\andre\Documents\Data Science\Master in Data Science\Second Year\Second Semester\Subjects\Data Management for Transportation\Projects\Project 2\dmt-1"
)

# ────────────────────────────────────────────────────────────────────────────────
# Helper for the population files
# ────────────────────────────────────────────────────────────────────────────────
def get_connection_string() -> str:
    return (
        f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}"
        f"@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
    )

def upload_population_raw(
    engine,
    pop_dir: Path = POPULATION_DIR,
    table_name: str = POPULATION_TABLE,
    if_exists: str = "replace",
) -> None:
    """
    Read every *.csv in `pop_dir`, stack them, and write to Postgres.
    Column names are lower-cased; no geometry is involved.
    """
    csv_paths = sorted(pop_dir.glob("*.csv"))
    if not csv_paths:
        raise FileNotFoundError(f"No CSV files found in {pop_dir}")

    frames = []
    for path in csv_paths:
        logger.info("→ population_raw (%s)", path.name)
        df = pd.read_csv(path)
        df.columns = df.columns.str.lower()   # normalise column names
        frames.append(df)

    full_df = pd.concat(frames, ignore_index=True)
    full_df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    logger.info("   Uploaded %d rows into %s", len(full_df), table_name)

# ────────────────────────────────────────────────────────────────────────────────
# Main routine 
# ────────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("upload_admin_units_raw")
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(levelname)s: %(message)s")

def main(base_dir: Path | str = BASE_DIR, if_exists: str = "replace") -> None:
    base_dir = Path(base_dir).expanduser()
    os.chdir(base_dir)
    logger.info("Working directory: %s", Path.cwd())

    engine = create_engine(get_connection_string())

    upload_population_raw(engine, if_exists=if_exists)   # ← ONLY this

if __name__ == "__main__":
    main()