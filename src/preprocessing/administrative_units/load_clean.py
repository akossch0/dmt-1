import logging
import os
from pathlib import Path

import geopandas as gpd
from sqlalchemy import create_engine

# ────────────────────────────────────────────────────────────────────────────────
# Configuration  ─ adjust as needed
# ────────────────────────────────────────────────────────────────────────────────
DB_PARAMS: dict[str, str | int] = {
    "host": "dtim.essi.upc.edu",
    "port": 5432,
    "dbname": "dbakosschneider",
    "user": "akosschneider",
    "password": "DMT2025!",  # consider an env-var instead
}

BASE_DIR: Path | str = (
    r"C:\Users\andre\Documents\Data Science\Master in Data Science\Second Year\Second Semester\Subjects\Data Management for Transportation\Projects\Project 2\dmt-1"
)

KM2_CONVERSION = 1e6
TARGET_CRS = 4326

RAW_TABLES = {
    "districts_raw": "districts_clean",
    "neighbourhoods_raw": "neighbourhoods_clean",
    "census_tracts_raw": "census_tracts_clean",
}

# columns to keep for each target table
COLUMNS = {
    "districts_clean": ["districte", "nom", "area", "geometry"],
    "neighbourhoods_clean": ["districte", "barri", "nom", "area", "geometry"],
    "census_tracts_clean": ["districte", "barri", "sec_cens", "area", "geometry"],
}

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def get_engine():
    url = (
        f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@"
        f"{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
    )
    return create_engine(url)


def missing_summary(gdf: gpd.GeoDataFrame, name: str = "") -> None:
    nan = gdf.isna().sum()
    pct = (nan / len(gdf) * 100).round(2)
    header = f"Missing values — {name}" if name else "Missing values"
    print("\n" + header)
    print("=" * len(header))
    for col in gdf.columns:
        print(f"{col:15}: {nan[col]:4}  ({pct[col]:5.2f}%)")


def clean_gdf(gdf: gpd.GeoDataFrame, cols: list[str]) -> gpd.GeoDataFrame:
    gdf = gdf[cols]
    if "area" in gdf.columns:
        gdf["area"] = gdf["area"] / KM2_CONVERSION
    # standardise geometry name & CRS, lower-case
    if gdf.geometry.name != "geometry":
        gdf = gdf.rename_geometry("geometry")
    if gdf.crs is not None and gdf.crs.to_epsg() != TARGET_CRS:
        gdf = gdf.to_crs(epsg=TARGET_CRS)
    gdf.rename(columns=lambda c: c.lower(), inplace=True)
    return gdf

# ────────────────────────────────────────────────────────────────────────────────
# Main routine
# ────────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("admin_units_clean_to_postgis")
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(levelname)s: %(message)s")


def main(base_dir: Path | str = BASE_DIR, if_exists: str = "replace", tables: list[str] | None = None) -> None:
    base_dir = Path(base_dir).expanduser()
    os.chdir(base_dir)
    logger.info("Working directory: %s", Path.cwd())

    engine = get_engine()

    targets = tables if tables else RAW_TABLES.keys()

    for raw_table in targets:
        cleaned_table = RAW_TABLES[raw_table]
        cols = COLUMNS[cleaned_table]

        logger.info("Reading raw layer: %s", raw_table)
        gdf = gpd.read_postgis(f'SELECT * FROM {raw_table}', engine, geom_col='geometry')

        gdf_clean = clean_gdf(gdf, cols)
        missing_summary(gdf_clean, cleaned_table)

        logger.info("→ Loading %s (%d rows) into PostGIS", cleaned_table, len(gdf_clean))
        gdf_clean.to_postgis(cleaned_table, engine, if_exists=if_exists, index=False)
        logger.info("   Done.")


if __name__ == "__main__":
    main()