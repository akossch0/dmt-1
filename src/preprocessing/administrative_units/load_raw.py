import logging
import os
from pathlib import Path
from typing import Optional

import geopandas as gpd
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

DATA_ROOT = Path("data/administrative_units/decompressed/BCN_UNITATS_ADM_shp")
TARGET_CRS = 4326

# table name → wildcard pattern for the shapefile we want
PATTERNS = {
    "districts_raw": "*distr*.*shp",        # e.g. *_Districtes_*.shp
    "neighbourhoods_raw": "*barr*.*shp",    # *_Barris_*.shp
    "census_tracts_raw": "*seccens*.*shp", # *_SecCens_*.shp
}

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def get_connection_string() -> str:
    return (
        f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}"
        f"@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
    )


def find_shapefile(pattern: str, root: Path = DATA_ROOT) -> Path:
    try:
        return next(root.rglob(pattern))
    except StopIteration as e:
        raise FileNotFoundError(f"No shapefile matching {pattern!r} in {root}") from e


def prepare_gdf(gdf: gpd.GeoDataFrame, target_crs: int = TARGET_CRS) -> gpd.GeoDataFrame:
    if gdf.crs is not None and gdf.crs.to_epsg() != target_crs:
        gdf = gdf.to_crs(epsg=target_crs)
    if gdf.geometry.name != "geometry":
        gdf = gdf.rename_geometry("geometry")
    gdf.rename(columns=lambda c: c.lower(), inplace=True)
    return gdf

# ────────────────────────────────────────────────────────────────────────────────
# Main routine
# ────────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("upload_admin_units_raw")
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(levelname)s: %(message)s")


def upload_layer(table: str, engine, if_exists: str = "replace") -> None:
    shp_path = find_shapefile(PATTERNS[table])
    logger.info("→ %s (%s)", table, shp_path.name)

    gdf = gpd.read_file(shp_path, encoding="latin-1")
    gdf = prepare_gdf(gdf)

    gdf.to_postgis(table, engine, if_exists=if_exists, index=False)
    logger.info("   Uploaded %d rows", len(gdf))


def main(base_dir: Path | str = BASE_DIR, if_exists: str = "replace", tables: Optional[list[str]] = None) -> None:
    base_dir = Path(base_dir).expanduser()
    os.chdir(base_dir)
    logger.info("Working directory: %s", Path.cwd())

    engine = create_engine(get_connection_string())

    target_keys = tables if tables is not None else PATTERNS.keys()

    for key in target_keys:
        upload_layer(key, engine, if_exists=if_exists)


if __name__ == "__main__":
    main()
