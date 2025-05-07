import logging
import pandas as pd
import geopandas as gpd
from pathlib import Path
from tqdm import tqdm
from sqlalchemy import create_engine, text
from typing import Optional
import psycopg2


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


DB_PARAMS = {
    "host": "dtim.essi.upc.edu",
    "port": 5432,
    "dbname": "dbakosschneider",
    "user": "akosschneider",
    "password": "DMT2025!"
}
BASE_PATH = Path("data")
CHUNKSIZE = 50_000


def get_connection_string():
    return f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"


def load_geospatial_lanes(folder: Path, table_name: str, engine, filter_years: Optional[range] = None):
    files = list(folder.glob("**/*.geojson")) + list(folder.glob("**/*.shp"))
    
    # Apply year filter if provided
    if filter_years:
        filtered_files = []
        for file in files:
            if any(str(year) in file.name for year in filter_years):
                filtered_files.append(file)
        files = filtered_files

    log.debug(f"Loading from {len(files)} files:")
    log.debug("\n".join([str(file) for file in files]))

    for file in tqdm(files, desc=f"Loading lanes to {table_name}"):
        # print columns and their data types
        # print(f"Columns and data types for {file}:")
        # gdf = gpd.read_file(file)
        # print(gdf.dtypes)
        # # print the first row
        # print(gdf.head())
        # break
        try:
            gdf: gpd.GeoDataFrame = gpd.read_file(file)
            gdf = gdf.to_crs(epsg=4326)
            gdf.rename(columns={gdf.geometry.name: 'geometry'}, inplace=True)
            gdf.rename(columns=lambda x: x.lower(), inplace=True)
            gdf.rename(columns={'any': '_any'}, inplace=True)
            gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
        except Exception as e:
            tqdm.write(f"[ERROR] {file}: {e}")


def load_csv_to_postgres(folder: Path, table_name: str, engine, filter_years: Optional[range] = None):
    files = list(folder.glob("**/*.csv"))
    
    if filter_years:
        filtered_files = []
        for file in files:
            if any(str(year) in file.name for year in filter_years):
                filtered_files.append(file)
        files = filtered_files
    
    tqdm.write(f"Loading {len(files)} files matching filter criteria")

    for i, file in enumerate(tqdm(files, desc=f"Loading CSVs to {table_name}")):
        try:
            for j, chunk in tqdm(enumerate(pd.read_csv(file, chunksize=CHUNKSIZE)), desc=f"Loading {file}"):
                if_exists = 'replace' if i == 0 and j == 0 else 'append'
                chunk.to_sql(table_name, engine, if_exists=if_exists, index=False, method='multi')
        except Exception as e:
            tqdm.write(f"[ERROR] Failed to load {file}: {e}")


def load_csv_to_postgres_optimized(folder: Path, table_name: str, engine, filter_years: Optional[range] = None):
    files = list(folder.glob("**/*.csv"))
    if filter_years:
        filtered_files = []
        for file in files:
            if any(str(year) in file.name for year in filter_years):
                filtered_files.append(file)
        files = filtered_files
    
    tqdm.write(f"Loading {len(files)} files matching filter criteria")
    
    # Create a union schema by sampling all files
    all_columns = set()
    for file in tqdm(files, desc="Building unified schema"):
        try:
            sample = pd.read_csv(file, nrows=5)
            # Add all columns from this file to our set of all columns
            all_columns.update(sample.columns)
        except Exception as e:
            tqdm.write(f"[WARNING] Could not read schema from {file}: {e}")
    
    # Convert to sorted list for consistent column order
    all_columns = sorted(list(all_columns))
    tqdm.write(f"Created unified schema with {len(all_columns)} columns")
    
    # Create table with all text columns to avoid type issues
    with engine.connect() as conn:
        # Drop table if exists
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        
        # Create table with all columns as text
        create_table_sql = f"CREATE TABLE {table_name} ("
        create_table_sql += ", ".join([f"\"{col}\" TEXT" for col in all_columns])
        create_table_sql += ")"
        conn.execute(text(create_table_sql))
        
        # Set unlogged for faster inserts
        conn.execute(text(f"ALTER TABLE {table_name} SET UNLOGGED"))
        conn.commit()
    
    # Use COPY with appropriate options
    conn_params = {k: DB_PARAMS[k] for k in ['host', 'port', 'dbname', 'user', 'password']}
    
    for file in tqdm(files, desc=f"Loading CSVs to {table_name}"):
        try:
            # Try direct COPY first (faster)
            conn = psycopg2.connect(**conn_params)
            with conn.cursor() as cursor:
                # First read the file header to get its column structure
                file_columns = pd.read_csv(file, nrows=0).columns.tolist()
                
                # Check if all file columns are in our schema
                missing_columns = [col for col in file_columns if col not in all_columns]
                if missing_columns:
                    raise ValueError(f"File has columns not in schema: {missing_columns}")
                
                # Generate COPY command with columns
                copy_sql = f"COPY {table_name}("
                copy_sql += ", ".join([f"\"{col}\"" for col in file_columns])
                copy_sql += ") FROM STDIN WITH CSV"
                
                with open(file, 'r', encoding='utf-8') as f:
                    # Skip header
                    header = next(f)
                    cursor.copy_expert(copy_sql, f)
                
                conn.commit()
            conn.close()
            tqdm.write(f"Successfully loaded {file} (direct COPY)")
            
        except Exception as e:
            tqdm.write(f"[WARNING] Direct COPY failed for {file}: {e}")
            tqdm.write(f"Falling back to temp table approach...")
            
            try:
                # Fall back to temp table approach
                # First read the file header to get its column structure
                file_columns = pd.read_csv(file, nrows=0).columns.tolist()
                
                # Connect for each file
                conn = psycopg2.connect(**conn_params)
                
                with conn.cursor() as cursor:
                    # Create a temporary table matching this file's schema
                    temp_table = f"temp_{table_name}"
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                    
                    temp_table_sql = f"CREATE TEMPORARY TABLE {temp_table} ("
                    temp_table_sql += ", ".join([f"\"{col}\" TEXT" for col in file_columns])
                    temp_table_sql += ")"
                    cursor.execute(temp_table_sql)
                    
                    # Load data into temporary table
                    with open(file, 'r', encoding='utf-8') as f:
                        # Skip header
                        header = next(f)
                        
                        cursor.copy_expert(
                            f"COPY {temp_table} FROM STDIN WITH CSV",
                            f
                        )
                    
                    # Insert from temp table to main table with column mapping
                    insert_sql = f"INSERT INTO {table_name} ("
                    insert_sql += ", ".join([f"\"{col}\"" for col in all_columns])
                    insert_sql += ") SELECT "
                    
                    # For each target column, either select from temp table or NULL
                    select_parts = []
                    for col in all_columns:
                        if col in file_columns:
                            select_parts.append(f"\"{col}\"")
                        else:
                            select_parts.append("NULL")
                    
                    insert_sql += ", ".join(select_parts)
                    insert_sql += f" FROM {temp_table}"
                    
                    cursor.execute(insert_sql)
                    cursor.execute(f"DROP TABLE {temp_table}")
                    conn.commit()
                
                conn.close()
                tqdm.write(f"Successfully loaded {file} (fallback method)")
            except Exception as e2:
                tqdm.write(f"[ERROR] Both methods failed for {file}. Final error: {e2}")
    
    # Re-enable logging
    with engine.connect() as conn:
        conn.execute(text(f"ALTER TABLE {table_name} SET LOGGED"))
        conn.commit()


if __name__ == "__main__":
    engine = create_engine(get_connection_string())
    
    # Define years to filter, similar to download.py
    filter_years = range(2019, 2022)

    # # 1. Bicycle lanes (Geo)
    # load_geospatial_lanes(
    #     folder=BASE_PATH / "bicycle_lanes/decompressed",
    #     table_name="bicycle_lanes_raw",
    #     engine=engine,
    #     filter_years=filter_years
    # )

    # 2. Station Information
    load_csv_to_postgres_optimized(
        folder=BASE_PATH / "bicycle_stations/information/decompressed",
        table_name="bicycle_station_information_raw",
        engine=engine,
        filter_years=filter_years
    )

    # # 3. Station Status
    # load_csv_to_postgres(
    #     folder=BASE_PATH / "bicycle_stations/stations/decompressed",
    #     table_name="bicycle_station_status_raw",
    #     engine=engine,
    #     filter_years=filter_years
    # )
