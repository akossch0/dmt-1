
import logging
import psycopg2
import geopandas as gpd
from sqlalchemy import create_engine

DB_PARAMS = {
    "host": "dtim.essi.upc.edu",
    "port": 5432,
    "dbname": "dbakosschneider",
    "user": "akosschneider",
    "password": "DMT2025!"
}

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("clean_income_with_geometry")

def get_connection():
    return psycopg2.connect(
        host=DB_PARAMS['host'],
        port=DB_PARAMS['port'],
        dbname=DB_PARAMS['dbname'],
        user=DB_PARAMS['user'],
        password=DB_PARAMS['password']
    )

def execute_sql(sql: str, fetch=False):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            if fetch:
                return cursor.fetchall()
            conn.commit()
    except Exception as e:
        log.error(f"Error executing SQL: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def clean_income_data(source_table="income_raw", clean_table="income_clean"):
    log.info(f"Dropping table if exists: {clean_table}")
    execute_sql(f"DROP TABLE IF EXISTS {clean_table}")

    log.info("Creating clean income table...")
    sql = f'''
    CREATE TABLE {clean_table} AS
    WITH cleaned AS (
        SELECT
            LPAD(CAST("codi_districte" AS TEXT), 2, '0') AS codi_districte,
            CAST("nom_districte" AS TEXT) AS nom_districte,
            LPAD(CAST("codi_barri" AS TEXT), 2, '0') AS codi_barri,
            CAST("nom_barri" AS TEXT) AS nom_barri,
            LPAD(CAST("seccio_censal" AS TEXT), 4, '0') AS seccio_censal,
            CAST("import_euros" AS FLOAT) AS import_euros
        FROM {source_table}
        WHERE "import_euros" IS NOT NULL
    ),
    stats AS (
        SELECT MIN(import_euros) AS min_income, MAX(import_euros) AS max_income FROM cleaned
    )
    SELECT 
        c.*,
        ROUND(((c.import_euros - s.min_income) / NULLIF(s.max_income - s.min_income, 0))::NUMERIC, 6) AS income_norm
    FROM cleaned c CROSS JOIN stats s
    '''
    execute_sql(sql)

    count = execute_sql(f"SELECT COUNT(*) FROM {clean_table}", fetch=True)[0][0]
    log.info(f"✅ {clean_table} created with {count:,} rows")

def add_geometry_from_geojson(clean_table="income_clean", geojson_path="../../../data/income/clean/final_output.geojson"):
    log.info("Loading GeoJSON and uploading temp geometry table...")
    engine = create_engine(
        f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@"
        f"{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
    )

    gdf = gpd.read_file(geojson_path)

    # Normalize column names and format
    gdf.rename(columns=lambda c: c.lower(), inplace=True)
    gdf = gdf.loc[:, ~gdf.columns.duplicated()]
    gdf["codi_districte"] = gdf["codi_districte"].astype(str).str.zfill(2)
    gdf["codi_barri"] = gdf["codi_barri"].astype(str).str.zfill(2)
    gdf["seccio_censal"] = gdf["seccio_censal"].astype(str).str.zfill(4)

    gdf.to_postgis("income_geometry_temp", engine, if_exists="replace", index=False)

    log.info("Adding geometry column to clean table and joining geometries...")
    execute_sql("ALTER TABLE income_clean ADD COLUMN IF NOT EXISTS geometry geometry")

    sql_update = '''
    UPDATE income_clean
    SET geometry = g.geometry
    FROM income_geometry_temp g
    WHERE income_clean.codi_districte = g.codi_districte
      AND income_clean.codi_barri = g.codi_barri
      AND income_clean.seccio_censal = g.seccio_censal
    '''
    execute_sql(sql_update)

    log.info("Dropping temporary geometry table...")
    execute_sql("DROP TABLE IF EXISTS income_geometry_temp")

    count = execute_sql("SELECT COUNT(*) FROM income_clean WHERE geometry IS NOT NULL", fetch=True)[0][0]
    log.info(f"✅ Geometry added to {count:,} rows in income_clean")

if __name__ == "__main__":
    clean_income_data()
    add_geometry_from_geojson()

