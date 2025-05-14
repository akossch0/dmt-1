
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
            LPAD(CAST("codi_barri" AS TEXT), 2, '0') AS codi_barri,
            CAST("nom_districte" AS TEXT) AS nom_districte,
            CAST("nom_barri" AS TEXT) AS nom_barri,
            CAST("any" AS DATE) AS "any",
            CAST("import_euros" AS FLOAT) AS import_euros,
            CAST("codi_districte" AS INT) AS codi_districte,
            CAST("seccio_censal" AS INT) AS seccio_censal
        FROM {source_table}
        WHERE "import_euros" IS NOT NULL
    ),
    enriched AS (
        SELECT *,
            (CAST(codi_districte AS TEXT) || LPAD(CAST(seccio_censal AS TEXT), 3, '0'))::INT AS seccio_censal_concat
        FROM cleaned
    ),
    stats AS (
        SELECT MIN(import_euros) AS min_income, MAX(import_euros) AS max_income FROM enriched
    )
    SELECT 
        codi_districte,
        nom_districte,
        codi_barri,
        nom_barri,
        seccio_censal_concat AS seccio_censal,
        import_euros,
        "any",
        ROUND(((import_euros - s.min_income) / NULLIF(s.max_income - s.min_income, 0))::NUMERIC, 6) AS income_norm
    FROM enriched CROSS JOIN stats s
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
    gdf.rename(columns=lambda c: c.lower(), inplace=True)
    gdf = gdf.loc[:, ~gdf.columns.duplicated()]
    gdf["codi_districte"] = gdf["codi_districte"].astype(int)
    gdf["seccio_censal"] = gdf["seccio_censal"].astype(int)

    gdf.to_postgis("income_geometry_temp", engine, if_exists="replace", index=False)

    log.info("Adding geometry column to clean table and joining geometries...")
    execute_sql("ALTER TABLE income_clean ADD COLUMN IF NOT EXISTS geometry geometry")

    sql_update = '''
    UPDATE income_clean
    SET geometry = g.geometry
    FROM income_geometry_temp g
    WHERE income_clean.codi_districte = g.codi_districte
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
