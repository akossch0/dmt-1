import logging
import psycopg2
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

# ────────────────────────────────────────────────────────────────────────────────
# DB connection parameters
# ────────────────────────────────────────────────────────────────────────────────
DB_PARAMS: dict[str, str | int] = {
    "host": "dtim.essi.upc.edu",
    "port": 5432,
    "dbname": "dbakosschneider",
    "user": "akosschneider",
    "password": "DMT2025!",
}

# ────────────────────────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("clean_income_with_geometry")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def get_connection():
    """psycopg2 connection — used for DDL/DML."""
    return psycopg2.connect(
        host=DB_PARAMS["host"],
        port=DB_PARAMS["port"],
        dbname=DB_PARAMS["dbname"],
        user=DB_PARAMS["user"],
        password=DB_PARAMS["password"],
    )


def get_engine():
    """SQLAlchemy engine — convenient for pandas `read_sql`."""
    url = (
        f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@"
        f"{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
    )
    return create_engine(url)


def execute_sql(sql: str, fetch: bool = False):
    """Run arbitrary SQL with psycopg2, optionally returning the result."""
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            if fetch:
                return cursor.fetchall()
            conn.commit()
    except Exception as e:
        log.error("Error executing SQL: %s", e)
        conn.rollback()
        raise
    finally:
        conn.close()


def missing_summary(df: pd.DataFrame, name: str = "") -> None:
    """Pretty print a per-column missing-values summary."""
    nan = df.isna().sum()
    pct = (nan / len(df) * 100).round(2)
    header = f"Missing values — {name}" if name else "Missing values"
    log.info(header)
    log.info("-" * len(header))
    for col in df.columns:
        log.info("%-15s: %4d  (%5.2f%%)", col, nan[col], pct[col])


# ────────────────────────────────────────────────────────────────────────────────
# Main cleaning routine
# ────────────────────────────────────────────────────────────────────────────────
def clean_income_data(
    source_table: str = "income_raw", clean_table: str = "income_clean"
) -> None:
    # ----------------------------------------------------------------
    # 1)  Show missing values in the *raw* table
    # ----------------------------------------------------------------
    engine = get_engine()
    df_raw = pd.read_sql(f'SELECT * FROM "{source_table}"', con=engine)
    missing_summary(df_raw, source_table)
    engine.dispose()

    # ----------------------------------------------------------------
    # 2)  Re-create the clean table
    # ----------------------------------------------------------------
    log.info("Dropping table if exists: %s", clean_table)
    execute_sql(f'DROP TABLE IF EXISTS "{clean_table}"')

    log.info("Creating clean income table …")
    sql = f'''
    CREATE TABLE "{clean_table}" AS
    WITH cleaned AS (
        SELECT
            LPAD(CAST("codi_barri"    AS TEXT), 2, '0') AS codi_barri,
            CAST("nom_districte"  AS TEXT)  AS nom_districte,
            CAST("nom_barri"      AS TEXT)  AS nom_barri,
            CAST("any"            AS DATE)  AS "any",
            CAST("import_euros"   AS FLOAT) AS import_euros,
            CAST("codi_districte" AS INT)   AS codi_districte,
            CAST("seccio_censal"  AS INT)   AS seccio_censal
        FROM "{source_table}"
    ),
    enriched AS (
        SELECT *,
               (CAST(codi_districte AS TEXT) ||
                LPAD(CAST(seccio_censal AS TEXT), 3, '0'))::INT
               AS seccio_censal_concat
        FROM cleaned
    ),
    stats AS (
        SELECT
            MIN(import_euros) AS min_income,
            MAX(import_euros) AS max_income
        FROM enriched
    )
    SELECT
        "any",
        seccio_censal_concat                    AS seccio_censal,
        import_euros,
        ROUND(
            ((import_euros - s.min_income) /
             NULLIF(s.max_income - s.min_income, 0))::NUMERIC,
            6
        )                                       AS income_norm
    FROM enriched
    CROSS JOIN stats s;
    '''
    execute_sql(sql)

    # ----------------------------------------------------------------
    # 3)  Basic sanity check: row count & missing values in the **clean** table
    # ----------------------------------------------------------------
    count = execute_sql(f'SELECT COUNT(*) FROM "{clean_table}"', fetch=True)[0][0]
    log.info("✅ %s created with %,d rows", clean_table, count)

    engine = get_engine()
    df_clean = pd.read_sql(f'SELECT * FROM "{clean_table}"', con=engine)
    missing_summary(df_clean, clean_table)
    engine.dispose()


# ────────────────────────────────────────────────────────────────────────────────
# Script entry point
# ────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    clean_income_data()
