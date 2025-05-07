import logging
import psycopg2
from sqlalchemy import create_engine, text
from tqdm import tqdm
from pathlib import Path
from typing import Dict, List, Optional

# Use same DB parameters as in load_raw.py
DB_PARAMS = {
    "host": "dtim.essi.upc.edu",
    "port": 5432,
    "dbname": "dbakosschneider",
    "user": "akosschneider",
    "password": "DMT2025!"
}

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def get_connection_string():
    return f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"


def get_connection():
    """Get a psycopg2 connection to the database."""
    return psycopg2.connect(
        host=DB_PARAMS['host'],
        port=DB_PARAMS['port'],
        dbname=DB_PARAMS['dbname'],
        user=DB_PARAMS['user'],
        password=DB_PARAMS['password']
    )


def execute_sql(sql: str, fetch=False):
    """Execute SQL statements and optionally fetch results."""
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


def get_timestamp_format(table_name: str):
    """Determine the format of the timestamp column."""
    log.info(f"Checking timestamp format in {table_name}...")
    
    format_check = execute_sql(f"""
    SELECT "last_updated" 
    FROM {table_name} 
    WHERE "last_updated" IS NOT NULL
    LIMIT 1
    """, fetch=True)
    
    if not format_check:
        log.error("No non-null last_updated values found.")
        return None
    
    sample_timestamp = format_check[0][0]
    log.info(f"Sample timestamp: {sample_timestamp}")
    
    if isinstance(sample_timestamp, int) or (isinstance(sample_timestamp, str) and sample_timestamp.isdigit()):
        # Unix timestamp (seconds since epoch)
        log.info("Using Unix timestamp conversion")
        return 'unix'
    else:
        # Already a timestamp format
        log.info("Using direct timestamp casting")
        return 'timestamp'


def analyze_missing_values(clean_table: str, needed_columns: List[str]):
    """Analyze missing values in the clean table and report statistics."""
    log.info("Analyzing missing values in clean table...")
    
    # For each column, calculate % of missing values
    missing_stats = {}
    for column in needed_columns:
        sql_calc_missing = f"""
        SELECT 
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE "{column}" IS NULL) AS missing_count,
            ROUND(100.0 * COUNT(*) FILTER (WHERE "{column}" IS NULL) / COUNT(*), 2) AS missing_percentage
        FROM {clean_table}
        """
        result = execute_sql(sql_calc_missing, fetch=True)
        total_rows, missing_count, missing_percentage = result[0]
        missing_stats[column] = {
            'total_rows': total_rows,
            'missing_count': missing_count,
            'missing_percentage': missing_percentage
        }
        log.info(f"Column '{column}': {missing_percentage}% missing ({missing_count}/{total_rows})")
    
    return missing_stats


def clean_data_with_cte(source_table: str, clean_table: str, window_minutes: int = 1440):
    """
    Clean the data using a single SQL query with CTEs to:
    1. Sample the data to one record per station per time window
    2. Convert data types
    3. Select only needed columns
    """
    log.info(f"Cleaning data with CTE approach, window size: {window_minutes} minutes")
    
    # Check timestamp format
    ts_format = get_timestamp_format(source_table)
    if not ts_format:
        return {'error': 'Could not determine timestamp format'}
    
    # Define timestamp conversion expression based on format
    if ts_format == 'unix':
        ts_expr = 'TO_TIMESTAMP(CAST("last_updated" AS BIGINT))'
        date_expr = 'TO_TIMESTAMP(CAST("last_updated" AS BIGINT))::DATE'
    else:
        ts_expr = 'CAST("last_updated" AS TIMESTAMP)'
        date_expr = 'CAST("last_updated" AS DATE)'
    
    # Define window expression based on minutes
    if window_minutes == 1440:  # Daily
        window_expr = date_expr
        log.info("Using daily time buckets")
    elif window_minutes == 60:  # Hourly
        window_expr = f"date_trunc('hour', {ts_expr})"
        log.info("Using hourly time buckets")
    else:  # Custom minutes
        window_expr = f"date_trunc('hour', {ts_expr}) + INTERVAL '{window_minutes} minutes' * (EXTRACT(MINUTE FROM {ts_expr}) / {window_minutes})::integer"
        log.info(f"Using {window_minutes} minute time buckets")
    
    # Drop existing table if it exists
    execute_sql(f"DROP TABLE IF EXISTS {clean_table}")
    
    # Comprehensive CTE-based query that:
    # 1. Converts data types
    # 2. Creates time buckets
    # 3. Samples data to one record per station per time bucket
    # 4. Performs any needed imputations
    
    log.info("Building and executing comprehensive cleaning query...")
    sql_clean = f"""
    CREATE TABLE {clean_table} AS
    WITH 
    -- Step 1: Convert raw data types
    converted AS (
        SELECT 
            CAST("station_id" AS INTEGER) AS station_id,
            "name"::TEXT AS name,
            CASE 
                WHEN "lat" ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST("lat" AS NUMERIC)
                ELSE NULL
            END AS lat,
            CASE 
                WHEN "lon" ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST("lon" AS NUMERIC)
                ELSE NULL 
            END AS lon,
            CASE 
                WHEN "altitude" ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST("altitude" AS NUMERIC)
                ELSE NULL
            END AS altitude,
            CASE 
                WHEN "capacity" ~ '^[0-9]+$' THEN CAST("capacity" AS INTEGER)
                ELSE NULL
            END AS capacity,
            {ts_expr} AS last_updated,
            {window_expr} AS time_bucket
        FROM {source_table}
        WHERE "last_updated" IS NOT NULL
    ),
    
    -- Step 2: Select distinct records (one per station per time bucket)
    sampled AS (
        SELECT DISTINCT ON (station_id, time_bucket)
            station_id,
            name,
            lat,
            lon,
            altitude,
            capacity,
            last_updated,
            time_bucket
        FROM converted
        ORDER BY station_id, time_bucket, last_updated
    )
    
    -- Final selection
    SELECT 
        station_id,
        name,
        lat,
        lon,
        altitude,
        capacity,
        last_updated,
        time_bucket
    FROM sampled
    """
    
    execute_sql(sql_clean)
    
    # Count rows
    row_count = execute_sql(f"SELECT COUNT(*) FROM {clean_table}", fetch=True)[0][0]
    log.info(f"Created clean table with {row_count:,} rows")
    
    # Create indexes
    log.info("Creating indexes...")
    execute_sql(f"CREATE INDEX idx_{clean_table}_station_id ON {clean_table}(station_id)")
    execute_sql(f"CREATE INDEX idx_{clean_table}_time ON {clean_table}(time_bucket)")
    
    return {'table': clean_table, 'rows': row_count}


def impute_missing_values_cte(table_name: str, needs_imputation: bool):
    """
    Impute missing values using a CTE approach, only if needed.
    Returns a new table name with the imputed data.
    """
    if not needs_imputation:
        log.info("Skipping imputation (no significant missing values)")
        return table_name
    
    log.info("Imputing missing values with CTE...")
    imputed_table = f"{table_name}_imputed"
    execute_sql(f"DROP TABLE IF EXISTS {imputed_table}")
    
    sql_impute = f"""
    CREATE TABLE {imputed_table} AS
    WITH 
    -- Calculate median altitude per station
    altitude_medians AS (
        SELECT 
            station_id,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY altitude) AS median_altitude
        FROM {table_name}
        WHERE altitude IS NOT NULL
        GROUP BY station_id
    ),
    
    -- Calculate most common capacity per station
    capacity_modes AS (
        SELECT 
            station_id,
            (array_agg(capacity ORDER BY COUNT(*) DESC))[1] AS mode_capacity
        FROM {table_name}
        WHERE capacity IS NOT NULL
        GROUP BY station_id
    )
    
    -- Join everything together with imputed values
    SELECT 
        t.station_id,
        t.name,
        t.lat,
        t.lon,
        COALESCE(t.altitude, a.median_altitude) AS altitude,
        COALESCE(t.capacity, c.mode_capacity) AS capacity,
        t.last_updated,
        t.time_bucket
    FROM {table_name} t
    LEFT JOIN altitude_medians a ON t.station_id = a.station_id
    LEFT JOIN capacity_modes c ON t.station_id = c.station_id
    """
    
    execute_sql(sql_impute)
    
    # Create indexes
    execute_sql(f"CREATE INDEX idx_{imputed_table}_station_id ON {imputed_table}(station_id)")
    execute_sql(f"CREATE INDEX idx_{imputed_table}_time ON {imputed_table}(time_bucket)")
    
    # Check imputation results
    result = execute_sql(f"""
    SELECT 
        COUNT(*) FILTER (WHERE altitude IS NULL) AS null_altitude,
        COUNT(*) FILTER (WHERE capacity IS NULL) AS null_capacity
    FROM {imputed_table}
    """, fetch=True)
    
    log.info(f"After imputation: {result[0][0]} null altitude, {result[0][1]} null capacity")
    
    return imputed_table


def clean_bicing_station_information():
    """Master function to clean bicycle station information data using CTEs."""
    source_table = "bicycle_station_information_raw"
    clean_table = "bicycle_station_information_clean"
    
    # Define the columns we need
    needed_columns = ["station_id", "name", "lat", "lon", "altitude", "capacity", "last_updated"]
    
    # Step 1: Clean and sample the data with CTEs
    log.info("Step 1: Cleaning data with CTE approach")
    clean_result = clean_data_with_cte(source_table, "temp_clean_table", window_minutes=1440)
    
    if 'error' in clean_result:
        log.error(f"Error in cleaning: {clean_result['error']}")
        return {'error': clean_result['error']}
    
    # Step 2: Analyze missing values
    log.info("Step 2: Analyzing missing values")
    missing_stats = analyze_missing_values("temp_clean_table", needed_columns)
    
    # Check if we need imputation (>0.1% missing in any target column)
    needs_imputation = False
    for column, stats in missing_stats.items():
        if stats['missing_percentage'] > 0.1 and column in ['altitude', 'capacity']:
            log.warning(f"Column '{column}' has {stats['missing_percentage']}% missing values")
            needs_imputation = True
    
    # Step 3: Impute if needed and create final table
    log.info("Step 3: Finalizing clean table")
    final_table = impute_missing_values_cte("temp_clean_table", needs_imputation)
    
    # Rename the final table to the target name
    if final_table != clean_table:
        execute_sql(f"DROP TABLE IF EXISTS {clean_table}")
        execute_sql(f"ALTER TABLE {final_table} RENAME TO {clean_table}")
        log.info(f"Renamed {final_table} to {clean_table}")
    
    # Clean up temporary tables
    execute_sql("DROP TABLE IF EXISTS temp_clean_table")
    execute_sql("DROP TABLE IF EXISTS temp_clean_table_imputed")
    
    log.info(f"Cleaning complete. Final table: {clean_table}")
    
    # Get final row count
    final_count = execute_sql(f"SELECT COUNT(*) FROM {clean_table}", fetch=True)[0][0]
    
    return {
        'missing_stats': missing_stats,
        'final_row_count': final_count
    }


if __name__ == "__main__":
    # Run the full cleaning process
    results = clean_bicing_station_information()
    
    # Print summary
    log.info("===== DATA CLEANING SUMMARY =====")
    log.info(f"Missing values analysis complete")
    
    if 'error' in results:
        log.error(f"Cleaning process had errors: {results['error']}")
    else:
        log.info(f"Final clean table created: bicycle_station_information_clean with {results['final_row_count']:,} rows") 