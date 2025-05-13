import logging
import psycopg2
from sqlalchemy import create_engine, text
from tqdm import tqdm
from pathlib import Path
from typing import Dict, List, Optional

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


def clean_data_with_cte(source_table: str, clean_table: str):
    """
    Clean the data using a single SQL query with CTEs to:
    1. Convert data types
    2. Handle missing values
    """
    log.info("Cleaning data with CTE approach")
    
    # Check timestamp format
    ts_format = get_timestamp_format(source_table)
    if not ts_format:
        return {'error': 'Could not determine timestamp format'}
    
    # Define timestamp conversion expression based on format
    if ts_format == 'unix':
        ts_expr = 'TO_TIMESTAMP(CAST(CAST("last_updated" AS NUMERIC) AS BIGINT))'
    else:
        ts_expr = 'CAST("last_updated" AS TIMESTAMP)'
    
    # Drop existing table if it exists
    execute_sql(f"DROP TABLE IF EXISTS {clean_table}")
    
    # Comprehensive CTE-based query that:
    # 1. Converts data types
    # 2. Handles missing values
    
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
                WHEN "lat" = 'NA' THEN NULL
                WHEN "lat" ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST("lat" AS NUMERIC)
                ELSE NULL
            END AS lat,
            CASE 
                WHEN "lon" = 'NA' THEN NULL
                WHEN "lon" ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST("lon" AS NUMERIC)
                ELSE NULL 
            END AS lon,
            CASE 
                WHEN "altitude" = 'NA' THEN NULL
                WHEN "altitude" ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST("altitude" AS NUMERIC)
                ELSE NULL
            END AS altitude,
            CASE 
                WHEN "capacity" = 'NA' THEN NULL
                WHEN "capacity" ~ '^[0-9]+$' THEN CAST("capacity" AS INTEGER)
                ELSE NULL
            END AS capacity,
            CASE
                WHEN "last_updated" = 'NA' THEN NULL
                ELSE {ts_expr}
            END AS last_updated
        FROM {source_table}
        WHERE "last_updated" IS NOT NULL AND "last_updated" != 'NA'
    )
    
    -- Final selection
    SELECT 
        station_id,
        name,
        lat,
        lon,
        altitude,
        capacity,
        last_updated
    FROM converted
    """
    
    execute_sql(sql_clean)
    
    # Count rows
    row_count = execute_sql(f"SELECT COUNT(*) FROM {clean_table}", fetch=True)[0][0]
    log.info(f"Created clean table with {row_count:,} rows")
    
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
        t.last_updated
    FROM {table_name} t
    LEFT JOIN altitude_medians a ON t.station_id = a.station_id
    LEFT JOIN capacity_modes c ON t.station_id = c.station_id
    """
    
    execute_sql(sql_impute)
    
    # Create indexes
    execute_sql(f"CREATE INDEX idx_{imputed_table}_station_id ON {imputed_table}(station_id)")
    execute_sql(f"CREATE INDEX idx_{imputed_table}_time ON {imputed_table}(last_updated)")
    
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
    
    # Step 1: Clean the data with CTEs
    log.info("Step 1: Cleaning data with CTE approach")
    clean_result = clean_data_with_cte(source_table, "temp_clean_table")
    
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


def clean_bicing_station_status():
    """Master function to clean bicycle station status data using CTEs."""
    source_table = "bicycle_station_status_raw"
    clean_table = "bicycle_station_status_clean"
    
    # Define the columns we need
    needed_columns = ["station_id", "num_bikes_available", "num_bikes_available_types.mechanical", 
                      "num_bikes_available_types.ebike", "num_docks_available", "last_reported", 
                      "status", "last_updated"]
    
    # Step 1: Clean the data with CTEs
    log.info("Step 1: Cleaning status data with CTE approach")
    
    # Check timestamp format for last_updated
    ts_format = get_timestamp_format(source_table)
    if not ts_format:
        return {'error': 'Could not determine timestamp format'}
    
    # Define timestamp conversion expressions based on format
    if ts_format == 'unix':
        # Handle scientific notation (e.g. "1.578e+09") by first casting to numeric
        last_updated_expr = 'TO_TIMESTAMP(CAST(CAST("last_updated" AS NUMERIC) AS BIGINT))'
        last_reported_expr = 'TO_TIMESTAMP(CAST(CAST("last_reported" AS NUMERIC) AS BIGINT))'
    else:
        last_updated_expr = 'CAST("last_updated" AS TIMESTAMP)'
        last_reported_expr = 'CAST("last_reported" AS TIMESTAMP)'
    
    # Drop existing table if it exists
    execute_sql(f"DROP TABLE IF EXISTS temp_clean_status")
    
    # Comprehensive CTE-based query for status data
    log.info("Building and executing comprehensive cleaning query for status data...")
    sql_clean = f"""
    CREATE TABLE temp_clean_status AS
    WITH 
    -- Step 1: Convert raw data types
    converted AS (
        SELECT 
            CAST("station_id" AS INTEGER) AS station_id,
            CASE 
                WHEN "num_bikes_available" = 'NA' THEN NULL
                WHEN "num_bikes_available" ~ '^[0-9]+$' THEN CAST("num_bikes_available" AS INTEGER)
                ELSE NULL
            END AS num_bikes_available,
            CASE 
                WHEN "num_bikes_available_types.mechanical" = 'NA' THEN NULL
                WHEN "num_bikes_available_types.mechanical" ~ '^[0-9]+$' THEN CAST("num_bikes_available_types.mechanical" AS INTEGER)
                ELSE NULL
            END AS mechanical_bikes,
            CASE 
                WHEN "num_bikes_available_types.ebike" = 'NA' THEN NULL
                WHEN "num_bikes_available_types.ebike" ~ '^[0-9]+$' THEN CAST("num_bikes_available_types.ebike" AS INTEGER)
                ELSE NULL
            END AS ebikes,
            CASE 
                WHEN "num_docks_available" = 'NA' THEN NULL
                WHEN "num_docks_available" ~ '^[0-9]+$' THEN CAST("num_docks_available" AS INTEGER)
                ELSE NULL
            END AS num_docks_available,
            CASE
                WHEN "last_reported" = 'NA' THEN NULL
                ELSE {last_reported_expr}
            END AS last_reported,
            CASE
                WHEN "status" = 'NA' THEN NULL
                ELSE "status"::TEXT
            END AS status,
            CASE
                WHEN "last_updated" = 'NA' THEN NULL
                ELSE {last_updated_expr}
            END AS last_updated
        FROM {source_table}
        WHERE "last_updated" IS NOT NULL AND "last_updated" != 'NA'
    )
    
    -- Final selection
    SELECT 
        station_id,
        num_bikes_available,
        mechanical_bikes,
        ebikes,
        num_docks_available,
        last_reported,
        status,
        last_updated
    FROM converted
    """
    
    execute_sql(sql_clean)
    
    # Count rows
    row_count = execute_sql(f"SELECT COUNT(*) FROM temp_clean_status", fetch=True)[0][0]
    log.info(f"Created temp clean status table with {row_count:,} rows")
    
    # Step 2: Analyze missing values
    log.info("Step 2: Analyzing missing values in status data")
    status_columns = ["station_id", "num_bikes_available", "mechanical_bikes", "ebikes", 
                       "num_docks_available", "last_reported", "status", "last_updated"]
    
    missing_stats = analyze_missing_values("temp_clean_status", status_columns)
    
    # Step 3: Impute if needed
    log.info("Step 3: Handling missing values in status data")
    
    # Check if we need imputation (>0.1% missing in any important column)
    needs_imputation = False
    imputation_columns = ["num_bikes_available", "mechanical_bikes", "ebikes", "num_docks_available"]
    
    for column, stats in missing_stats.items():
        if stats['missing_percentage'] > 0.1 and column in imputation_columns:
            log.warning(f"Column '{column}' has {stats['missing_percentage']}% missing values")
            needs_imputation = True
    
    if needs_imputation:
        log.info("Imputing missing values for station status data...")
        execute_sql(f"DROP TABLE IF EXISTS {clean_table}")
        
        # Impute missing values with a CTE approach
        sql_impute = f"""
        CREATE TABLE {clean_table} AS
        WITH 
        -- Calculate median num_bikes_available per station
        bike_medians AS (
            SELECT 
                station_id,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY num_bikes_available) AS median_bikes,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mechanical_bikes) AS median_mechanical,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ebikes) AS median_ebikes,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY num_docks_available) AS median_docks
            FROM temp_clean_status
            WHERE num_bikes_available IS NOT NULL 
              OR mechanical_bikes IS NOT NULL 
              OR ebikes IS NOT NULL 
              OR num_docks_available IS NOT NULL
            GROUP BY station_id
        )
        
        -- Join everything together with imputed values
        SELECT 
            t.station_id,
            COALESCE(t.num_bikes_available, b.median_bikes) AS num_bikes_available,
            COALESCE(t.mechanical_bikes, b.median_mechanical) AS mechanical_bikes,
            COALESCE(t.ebikes, b.median_ebikes) AS ebikes,
            COALESCE(t.num_docks_available, b.median_docks) AS num_docks_available,
            t.last_reported,
            t.status,
            t.last_updated
        FROM temp_clean_status t
        LEFT JOIN bike_medians b ON t.station_id = b.station_id
        """
        
        execute_sql(sql_impute)
    else:
        # If no imputation needed, just rename the table
        execute_sql(f"DROP TABLE IF EXISTS {clean_table}")
        execute_sql(f"ALTER TABLE temp_clean_status RENAME TO {clean_table}")
    
    # Clean up temporary tables
    execute_sql("DROP TABLE IF EXISTS temp_clean_status")
    
    log.info(f"Cleaning complete for station status. Final table: {clean_table}")
    
    # Get final row count
    final_count = execute_sql(f"SELECT COUNT(*) FROM {clean_table}", fetch=True)[0][0]
    
    return {
        'missing_stats': missing_stats,
        'final_row_count': final_count
    }


if __name__ == "__main__":
    # Run the full cleaning process for both information and status
    log.info("===== CLEANING STATION INFORMATION =====")
    info_results = clean_bicing_station_information()
    
    log.info("\n===== CLEANING STATION STATUS =====")
    status_results = clean_bicing_station_status()
    
    # Print summary
    log.info("\n===== DATA CLEANING SUMMARY =====")
    
    if 'error' in info_results:
        log.error(f"Station information cleaning had errors: {info_results['error']}")
    else:
        log.info(f"Station information clean table created with {info_results['final_row_count']:,} rows")
    
    if 'error' in status_results:
        log.error(f"Station status cleaning had errors: {status_results['error']}")
    else:
        log.info(f"Station status clean table created with {status_results['final_row_count']:,} rows") 