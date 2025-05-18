import pandas as pd
import typer
from sqlalchemy import create_engine, text, inspect, types
from sqlalchemy.exc import ProgrammingError
from tqdm import tqdm

DB_PARAMS = {
    "host": "dtim.essi.upc.edu",
    "port": 5432,
    "dbname": "dbakosschneider",
    "user": "akosschneider",
    "password": "DMT2025!"
}

def get_connection_string():
    return f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"

def execute_sql(engine, sql, print_error=True):
    """Execute SQL statement and print result message"""
    try:
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        print(f"Successfully executed: {sql.split()[0]}")
        return True
    except Exception as e:
        if print_error:
            print(f"Error executing {sql.split()[0]}: {e}")
        return False

def table_exists(engine, table_name):
    """Check if table exists in database"""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()

def table_is_empty(engine, table_name):
    """Check if table is empty"""
    if not table_exists(engine, table_name):
        return True
    
    try:
        query = f"SELECT EXISTS (SELECT 1 FROM {table_name} LIMIT 1)"
        with engine.connect() as conn:
            result = conn.execute(text(query)).scalar()
        return not result
    except Exception as e:
        print(f"Error checking if {table_name} is empty: {e}")
        return False

def drop_tables_if_exist(engine):
    """Drop bicycle station tables if they exist"""
    tables = [
        "fact_station_status",
        "fact_station_information", 
        "dim_ten_minute",
        "dim_hour",
        "dim_station"
    ]
    
    for table in tables:
        if table_exists(engine, table):
            execute_sql(engine, f"DROP TABLE {table} CASCADE")

def ensure_time_hierarchy_exists(engine):
    """Ensure all necessary timestamps exist in the time dimension hierarchy"""
    print("\nEnsuring time dimension hierarchy is complete...")
    
    # Extract all unique timestamps from the bicycle station data
    extract_timestamps_sql = """
    WITH all_timestamps AS (
        -- Get timestamps from station information
        SELECT 
            last_updated AS timestamp
        FROM 
            bicycle_station_information_clean
        UNION
        -- Get timestamps from station status
        SELECT 
            last_updated AS timestamp
        FROM 
            bicycle_station_status_clean
    )
    SELECT
        EXTRACT(YEAR FROM timestamp)::INT AS year,
        TO_CHAR(timestamp, 'YYYY-MM') AS year_month,
        timestamp::DATE AS date_value
    FROM
        all_timestamps;
    """
    timestamps_df = pd.read_sql(extract_timestamps_sql, engine)
    
    # Insert missing years
    all_years = sorted(timestamps_df['year'].unique())
    print(f"Found {len(all_years)} distinct years in station data")
    
    existing_years_df = pd.read_sql("SELECT year FROM dim_year", engine)
    existing_years = set(existing_years_df['year'].tolist())
    
    years_to_insert = [year for year in all_years if year not in existing_years]
    if years_to_insert:
        print(f"Inserting {len(years_to_insert)} missing years: {years_to_insert}")
        years_df = pd.DataFrame({'year': years_to_insert})
        years_df.to_sql('dim_year', engine, if_exists='append', index=False)
    else:
        print("No new years to insert")
    
    # Insert missing months
    all_year_months = sorted(timestamps_df['year_month'].unique())
    print(f"Found {len(all_year_months)} distinct year-months in station data")
    
    existing_months_df = pd.read_sql("SELECT year_month FROM dim_month", engine)
    existing_months = set(existing_months_df['year_month'].tolist())
    
    months_to_insert = [ym for ym in all_year_months if ym not in existing_months]
    if months_to_insert:
        print(f"Inserting {len(months_to_insert)} missing year-months")
        
        # Create DataFrame with proper structure for dim_month
        months_data = []
        for ym in months_to_insert:
            year = int(ym.split('-')[0])
            month = int(ym.split('-')[1])
            months_data.append({'year_month': ym, 'year': year, 'month': month})
        
        months_df = pd.DataFrame(months_data)
        months_df.to_sql('dim_month', engine, if_exists='append', index=False)
    else:
        print("No new year-months to insert")
    
    # Insert missing days
    all_dates = sorted(timestamps_df['date_value'].unique())
    print(f"Found {len(all_dates)} distinct dates in station data")
    
    existing_days_df = pd.read_sql("SELECT date_value FROM dim_day", engine)
    existing_days = set(existing_days_df['date_value'].astype(str).tolist())
    
    # Convert dates to strings for comparison
    all_dates_str = [str(d) for d in all_dates]
    dates_to_insert = [d for d in all_dates_str if d not in existing_days]
    
    if dates_to_insert:
        print(f"Inserting {len(dates_to_insert)} missing dates")
        
        # Create DataFrame with proper structure for dim_day
        days_data = []
        for date_str in dates_to_insert:
            date_obj = pd.to_datetime(date_str).date()
            year_month = date_obj.strftime('%Y-%m')
            day = date_obj.day
            days_data.append({'date_value': date_obj, 'year_month': year_month, 'day': day})
        
        days_df = pd.DataFrame(days_data)
        days_df.to_sql('dim_day', engine, if_exists='append', index=False, dtype={'date_value': types.Date})
    else:
        print("No new dates to insert")
    
    print("Time dimension hierarchy is now complete")

def create_dim_hour(engine):
    """Create and populate hour dimension table"""
    print("\nCreating hour dimension table...")
    
    if not table_exists(engine, "dim_hour"):
        create_sql = """
        CREATE TABLE dim_hour (
            hour_datetime TIMESTAMP PRIMARY KEY,
            date_value DATE,
            year INT,
            month INT,
            day INT,
            hour INT,
            day_part TEXT,
            FOREIGN KEY (date_value) REFERENCES dim_day(date_value)
        )
        """
        execute_sql(engine, create_sql)
    
    if table_is_empty(engine, "dim_hour"):
        populate_sql = """
        WITH all_timestamps AS (
            -- Get timestamps from station information
            SELECT DISTINCT 
                DATE_TRUNC('hour', last_updated) AS hour_datetime
            FROM 
                bicycle_station_information_clean
            UNION
            -- Get timestamps from station status
            SELECT DISTINCT 
                DATE_TRUNC('hour', last_updated) AS hour_datetime
            FROM 
                bicycle_station_status_clean
        )
        INSERT INTO dim_hour (
            hour_datetime,
            date_value,
            year,
            month,
            day,
            hour,
            day_part
        )
        SELECT 
            t.hour_datetime,
            t.hour_datetime::DATE AS date_value,
            EXTRACT(YEAR FROM t.hour_datetime)::INT AS year,
            EXTRACT(MONTH FROM t.hour_datetime)::INT AS month,
            EXTRACT(DAY FROM t.hour_datetime)::INT AS day,
            EXTRACT(HOUR FROM t.hour_datetime)::INT AS hour,
            CASE 
                WHEN EXTRACT(HOUR FROM t.hour_datetime) BETWEEN 5 AND 11 THEN 'morning'
                WHEN EXTRACT(HOUR FROM t.hour_datetime) BETWEEN 12 AND 16 THEN 'afternoon'
                WHEN EXTRACT(HOUR FROM t.hour_datetime) BETWEEN 17 AND 20 THEN 'evening'
                ELSE 'night'
            END AS day_part
        FROM 
            all_timestamps t
        JOIN 
            dim_day d ON t.hour_datetime::DATE = d.date_value
        ORDER BY 
            t.hour_datetime
        """
        execute_sql(engine, populate_sql)
        
        count_df = pd.read_sql("SELECT COUNT(*) FROM dim_hour", engine)
        print(f"Populated dim_hour with {count_df.iloc[0, 0]} rows")
    else:
        print("Table dim_hour already exists and contains data")

def create_dim_ten_minute(engine):
    """Create and populate ten-minute dimension table"""
    print("\nCreating ten-minute dimension table...")
    
    if not table_exists(engine, "dim_ten_minute"):
        create_sql = """
        CREATE TABLE dim_ten_minute (
            ten_min_datetime TIMESTAMP PRIMARY KEY,
            hour_datetime TIMESTAMP,
            minute_bucket INT,
            FOREIGN KEY (hour_datetime) REFERENCES dim_hour(hour_datetime)
        )
        """
        execute_sql(engine, create_sql)
    
    if table_is_empty(engine, "dim_ten_minute"):
        populate_sql = """
        WITH status_timestamps AS (
            SELECT DISTINCT 
                DATE_TRUNC('hour', last_updated) 
                + INTERVAL '10 minutes' * (EXTRACT(MINUTE FROM last_updated)::INT / 10) AS ten_min_datetime
            FROM 
                bicycle_station_status_clean
        )
        INSERT INTO dim_ten_minute (
            ten_min_datetime,
            hour_datetime,
            minute_bucket
        )
        SELECT 
            t.ten_min_datetime,
            DATE_TRUNC('hour', t.ten_min_datetime) AS hour_datetime,
            (EXTRACT(MINUTE FROM t.ten_min_datetime)::INT) AS minute_bucket
        FROM 
            status_timestamps t
        JOIN 
            dim_hour h ON DATE_TRUNC('hour', t.ten_min_datetime) = h.hour_datetime
        ORDER BY 
            t.ten_min_datetime
        """
        execute_sql(engine, populate_sql)
        
        count_df = pd.read_sql("SELECT COUNT(*) FROM dim_ten_minute", engine)
        print(f"Populated dim_ten_minute with {count_df.iloc[0, 0]} rows")
    else:
        print("Table dim_ten_minute already exists and contains data")

def create_dim_station(engine):
    """Create and populate station dimension table"""
    print("\nCreating station dimension table...")
    
    if not table_exists(engine, "dim_station"):
        create_sql = """
        CREATE TABLE dim_station (
            station_id INTEGER PRIMARY KEY,
            name TEXT,
            geometry GEOMETRY(POINT, 4326),
            altitude NUMERIC
        )
        """
        execute_sql(engine, create_sql)
    
    if table_is_empty(engine, "dim_station"):
        # First check for any duplicates
        check_dupes = """
        SELECT station_id, COUNT(*) 
        FROM bicycle_station_information_clean 
        GROUP BY station_id 
        HAVING COUNT(*) > 1
        """
        dupes_df = pd.read_sql(check_dupes, engine)
        if len(dupes_df) > 0:
            print(f"Found {len(dupes_df)} station IDs with multiple records. Will use most recent record for each.")
        
        populate_sql = """
        WITH latest_station_info AS (
            SELECT 
                station_id,
                name,
                lat,
                lon,
                altitude,
                ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY last_updated DESC) as rn
            FROM 
                bicycle_station_information_clean
        )
        INSERT INTO dim_station (
            station_id,
            name,
            geometry,
            altitude
        )
        SELECT
            station_id,
            name,
            ST_SetSRID(ST_MakePoint(lon, lat), 4326) AS geometry,
            altitude
        FROM 
            latest_station_info
        WHERE
            rn = 1
        """
        execute_sql(engine, populate_sql)
        
        # Create spatial index
        index_sql = """
        CREATE INDEX IF NOT EXISTS station_geom_idx ON dim_station USING GIST(geometry);
        """
        execute_sql(engine, index_sql)
        
        count_df = pd.read_sql("SELECT COUNT(*) FROM dim_station", engine)
        print(f"Populated dim_station with {count_df.iloc[0, 0]} rows")
    else:
        print("Table dim_station already exists and contains data")

def create_fact_station_information(engine):
    """Create and populate station information fact table"""
    print("\nCreating station information fact table...")
    
    if not table_exists(engine, "fact_station_information"):
        create_sql = """
        CREATE TABLE fact_station_information (
            station_id INTEGER,
            hour_datetime TIMESTAMP,
            capacity INTEGER,
            last_updated TIMESTAMP,
            PRIMARY KEY (station_id, hour_datetime),
            FOREIGN KEY (station_id) REFERENCES dim_station(station_id),
            FOREIGN KEY (hour_datetime) REFERENCES dim_hour(hour_datetime)
        )
        """
        execute_sql(engine, create_sql)
    
    if table_is_empty(engine, "fact_station_information"):
        populate_sql = """
        WITH latest_info AS (
            SELECT 
                i.station_id,
                h.hour_datetime,
                i.capacity,
                i.last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY i.station_id, h.hour_datetime 
                    ORDER BY i.last_updated DESC
                ) as rn
            FROM 
                bicycle_station_information_clean i
            JOIN 
                dim_hour h ON DATE_TRUNC('hour', i.last_updated) = h.hour_datetime
            JOIN 
                dim_station s ON i.station_id = s.station_id
        )
        INSERT INTO fact_station_information (
            station_id,
            hour_datetime,
            capacity,
            last_updated
        )
        SELECT 
            station_id,
            hour_datetime,
            capacity,
            last_updated
        FROM 
            latest_info
        WHERE
            rn = 1
        """
        execute_sql(engine, populate_sql)
        
        count_df = pd.read_sql("SELECT COUNT(*) FROM fact_station_information", engine)
        print(f"Populated fact_station_information with {count_df.iloc[0, 0]} rows")
    else:
        print("Table fact_station_information already exists and contains data")

def create_fact_station_status(engine):
    """Create and populate station status fact table"""
    print("\nCreating station status fact table...")
    
    if not table_exists(engine, "fact_station_status"):
        create_sql = """
        CREATE TABLE fact_station_status (
            station_id INTEGER,
            ten_min_datetime TIMESTAMP,
            num_bikes_available INTEGER,
            mechanical_bikes INTEGER,
            ebikes INTEGER,
            num_docks_available INTEGER,
            status TEXT,
            last_reported TIMESTAMP,
            last_updated TIMESTAMP,
            PRIMARY KEY (station_id, ten_min_datetime),
            FOREIGN KEY (station_id) REFERENCES dim_station(station_id),
            FOREIGN KEY (ten_min_datetime) REFERENCES dim_ten_minute(ten_min_datetime)
        )
        """
        execute_sql(engine, create_sql)
    
    if table_is_empty(engine, "fact_station_status"):
        # Create more efficient indexes to speed up joins
        print("Creating optimized indexes to speed up joins...")
        execute_sql(engine, "CREATE INDEX IF NOT EXISTS temp_station_status_idx ON bicycle_station_status_clean(station_id)")
        execute_sql(engine, "CREATE INDEX IF NOT EXISTS temp_status_date_idx ON bicycle_station_status_clean(last_updated)")
        execute_sql(engine, "CREATE INDEX IF NOT EXISTS temp_ten_min_idx ON dim_ten_minute(ten_min_datetime)")
        execute_sql(engine, "ANALYZE bicycle_station_status_clean")
        execute_sql(engine, "ANALYZE dim_ten_minute")
        execute_sql(engine, "ANALYZE dim_station")
        
        # Get list of unique months to process in larger batches
        print("Getting distinct months to process in batches...")
        months_query = """
        SELECT DISTINCT TO_CHAR(last_updated, 'YYYY-MM') as month_year 
        FROM bicycle_station_status_clean 
        ORDER BY month_year
        """
        months_df = pd.read_sql(months_query, engine)
        total_months = len(months_df)
        print(f"Found {total_months} distinct months to process")
        
        total_processed = 0
        
        # Using a more efficient loading approach with month-based batches
        for row in tqdm(months_df.iterrows(), total=total_months, desc="Processing by month"):
            _, month_row = row
            month_year = month_row['month_year']
            
            # Use a more direct approach for loading - prepare staging table first
            # This eliminates the need for repeated row_number calculations
            staging_sql = f"""
            CREATE TEMP TABLE tmp_status_batch AS
            WITH status_with_ten_min AS (
                SELECT 
                    s.station_id,
                    DATE_TRUNC('hour', s.last_updated) 
                    + INTERVAL '10 minutes' * (EXTRACT(MINUTE FROM s.last_updated)::INT / 10) AS ten_min_datetime,
                    s.num_bikes_available,
                    s.mechanical_bikes,
                    s.ebikes,
                    s.num_docks_available,
                    s.status,
                    s.last_reported,
                    s.last_updated
                FROM 
                    bicycle_station_status_clean s
                WHERE
                    TO_CHAR(s.last_updated, 'YYYY-MM') = '{month_year}'
            )
            SELECT DISTINCT ON (station_id, ten_min_datetime)
                station_id,
                ten_min_datetime,
                num_bikes_available,
                mechanical_bikes,
                ebikes,
                num_docks_available,
                status,
                last_reported,
                last_updated
            FROM 
                status_with_ten_min
            ORDER BY 
                station_id, ten_min_datetime, last_updated DESC;
            
            CREATE INDEX ON tmp_status_batch(station_id, ten_min_datetime);
            """
            execute_sql(engine, staging_sql)
            
            # Then insert only from valid stations and ten_min buckets
            insert_sql = """
            INSERT INTO fact_station_status (
                station_id,
                ten_min_datetime,
                num_bikes_available,
                mechanical_bikes,
                ebikes,
                num_docks_available,
                status,
                last_reported,
                last_updated
            )
            SELECT 
                t.station_id,
                t.ten_min_datetime,
                t.num_bikes_available,
                t.mechanical_bikes,
                t.ebikes,
                t.num_docks_available,
                t.status,
                t.last_reported,
                t.last_updated
            FROM 
                tmp_status_batch t
            JOIN 
                dim_station ds ON t.station_id = ds.station_id
            JOIN 
                dim_ten_minute dm ON t.ten_min_datetime = dm.ten_min_datetime;
            """
            execute_sql(engine, insert_sql)
            
            # Get count of processed rows for this batch
            batch_count_query = f"""
            SELECT COUNT(*) FROM fact_station_status 
            WHERE TO_CHAR(last_updated, 'YYYY-MM') = '{month_year}'
            """
            batch_count_df = pd.read_sql(batch_count_query, engine)
            batch_processed = batch_count_df.iloc[0, 0]
            total_processed += batch_processed
            
            # Drop the temp table
            execute_sql(engine, "DROP TABLE tmp_status_batch")
        
        # Drop temporary indexes
        print("Dropping temporary indexes...")
        execute_sql(engine, "DROP INDEX IF EXISTS temp_station_status_idx")
        execute_sql(engine, "DROP INDEX IF EXISTS temp_status_date_idx")
        execute_sql(engine, "DROP INDEX IF EXISTS temp_ten_min_idx")
        
        print(f"Completed populating fact_station_status with {total_processed} total rows")
    else:
        print("Table fact_station_status already exists and contains data")

def validate_schema(engine):
    """Validate the bicycle station schema with sample queries"""
    print("\nValidating bicycle station schema with sample queries...")
    
    dim_counts = pd.read_sql("""
        SELECT 
            (SELECT COUNT(*) FROM dim_station) AS station_count,
            (SELECT COUNT(*) FROM dim_hour) AS hour_count,
            (SELECT COUNT(*) FROM dim_ten_minute) AS ten_minute_count,
            (SELECT COUNT(*) FROM fact_station_information) AS station_info_count,
            (SELECT COUNT(*) FROM fact_station_status) AS station_status_count
        """, engine)
    
    print(f"Table counts: {dim_counts.to_dict('records')[0]}")
    
    sample_hourly_query = """
    SELECT 
        h.hour_datetime,
        h.day_part,
        COUNT(DISTINCT f.station_id) AS stations_reporting,
        AVG(f.capacity) AS avg_capacity
    FROM 
        fact_station_information f
    JOIN
        dim_hour h ON f.hour_datetime = h.hour_datetime
    GROUP BY
        h.hour_datetime, h.day_part
    ORDER BY
        h.hour_datetime
    LIMIT 5
    """
    
    sample_status_query = """
    SELECT 
        s.station_id,
        ds.name,
        t.ten_min_datetime,
        s.num_bikes_available,
        s.mechanical_bikes,
        s.ebikes,
        s.num_docks_available,
        s.status
    FROM 
        fact_station_status s
    JOIN 
        dim_station ds ON s.station_id = ds.station_id
    JOIN 
        dim_ten_minute t ON s.ten_min_datetime = t.ten_min_datetime
    ORDER BY
        t.ten_min_datetime, s.station_id
    LIMIT 5
    """
    
    try:
        print("\nSample hourly station information:")
        sample_hourly_df = pd.read_sql(sample_hourly_query, engine)
        print(sample_hourly_df)
        
        print("\nSample station status data:")
        sample_status_df = pd.read_sql(sample_status_query, engine)
        print(sample_status_df)
    except Exception as e:
        print(f"Error running sample queries: {e}")

def main(force: bool = False):
    conn_string = get_connection_string()
    engine = create_engine(conn_string)
    
    # Verify that the base star schema exists
    if not all(table_exists(engine, table) for table in ["dim_year", "dim_month", "dim_day"]):
        print("ERROR: Base time dimension tables not found. Please run demographics.py first.")
        return
    
    if force:
        print("Force flag enabled: dropping existing tables and recreating them")
        drop_tables_if_exist(engine)
    else:
        print("Force flag disabled: only creating and loading tables if they don't exist or are empty")
    
    # Ensure time hierarchy is complete before creating new dimensions
    ensure_time_hierarchy_exists(engine)
    
    # Create and populate dimension tables
    create_dim_hour(engine)
    create_dim_ten_minute(engine)
    create_dim_station(engine)
    
    # Create and populate fact tables
    create_fact_station_information(engine)
    create_fact_station_status(engine)
    
    # Validate the schema
    validate_schema(engine)
    
    print("\nBicycle station schema integration completed!")

app = typer.Typer()

@app.command()
def run(force: bool = typer.Option(False, "--force", "-f", help="Force recreate and reload tables")):
    """
    Create and load bicycle station schema.
    
    If force is True, all tables will be dropped, recreated, and reloaded.
    If force is False (default), tables will only be created if they don't exist,
    and data will only be loaded if tables are empty.
    """
    main(force=force)

if __name__ == "__main__":
    app() 