import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import ProgrammingError

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

def drop_tables_if_exist(engine):
    """Drop bicycle lanes tables if they exist"""
    tables = [
        "fact_bike_tract_metrics", 
        "fact_bike_network_metrics", 
        "fact_bicycle_lane_state", 
        "fact_bike_lane_tract", 
        "dim_trimester"
    ]
    
    for table in tables:
        if table_exists(engine, table):
            execute_sql(engine, f"DROP TABLE {table} CASCADE")

def create_dim_trimester(engine):
    """Create and populate trimester dimension table"""
    print("\nCreating trimester dimension table...")
    
    create_sql = """
    CREATE TABLE dim_trimester (
        year_trimester TEXT PRIMARY KEY,
        year INT,
        trimester TEXT,
        FOREIGN KEY (year) REFERENCES dim_year(year)
    )
    """
    execute_sql(engine, create_sql)
    
    populate_sql = """
    INSERT INTO dim_trimester (year_trimester, year, trimester)
    SELECT DISTINCT 
        CONCAT(year, '-', trimester) as year_trimester,
        year,
        trimester
    FROM 
        bicycle_lanes_clean
    ORDER BY 
        year, trimester
    """
    execute_sql(engine, populate_sql)
    
    count_df = pd.read_sql("SELECT COUNT(*) FROM dim_trimester", engine)
    print(f"Populated dim_trimester with {count_df.iloc[0, 0]} rows")

def create_fact_bicycle_lane_state(engine):
    """Create and populate fact table for bicycle lane states over time"""
    print("\nCreating bicycle lane state fact table...")
    
    create_sql = """
    CREATE TABLE fact_bicycle_lane_state (
        lane_id TEXT,
        year_trimester TEXT,
        lane_type TEXT,
        description TEXT,
        location TEXT,
        length_meters FLOAT,
        geometry GEOMETRY,
        PRIMARY KEY (lane_id, year_trimester),
        FOREIGN KEY (year_trimester) REFERENCES dim_trimester(year_trimester)
    )
    """
    execute_sql(engine, create_sql)
    
    populate_sql = """
    INSERT INTO fact_bicycle_lane_state (
        lane_id,
        year_trimester,
        lane_type,
        description,
        location,
        length_meters,
        geometry
    )
    SELECT 
        lane_id,
        CONCAT(year, '-', trimester) as year_trimester,
        lane_type,
        description,
        location,
        ST_Length(geometry::geography) as length_meters,
        geometry
    FROM 
        bicycle_lanes_clean
    WHERE 
        lane_id IS NOT NULL
    """
    execute_sql(engine, populate_sql)
    
    create_index_sql = """
    CREATE INDEX IF NOT EXISTS bike_lane_state_geom_idx 
    ON fact_bicycle_lane_state USING GIST (geometry);
    """
    execute_sql(engine, create_index_sql)
    
    count_df = pd.read_sql("SELECT COUNT(*) FROM fact_bicycle_lane_state", engine)
    print(f"Populated fact_bicycle_lane_state with {count_df.iloc[0, 0]} rows")

def create_fact_bike_lane_tract(engine):
    """Create and populate fact table for relationships between bike lanes and census tracts"""
    print("\nCreating bike lane-tract intersection fact table...")
    
    create_sql = """
    CREATE TABLE fact_bike_lane_tract (
        lane_id TEXT,
        year_trimester TEXT,
        census_tract_id BIGINT,
        length_in_tract FLOAT,
        PRIMARY KEY (lane_id, year_trimester, census_tract_id),
        FOREIGN KEY (lane_id, year_trimester) REFERENCES fact_bicycle_lane_state(lane_id, year_trimester),
        FOREIGN KEY (census_tract_id) REFERENCES dim_location(census_tract_id),
        FOREIGN KEY (year_trimester) REFERENCES dim_trimester(year_trimester)
    )
    """
    execute_sql(engine, create_sql)
    
    create_index_sql = """
    CREATE INDEX IF NOT EXISTS census_tract_geom_idx 
    ON dim_location USING GIST (geometry);
    """
    execute_sql(engine, create_index_sql)
    
    populate_sql = """
    INSERT INTO fact_bike_lane_tract (
        lane_id,
        year_trimester,
        census_tract_id,
        length_in_tract
    )
    SELECT
        b.lane_id,
        b.year_trimester,
        l.census_tract_id,
        ST_Length(ST_Intersection(b.geometry, l.geometry)::geography) AS length_in_tract
    FROM
        fact_bicycle_lane_state b
    JOIN
        dim_location l ON ST_Intersects(b.geometry, l.geometry)
    WHERE
        ST_Length(ST_Intersection(b.geometry, l.geometry)::geography) > 1  -- Only meaningful intersections (> 1 meter)
    """
    execute_sql(engine, populate_sql)
    
    count_df = pd.read_sql("SELECT COUNT(*) FROM fact_bike_lane_tract", engine)
    print(f"Populated fact_bike_lane_tract with {count_df.iloc[0, 0]} rows")

def create_fact_bike_network_metrics(engine):
    """Create and populate bike network metrics fact table"""
    print("\nCreating bike network metrics fact table...")
    
    create_sql = """
    CREATE TABLE fact_bike_network_metrics (
        year_trimester TEXT PRIMARY KEY,
        total_lanes INT,
        total_length_meters FLOAT,
        connected_lanes INT,
        isolated_lanes INT,
        connectivity_ratio FLOAT,
        FOREIGN KEY (year_trimester) REFERENCES dim_trimester(year_trimester)
    )
    """
    execute_sql(engine, create_sql)
    
    populate_sql = """
    WITH lane_counts AS (
        SELECT
            year_trimester,
            COUNT(DISTINCT lane_id) AS total_lanes,
            SUM(length_meters) AS total_length_meters
        FROM
            fact_bicycle_lane_state
        GROUP BY
            year_trimester
    ),
    lane_connections AS (
        SELECT
            b1.year_trimester,
            b1.lane_id,
            CASE 
                WHEN COUNT(DISTINCT b2.lane_id) > 0 THEN 1
                ELSE 0
            END AS is_connected
        FROM
            fact_bicycle_lane_state b1
        LEFT JOIN
            fact_bicycle_lane_state b2 ON 
                b1.year_trimester = b2.year_trimester AND
                b1.lane_id <> b2.lane_id AND
                ST_Intersects(b1.geometry, b2.geometry)
        GROUP BY
            b1.year_trimester, b1.lane_id
    ),
    connectivity_metrics AS (
        SELECT
            year_trimester,
            SUM(is_connected) AS connected_lanes,
            COUNT(*) - SUM(is_connected) AS isolated_lanes,
            SUM(is_connected)::FLOAT / COUNT(*) AS connectivity_ratio
        FROM
            lane_connections
        GROUP BY
            year_trimester
    )
    INSERT INTO fact_bike_network_metrics (
        year_trimester,
        total_lanes,
        total_length_meters,
        connected_lanes,
        isolated_lanes,
        connectivity_ratio
    )
    SELECT
        lc.year_trimester,
        lc.total_lanes,
        lc.total_length_meters,
        cm.connected_lanes,
        cm.isolated_lanes,
        cm.connectivity_ratio
    FROM
        lane_counts lc
    JOIN
        connectivity_metrics cm ON lc.year_trimester = cm.year_trimester
    """
    execute_sql(engine, populate_sql)
    
    count_df = pd.read_sql("SELECT COUNT(*) FROM fact_bike_network_metrics", engine)
    print(f"Populated fact_bike_network_metrics with {count_df.iloc[0, 0]} rows")

def create_fact_bike_tract_metrics(engine):
    """Create and populate census tract level bike network metrics fact table"""
    print("\nCreating census tract bike metrics fact table...")
    
    create_sql = """
    CREATE TABLE fact_bike_tract_metrics (
        census_tract_id BIGINT,
        year_trimester TEXT,
        total_lanes INT,
        total_lane_length FLOAT,
        coverage_score FLOAT,        -- total bike lane length / area
        connectivity_score FLOAT,    -- ratio of connected lanes
        network_quality_score FLOAT, -- combined score
        PRIMARY KEY (census_tract_id, year_trimester),
        FOREIGN KEY (census_tract_id) REFERENCES dim_location(census_tract_id),
        FOREIGN KEY (year_trimester) REFERENCES dim_trimester(year_trimester)
    )
    """
    execute_sql(engine, create_sql)
    
    populate_sql = """
    WITH tract_lane_data AS (
        SELECT
            bt.census_tract_id,
            bt.year_trimester,
            COUNT(DISTINCT bt.lane_id) AS total_lanes,
            SUM(bt.length_in_tract) AS total_lane_length,
            MAX(l.census_tract_area) AS area
        FROM
            fact_bike_lane_tract bt
        JOIN
            dim_location l ON bt.census_tract_id = l.census_tract_id
        GROUP BY
            bt.census_tract_id, bt.year_trimester
    ),
    max_coverage AS (
        SELECT 
            year_trimester,
            MAX(total_lane_length / area) AS max_coverage_ratio
        FROM 
            tract_lane_data
        GROUP BY
            year_trimester
    ),
    lane_connectivity AS (
        SELECT
            bt.census_tract_id,
            bt.year_trimester,
            bt.lane_id,
            CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM fact_bike_lane_tract bt2
                    JOIN fact_bicycle_lane_state bs1 ON bt.lane_id = bs1.lane_id AND bt.year_trimester = bs1.year_trimester
                    JOIN fact_bicycle_lane_state bs2 ON bt2.lane_id = bs2.lane_id AND bt2.year_trimester = bs2.year_trimester
                    WHERE 
                        bt.census_tract_id = bt2.census_tract_id AND
                        bt.year_trimester = bt2.year_trimester AND
                        bt.lane_id <> bt2.lane_id AND
                        ST_Intersects(bs1.geometry, bs2.geometry)
                ) THEN 1
                ELSE 0
            END AS is_connected
        FROM
            fact_bike_lane_tract bt
        GROUP BY
            bt.census_tract_id, bt.year_trimester, bt.lane_id
    ),
    tract_connectivity AS (
        SELECT
            census_tract_id,
            year_trimester,
            SUM(is_connected)::FLOAT / COUNT(*) AS connectivity_ratio
        FROM
            lane_connectivity
        GROUP BY
            census_tract_id, year_trimester
    )
    INSERT INTO fact_bike_tract_metrics (
        census_tract_id,
        year_trimester,
        total_lanes,
        total_lane_length,
        coverage_score,
        connectivity_score,
        network_quality_score
    )
    SELECT
        t.census_tract_id,
        t.year_trimester,
        t.total_lanes,
        t.total_lane_length,
        CASE 
            WHEN m.max_coverage_ratio = 0 THEN 0
            ELSE (t.total_lane_length / t.area) / m.max_coverage_ratio
        END AS coverage_score,
        COALESCE(c.connectivity_ratio, 0) AS connectivity_score,
        CASE 
            WHEN m.max_coverage_ratio = 0 THEN 0
            ELSE (
                CASE 
                    WHEN m.max_coverage_ratio = 0 THEN 0 
                    ELSE (t.total_lane_length / t.area) / m.max_coverage_ratio
                END +
                COALESCE(c.connectivity_ratio, 0)
            ) / 2
        END AS network_quality_score
    FROM
        tract_lane_data t
    JOIN
        max_coverage m ON t.year_trimester = m.year_trimester
    LEFT JOIN
        tract_connectivity c ON t.census_tract_id = c.census_tract_id AND t.year_trimester = c.year_trimester
    """
    execute_sql(engine, populate_sql)
    
    count_df = pd.read_sql("SELECT COUNT(*) FROM fact_bike_tract_metrics", engine)
    print(f"Populated fact_bike_tract_metrics with {count_df.iloc[0, 0]} rows")

def validate_schema(engine):
    """Validate the bicycle lanes schema with sample queries"""
    print("\nValidating bicycle lanes schema with sample queries...")
    
    dim_counts = pd.read_sql("""
        SELECT 
            (SELECT COUNT(*) FROM dim_trimester) AS trimester_count,
            (SELECT COUNT(*) FROM fact_bicycle_lane_state) AS lane_state_count,
            (SELECT COUNT(*) FROM fact_bike_lane_tract) AS lane_tract_count,
            (SELECT COUNT(*) FROM fact_bike_network_metrics) AS network_metrics_count,
            (SELECT COUNT(*) FROM fact_bike_tract_metrics) AS tract_metrics_count
        """, engine)
    
    print(f"Table counts: {dim_counts.to_dict('records')[0]}")
    
    sample_network_query = """
    SELECT 
        n.year_trimester,
        t.year,
        t.trimester,
        n.total_lanes,
        n.total_length_meters,
        n.connectivity_ratio
    FROM 
        fact_bike_network_metrics n
    JOIN
        dim_trimester t ON n.year_trimester = t.year_trimester
    ORDER BY
        t.year, t.trimester
    LIMIT 5
    """
    
    sample_tract_query = """
    SELECT 
        m.census_tract_id,
        l.neighbourhood_name,
        l.district_name,
        m.year_trimester,
        m.total_lanes,
        m.total_lane_length,
        m.coverage_score,
        m.connectivity_score,
        m.network_quality_score
    FROM 
        fact_bike_tract_metrics m
    JOIN 
        dim_location l ON m.census_tract_id = l.census_tract_id
    ORDER BY
        m.network_quality_score DESC
    LIMIT 5
    """
    
    try:
        print("\nSample bike network metrics over time:")
        sample_network_df = pd.read_sql(sample_network_query, engine)
        print(sample_network_df)
        
        print("\nSample tract-level bike metrics:")
        sample_tract_df = pd.read_sql(sample_tract_query, engine)
        print(sample_tract_df)
    except Exception as e:
        print(f"Error running sample queries: {e}")

def main():
    conn_string = get_connection_string()
    engine = create_engine(conn_string)
    
    # Verify that the base star schema exists
    if not all(table_exists(engine, table) for table in ["dim_location", "dim_year"]):
        print("ERROR: Base star schema not found. Please run star_schema_builder.py first.")
        return
    
    drop_tables_if_exist(engine)
    
    # Create and populate time dimension
    create_dim_trimester(engine)
    
    # Create and populate fact tables for bicycle lanes
    create_fact_bicycle_lane_state(engine)
    create_fact_bike_lane_tract(engine)
    
    # Create and populate metric fact tables
    create_fact_bike_network_metrics(engine)
    create_fact_bike_tract_metrics(engine)
    
    # Validate the schema
    validate_schema(engine)
    
    print("\nBicycle lanes temporal fact schema completed!")

if __name__ == "__main__":
    main() 