import pandas as pd
from sqlalchemy import create_engine, text

# Use the same database parameters from bicycle_stations.py
DB_PARAMS = {
    "host": "dtim.essi.upc.edu",
    "port": 5432,
    "dbname": "dbakosschneider",
    "user": "akosschneider",
    "password": "DMT2025!"
}

def get_connection_string():
    return f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"

def count_rows_and_size_by_table():
    # Create engine and connect to database
    conn_string = get_connection_string()
    engine = create_engine(conn_string)
    
    # Query to get all tables with their row counts and sizes
    query = """
    SELECT 
        s.schemaname, 
        s.relname AS table_name, 
        s.n_live_tup AS row_count,
        pg_total_relation_size(c.oid) AS total_bytes,
        pg_size_pretty(pg_total_relation_size(c.oid)) AS pretty_size
    FROM 
        pg_stat_user_tables s
    JOIN
        pg_class c ON s.relname = c.relname
    JOIN
        pg_namespace n ON c.relnamespace = n.oid AND s.schemaname = n.nspname
    ORDER BY 
        pg_total_relation_size(c.oid) DESC
    """
    
    # Execute query and fetch results
    with engine.connect() as conn:
        result = pd.read_sql(query, conn)
    
    # Calculate sizes in GB
    result['size_gb'] = result['total_bytes'] / (1024**3)
    
    # Print results in a formatted way
    print(f"{'Schema':<15} {'Table':<40} {'Row Count':<12} {'Size':<12} {'Size (GB)':<10}")
    print("-" * 92)
    
    for _, row in result.iterrows():
        print(f"{row['schemaname']:<15} {row['table_name']:<40} {row['row_count']:<12} {row['pretty_size']:<12} {row['size_gb']:.4f}")
    
    # Print total row count and size
    total_rows = result['row_count'].sum()
    total_size_gb = result['size_gb'].sum()
    print("-" * 92)
    print(f"{'Total':<56} {total_rows:<12} {'':12} {total_size_gb:.4f}")
    
    # Check available disk space
    disk_space_query = """
    SELECT
        pg_size_pretty(pg_database_size(current_database())) as database_size,
        pg_size_pretty(pg_tablespace_size('pg_default')) as tablespace_size
    """
    
    with engine.connect() as conn:
        disk_info = pd.read_sql(disk_space_query, conn)
    
    print("\nDatabase Info:")
    print(f"Database Size: {disk_info['database_size'][0]}")
    print(f"Default Tablespace Size: {disk_info['tablespace_size'][0]}")
    
    # Check disk usage info from server if possible
    try:
        with engine.connect() as conn:
            disk_usage = conn.execute(text("""
            SELECT
                current_setting('data_directory') as data_dir,
                pg_size_pretty(CAST(current_setting('max_files_per_process') AS BIGINT)) as max_files
            """))
            disk_usage_info = disk_usage.fetchone()
            
            print(f"\nPostgres Data Directory: {disk_usage_info[0]}")
            print(f"Max Files Per Process: {disk_usage_info[1]}")
    except:
        print("\nUnable to retrieve additional disk information from server")
        
    # Print info about the error
    print("\nDiagnostic Notes:")
    print("- A previous error 'No space left on device' occurred while populating fact_station_status")
    print("- This error indicates the database server's disk is full")
    print("- The bicycle_station_status_clean table is the largest at over 65 million rows")
    print("- Consider cleaning up unused data or requesting more disk space")

if __name__ == "__main__":
    count_rows_and_size_by_table()