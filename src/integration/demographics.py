import pandas as pd
import typer
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

def execute_sql(engine, sql):
    """Execute SQL statement and print result message"""
    try:
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        print(f"Successfully executed: {sql.split()[0]}")
        return True
    except Exception as e:
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
    """Drop star schema tables if they exist"""
    tables = ["fact_population_income", "dim_location", "dim_year", "dim_month", "dim_day"]
    
    for table in tables:
        if table_exists(engine, table):
            execute_sql(engine, f"DROP TABLE {table} CASCADE")

def create_dim_location(engine):
    """Create and populate the location dimension table with census tract ID as primary key"""
    print("\nCreating location dimension table...")
    
    if not table_exists(engine, "dim_location"):
        create_sql = """
        CREATE TABLE dim_location (
            census_tract_id BIGINT PRIMARY KEY,
            census_tract_area DOUBLE PRECISION,
            neighbourhood_code TEXT,
            neighbourhood_name TEXT,
            district_code TEXT,
            district_name TEXT,
            geometry geometry
        )
        """
        execute_sql(engine, create_sql)
    
    if table_is_empty(engine, "dim_location"):
        populate_sql = """
        INSERT INTO dim_location (
            census_tract_id, census_tract_area, 
            neighbourhood_code, neighbourhood_name,
            district_code, district_name, 
            geometry
        )
        SELECT 
            ct.sec_cens,
            ct.area,
            ct.barri,
            n.nom,
            ct.districte,
            d.nom,
            ct.geometry
        FROM 
            census_tracts_clean ct
        JOIN 
            neighbourhoods_clean n ON ct.districte = n.districte AND ct.barri = n.barri
        JOIN 
            districts_clean d ON ct.districte = d.districte
        """
        execute_sql(engine, populate_sql)
        
        count_df = pd.read_sql("SELECT COUNT(*) FROM dim_location", engine)
        print(f"Populated dim_location with {count_df.iloc[0, 0]} rows")
    else:
        print("Table dim_location already exists and contains data")

def create_date_dimensions(engine):
    """Create and populate the date dimension tables with hierarchy"""
    print("\nCreating date dimension tables...")
    
    # Create year dimension
    if not table_exists(engine, "dim_year"):
        year_sql = """
        CREATE TABLE dim_year (
            year INT PRIMARY KEY
        )
        """
        execute_sql(engine, year_sql)
    
    # Create month dimension
    if not table_exists(engine, "dim_month"):
        month_sql = """
        CREATE TABLE dim_month (
            year_month TEXT PRIMARY KEY,
            year INT,
            month INT,
            FOREIGN KEY (year) REFERENCES dim_year(year)
        )
        """
        execute_sql(engine, month_sql)
    
    # Create day dimension
    if not table_exists(engine, "dim_day"):
        day_sql = """
        CREATE TABLE dim_day (
            date_value DATE PRIMARY KEY,
            year_month TEXT,
            day INT,
            FOREIGN KEY (year_month) REFERENCES dim_month(year_month)
        )
        """
        execute_sql(engine, day_sql)
    
    # Populate year dimension if empty
    if table_is_empty(engine, "dim_year"):
        populate_year_sql = """
        INSERT INTO dim_year (year)
        SELECT DISTINCT 
            EXTRACT(YEAR FROM TO_DATE(data_referencia, 'YYYY-MM-DD'))::INT as year
        FROM 
            population_clean
        ORDER BY 
            year
        """
        execute_sql(engine, populate_year_sql)
        
        year_count_df = pd.read_sql("SELECT COUNT(*) FROM dim_year", engine)
        print(f"Populated dim_year with {year_count_df.iloc[0, 0]} rows")
    else:
        print("Table dim_year already exists and contains data")
    
    # Populate month dimension if empty
    if table_is_empty(engine, "dim_month"):
        populate_month_sql = """
        INSERT INTO dim_month (year_month, year, month)
        SELECT DISTINCT 
            TO_CHAR(TO_DATE(data_referencia, 'YYYY-MM-DD'), 'YYYY-MM') as year_month,
            EXTRACT(YEAR FROM TO_DATE(data_referencia, 'YYYY-MM-DD'))::INT as year,
            EXTRACT(MONTH FROM TO_DATE(data_referencia, 'YYYY-MM-DD'))::INT as month
        FROM 
            population_clean
        ORDER BY 
            year_month
        """
        execute_sql(engine, populate_month_sql)
        
        month_count_df = pd.read_sql("SELECT COUNT(*) FROM dim_month", engine)
        print(f"Populated dim_month with {month_count_df.iloc[0, 0]} rows")
    else:
        print("Table dim_month already exists and contains data")
    
    # Populate day dimension if empty
    if table_is_empty(engine, "dim_day"):
        populate_day_sql = """
        INSERT INTO dim_day (date_value, year_month, day)
        SELECT DISTINCT 
            TO_DATE(data_referencia, 'YYYY-MM-DD') as date_value,
            TO_CHAR(TO_DATE(data_referencia, 'YYYY-MM-DD'), 'YYYY-MM') as year_month,
            EXTRACT(DAY FROM TO_DATE(data_referencia, 'YYYY-MM-DD'))::INT as day
        FROM 
            population_clean
        ORDER BY 
            date_value
        """
        execute_sql(engine, populate_day_sql)
        
        day_count_df = pd.read_sql("SELECT COUNT(*) FROM dim_day", engine)
        print(f"Populated dim_day with {day_count_df.iloc[0, 0]} rows")
    else:
        print("Table dim_day already exists and contains data")

def create_fact_table(engine):
    """Create and populate the fact table with composite primary key"""
    print("\nCreating fact table for population and income...")
    
    if not table_exists(engine, "fact_population_income"):
        create_sql = """
        CREATE TABLE fact_population_income (
            census_tract_id BIGINT,
            year INT,
            population BIGINT,
            income_euros DOUBLE PRECISION,
            income_normalized NUMERIC,
            PRIMARY KEY (census_tract_id, year),
            FOREIGN KEY (census_tract_id) REFERENCES dim_location(census_tract_id),
            FOREIGN KEY (year) REFERENCES dim_year(year)
        )
        """
        execute_sql(engine, create_sql)
    
    if table_is_empty(engine, "fact_population_income"):
        populate_sql = """
        INSERT INTO fact_population_income (census_tract_id, year, population, income_euros, income_normalized)
        SELECT
            p.seccio_censal as census_tract_id,
            EXTRACT(YEAR FROM TO_DATE(p.data_referencia, 'YYYY-MM-DD'))::INT as year,
            MAX(p.valor) as population,
            MAX(i.import_euros) as income_euros,
            MAX(i.income_norm) as income_normalized
        FROM
            population_clean p
        JOIN
            dim_location l ON p.seccio_censal = l.census_tract_id
        JOIN
            dim_year y ON EXTRACT(YEAR FROM TO_DATE(p.data_referencia, 'YYYY-MM-DD'))::INT = y.year
        LEFT JOIN
            income_clean i ON p.seccio_censal = CAST(i.seccio_censal AS BIGINT)
        GROUP BY
            p.seccio_censal, 
            EXTRACT(YEAR FROM TO_DATE(p.data_referencia, 'YYYY-MM-DD'))::INT
        """
        execute_sql(engine, populate_sql)
        
        count_df = pd.read_sql("SELECT COUNT(*) FROM fact_population_income", engine)
        print(f"Populated fact_population_income with {count_df.iloc[0, 0]} records")
        
        income_count_df = pd.read_sql("SELECT COUNT(*) FROM fact_population_income WHERE income_euros IS NOT NULL", engine)
        print(f"Records with income data: {income_count_df.iloc[0, 0]}")
    else:
        print("Table fact_population_income already exists and contains data")

def validate_star_schema(engine):
    """Validate the star schema by running some test queries"""
    print("\nValidating star schema with sample queries...")
    
    dim_counts = pd.read_sql("""
        SELECT 
            (SELECT COUNT(*) FROM dim_location) AS location_count,
            (SELECT COUNT(*) FROM dim_year) AS year_count,
            (SELECT COUNT(*) FROM dim_month) AS month_count,
            (SELECT COUNT(*) FROM dim_day) AS day_count,
            (SELECT COUNT(*) FROM fact_population_income) AS fact_count
        """, engine)
    
    print(f"Dimension counts: {dim_counts.to_dict('records')[0]}")
    
    sample_query = """
    SELECT 
        f.census_tract_id,
        l.neighbourhood_name,
        l.district_name,
        f.year,
        f.population,
        f.income_euros,
        f.income_normalized
    FROM 
        fact_population_income f
    JOIN 
        dim_location l ON f.census_tract_id = l.census_tract_id
    JOIN 
        dim_year y ON f.year = y.year
    ORDER BY
        f.census_tract_id, f.year
    LIMIT 5
    """
    
    try:
        sample_df = pd.read_sql(sample_query, engine)
        print("\nSample data from star schema:")
        print(sample_df)
    except Exception as e:
        print(f"Error running sample query: {e}")

def main(force: bool = False):
    conn_string = get_connection_string()
    engine = create_engine(conn_string)
    
    if force:
        print("Force flag enabled: dropping existing tables and recreating them")
        drop_tables_if_exist(engine)
    else:
        print("Force flag disabled: only creating and loading tables if they don't exist or are empty")
    
    create_dim_location(engine)
    create_date_dimensions(engine)
    
    create_fact_table(engine)
    
    validate_star_schema(engine)
    
    print("\nStar schema creation completed!")

app = typer.Typer()

@app.command()
def run(force: bool = typer.Option(False, "--force", "-f", help="Force recreate and reload tables")):
    """
    Create and load demographic star schema.
    
    If force is True, all tables will be dropped, recreated, and reloaded.
    If force is False (default), tables will only be created if they don't exist,
    and data will only be loaded if tables are empty.
    """
    main(force=force)

if __name__ == "__main__":
    app() 