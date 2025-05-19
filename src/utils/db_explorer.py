import pandas as pd
from sqlalchemy import create_engine, inspect


DB_PARAMS = {
    "host": "dtim.essi.upc.edu",
    "port": 5432,
    "dbname": "dbakosschneider",
    "user": "akosschneider",
    "password": "DMT2025!"
}


def get_connection_string():
    return f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"


def explore_table(engine, table_name):
    """Load and print schema and first 5 rows of a table"""
    print(f"\n{'='*50}")
    print(f"Table: {table_name}")
    print(f"{'='*50}")
    
    # Get schema information
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    
    print("\nSchema:")
    for col in columns:
        print(f"  {col['name']} - {col['type']}")
    
    # Get primary key information
    pk_constraint = inspector.get_pk_constraint(table_name)
    if pk_constraint and pk_constraint['constrained_columns']:
        print("\nPrimary Key:")
        print(f"  Columns: {', '.join(pk_constraint['constrained_columns'])}")
        if 'name' in pk_constraint and pk_constraint['name']:
            print(f"  Constraint name: {pk_constraint['name']}")
    
    # Get foreign key information
    fk_constraints = inspector.get_foreign_keys(table_name)
    if fk_constraints:
        print("\nForeign Keys:")
        for fk in fk_constraints:
            print(f"  {', '.join(fk['constrained_columns'])} -> {fk['referred_table']}.{', '.join(fk['referred_columns'])}")
            if 'name' in fk and fk['name']:
                print(f"  Constraint name: {fk['name']}")
    
    # Load first 5 rows
    query = f"SELECT * FROM {table_name} LIMIT 5"
    try:
        df = pd.read_sql(query, engine)
        print("\nFirst 5 rows:")
        print(df)
    except Exception as e:
        print(f"Error loading data: {e}")

def main():
    # Connect to database
    conn_string = get_connection_string()
    engine = create_engine(conn_string)
    
    # Tables to explore
    tables = [
        # "income_clean",
        # "population_clean", 
        # "districts_clean",
        # "census_tracts_clean",
        # "neighbourhoods_clean"
        # "bicycle_lanes_clean",
        # "bicycle_station_information_clean",
        # "bicycle_station_status_clean"
        "dim_hour",
        "dim_day",
        "dim_year",
        "dim_month",
        "dim_location",
        "fact_station_information",
        "fact_station_status",
    ]
    
    # Explore each table
    for table in tables:
        explore_table(engine, table)

if __name__ == "__main__":
    main() 