-- This SQL script creates the tables needed for the project.

-- population table
CREATE TABLE population (
    id INTEGER PRIMARY KEY,
    administrative_unit_id INTEGER,
    reference_date TEXT,
    district_code TEXT,
    district_name TEXT,
    neighbourhood_code TEXT,
    neighbourhood_name TEXT,
    census_tract_code TEXT,
    number_of_inhabitants INTEGER
);

-- administrative_units table
CREATE TABLE administrative_units (
    id INTEGER PRIMARY KEY,
    population_id INTEGER,
    unit_type TEXT,
    district_code TEXT,
    neighbourhood_code TEXT,
    census_tract_code TEXT,
    unit_name TEXT,
    perimeter DOUBLE PRECISION,
    area DOUBLE PRECISION,
    scale_range TEXT,
    geometry geometry(Polygon,4326)
);
