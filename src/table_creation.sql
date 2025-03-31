-- This SQL script creates the tables needed for the project.

-- populations table
CREATE TABLE populations (
    id INTEGER PRIMARY KEY,
    administrative_unit_id INTEGER,
    reference_date VARCHAR,
    district_code VARCHAR,
    district_name VARCHAR,
    neighbourhood_code VARCHAR,
    neighbourhood_name VARCHAR,
    census_tract_code VARCHAR,
    number_of_inhabitants INTEGER,
    FOREIGN KEY (administrative_unit_id) REFERENCES administrative_units(id)
);

-- administrative_units table
CREATE TABLE administrative_units (
    id INTEGER PRIMARY KEY,
    unit_type VARCHAR,
    district_code VARCHAR,
    neighbourhood_code VARCHAR,
    census_tract_code VARCHAR,
    unit_name VARCHAR,
    perimeter DOUBLE PRECISION,
    area DOUBLE PRECISION,
    scale_range VARCHAR,
    geometry geometry(Polygon,4326),
);

