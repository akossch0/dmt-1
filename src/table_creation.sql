-- This SQL script creates the tables needed for the project.

-- populations table
CREATE TABLE populations (
    id INTEGER PRIMARY KEY,
    administrative_unit_id INTEGER NOT NULL,
    reference_date VARCHAR NOT NULL,
    district_code VARCHAR NOT NULL,
    district_name VARCHAR NOT NULL,
    neighbourhood_code VARCHAR NOT NULL,
    neighbourhood_name VARCHAR NOT NULL,
    census_tract_code VARCHAR NOT NULL,
    number_of_inhabitants INTEGER NOT NULL,
    FOREIGN KEY (administrative_unit_id) REFERENCES administrative_units(id)
);

-- administrative_units table
CREATE TABLE administrative_units (
    id INTEGER PRIMARY KEY,
    unit_type VARCHAR NOT NULL,
    district_code VARCHAR NOT NULL,
    neighbourhood_code VARCHAR NOT NULL,
    census_tract_code VARCHAR NOT NULL,
    unit_name VARCHAR NOT NULL,
    perimeter DOUBLE PRECISION,
    area DOUBLE PRECISION,
    scale_range VARCHAR,
    geometry geometry(Polygon,4326) NOT NULL
);


