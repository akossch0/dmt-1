# Data Preprocessing

This directory contains all the preprocessing stages for our data management and transformation pipeline. Each subfolder represents a distinct dataset that requires specific cleaning, transformation, and normalization steps.

## Structure and Methodology

### Administrative Units (`administrative_units/`)

This subfolder handles spatial administrative boundaries for Barcelona, including:

- Districts
- Neighborhoods
- Census tracts

**Methodology:**
- Downloading GIS data from official sources ([`download.py`](administrative_units/download.py))
- Decompressing and extracting spatial files ([`decompress.py`](administrative_units/decompress.py))
- Loading raw spatial data into PostgreSQL with PostGIS extension ([`load_raw.py`](administrative_units/load_raw.py))
- Standardizing geometries by ensuring consistent coordinate systems (EPSG:4326) ([`load_clean.py`](administrative_units/load_clean.py))
- Creating composite census-tract codes by combining district and census tract identifiers

### Income Data (`income/`)

This subfolder processes socioeconomic indicators at different administrative levels:

**Methodology:**
- Downloading income data from Barcelona's open data portal ([`00_download.py`](income/00_download.py))
- Loading raw data into staging tables ([`01_load_db_raw.py`](income/01_load_db_raw.py))
- Transforming and standardizing income metrics ([`02_load_db_clean.py`](income/02_load_db_clean.py))
- Associating income data with spatial units

### Population Data (`population/`)

This subfolder handles demographic information for Barcelona:

**Methodology:**
- Downloading population statistics ([`00_download.py`](population/00_download.py))
- Loading raw demographic data ([`01_load_raw.py`](population/01_load_raw.py)) 
- Standardizing population counts by administrative unit ([`02_load_clean.py`](population/02_load_clean.py))

### Bicing Data (`bicing/`)

This subfolder processes Barcelona's bicycle-sharing system data, including:
- Station information (locations, capacity)
- Station status (availability, usage patterns)

**Methodology:**
- Downloading current and historical Bicing data ([`00_download.py`](bicing/00_download.py))
- Decompressing data files ([`01_decompress.py`](bicing/01_decompress.py))
- Projecting coordinates to proper spatial reference system ([`02_project.py`](bicing/02_project.py))
- Sampling high-frequency data to reduce data density ([`03_sample.py`](bicing/03_sample.py))
- Loading raw data into staging tables ([`04_load_raw.py`](bicing/04_load_raw.py))
- Extensive cleaning and transformation process ([`05_clean.py`](bicing/05_clean.py))

## Data Cleaning Approach

### Missing Values Analysis

The code performs analysis of missing values through SQL queries to determine data quality and completeness.

### Sampling Frequency Reduction

For high-frequency Bicing data:
- Status data: reduced from per-minute to 10-minute intervals
- Information data: reduced to hourly intervals
- Timestamp-based sampling to maintain consistent temporal resolution

## Data Normalization and Standardization

### Spatial Information Homogenization

- All geographic data converted to consistent coordinate system (EPSG:4326)
- Composite identifiers created for spatial administrative units

### Data Type Standardization

- Station information: standardized data types for IDs, coordinates, capacity, and timestamps
- Numeric values cleaned and validated to ensure consistent formatting
- Temporal data normalized to consistent timestamp format
