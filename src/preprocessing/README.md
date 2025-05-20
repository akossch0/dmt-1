# Data Preprocessing

This directory contains all the preprocessing stages for our data management and transformation pipeline. Each subfolder represents a distinct dataset that requires specific cleaning, transformation, and normalization steps.

## Structure and Methodology

### Administrative Units (`administrative_units/`)

This subfolder handles spatial administrative boundaries for Barcelona, including:

- Districts
- Neighborhoods
- Census tracts

**Methodology:**
- Downloading GIS data from official sources
- Decompressing and extracting spatial files
- Loading raw spatial data into PostgreSQL with PostGIS extension
- Cleaning and standardizing geometries (correcting invalid geometries, ensuring proper projections)
- Creating a harmonized spatial reference system (EPSG:4326) for all geographic data

### Income Data (`income/`)

This subfolder processes socioeconomic indicators at different administrative levels:

**Methodology:**
- Downloading income data from Barcelona's open data portal
- Loading raw data into staging tables
- Cleaning and transforming income metrics:
  - Standardizing monetary units
  - Aggregating data at census tract level
  - Associating income data with spatial units

### Population Data (`population/`)

This subfolder handles demographic information for Barcelona:

**Methodology:**
- Downloading population statistics
- Loading raw demographic data
- Cleaning and standardizing population counts by:
  - Administrative unit (districts, neighborhoods, census tracts)
  - Time periods
  - Demographic segments

### Bicing Data (`bicing/`)

This subfolder processes Barcelona's bicycle-sharing system data, including:
- Station information (locations, capacity)
- Station status (availability, usage patterns)
- Bicycle lanes network

**Methodology:**
- Downloading current and historical Bicing data
- Decompressing and sampling data to reduce data density
- Projecting coordinates to proper spatial reference system
- Loading raw data into staging tables
- Extensive cleaning and transformation process

## Data Cleaning Approach

### Missing Values Analysis

For all critical datasets, we saw that there are no significant missing values.

### Sampling Frequency Reduction

For high-frequency trajectory data (particularly in Bicing):
- Original sampling rate reduced from per-minute to 15-minute intervals
- Adaptive sampling that preserves trajectory shape at critical points
- Rule-based filtering to eliminate redundant data points while maintaining data quality

## Data Normalization and Standardization

### Spatial Information Homogenization

- All geographic data converted to consistent coordinate system (EPSG:4326)
- Spatial joins between different administrative levels (districts, neighborhoods, census tracts)
- Spatial resolution standardized for interoperability between datasets

### Trajectory Data Calibration

- Bicing station coordinates aligned with official administrative boundaries
- Time series data normalized to consistent UTC timestamps
- Spatial outliers removed based on distance thresholds

### Alphanumerical Data Standardization

- Income data normalized to consistent monetary units (Euros) and reference periods
- Population counts standardized to enable temporal comparison
- Categorical variables encoded consistently across datasets
- Numeric features scaled appropriately for analysis
