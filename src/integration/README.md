# Data Integration

This directory contains scripts for integrating the preprocessed data from various sources into a coherent, unified data warehouse structure. The integration process resolves schema heterogeneities, establishes relationships between different datasets, and creates a dimensional model suitable for efficient analysis.

## Integration Strategy

### General Approach

Our integration approach follows these principles:

1. **Dimensional Modeling**: We implement a star schema design with dimension and fact tables to optimize for analytical queries
2. **Spatial Integration**: Leveraging spatial relationships to join datasets with geographic attributes
3. **Temporal Alignment**: Standardizing time granularity across different data sources
4. **Hierarchical Organization**: Creating hierarchical dimensions to support analysis at multiple levels of detail

## Data Sources & Integration Components

### Demographics Integration ([`demographics.py`](demographics.py))

This script integrates population and income data with administrative boundaries:

**Integration Features:**
- Creates a spatial dimension table (`dim_location`) that links census tracts to neighborhoods and districts
- Establishes a time dimension hierarchy (`dim_year`, `dim_month`, `dim_day`) to support temporal analysis
- Produces a fact table (`fact_population_income`) that combines socioeconomic indicators

**Implementation Details:**
- Uses SQL operations to join census tracts with neighborhood and district information
- Creates time dimensions with proper hierarchical relationships
- Validates schema integrity and referential constraints

### Bicycle Stations Integration ([`bicycle_stations.py`](bicycle_stations.py))

This script integrates bicycle station information and status data:

**Integration Features:**
- Creates station dimension (`dim_station`) with station attributes and location
- Establishes fine-grained time hierarchy with hour and ten-minute intervals in `dim_hour` and `dim_ten_minute`
- Produces two fact tables:
  - `fact_station_information`: Captures station metadata over time
  - `fact_station_status`: Tracks bicycle availability with high temporal resolution

**Implementation Details:**
- Ensures time dimension hierarchy is complete across all relevant timestamps
- Performs data validation and integrity checks
- Creates necessary indexes for query optimization

### Bicycle Lanes Integration ([`bicycle_lanes.py`](bicycle_lanes.py))

This script integrates bicycle lane network data with administrative boundaries:

**Integration Features:**
- Creates a trimester-based time dimension (`dim_trimester`) for tracking network changes
- Produces interconnected fact tables:
  - `fact_bicycle_lane_state`: Captures lane properties at each time period
  - `fact_bike_lane_tract`: Links lanes to census tracts through spatial intersection
  - `fact_bike_network_metrics`: Aggregates network metrics at district and neighborhood levels
  - `fact_bike_tract_metrics`: Provides detailed lane metrics for each census tract

**Implementation Details:**
- Uses spatial operations to calculate lane lengths within census tracts
- Creates spatial indexes to optimize intersection operations
- Validates referential integrity of the schema

## Critical Integration Attributes

The integration process uses these key joining attributes to link datasets:

1. **Spatial Attributes**:
   - Census tract identifiers and geometries
   - Latitude/longitude coordinates of bicycle stations
   - Linestring geometries of bicycle lanes

2. **Temporal Attributes**:
   - Timestamps for bicycle station status updates
   - Reference dates for population data
   - Year and trimester for bicycle lane network versions

3. **Administrative Identifiers**:
   - District codes/names
   - Neighborhood codes/names
   - Census tract identifiers

## Query Capabilities

The integrated data warehouse supports these analytical queries:

- Demographic analysis at different administrative levels
- Temporal analysis of bicycle availability patterns
- Spatial analysis of bicycle network coverage and growth
- Combined socioeconomic and transportation infrastructure analysis

## Technical Implementation

Each integration script follows this common workflow:

1. **Table Management**: Creates and drops tables as needed with proper error handling
2. **Dimension Creation**: Builds shared dimensional tables for consistent analysis
3. **Fact Table Population**: Creates fact tables with appropriate relationships
4. **Index Creation**: Establishes spatial and conventional indexes for query performance
5. **Validation**: Performs integrity checks on integrated data 