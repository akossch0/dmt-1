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

### Demographics Integration (`demographics.py`)

This script integrates population and income data with administrative boundaries:

**Integration Features:**
- Creates a spatial dimension table (`dim_location`) that links census tracts to neighborhoods and districts
- Establishes a time dimension hierarchy (`dim_year`, `dim_month`, `dim_day`) to support temporal analysis
- Produces a fact table (`fact_population_income`) that combines socioeconomic indicators

**Schema Resolution:**
- Standardizes census tract identifiers across population and income datasets
- Handles different date formats from various administrative data sources
- Creates a unified view of demographic data at multiple geographic levels

### Bicycle Stations Integration (`bicycle_stations.py`)

This script integrates bicycle station information and status data:

**Integration Features:**
- Creates station dimension (`dim_station`) with spatial attributes
- Establishes fine-grained time hierarchy with hour and ten-minute intervals
- Produces two fact tables:
  - `fact_station_information`: Captures changes in station metadata over time
  - `fact_station_status`: Tracks availability of bicycles with high temporal resolution

**Schema Resolution:**
- Handles different timestamp formats between station information and status datasets
- Standardizes coordinate systems for spatial analysis
- Creates join relationships between station data and administrative boundaries

### Bicycle Lanes Integration (`bicycle_lanes.py`)

This script integrates bicycle lane network data with administrative boundaries:

**Integration Features:**
- Creates a trimester-based time dimension for tracking network changes
- Produces a series of interconnected fact tables:
  - `fact_bicycle_lane_state`: Captures lane properties at each time period
  - `fact_bike_lane_tract`: Links lanes to census tracts through spatial intersection
  - `fact_bike_network_metrics`: Aggregates network metrics at district and neighborhood levels
  - `fact_bike_tract_metrics`: Provides detailed lane metrics for each census tract

**Schema Resolution:**
- Handles complex geometries and performs spatial operations to establish relationships
- Standardizes lane identifiers and categories
- Creates derived metrics to summarize network characteristics

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

1. **Schema Validation**: Verifies source data structure and quality
2. **Dimension Creation**: Builds shared dimensional tables for consistent analysis
3. **Fact Table Population**: Creates fact tables with appropriate relationships
4. **Index Creation**: Establishes spatial and conventional indexes for query performance
5. **Validation**: Performs integrity checks on integrated data 