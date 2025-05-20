# Key Performance Indicators (KPIs)

This directory contains SQL queries that measure critical performance indicators for Barcelona's bicycle infrastructure and its relationship to urban demographics. These KPIs enable data-driven evaluation of bicycle infrastructure equity, coverage, and accessibility.

## Overview

The KPIs in this directory address three major measurement dimensions:

1. **Equity**: Assessing whether transportation resources are fairly distributed across socioeconomic groups
2. **Coverage**: Measuring the physical presence of bicycle infrastructure throughout the city
3. **Accessibility**: Evaluating how easily citizens can access the bicycle network

## Key Performance Indicators

### 1. Station Capacity Per Capita ([`station_capacity_per_capita.sql`](station_capacity_per_capita.sql))

This KPI measures the equity of bicycle station distribution across different districts, normalized by population density.

**Methodology:**
- Calculates the most recent bicycle station capacity per district
- Computes capacity per 1,000 inhabitants per square kilometer
- Compares this metric to district average income levels 
- Classifies districts based on their deviation from city-wide averages

**Key Metrics:**
- Total station capacity per district
- Station capacity per 1,000 inhabitants per sq km
- Equity status classification:
  - Fair Distribution: within ±10% of city averages
  - Potential Underservice: low income, low capacity
  - Privileged Access: high income, high capacity
  - Other balanced distribution metrics

**Business Impact:**
- Identifies districts that may need additional bicycle station capacity
- Highlights potential socioeconomic inequities in transportation resource allocation
- Provides a data-driven basis for bicycle station expansion planning

### 2. Bicycle Lane Coverage ([`bicycle_lane_coverage.sql`](bicycle_lane_coverage.sql))

This KPI evaluates the physical coverage of bicycle lanes across census tracts and neighborhoods.

**Methodology:**
- Uses pre-calculated metrics from the `fact_bike_tract_metrics` table
- Reports total lane length and coverage score by census tract

**Key Metrics:**
- Total bicycle lane length per census tract
- Coverage score from the fact table
- Results grouped by year and trimester

**Business Impact:**
- Identifies areas with insufficient bicycle lane infrastructure
- Tracks improvements in coverage over time
- Enables targeted infrastructure development planning

### 3. Bicycle Lane Accessibility ([`bicycle_lane_accessibility.sql`](bicycle_lane_accessibility.sql))

This KPI measures how easily residents can access the nearest bicycle lane from each census tract.

**Methodology:**
- Calculates the geometric centroid of each census tract
- Determines the minimum distance to the nearest bicycle lane
- Converts distance to a normalized accessibility score

**Key Metrics:**
- Minimum distance from census tract centroid to nearest bicycle lane (in meters)
- Normalized accessibility score (1.0 = perfect access, 0.0 = no access)
- Results organized by administrative divisions and time periods

**Business Impact:**
- Identifies residential areas that are isolated from bicycle infrastructure
- Measures effectiveness of efforts to improve accessibility
- Supports planning for new connections to underserved areas

### 4. Bicycle Lane Connectivity ([`bicycle_lane_connectivity.sql`](bicycle_lane_connectivity.sql))

This KPI evaluates how well the bicycle lane network is connected using pre-calculated network metrics.

**Methodology:**
- Uses pre-calculated connectivity metrics from the `fact_bike_network_metrics` table
- Reports connectivity metrics by year and trimester

**Key Metrics:**
- Total number of bicycle lanes
- Number of connected lanes
- Number of isolated lanes
- Connectivity score (ratio of connected lanes to total lanes)

**Business Impact:**
- Identifies disconnected segments that reduce network utility
- Measures network integrity and usability
- Guides planning for strategic connections to improve overall network function
