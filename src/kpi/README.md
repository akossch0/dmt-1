# Key Performance Indicators (KPIs)

This directory contains SQL queries that measure critical performance indicators for Barcelona's bicycle infrastructure and its relationship to urban demographics. These KPIs enable data-driven evaluation of bicycle infrastructure equity, quality, and effectiveness.

## Overview

The KPIs in this directory address three major measurement dimensions:

1. **Equity**: Assessing whether transportation resources are fairly distributed across socioeconomic groups
2. **Coverage**: Measuring the physical presence of bicycle infrastructure throughout the city
3. **Accessibility**: Evaluating how easily citizens can access and utilize the bicycle network

## Key Performance Indicators

### 1. Station Capacity Per Capita (`station_capacity_per_capita.sql`)

This KPI measures the equity of bicycle station distribution across different districts, normalized by population density.

**Methodology:**
- Calculates bicycle station capacity per 1,000 inhabitants per square kilometer for each district
- Compares this metric to district income levels to identify potential service inequities
- Establishes a classification system to identify areas with balanced, privileged, or underserved access

**Key Metrics:**
- Total station capacity per district
- Station capacity per 1,000 inhabitants per sq km
- Equity status classification (Fair Distribution, Potential Underservice, Privileged Access, etc.)

**Business Impact:**
- Identifies districts that may need additional bicycle station capacity
- Highlights potential socioeconomic inequities in transportation resource allocation
- Provides a data-driven basis for bicycle station expansion planning

### 2. Bicycle Lane Coverage (`bicycle_lane_coverage.sql`)

This KPI evaluates the physical coverage of bicycle lanes across census tracts and neighborhoods.

**Methodology:**
- Leverages pre-calculated metrics from the integrated data warehouse
- Measures total lane length per census tract
- Calculates a coverage score that accounts for area size and population density

**Key Metrics:**
- Total bicycle lane length per census tract
- Coverage score (normalized metric accounting for population and area)
- Temporal trends by year/trimester

**Business Impact:**
- Identifies areas with insufficient bicycle lane infrastructure
- Tracks improvements in coverage over time
- Enables targeted infrastructure development planning

### 3. Bicycle Lane Accessibility (`bicycle_lane_accessibility.sql`)

This KPI measures how easily residents can access the nearest bicycle lane from each census tract.

**Methodology:**
- Calculates the geometric centroid of each census tract
- Determines the minimum distance to the nearest bicycle lane
- Converts distance to an accessibility score (inverse relationship - closer means more accessible)

**Key Metrics:**
- Minimum distance from census tract centroid to nearest bicycle lane
- Normalized accessibility score (1.0 = perfect access, 0.0 = no access)
- Distribution of accessibility across neighborhoods

**Business Impact:**
- Identifies residential areas that are isolated from bicycle infrastructure
- Measures effectiveness of efforts to improve accessibility
- Supports planning for new connections to underserved areas

### 4. Bicycle Lane Connectivity (`bicycle_lane_connectivity.sql`)

This KPI evaluates how well the bicycle lane network is connected, measuring isolated segments versus a cohesive network.

**Methodology:**
- Leverages pre-calculated network metrics from the integrated data warehouse
- Counts total lanes, connected lanes, and isolated lanes
- Calculates connectivity ratio (connected lanes / total lanes)

**Key Metrics:**
- Total number of bicycle lanes
- Number of connected versus isolated lanes
- Connectivity score (ratio from 0.0 to 1.0)
- Temporal trends by year/trimester

**Business Impact:**
- Identifies disconnected segments that reduce network utility
- Measures network integrity and usability
- Guides planning for strategic connections to improve overall network function
