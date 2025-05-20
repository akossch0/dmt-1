# Data Management for Transportation - Barcelona Bicing Service

Urban infrastructure planning and analysis for Barcelona's Bicing service

## Documentation

Detailed documentation for each project component:

- [Data Preprocessing](src/preprocessing/README.md) - Data cleaning, transformation, and preparation workflows
- [Data Integration](src/integration/README.md) - Integration strategy and component descriptions
- [Key Performance Indicators](src/kpi/README.md) - Metrics and SQL queries for performance analysis

## Project Structure

```
├── data/                       # Data files
│   ├── administrative_units/   # Barcelona administrative boundaries
│   ├── bicycle_lanes/          # Bicycle lane network data
│   ├── bicycle_stations/       # Bicing station data
│   ├── income/                 # Income data by neighborhood
│   └── population/             # Population statistics
├── src/                        # Source code
│   ├── integration/            # Data integration scripts
│   ├── kpi/                    # Key Performance Indicators
│   ├── materialized_views/     # Database view definitions
│   ├── preprocessing/          # Data preprocessing scripts
│   ├── utils/                  # Utility functions
│   └── visualization/          # Visualization modules
├── notebooks/                  # Jupyter notebooks for analysis
│   ├── eda_administrative_units.ipynb
│   ├── eda_bicycle_lanes.ipynb
│   ├── eda_bicycle_stations.ipynb
│   ├── eda_neigh_income.ipynb
│   ├── eda_population.ipynb
│   └── outputs/                # Notebook outputs
├── requirements.txt            # Project dependencies
└── README.md                   # Project documentation
```

## Setup Instructions

### Prerequisites
- Python 3.9 or higher
- pip (Python package installer)

### Installation

1. Clone this repository
   ```bash
   git clone [repository URL]
   cd [repository name]
   ```

2. Create and activate a virtual environment (recommended)
   ```bash
   # Using venv
   python -m venv .venv
   
   # On Windows
   .venv\Scripts\activate
   
   # On macOS/Linux
   source .venv/bin/activate
   ```

3. Install the required packages
   ```bash
   pip install -r requirements.txt
   ```

## Project Components

### Data Sources
- Bicycle stations data from Barcelona's Bicing service
- Bicycle lane network information
- Administrative boundaries (neighborhoods, districts)
- Population statistics
- Income data by neighborhood

### Analysis Notebooks
- Exploratory data analysis of administrative units
- Analysis of bicycle lanes network
- Bicing stations distribution and usage patterns
- Neighborhood income analysis
- Population distribution analysis

### Key Dependencies
- Data manipulation: numpy, pandas, geopandas
- Visualization: matplotlib, seaborn, plotly, folium, manim, bokeh
- Geospatial: shapely, contextily
- Machine learning: scikit-learn, scipy
- Database: SQLAlchemy, GeoAlchemy2, psycopg2
