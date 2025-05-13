
import geopandas as gpd
import pandas as pd
import json
from shapely.geometry import Polygon, shape

# --- STEP 1: Load and Parse JSON with WKT polygons ---
# Load the raw JSON with polygon data
with open("../../data/BarcelonaCiutat_Barris.json", "r", encoding="utf-8") as f:
    barri_data = json.load(f)

# Function to parse WKT-style POLYGON string into Shapely Polygon
def parse_wkt_polygon(polygon_str):
    cleaned = polygon_str.replace("POLYGON ((", "").replace("))", "")
    coords = [tuple(map(float, point.split())) for point in cleaned.split(", ")]
    return Polygon(coords)

# Build list of records with geometry
parsed_data = []
for entry in barri_data:
    try:
        polygon = parse_wkt_polygon(entry["geometria_wgs84"])
        parsed_data.append({
            "codi_districte": entry["codi_districte"],
            "nom_districte": entry["nom_districte"],
            "codi_barri": entry["codi_barri"],
            "nom_barri": entry["nom_barri"],
            "geometry": polygon
        })
    except Exception as e:
        print(f"Error parsing polygon for {entry['nom_barri']}: {e}")

# Create a GeoDataFrame from the parsed data
gdf = gpd.GeoDataFrame(parsed_data, geometry="geometry", crs="EPSG:4326")

# --- STEP 2: Load and Clean Income Data ---
income_df = pd.read_csv("../../data/income.csv")

# Standardize column formats
income_df['Codi_Districte'] = income_df['Codi_Districte'].astype(str).str.zfill(2)
income_df['Codi_Barri'] = income_df['Codi_Barri'].astype(str).str.zfill(2)

# Drop rows with missing income
income_df = income_df.dropna(subset=["Import_Euros"])

# --- STEP 3: Merge Datasets ---
# Prepare spatial GeoDataFrame for merging
gdf['codi_districte'] = gdf['codi_districte'].astype(str).str.zfill(2)
gdf['codi_barri'] = gdf['codi_barri'].astype(str).str.zfill(2)

# Merge on district and neighborhood codes
merged = gdf.merge(income_df, left_on=['codi_districte', 'codi_barri'],
                             right_on=['Codi_Districte', 'Codi_Barri'])

# --- STEP 4: Normalize Income ---
income_min = merged["Import_Euros"].min()
income_max = merged["Import_Euros"].max()
merged["income_norm"] = (merged["Import_Euros"] - income_min) / (income_max - income_min)

# --- STEP 5: Export Final Preprocessed GeoData ---
merged.to_file("final_output.geojson", driver="GeoJSON")
merged.to_csv("final_output.csv", index=False)

print("Final preprocessed data exported: 'final_output.geojson' and 'final_output.csv'")

