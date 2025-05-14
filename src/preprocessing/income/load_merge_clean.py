

import geopandas as gpd
import pandas as pd
import json
from pathlib import Path
from shapely.geometry import Polygon

# --- STEP 1: Load and Parse JSON with WKT polygons ---
with open("../../../data/income/raw/BarcelonaCiutat_Barris.json", "r", encoding="utf-8") as f:
    barri_data = json.load(f)

def parse_wkt_polygon(polygon_str):
    cleaned = polygon_str.replace("POLYGON ((", "").replace("))", "")
    coords = [tuple(map(float, point.split())) for point in cleaned.split(", ")]
    return Polygon(coords)

parsed_data = []
for entry in barri_data:
    try:
        polygon = parse_wkt_polygon(entry["geometria_wgs84"])
        parsed_data.append({
            "codi_districte": str(entry["codi_districte"]).zfill(2),
            "nom_districte": entry["nom_districte"],
            "codi_barri": str(entry["codi_barri"]).zfill(2),
            "nom_barri": entry["nom_barri"],
            "geometry": polygon
        })
    except Exception as e:
        print(f"Error parsing polygon for {entry['nom_barri']}: {e}")

gdf = gpd.GeoDataFrame(parsed_data, geometry="geometry", crs="EPSG:4326")

# --- STEP 2: Load and Concatenate All Income CSVs ---
raw_path = Path("../../../data/income/raw")
all_csvs = list(raw_path.glob("income_*.csv"))
income_df = pd.concat([pd.read_csv(f, sep=",") for f in all_csvs], ignore_index=True)

# --- STEP 3: Standardize and Join Keys ---
income_df.columns = income_df.columns.str.lower()
income_df["any"] = pd.to_datetime(income_df["any"]).dt.strftime("%Y-%m-%d")
income_df["codi_districte"] = income_df["codi_districte"].astype(int)
income_df["codi_barri"] = income_df["codi_barri"].astype(str).str.zfill(2)
income_df["seccio_censal"] = (
    income_df["codi_districte"].astype(str) +
    income_df["seccio_censal"].astype(int).astype(str).str.zfill(3)
).astype(int)

# --- STEP 4: Merge ---
gdf["codi_districte"] = gdf["codi_districte"].astype(int)
merged = gdf.merge(income_df, on=["codi_districte", "codi_barri"])

# --- STEP 5: Normalize Income ---
income_min = merged["import_euros"].min()
income_max = merged["import_euros"].max()
merged["income_norm"] = (merged["import_euros"] - income_min) / (income_max - income_min)

# --- STEP 6: Export Final Data ---
output_dir = Path("../../../data/income/clean")
output_dir.mkdir(parents=True, exist_ok=True)

merged.to_file(output_dir / "final_output.geojson", driver="GeoJSON")
merged.to_csv(output_dir / "final_output.csv", index=False)

print("✅ Final preprocessed data exported to 'final_output.geojson' and 'final_output.csv'")

