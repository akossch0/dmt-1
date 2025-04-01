import json
import geopandas as gpd
from shapely.wkt import loads


file_path = '../data/income/BarcelonaCiutat_Barris.json'
# Load JSON file
with open(file_path, "r", encoding="utf-8") as file:
    data = json.load(file)

# Convert JSON to GeoJSON format
features = []
for entry in data:
    polygon = loads(entry["geometria_etrs89"])  # Convert WKT to geometry
    features.append({
        "type": "Feature",
        "properties": {
            "codi_districte": entry["codi_districte"],
            "nom_districte": entry["nom_districte"],
            "codi_barri": entry["codi_barri"],
            "nom_barri": entry["nom_barri"]
        },
        "geometry": polygon.__geo_interface__  # Convert Shapely to GeoJSON format
    })

# Create final GeoJSON structure
geojson_data = {
    "type": "FeatureCollection",
    "features": features
}

# Save as GeoJSON file
with open("barcelona_neighborhoods.geojson", "w", encoding="utf-8") as geojson_file:
    json.dump(geojson_data, geojson_file, indent=4)

print("GeoJSON file created successfully!")
