from shapely.geometry import Polygon

# Helper function to parse WKT POLYGON string manually
def parse_wkt_polygon(polygon_str):
    # Remove 'POLYGON ((' and '))'
    cleaned = polygon_str.replace("POLYGON ((", "").replace("))", "")
    # Split into coordinate pairs
    coords = [tuple(map(float, point.split())) for point in cleaned.split(", ")]
    return Polygon(coords)

# Reconstruct data
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
        print(f"Error parsing polygon: {e}")

# Create the GeoDataFrame
barri_gdf = gpd.GeoDataFrame(parsed_data, geometry="geometry", crs="EPSG:4326")

# Display the resulting dataframe
import ace_tools as tools; tools.display_dataframe_to_user(name="Barri GeoData", dataframe=barri_gdf.head())
