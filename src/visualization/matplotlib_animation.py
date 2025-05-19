import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np
import contextily as ctx
from sqlalchemy import create_engine
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable
from matplotlib.gridspec import GridSpec

DB_PARAMS = {
    "host": "dtim.essi.upc.edu",
    "port": 5432,
    "dbname": "dbakosschneider",
    "user": "akosschneider",
    "password": "DMT2025!"
}

def get_connection_string():
    return f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"

engine = create_engine(get_connection_string())

# SQL queries
query = """
SELECT *
FROM mv_bike_availability_by_neighborhood_10min
ORDER BY ten_min_datetime, neighbourhood_name;
"""

geo_query = """
SELECT DISTINCT 
    neighbourhood_code, 
    neighbourhood_name, 
    ST_Simplify(ST_Union(geometry), 0.0001) as geometry 
FROM 
    dim_location 
GROUP BY 
    neighbourhood_code, neighbourhood_name;
"""

# Load data
print("Loading data...")
bike_data = pd.read_sql(query, engine)
print(f"Loaded {len(bike_data)} 10-minute neighborhood records")

# Load neighborhood geometry
neighborhoods_gdf = gpd.read_postgis(geo_query, engine, geom_col='geometry')
neighborhoods_gdf = neighborhoods_gdf.to_crs(epsg=3857)  # Web Mercator for basemap compatibility
print(f"Loaded {len(neighborhoods_gdf)} neighborhoods")

# Get unique timestamps
timestamps = bike_data['ten_min_datetime'].unique()
timestamps = np.sort(timestamps)

# Animation settings - 18 fps gives ~8 seconds per day for 10-minute intervals
fps = 18

# Setup the figure and plot with grid layout
fig = plt.figure(figsize=(12, 10))
gs = GridSpec(30, 1, figure=fig)

# Create axes for map and slider
map_ax = fig.add_subplot(gs[0:27, 0])
slider_ax = fig.add_subplot(gs[28:29, 0])

# Create a colorbar
norm = Normalize(vmin=0, vmax=100)
sm = ScalarMappable(norm=norm, cmap='YlGn')
sm.set_array([])
cbar = fig.colorbar(sm, ax=map_ax, label='Bike Availability (%)')

# Create a persistent text object for the date
date_text = fig.text(0.5, 0.92, "", ha='center', fontsize=12)

# Function to update the map for each frame
def update_map(frame_num):
    timestamp = timestamps[frame_num]
    timestamp_dt = pd.Timestamp(timestamp).to_pydatetime()
    
    # Filter data for this timestamp
    frame_data = bike_data[bike_data['ten_min_datetime'] == timestamp]
    
    # Merge with geometry
    merged_data = neighborhoods_gdf.merge(frame_data, on=['neighbourhood_code', 'neighbourhood_name'], how='left')
    
    # Clear previous plots
    map_ax.clear()
    slider_ax.clear()
    
    # Plot neighborhoods colored by bike availability
    merged_data.plot(
        column='avg_bike_availability', 
        ax=map_ax,
        cmap='YlGn',
        legend=False,
        vmin=0,
        vmax=100,
        missing_kwds={'color': 'lightgrey'}
    )
    
    # Add basemap
    ctx.add_basemap(map_ax, source=ctx.providers.CartoDB.Positron, zoom=12)
    
    # Add title
    map_ax.set_title('Bicycle Availability in Barcelona Neighborhoods', fontsize=14)
    
    # Format the date string with proper ordinal suffix
    day = timestamp_dt.day
    suffix = "th" if 4 <= day <= 20 or 24 <= day <= 30 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    date_str = timestamp_dt.strftime(f'%A, {day}{suffix} of %B, %Y')
    date_text.set_text(date_str)
    
    # Calculate hour as float for the time slider
    hour_float = timestamp_dt.hour + timestamp_dt.minute / 60
    
    # Setup time slider
    slider_ax.set_xlim(0, 24)
    slider_ax.set_xticks(np.arange(0, 25, 3))
    slider_ax.set_yticks([])
    slider_ax.set_facecolor('lightgoldenrodyellow')
    slider_ax.axvline(x=hour_float, color='red', linewidth=2)
    slider_ax.set_title(f'Time: {timestamp_dt.strftime("%H:%M")}', fontsize=10)
    
    # Remove axes from main plot
    map_ax.set_axis_off()
    
    # Update colorbar
    sm.set_array(merged_data['avg_bike_availability'])
    
    # Add percentage as text annotations
    for idx, row in merged_data.iterrows():
        if pd.notna(row['avg_bike_availability']):
            centroid = row['geometry'].centroid
            map_ax.text(
                centroid.x, centroid.y,
                f"{row['avg_bike_availability']:.1f}%",
                ha='center', va='center',
                fontsize=7, color='black',
                bbox=dict(facecolor='white', alpha=0.6, boxstyle='round,pad=0.2')
            )

# Display first frame for validation
print("Displaying first frame for validation...")
update_map(0)
plt.draw()
plt.pause(0.1)

# Ask user if they want to continue
input("First frame displayed. Press Enter to continue with full animation...")

# Create animation
print("Creating animation...")
ani = animation.FuncAnimation(
    fig, 
    update_map, 
    frames=len(timestamps),
    repeat=True,
    interval=1000/fps,
)

# Save animation
print("Saving animation to neighborhood_bike_availability_animation.mp4...")
plt.rcParams['savefig.bbox'] = 'tight'
plt.rcParams['savefig.pad_inches'] = 0.1
ani.save('neighborhood_bike_availability_animation.mp4', writer='ffmpeg', fps=fps, dpi=150)

# Show the plot
plt.show()

print("Animation complete!")