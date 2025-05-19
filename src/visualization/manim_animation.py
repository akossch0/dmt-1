import pandas as pd
import geopandas as gpd
import numpy as np
from sqlalchemy import create_engine
import tempfile
import os
from tqdm import tqdm
import contextily as ctx
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
import matplotlib.cm as cm
from manim import *
from pyproj import Transformer
from contextily.tile import bounds2img

# Database connection parameters
DB_PARAMS = {
    "host": "dtim.essi.upc.edu",
    "port": 5432,
    "dbname": "dbakosschneider",
    "user": "akosschneider",
    "password": "DMT2025!"
}

def get_connection_string():
    return f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"

class BikeAvailabilityVisualization(Scene):
    def construct(self):
        self.camera.background_color = "#1E1E1E"
        
        # Connect to database
        engine = create_engine(get_connection_string())
        
        # Load district geometry data
        geo_query = """
        SELECT DISTINCT 
            district_code, 
            district_name, 
            ST_Transform(ST_Simplify(ST_Union(geometry), 0.0001), 4326) as geometry 
        FROM 
            dim_location 
        GROUP BY 
            district_code, district_name;
        """
        districts_gdf = gpd.read_postgis(geo_query, engine, geom_col='geometry')
        
        # Get district boundaries for setting the scene dimensions
        min_x, min_y, max_x, max_y = districts_gdf.total_bounds
        
        # Calculate scaling factor to fit the scene
        scale_factor = 32.0 
        
        # Create a reference point for centering the map
        barcelona_center = np.array([2.168365, 41.387875])  
        
        # Create a function to transform from GIS coordinates to Manim coordinates
        def gis_to_manim(lon, lat, center_lon=barcelona_center[0], center_lat=barcelona_center[1], scale=scale_factor):
            manim_x = (lon - center_lon) * scale
            manim_y = (lat - center_lat) * scale
            return [manim_x, manim_y, 0]
        
        # Function to convert from Web Mercator to WGS84 (lon/lat)
        def webmerc_to_lonlat(x, y):
            transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
            lon, lat = transformer.transform(x, y)
            return lon, lat
        
        # Create a high-resolution map background
        districts_webmerc = districts_gdf.to_crs(epsg=3857)
        
        # Get exact bounds in Web Mercator
        bounds = districts_webmerc.total_bounds
        x_min, y_min, x_max, y_max = bounds
        
        # Add padding for full-screen background while keeping districts centered
        pad_x = (x_max - x_min) * 1.0
        pad_y = (y_max - y_min) * 1.0
        x_min -= pad_x
        y_min -= pad_y
        x_max += pad_x
        y_max += pad_y
        
        # Calculate center and dimensions in Web Mercator
        center_x = (x_min + x_max) / 2
        center_y = (y_min + y_max) / 2
        width = x_max - x_min
        height = y_max - y_min
        
        # Create a temporary figure to get the basemap
        dpi = 600
        aspect_ratio = width / height
        if aspect_ratio > 1:
            fig_width = 20
            fig_height = 20 / aspect_ratio
        else:
            fig_height = 20
            fig_width = 20 * aspect_ratio
        
        zoom = 13
        source = ctx.providers.CartoDB.DarkMatter
        
        # Get the map image
        img, extent = ctx.bounds2img(
            x_min, y_min, x_max, y_max, 
            zoom=zoom, 
            source=source,
            ll=False
        )
        
        # Save the image to a temporary file
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
            map_image_path = tmp.name
            plt.imsave(map_image_path, img, format='png', dpi=dpi)
        
        # Load the image as a Manim object
        map_image = ImageMobject(map_image_path)
        
        # Convert Web Mercator bounds back to lon/lat
        lon_min, lat_min = webmerc_to_lonlat(x_min, y_min)
        lon_max, lat_max = webmerc_to_lonlat(x_max, y_max)
        
        # Convert to Manim coordinates
        bl_corner_manim = gis_to_manim(lon_min, lat_min)  # Bottom-left
        tr_corner_manim = gis_to_manim(lon_max, lat_max)  # Top-right
        
        # Calculate width and height in Manim units
        manim_width = tr_corner_manim[0] - bl_corner_manim[0]
        manim_height = tr_corner_manim[1] - bl_corner_manim[1]
        
        # Calculate center in Manim coordinates
        center_lon = (lon_min + lon_max) / 2
        center_lat = (lat_min + lat_max) / 2
        center_manim = gis_to_manim(center_lon, center_lat)
        
        # Position map
        map_image.move_to(ORIGIN)
        map_image.stretch_to_fit_width(abs(manim_width))
        map_image.stretch_to_fit_height(abs(manim_height))
        map_image.move_to([center_manim[0], center_manim[1], 0])
        
        # Apply a small correction to account for any projection distortion
        x_correction = -0.245
        y_correction = -0.16
        map_image.shift(x_correction * RIGHT + y_correction * UP)
        
        self.add(map_image)
        
        # Query bike availability data - ALL 7 DAYS
        query = """
        SELECT *
        FROM mv_bike_availability_by_district_10min
        WHERE date_value BETWEEN '2019-06-01' AND '2019-06-07'
        ORDER BY ten_min_datetime, district_name;
        """
        bike_data = pd.read_sql(query, engine)
        
        # Convert timestamp to datetime
        bike_data['ten_min_datetime'] = pd.to_datetime(bike_data['ten_min_datetime'])
        
        # Get unique timestamps
        timestamps = np.sort(bike_data['ten_min_datetime'].unique())
        
        # Calculate how many timestamps to use for animation
        frames_per_day = 32
        total_frames = frames_per_day * 7
        
        # Get timestamps per day
        timestamps_by_day = {}
        for timestamp in timestamps:
            timestamp_dt = pd.Timestamp(timestamp)
            day = timestamp_dt.date()
            if day not in timestamps_by_day:
                timestamps_by_day[day] = []
            timestamps_by_day[day].append(timestamp)
        
        # Select evenly spaced timestamps for each day
        animation_timestamps = []
        for day, day_timestamps in sorted(timestamps_by_day.items()):
            if len(day_timestamps) > frames_per_day:
                interval = len(day_timestamps) // frames_per_day
                day_animation_timestamps = day_timestamps[::interval]
                day_animation_timestamps = day_animation_timestamps[:frames_per_day]
            else:
                day_animation_timestamps = day_timestamps
            
            animation_timestamps.extend(day_animation_timestamps)
        
        # Create a color map function using the YlGn colormap from matplotlib
        norm = Normalize(vmin=0, vmax=100)
        cmap = cm.get_cmap('YlGn')
        
        def get_color(availability):
            if pd.isna(availability):
                return "#333333"  # Dark grey for missing data
            
            # Use the YlGn colormap to get color
            rgba = cmap(norm(availability))
            
            # Convert RGBA to hex
            r, g, b, _ = rgba
            hex_color = "#{:02x}{:02x}{:02x}".format(int(r * 255), int(g * 255), int(b * 255))
            return hex_color
        
        # Create district polygons
        district_shapes = {}
        district_labels = {}
        
        for idx, row in districts_gdf.iterrows():
            # Convert shapely geometry to coordinates
            coords = []
            if row.geometry.geom_type == 'Polygon':
                for point in row.geometry.exterior.coords:
                    x, y = point[0], point[1]
                    manim_x = (x - barcelona_center[0]) * scale_factor
                    manim_y = (y - barcelona_center[1]) * scale_factor
                    coords.append([manim_x, manim_y, 0])
            elif row.geometry.geom_type == 'MultiPolygon':
                # Take the largest polygon for simplicity
                largest = max(row.geometry.geoms, key=lambda x: x.area)
                for point in largest.exterior.coords:
                    x, y = point[0], point[1]
                    manim_x = (x - barcelona_center[0]) * scale_factor
                    manim_y = (y - barcelona_center[1]) * scale_factor
                    coords.append([manim_x, manim_y, 0])
            
            # Create polygon with semi-transparent fill to see the background map
            polygon = Polygon(*coords, stroke_color=WHITE, stroke_width=1, fill_opacity=0.6)
            district_shapes[row.district_code] = polygon
            
            # Calculate centroid for label placement
            centroid = row.geometry.centroid
            centroid_x = (centroid.x - barcelona_center[0]) * scale_factor
            centroid_y = (centroid.y - barcelona_center[1]) * scale_factor
            
            district_labels[row.district_code] = [centroid_x, centroid_y]
        
        # Create a title
        title = Text("Bicycle Availability in Barcelona", font_size=32, color=WHITE)
        title.to_edge(UP, buff=0.3)
        self.add(title)
        
        # Create a time display
        timestamp_text = Text("", font_size=14, color=WHITE)
        timestamp_text.to_edge(DOWN, buff=0.5)
        self.add(timestamp_text)
        
        # Create legend colors using the YlGn colormap
        legend_items = []
        percentages = [10, 30, 50, 70, 90]
        labels = ["0-20%", "20-40%", "40-60%", "60-80%", "80-100%"]
        
        base_position = RIGHT * 6 + UP * 2
        
        for i, (percentage, label) in enumerate(zip(percentages, labels)):
            color = get_color(percentage)
            rect = Rectangle(height=0.3, width=0.5, fill_color=color, fill_opacity=1, stroke_width=0)
            rect.move_to(base_position + DOWN * (i * 0.5))
            text = Text(label, font_size=14, color=WHITE)
            text.next_to(rect, RIGHT, buff=0.1)
            legend_items.extend([rect, text])
        
        self.add(*legend_items)
        
        # Create time slider at the bottom
        slider_bg = Rectangle(height=0.2, width=10, fill_color="#444444", 
                             fill_opacity=1, stroke_color=WHITE)
        slider_bg.to_edge(DOWN, buff=1.2)
        
        # Add hour markers
        hour_markers = []
        for hour in range(0, 25, 4):
            marker = Line(
                slider_bg.get_left() + RIGHT * (hour/24 * slider_bg.width) + UP * 0.15,
                slider_bg.get_left() + RIGHT * (hour/24 * slider_bg.width) + DOWN * 0.15,
                color=WHITE
            )
            hour_text = Text(f"{hour:02d}h", font_size=14, color=WHITE)
            hour_text.next_to(marker, DOWN, buff=0.1)
            hour_markers.extend([marker, hour_text])
        
        slider_handle = Circle(radius=0.1, fill_color="#FFFFFF", fill_opacity=1, stroke_width=0)
        slider_handle.move_to(slider_bg.get_left())
        
        self.add(slider_bg, *hour_markers, slider_handle)
        
        # Add all district shapes to the scene
        for district_code, shape in district_shapes.items():
            self.add(shape)
        
        # Create animations for updating the map
        district_label_groups = {}

        for i, timestamp in tqdm(enumerate(animation_timestamps)):
            frame_data = bike_data[bike_data['ten_min_datetime'] == timestamp]
            
            # Calculate time of day
            timestamp_dt = pd.Timestamp(timestamp)
            hour_of_day = timestamp_dt.hour + timestamp_dt.minute / 60
            slider_x_pos = slider_bg.get_left() + RIGHT * (hour_of_day/24 * slider_bg.width)
            
            # Update district colors and labels based on availability
            updates = []
            for _, row in frame_data.iterrows():
                district_code = row['district_code']
                if district_code in district_shapes:
                    polygon = district_shapes[district_code]
                    availability = row['avg_bike_availability']
                    color = get_color(availability)
                    
                    # Create animation for color change
                    updates.append(polygon.animate.set_fill(color))
                    
                    # Remove old label group if it exists
                    if district_code in district_label_groups and district_label_groups[district_code] in self.mobjects:
                        self.remove(district_label_groups[district_code])
                    
                    # Create new label with district info
                    label_text = f"{row['district_name']}\n{availability:.1f}%\n({row['station_count']} stations)"
                    label = Text(
                        label_text,
                        font_size=6,
                        color=BLACK,
                        stroke_width=0,
                    )
                    
                    # Create a background box for the label
                    padding = 0.1
                    label_width = label.width + padding
                    label_height = label.height + padding
                    label_box = Rectangle(
                        width=label_width,
                        height=label_height,
                        color="#DDDDDD",
                        fill_opacity=0.6,
                        stroke_width=0.5,
                        stroke_color=WHITE,
                    )
                    
                    # Group the label and background together
                    label_group = VGroup(label_box, label)
                    
                    # Store the reference to this label group
                    district_label_groups[district_code] = label_group
                    
                    # Position at district center
                    center_pos = district_labels[district_code]
                    label_group.move_to([center_pos[0], center_pos[1], 0])
                    
                    self.add(label_group)
            
            # Update timestamp text with nicer format
            day = timestamp_dt.day
            day_suffix = 'th' if 11 <= day <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')
            weekday = timestamp_dt.strftime('%A')
            formatted_date = f"{weekday}, {timestamp_dt.strftime(f'{day}{day_suffix} of %B, %Y')}"
            
            new_timestamp_text = Text(formatted_date, font_size=18, color=WHITE)
            new_timestamp_text.to_edge(DOWN, buff=0.5)
            
            # Update slider position
            new_slider_handle = slider_handle.copy()
            new_slider_handle.move_to(slider_x_pos)
            
            # Run all animations together
            self.play(
                *updates,
                Transform(timestamp_text, new_timestamp_text),
                Transform(slider_handle, new_slider_handle),
                run_time=0.25
            )
        
        # Final pause at the end
        self.wait(1)
        
        # Clean up the temporary image file
        try:
            os.unlink(map_image_path)
        except:
            pass


if __name__ == "__main__":
    # Run with:
    # manim -pql src/visualization/manim_visualization.py BikeAvailabilityVisualization
    # For faster rendering during development, or:
    # manim -pqh src/visualization/manim_visualization.py BikeAvailabilityVisualization
    # For final high-quality render
    pass 