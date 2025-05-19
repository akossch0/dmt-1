import pandas as pd
import geopandas as gpd
import folium
import numpy as np
import json
from sqlalchemy import create_engine
from folium.plugins import TimeSliderChoropleth
from branca.colormap import LinearColormap
from datetime import datetime, timedelta

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

# Connect to the database
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

# Convert datetime to more usable format
bike_data['ten_min_datetime'] = pd.to_datetime(bike_data['ten_min_datetime'])
bike_data['date'] = bike_data['ten_min_datetime'].dt.date
bike_data['time'] = bike_data['ten_min_datetime'].dt.strftime('%H:%M')
bike_data['hour_float'] = bike_data['ten_min_datetime'].dt.hour + bike_data['ten_min_datetime'].dt.minute / 60

# Load neighborhood geometry
neighborhoods_gdf = gpd.read_postgis(geo_query, engine, geom_col='geometry')
# Convert to WGS84 (standard for web maps)
neighborhoods_gdf = neighborhoods_gdf.to_crs(epsg=4326)
print(f"Loaded {len(neighborhoods_gdf)} neighborhoods")

# Get unique dates
unique_dates = sorted(bike_data['date'].unique())
date_options = [date.strftime('%Y-%m-%d') for date in unique_dates]

# Create a base map centered on Barcelona
barcelona_coords = [41.3851, 2.1734]
m = folium.Map(
    location=barcelona_coords,
    zoom_start=12,
    tiles='CartoDB positron'
)

# Create a colormap similar to YlGn used in the original script
# Fix: Use the correct LinearColormap class
colormap = LinearColormap(
    colors=['#f7fcb9', '#d9f0a3', '#addd8e', '#78c679', '#41ab5d', '#006837'],
    index=[0, 20, 40, 60, 80, 100],
    vmin=0,
    vmax=100
)
colormap.caption = 'Bike Availability (%)'
colormap.add_to(m)

# Convert GeoDataFrame to GeoJSON for Folium
neighborhoods_geojson = json.loads(neighborhoods_gdf.to_json())

# Function to create style_dict for TimeSliderChoropleth
def create_style_function(selected_date, selected_hour):
    # Initial style dictionary
    style_dict = {}
    
    # Filter for selected date
    date_data = bike_data[bike_data['date'] == pd.to_datetime(selected_date).date()]
    
    # Find closest time 
    date_data['time_diff'] = abs(date_data['hour_float'] - selected_hour)
    closest_times = date_data.loc[date_data.groupby('neighbourhood_code')['time_diff'].idxmin()]
    
    for _, row in closest_times.iterrows():
        # Get availability
        availability = row['avg_bike_availability']
        # Skip if NaN
        if pd.isna(availability):
            continue
            
        # Color based on availability
        color = colormap(availability)
        
        # Create a dictionary of the most important metrics to display
        important_metrics = {}
        
        # Add key metrics if they exist in the data
        for key in ['total_bikes_available', 'total_ebikes', 'total_mechanical_bikes', 'station_count']:
            if key in row and pd.notna(row[key]):
                important_metrics[key] = int(row[key])
        
        # Collect other available information
        additional_info = {}
        # Include any other numeric/useful columns
        for col in row.index:
            if col not in ['neighbourhood_code', 'neighbourhood_name', 'avg_bike_availability', 
                         'time_diff', 'date', 'time', 'hour_float', 'ten_min_datetime'] + list(important_metrics.keys()):
                if pd.notna(row[col]) and not isinstance(row[col], (bytes, bytearray)):
                    additional_info[col] = str(row[col])
        
        # Add to style dictionary
        if row['neighbourhood_code'] not in style_dict:
            style_dict[row['neighbourhood_code']] = {}
        
        style_dict[row['neighbourhood_code']] = {
            'color': 'black',
            'fillColor': color,
            'weight': 1,
            'fillOpacity': 0.7,
            'availability': f"{row['avg_bike_availability']:.1f}%",
            'important_metrics': important_metrics,
            'additional_info': additional_info
        }
    
    return style_dict

# Add GeoJSON to map
geojson_layer = folium.GeoJson(
    neighborhoods_geojson,
    name='geojson',
    style_function=lambda x: {
        'fillColor': 'lightgrey',
        'color': 'black',
        'weight': 1,
        'fillOpacity': 0.7
    }
)
geojson_layer.add_to(m)

# Add neighborhood names as tooltips
tooltip_layer = folium.GeoJson(
    neighborhoods_geojson,
    name='neighborhoods_tooltip',
    style_function=lambda x: {
        'fillColor': 'transparent',
        'color': 'transparent',
        'weight': 0,
        'fillOpacity': 0
    },
    tooltip=folium.GeoJsonTooltip(
        fields=['neighbourhood_name'],
        aliases=['Neighborhood:'],
        style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;")
    )
)
tooltip_layer.add_to(m)

# Create a layer to display availability values
label_layer = folium.FeatureGroup(name='labels')
m.add_child(label_layer)

# Add custom HTML and JavaScript for interactive controls
html_controls = """
<div id="control-panel" style="position: absolute; 
                              top: 10px; 
                              right: 10px; 
                              z-index: 999; 
                              background-color: white; 
                              padding: 10px; 
                              border-radius: 5px;
                              box-shadow: 0 1px 5px rgba(0,0,0,0.4);">
    <div style="margin-bottom: 10px;">
        <label for="date-select">Select Date:</label>
        <select id="date-select" style="margin-left: 5px;">
"""

# Add date options
for date in date_options:
    html_controls += f'<option value="{date}">{date}</option>\n'

html_controls += """
        </select>
    </div>
    <div>
        <label for="time-slider">Time: <span id="time-value">00:00</span></label>
        <input type="range" id="time-slider" min="0" max="24" step="0.1667" value="12" style="width: 200px;">
    </div>
</div>

<script>
(function() {
    // Get elements
    var dateSelect = document.getElementById('date-select');
    var timeSlider = document.getElementById('time-slider');
    var timeValue = document.getElementById('time-value');
    
    // Function to format time
    function formatTime(hourFloat) {
        var hours = Math.floor(hourFloat);
        var minutes = Math.round((hourFloat - hours) * 60);
        return hours.toString().padStart(2, '0') + ':' + minutes.toString().padStart(2, '0');
    }
    
    // Update time display
    function updateTimeDisplay() {
        timeValue.textContent = formatTime(parseFloat(timeSlider.value));
    }
    
    // Initialize time display
    updateTimeDisplay();
    
    // Function to update visualization based on selected date and time
    function updateVisualization() {
        var selectedDate = dateSelect.value;
        var selectedHour = parseFloat(timeSlider.value);
        
        // Custom event to be handled in Python callback
        var event = new CustomEvent('update_map', {
            detail: {
                date: selectedDate,
                hour: selectedHour
            }
        });
        document.dispatchEvent(event);
    }
    
    // Add event listeners
    dateSelect.addEventListener('change', function() {
        updateVisualization();
    });
    
    timeSlider.addEventListener('input', function() {
        updateTimeDisplay();
    });
    
    timeSlider.addEventListener('change', function() {
        updateVisualization();
    });
    
    // Initial visualization update
    setTimeout(updateVisualization, 500);
})();
</script>
"""

# Add the HTML controls to the map
m.get_root().html.add_child(folium.Element(html_controls))

# Add JavaScript for updating the map
js_update = """
<script>
// Store references to layers
var geojsonLayer = null;
var labelsLayer = null;

// GeoJSON data
var neighborhoods = """ + json.dumps(neighborhoods_geojson) + """;

// Function to create style for a feature
function getFeatureStyle(feature, styleDict) {
    var code = feature.properties.neighbourhood_code;
    if (styleDict[code]) {
        return styleDict[code];
    } else {
        return {
            fillColor: 'lightgrey',
            color: 'black',
            weight: 1,
            fillOpacity: 0.7
        };
    }
}

// Function to update the map
function updateMap(date, hour) {
    // Send data to Python (via Brython or Flask if needed)
    fetch(`data?date=${date}&hour=${hour}`)
        .then(response => response.json())
        .then(styleDict => {
            // Remove old layers
            if (geojsonLayer) {
                map.removeLayer(geojsonLayer);
            }
            
            // Create new GeoJSON layer with updated styles
            geojsonLayer = L.geoJson(neighborhoods, {
                style: function(feature) {
                    return getFeatureStyle(feature, styleDict);
                }
            }).addTo(map);
            
            // Update labels
            updateLabels(date, hour);
        });
}

// Function to update labels
function updateLabels(date, hour) {
    // Implementation depends on how you want to display labels
}

// Listen for custom event from control panel
document.addEventListener('update_map', function(e) {
    updateMap(e.detail.date, e.detail.hour);
});
</script>
"""

# Function to update availability values
def update_availability(date_str, hour_float):
    # Clear existing labels
    label_layer.clear_layers()
    
    # Convert date_str to datetime.date
    selected_date = pd.to_datetime(date_str).date()
    
    # Filter data for the selected date
    date_data = bike_data[bike_data['date'] == selected_date]
    
    # Find closest time records
    date_data['time_diff'] = abs(date_data['hour_float'] - hour_float)
    closest_times = date_data.loc[date_data.groupby('neighbourhood_code')['time_diff'].idxmin()]
    
    # Prepare style dictionary
    style_dict = {}
    
    # Update geojson layer and add labels
    for _, row in closest_times.iterrows():
        neighborhood_code = row['neighbourhood_code']
        availability = row['avg_bike_availability']
        
        # Skip if NaN
        if pd.isna(availability):
            continue
            
        # Get color based on availability
        color = colormap(availability)
        
        # Create a dictionary of the most important metrics to display
        important_metrics = {}
        
        # Add key metrics if they exist in the data
        for key in ['total_bikes_available', 'total_ebikes', 'total_mechanical_bikes', 'station_count']:
            if key in row and pd.notna(row[key]):
                important_metrics[key] = int(row[key])
        
        # Collect other available information
        additional_info = {}
        # Include any other numeric/useful columns
        for col in row.index:
            if col not in ['neighbourhood_code', 'neighbourhood_name', 'avg_bike_availability', 
                         'time_diff', 'date', 'time', 'hour_float', 'ten_min_datetime'] + list(important_metrics.keys()):
                if pd.notna(row[col]) and not isinstance(row[col], (bytes, bytearray)):
                    additional_info[col] = str(row[col])
        
        # Add to style dictionary
        style_dict[neighborhood_code] = {
            'color': 'black',
            'fillColor': color,
            'weight': 1,
            'fillOpacity': 0.7,
            'availability': f"{row['avg_bike_availability']:.1f}%",
            'important_metrics': important_metrics,
            'additional_info': additional_info
        }
        
        # Find the neighborhood geometry
        neighborhood = neighborhoods_gdf[neighborhoods_gdf['neighbourhood_code'] == neighborhood_code]
        if not neighborhood.empty:
            # Get centroid for label placement
            centroid = neighborhood.iloc[0].geometry.centroid
            
            # Create marker for availability value
            folium.Marker(
                location=[centroid.y, centroid.x],
                icon=folium.DivIcon(
                    icon_size=(150, 36),
                    icon_anchor=(75, 18),
                    html=f'<div style="text-align: center; font-weight: bold; background-color: white; border-radius: 4px; padding: 2px 5px; opacity: 0.8;">{availability:.1f}%</div>'
                )
            ).add_to(label_layer)
    
    return style_dict

# Create a JavaScript callback to handle the map updates
callback = """
function(map, date, hour) {
    var styleDict = %s;
    
    // Update styles for each feature
    geojsonLayer.eachLayer(function(layer) {
        var code = layer.feature.properties.neighbourhood_code;
        if (styleDict[code]) {
            layer.setStyle(styleDict[code]);
        } else {
            layer.setStyle({
                fillColor: 'lightgrey',
                color: 'black',
                weight: 1,
                fillOpacity: 0.7
            });
        }
    });
}
"""

# Save the map to HTML
output_file = 'bike_availability_interactive.html'
print(f"Saving interactive map to {output_file}...")

# Add custom JS to handle updates
js_callback = """
<script>
document.addEventListener('update_map', function(e) {
    // Get selected date and hour
    var selectedDate = e.detail.date;
    var selectedHour = e.detail.hour;
    
    // Make a request to update the map
    var xhr = new XMLHttpRequest();
    xhr.open('GET', `data?date=${selectedDate}&hour=${selectedHour}`, true);
    xhr.onreadystatechange = function() {
        if (xhr.readyState === 4 && xhr.status === 200) {
            var styleDict = JSON.parse(xhr.responseText);
            
            // Remove old GeoJSON layer and add new one
            map.eachLayer(function(layer) {
                if (layer.feature) {
                    map.removeLayer(layer);
                }
            });
            
            L.geoJson(neighborhoods, {
                style: function(feature) {
                    var code = feature.properties.neighbourhood_code;
                    if (styleDict[code]) {
                        return styleDict[code];
                    } else {
                        return {
                            fillColor: 'lightgrey',
                            color: 'black',
                            weight: 1,
                            fillOpacity: 0.7
                        };
                    }
                }
            }).addTo(map);
        }
    };
    xhr.send();
});
</script>
"""

# Since we can't rely on a server for the standalone HTML,
# we'll create a self-contained solution with all data embedded

# Generate data for all date/time combinations
print("Preparing data for interactive visualization...")
all_date_time_data = {}

# Ensure we have exactly 6 time points per hour (every 10 minutes)
time_steps = np.linspace(0, 24, 145)  # 0:00 to 24:00 in 10-minute steps (144 intervals + endpoint)

for date_str in date_options:
    print(f"Processing date: {date_str}")
    all_date_time_data[date_str] = {}
    date_obj = pd.to_datetime(date_str).date()
    
    # Get all data for this date once
    date_data = bike_data[bike_data['date'] == date_obj].copy()
    
    if len(date_data) == 0:
        print(f"Warning: No data found for date {date_str}")
        continue
        
    # Process each time step
    for hour_float in time_steps:
        # Create a copy to avoid SettingWithCopyWarning
        time_data = date_data.copy()
        time_data['time_diff'] = abs(time_data['hour_float'] - hour_float)
        
        # Find closest time for each neighborhood
        closest_times = time_data.loc[time_data.groupby('neighbourhood_code')['time_diff'].idxmin()]
        
        # Filter out entries that are too far (more than 15 minutes away)
        closest_times = closest_times[closest_times['time_diff'] <= 0.25]
        
        # Create style dictionary for this time
        style_dict = {}
        for _, row in closest_times.iterrows():
            if pd.notna(row['avg_bike_availability']):
                color = colormap(row['avg_bike_availability'])
                
                # Create a dictionary of the most important metrics to display
                important_metrics = {}
                
                # Add key metrics if they exist in the data
                for key in ['total_bikes_available', 'total_ebikes', 'total_mechanical_bikes', 'station_count']:
                    if key in row and pd.notna(row[key]):
                        important_metrics[key] = int(row[key])
                
                # Collect other available information
                additional_info = {}
                # Include any other numeric/useful columns
                for col in row.index:
                    if col not in ['neighbourhood_code', 'neighbourhood_name', 'avg_bike_availability', 
                                 'time_diff', 'date', 'time', 'hour_float', 'ten_min_datetime'] + list(important_metrics.keys()):
                        if pd.notna(row[col]) and not isinstance(row[col], (bytes, bytearray)):
                            additional_info[col] = str(row[col])
                
                style_dict[row['neighbourhood_code']] = {
                    'color': 'black',
                    'fillColor': color,
                    'weight': 1,
                    'fillOpacity': 0.7,
                    'availability': f"{row['avg_bike_availability']:.1f}%",
                    'important_metrics': important_metrics,
                    'additional_info': additional_info
                }
        
        # If we have no data for this time, use data from the closest time we do have
        if not style_dict and len(date_data) > 0:
            # Find the closest time in the dataset to this hour_float
            avg_time = date_data['hour_float'].values
            closest_time_idx = np.abs(avg_time - hour_float).argmin()
            closest_hour = date_data.iloc[closest_time_idx]['hour_float']
            
            print(f"No data at {hour_float:.2f} hours, using closest time {closest_hour:.2f}")
            
            # Reuse logic from above with the closest time
            time_data = date_data.copy()
            time_data['time_diff'] = abs(time_data['hour_float'] - closest_hour)
            closest_times = time_data.loc[time_data.groupby('neighbourhood_code')['time_diff'].idxmin()]
            
            for _, row in closest_times.iterrows():
                if pd.notna(row['avg_bike_availability']):
                    color = colormap(row['avg_bike_availability'])
                    
                    # Create a dictionary of the most important metrics to display
                    important_metrics = {}
                    
                    # Add key metrics if they exist in the data
                    for key in ['total_bikes_available', 'total_ebikes', 'total_mechanical_bikes', 'station_count']:
                        if key in row and pd.notna(row[key]):
                            important_metrics[key] = int(row[key])
                    
                    # Collect other available information
                    additional_info = {}
                    # Include any other numeric/useful columns
                    for col in row.index:
                        if col not in ['neighbourhood_code', 'neighbourhood_name', 'avg_bike_availability', 
                                     'time_diff', 'date', 'time', 'hour_float', 'ten_min_datetime'] + list(important_metrics.keys()):
                            if pd.notna(row[col]) and not isinstance(row[col], (bytes, bytearray)):
                                additional_info[col] = str(row[col])
                    
                    style_dict[row['neighbourhood_code']] = {
                        'color': 'black',
                        'fillColor': color,
                        'weight': 1,
                        'fillOpacity': 0.7,
                        'availability': f"{row['avg_bike_availability']:.1f}%",
                        'important_metrics': important_metrics,
                        'additional_info': additional_info
                    }
        
        # Store data with exact string representation of the hour float
        hour_str = f"{hour_float:.4f}"
        all_date_time_data[date_str][hour_str] = style_dict

# Convert centroids to a dictionary for labels
centroids = {}
for _, row in neighborhoods_gdf.iterrows():
    centroids[row['neighbourhood_code']] = {
        'lat': row.geometry.centroid.y,
        'lng': row.geometry.centroid.x,
        'name': row['neighbourhood_name']
    }

# Create a completely self-contained HTML with embedded data
html_template = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Barcelona Bike Availability</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/leaflet@1.7.1/dist/leaflet.css" />
    <style>
        body, html {{
            height: 100%;
            margin: 0;
            padding: 0;
            font-family: Arial, sans-serif;
        }}
        #map {{
            height: 100%;
            width: 100%;
            position: absolute;
        }}
        .info {{
            padding: 6px 8px;
            background: white;
            background: rgba(255,255,255,0.8);
            box-shadow: 0 0 15px rgba(0,0,0,0.2);
            border-radius: 5px;
        }}
        .legend {{
            line-height: 18px;
            color: #555;
        }}
        .legend i {{
            width: 18px;
            height: 18px;
            float: left;
            margin-right: 8px;
            opacity: 0.7;
        }}
        .availLabel {{
            text-align: center;
            font-weight: bold;
            background-color: white;
            border-radius: 4px;
            padding: 2px 5px;
            opacity: 0.9;
            white-space: nowrap;
            box-shadow: 0 1px 3px rgba(0,0,0,0.3);
        }}
        #control-panel {{
            position: absolute;
            top: 10px;
            right: 10px;
            z-index: 1000;
            background-color: white;
            padding: 10px;
            border-radius: 5px;
            box-shadow: 0 1px 5px rgba(0,0,0,0.4);
            min-width: 250px;
        }}
        .control-row {{
            margin-bottom: 10px;
        }}
        .title {{
            position: absolute;
            top: 10px;
            left: 50px;
            z-index: 1000;
            background-color: white;
            padding: 10px;
            border-radius: 5px;
            box-shadow: 0 1px 5px rgba(0,0,0,0.4);
            font-weight: bold;
            font-size: 16px;
        }}
        /* Enhanced tooltip style */
        .custom-tooltip {{
            background-color: white;
            border: 1px solid #ccc;
            padding: 8px 12px;
            border-radius: 4px;
            box-shadow: 0 1px 5px rgba(0,0,0,0.4);
            font-size: 12px;
            max-width: 250px;
        }}
    </style>
</head>
<body>
    <div id="map"></div>
    <div class="title">Barcelona Bike Availability</div>
    <div id="control-panel">
        <div class="control-row">
            <label for="date-select">Select Date:</label>
            <select id="date-select" style="margin-left: 5px;">
                {date_options}
            </select>
        </div>
        <div class="control-row">
            <label for="time-slider">Time: <span id="time-value">12:00</span></label>
            <input type="range" id="time-slider" min="0" max="24" step="0.1667" value="12" style="width: 100%;">
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/leaflet@1.7.1/dist/leaflet.js"></script>
    <script>
        // All the data is embedded in the HTML
        var mapData = {map_data};
        var neighborhoods = {neighborhoods};
        var centroids = {centroids};
        
        // Get all possible time values
        var timeValues = [];
        var firstDate = Object.keys(mapData)[0];
        if (firstDate) {{
            timeValues = Object.keys(mapData[firstDate]);
        }}
        
        // Initialize map
        var map = L.map('map').setView([41.3851, 2.1734], 12);
        
        // Add base layer
        L.tileLayer('https://cartodb-basemaps-{{s}}.global.ssl.fastly.net/light_all/{{z}}/{{x}}/{{y}}.png', {{
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://cartodb.com/attributions">CartoDB</a>',
            subdomains: 'abcd',
            maxZoom: 19
        }}).addTo(map);
        
        // GeoJSON layer
        var geojsonLayer;
        var labelLayer = L.layerGroup().addTo(map);
        
        // Function to format time
        function formatTime(hourFloat) {{
            var hours = Math.floor(hourFloat);
            var minutes = Math.round((hourFloat - hours) * 60);
            return hours.toString().padStart(2, '0') + ':' + minutes.toString().padStart(2, '0');
        }}
        
        // Function to find closest time value
        function findClosestTime(targetHour) {{
            var closest = timeValues[0];
            var minDiff = Number.MAX_VALUE;
            
            for (var i = 0; i < timeValues.length; i++) {{
                var diff = Math.abs(parseFloat(timeValues[i]) - targetHour);
                if (diff < minDiff) {{
                    minDiff = diff;
                    closest = timeValues[i];
                }}
            }}
            
            return closest;
        }}
        
        // Function to update the map
        function updateMap() {{
            // Get selected date and time
            var selectedDate = document.getElementById('date-select').value;
            var sliderValue = parseFloat(document.getElementById('time-slider').value);
            
            // Find the closest time in our data
            var selectedHour = findClosestTime(sliderValue);
            
            // Update time display with the formatted slider value
            document.getElementById('time-value').textContent = formatTime(sliderValue);
            
            // Get style dictionary for selected date/time
            var styleDict = mapData[selectedDate][selectedHour] || {{}};
            
            // Remove old layers
            if (geojsonLayer) {{
                map.removeLayer(geojsonLayer);
            }}
            labelLayer.clearLayers();
            
            // Create new GeoJSON layer
            geojsonLayer = L.geoJson(neighborhoods, {{
                style: function(feature) {{
                    var code = feature.properties.neighbourhood_code;
                    if (styleDict[code]) {{
                        return {{
                            fillColor: styleDict[code].fillColor,
                            color: 'black',
                            weight: 1,
                            fillOpacity: 0.7
                        }};
                    }} else {{
                        return {{
                            fillColor: 'lightgrey',
                            color: 'black',
                            weight: 1,
                            fillOpacity: 0.7
                        }};
                    }}
                }},
                onEachFeature: function(feature, layer) {{
                    var code = feature.properties.neighbourhood_code;
                    var name = feature.properties.neighbourhood_name;
                    
                    // Enhanced tooltip with all available information
                    var tooltipContent = `<div style="font-weight:bold;font-size:14px;margin-bottom:8px;">${{name}}</div>`;
                    
                    if (styleDict[code]) {{
                        tooltipContent += `<div style="font-weight:bold;margin-bottom:5px;">Bike Availability: ${{styleDict[code].availability || 'N/A'}}</div>`;
                        
                        // Add important metrics first in a table format
                        if (styleDict[code].important_metrics && Object.keys(styleDict[code].important_metrics).length > 0) {{
                            tooltipContent += `<table style="width:100%;border-collapse:collapse;margin-bottom:8px;">`;
                            
                            // Format column names nicely
                            function formatColumnName(name) {{
                                return name
                                    .replace(/_/g, ' ')
                                    .replace(/\b\w/g, l => l.toUpperCase());
                            }}
                            
                            // Add important metrics in table rows
                            for (var key in styleDict[code].important_metrics) {{
                                tooltipContent += `<tr>
                                    <td style="padding:2px 0;"><b>${{formatColumnName(key)}}:</b></td>
                                    <td style="text-align:right;padding:2px 0;">${{styleDict[code].important_metrics[key]}}</td>
                                </tr>`;
                            }}
                            
                            tooltipContent += `</table>`;
                        }}
                        
                        // Add other available properties if they exist
                        if (styleDict[code].additional_info && Object.keys(styleDict[code].additional_info).length > 0) {{
                            tooltipContent += `<div style="font-size:11px;margin-top:5px;border-top:1px solid #eee;padding-top:5px;">`;
                            tooltipContent += `<div style="font-weight:bold;margin-bottom:3px;">Additional Information:</div>`;
                            
                            for (var key in styleDict[code].additional_info) {{
                                tooltipContent += `<div><b>${{formatColumnName(key)}}:</b> ${{styleDict[code].additional_info[key]}}</div>`;
                            }}
                            
                            tooltipContent += `</div>`;
                        }}
                    }} else {{
                        tooltipContent += '<div>No data available</div>';
                    }}
                    
                    layer.bindTooltip(tooltipContent, {{
                        sticky: true,
                        opacity: 0.9,
                        className: 'custom-tooltip'
                    }});
                }}
            }}).addTo(map);
            
            // Add labels
            for (var code in styleDict) {{
                if (centroids[code] && styleDict[code].availability) {{
                    var icon = L.divIcon({{
                        className: 'availLabel',
                        html: styleDict[code].availability,
                        iconSize: [40, 20],
                        iconAnchor: [20, 10]
                    }});
                    
                    L.marker([centroids[code].lat, centroids[code].lng], {{
                        icon: icon
                    }}).addTo(labelLayer);
                }}
            }}
        }}
        
        // Add event listeners
        document.getElementById('date-select').addEventListener('change', updateMap);
        document.getElementById('time-slider').addEventListener('input', function() {{
            document.getElementById('time-value').textContent = formatTime(parseFloat(this.value));
        }});
        document.getElementById('time-slider').addEventListener('change', updateMap);
        
        // Add legend
        var legend = L.control({{position: 'bottomright'}});
        legend.onAdd = function(map) {{
            var div = L.DomUtil.create('div', 'info legend');
            var grades = [0, 20, 40, 60, 80, 100];
            var colors = ['#f7fcb9', '#d9f0a3', '#addd8e', '#78c679', '#41ab5d', '#006837'];
            
            div.innerHTML = '<h4>Bike Availability (%)</h4>';
            
            for (var i = 0; i < grades.length; i++) {{
                div.innerHTML +=
                    '<i style="background:' + colors[i] + '"></i> ' +
                    grades[i] + (grades[i + 1] ? '&ndash;' + grades[i + 1] + '<br>' : '+');
            }}
            
            return div;
        }};
        legend.addTo(map);
        
        // Initial update
        setTimeout(updateMap, 100);
    </script>
</body>
</html>
"""

# Generate date options HTML
date_options_html = ""
for date in date_options:
    date_options_html += f'<option value="{date}">{date}</option>\n'

# Create the final HTML with all data embedded
final_html = html_template.format(
    date_options=date_options_html,
    map_data=json.dumps(all_date_time_data),
    neighborhoods=json.dumps(neighborhoods_geojson),
    centroids=json.dumps(centroids)
)

# Write the HTML file
with open(output_file, 'w') as f:
    f.write(final_html)

print(f"Interactive visualization saved to {output_file}")
print("Open this HTML file in a web browser to view the visualization") 