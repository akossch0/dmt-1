#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sqlalchemy import create_engine, text
import os

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

# Read the SQL query file
sql_file_path = os.path.join('src', 'kpi', 'station_capacity_per_capita.sql')
with open(sql_file_path, 'r') as f:
    sql_query = f.read()

# Execute query and load results into DataFrame
df = pd.read_sql_query(sql=text(sql_query), con=engine)

# Print data summary to verify
print(f"Loaded {len(df)} districts from database")
print(df.columns.tolist())

# Set a clean style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette('viridis')

# Create a figure
fig, ax = plt.subplots(figsize=(14, 10))

# Define color mapping for equity status
status_colors = {
    'Potential Underservice': '#e74c3c',  # red
    'Fair Distribution': '#3498db',       # blue
    'Privileged Access': '#2ecc71',       # green
    'Good Service Despite Low Income': '#9b59b6',  # purple
    'Poor Service Despite High Income': '#f39c12',   # orange
    'Moderate Distribution': '#7f8c8d'    # gray
}

# Apply colors to the dataframe
df['color'] = df['equity_status'].map(status_colors)

# Income vs Bike Capacity (normalized by population density) Scatter Plot
scatter = ax.scatter(
    df['avg_income'], 
    df['capacity_per_1000_inhabitants_per_sqkm'],
    c=df['equity_status'].map(status_colors),
    s=df['total_population']/500,
    alpha=0.7,
    edgecolors='w'
)

# Add district labels
for i, row in df.iterrows():
    ax.annotate(row['district_name'], 
                (row['avg_income'], row['capacity_per_1000_inhabitants_per_sqkm']),
                xytext=(5, 5), textcoords='offset points')

# Add city average reference lines
avg_income = df['avg_income'].mean()
avg_capacity_density = df['capacity_per_1000_inhabitants_per_sqkm'].mean()

# Define a "fair distribution" zone (± 10% around the averages)
fair_income_low = avg_income * 0.9
fair_income_high = avg_income * 1.1
fair_capacity_low = avg_capacity_density * 0.9
fair_capacity_high = avg_capacity_density * 1.1

# Draw the fair distribution zone
fair_rect = plt.Rectangle((fair_income_low, fair_capacity_low), 
                         fair_income_high - fair_income_low, 
                         fair_capacity_high - fair_capacity_low,
                         color='lightgray', alpha=0.3, zorder=0)
ax.add_patch(fair_rect)
ax.text(avg_income, avg_capacity_density*0.95, 'Fair Distribution Zone', 
        fontsize=9, ha='center', va='top', 
        bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', boxstyle='round,pad=0.2'))

# Draw the reference lines
ax.axhline(y=avg_capacity_density, color='gray', linestyle='--', alpha=0.5)
ax.axvline(x=avg_income, color='gray', linestyle='--', alpha=0.5)

# Add quadrant labels - moved further away from the center and adjusted position
ax.text(avg_income*1.15, avg_capacity_density*1.15, 'High Income, High Capacity', 
         fontsize=10, ha='left', va='bottom', bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', boxstyle='round,pad=0.3'))
ax.text(avg_income*0.85, avg_capacity_density*1.15, 'Low Income, High Capacity', 
         fontsize=10, ha='right', va='bottom', bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', boxstyle='round,pad=0.3'))
ax.text(avg_income*1.15, avg_capacity_density*0.85, 'High Income, Low Capacity', 
         fontsize=10, ha='left', va='top', bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', boxstyle='round,pad=0.3'))
ax.text(avg_income*0.85, avg_capacity_density*0.85, 'Low Income, Low Capacity', 
         fontsize=10, ha='right', va='top', bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', boxstyle='round,pad=0.3'))

# Create a legend for district sizes - moved to upper left outside the main plot
handles, labels = scatter.legend_elements(prop="sizes", alpha=0.6, 
                                         func=lambda s: s*1000, 
                                         num=4)
legend1 = ax.legend(handles, labels, loc="upper left", bbox_to_anchor=(1.02, 0.5), title="Population")

# Create a legend for equity status - positioned below the first legend
equity_elements = [plt.Line2D([0], [0], marker='o', color='w', 
                             markerfacecolor=color, markersize=10) 
                  for status, color in status_colors.items()]
legend2 = ax.legend(equity_elements, status_colors.keys(), 
                    loc="upper left", bbox_to_anchor=(1.02, 1.0), title="Equity Status")
ax.add_artist(legend1)

ax.set_xlabel('Average Income (€)', fontsize=12)
ax.set_ylabel('Bike Capacity per Population Density', fontsize=12)
ax.set_title('Income vs. Bike Capacity (Normalized by Population Density)', fontsize=14)

# Adjust layout to make room for legends on the right
plt.tight_layout()
plt.subplots_adjust(right=0.8)

# Save the figure
output_path = 'src/visualization/outputs/bicing_equity_analysis.png'
plt.savefig(output_path, dpi=300, bbox_inches='tight')
print(f"Visualization saved to: {output_path}")
plt.show() 