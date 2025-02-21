import pandas as pd
from sqlalchemy import create_engine
import pymysql
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

# MySQL Connection
MYSQL_USER = os.getenv("MYSQL_USER", "your_username")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "your_password")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_DATABASE = "nyc_311"

# File Path
CSV_FILE = "C:/Users/mitch/Desktop/nyc-311-etl/nyc311data.csv"

# Load in chunks
chunksize = 100000
engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}")

total_imported = 0
for chunk in pd.read_csv(CSV_FILE, chunksize=chunksize, low_memory=False):
    chunk['Created Date'] = pd.to_datetime(chunk['Created Date'], errors='coerce', format='%m/%d/%Y %I:%M:%S %p')
    chunk = chunk[['Created Date', 'Agency', 'Agency Name', 'Complaint Type', 'Descriptor',
                   'Incident Zip', 'Borough', 'Resolution Description']]
    chunk.columns = ['created_date', 'agency', 'agency_name', 'complaint_type', 'descriptor',
                     'incident_zip', 'borough', 'resolution_description']
    chunk.to_sql('service_requests', con=engine, if_exists='append', index=False)
    total_imported += len(chunk)
    if total_imported % 100000 == 0:
        print(f"Imported {total_imported} rows so far...")
print(f"Import Complete: {total_imported} rows loaded into MySQL.")

# MySQL connection for analysis
engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}")

# Complaints by Year and Month
query = """
SELECT 
    YEAR(created_date) AS year, 
    MONTH(created_date) AS month, 
    COUNT(*) AS complaint_count
FROM service_requests
GROUP BY year, month
ORDER BY year, month;
"""
df = pd.read_sql(query, engine)

def get_season(month):
    if month in [12, 1, 2]:
        return 'Winter'
    elif month in [3, 4, 5]:
        return 'Spring'
    elif month in [6, 7, 8]:
        return 'Summer'
    elif month in [9, 10, 11]:
        return 'Fall'

df['season'] = df['month'].apply(get_season)
seasonality = df.groupby(['year', 'season']).agg({'complaint_count': 'sum'}).reset_index()

# Colors for the seasons
season_colors = {
    'Winter': 'blue',
    'Spring': 'green',
    'Summer': 'orange',
    'Fall': 'brown'
}

plt.figure(figsize=(12, 6))
for season in ['Winter', 'Spring', 'Summer', 'Fall']:
    season_data = seasonality[seasonality['season'] == season]
    sns.lineplot(data=season_data, x='year', y='complaint_count', label=season, color=season_colors[season], marker='o')
plt.title('Seasonality of Complaints by Year')
plt.xlabel('Year')
plt.ylabel('Complaint Count')
plt.legend(title='Season')
plt.grid(True)
plt.show()

# Complaint Types Breakdown
query = """
SELECT complaint_type, COUNT(*) AS complaint_count
FROM service_requests
GROUP BY complaint_type
ORDER BY complaint_count DESC;
"""
df = pd.read_sql(query, engine)
df_top = df.head(10)

plt.figure(figsize=(10, 6))
plt.pie(df_top['complaint_count'], labels=df_top['complaint_type'], autopct='%1.1f%%', startangle=140)
plt.title('Top 10 Complaint Types')
plt.axis('equal')
plt.show()

# Complaints by Borough
query_borough = """
SELECT borough, COUNT(*) AS complaint_count
FROM service_requests
GROUP BY borough
ORDER BY complaint_count DESC;
"""
df_borough = pd.read_sql(query_borough, engine)
df_borough = df_borough[df_borough['borough'].notna() & (df_borough['borough'] != '')]

plt.figure(figsize=(10, 6))
plt.pie(df_borough['complaint_count'], labels=df_borough['borough'], autopct='%1.1f%%', startangle=140)
plt.title('Complaints by Borough')
plt.axis('equal')
plt.show()

# Year-over-Year Complaint Trends
query = """
WITH YearlyComplaints AS (
    SELECT
        YEAR(created_date) AS year,
        MONTH(created_date) AS month,
        COUNT(*) AS complaint_count,
        LAG(COUNT(*)) OVER (PARTITION BY MONTH(created_date) ORDER BY YEAR(created_date)) AS prev_year_count
    FROM service_requests
    GROUP BY year, month
)
SELECT
    year,
    month,
    complaint_count,
    prev_year_count,
    ROUND((complaint_count - prev_year_count) / prev_year_count * 100, 2) AS pct_change
FROM YearlyComplaints
WHERE prev_year_count IS NOT NULL
ORDER BY year, month;
"""
df = pd.read_sql(query, engine)
df['month'] = df['month'].apply(lambda x: pd.to_datetime(str(x), format='%m').strftime('%B'))

fig, ax = plt.subplots(figsize=(12, 8))
ax.axis("tight")
ax.axis("off")
table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc="center", loc="center")
plt.savefig("complaints_table.png", dpi=300, bbox_inches="tight")
plt.show()
