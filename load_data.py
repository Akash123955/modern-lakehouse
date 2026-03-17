import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

conn = snowflake.connector.connect(
    account='tyczfui-mhc52087',
    user='nyc_taxi_user',
    password='Lakehouse@2024!',
    warehouse='nyc_taxi_wh',
    database='nyc_taxi_lakehouse',
    schema='raw_data',
    role='nyc_taxi_role'
)
cur = conn.cursor()

# Load zones CSV
print("Loading taxi zones...")
df_zones = pd.read_csv('/Users/satishvarma/Desktop/modern-lakehouse/data/taxi_zone_lookup.csv')
df_zones.columns = ['LOCATIONID', 'BOROUGH', 'ZONE', 'SERVICE_ZONE']
write_pandas(conn, df_zones, 'NYC_TAXI_ZONES', quote_identifiers=False)
print(f"Zones loaded: {len(df_zones)} rows")

# Load trips parquet
print("Loading taxi trips (this may take a minute)...")
df_trips = pd.read_parquet('/Users/satishvarma/Desktop/modern-lakehouse/data/yellow_tripdata_2024-01.parquet')
df_trips.columns = [c.upper() for c in df_trips.columns]
write_pandas(conn, df_trips, 'NYC_TAXI_TRIPS', quote_identifiers=False)
print(f"Trips loaded: {len(df_trips)} rows")

cur.execute("SELECT COUNT(*) FROM nyc_taxi_trips")
print(f"Verified rows in Snowflake: {cur.fetchone()[0]}")

conn.close()
print("Done!")
