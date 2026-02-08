import requests
import os
from pathlib import Path
import pandas as pd
import dask.dataframe as dd


class TLCScraper:
    def __init__(self, download_dir="data/raw", processed_dir="data/processed"):
        self.download_dir = Path(download_dir)
        self.processed_dir = Path(processed_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    def download_month(self, year, month, taxi_type="yellow"):
        """Download a single month"""
        month_str = f"{month:02d}"
        filename = f"{taxi_type}_tripdata_{year}-{month_str}.parquet"
        url = f"{self.base_url}{filename}"
        filepath = self.download_dir / filename

        if filepath.exists():
            print(f"⏩ {filename} already exists")
            return filepath

        print(f"⬇️  Downloading {filename}...")
        try:
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()

            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"✅ Downloaded: {filename}")
            return filepath
        except Exception as e:
            print(f"❌ Error: {e}")
            return None

    def process_file(self, filepath):
        """Process parquet file with Dask (memory efficient)"""
        print(f"⚙️  Processing {filepath.name}...")

        # Use Dask for big data processing
        ddf = dd.read_parquet(filepath)

        # Show columns
        print(f"   Columns: {list(ddf.columns)}")

        # Basic cleaning - rename columns to standard names
        column_map = {}
        if 'tpep_pickup_datetime' in ddf.columns:
            column_map['tpep_pickup_datetime'] = 'pickup_time'
        if 'tpep_dropoff_datetime' in ddf.columns:
            column_map['tpep_dropoff_datetime'] = 'dropoff_time'
        if 'lpep_pickup_datetime' in ddf.columns:
            column_map['lpep_pickup_datetime'] = 'pickup_time'
        if 'lpep_dropoff_datetime' in ddf.columns:
            column_map['lpep_dropoff_datetime'] = 'dropoff_time'
        if 'PULocationID' in ddf.columns:
            column_map['PULocationID'] = 'pickup_loc'
        if 'DOLocationID' in ddf.columns:
            column_map['DOLocationID'] = 'dropoff_loc'
        if 'fare_amount' in ddf.columns:
            column_map['fare_amount'] = 'fare'
        if 'tip_amount' in ddf.columns:
            column_map['tip_amount'] = 'tip'

        ddf = ddf.rename(columns=column_map)

        # Keep only needed columns
        keep_cols = ['pickup_time', 'dropoff_time', 'pickup_loc', 'dropoff_loc',
                     'trip_distance', 'fare', 'total_amount', 'congestion_surcharge', 'tip']
        keep_cols = [c for c in keep_cols if c in ddf.columns]
        ddf = ddf[keep_cols]

        # Add computed columns
        ddf['pickup_time'] = dd.to_datetime(ddf['pickup_time'])
        ddf['hour'] = ddf['pickup_time'].dt.hour
        ddf['day_of_week'] = ddf['pickup_time'].dt.dayofweek
        ddf['month'] = ddf['pickup_time'].dt.month

        # Calculate trip duration and speed
        ddf['trip_duration'] = (ddf['dropoff_time'] - ddf['pickup_time']).dt.total_seconds() / 60
        ddf['avg_speed'] = ddf['trip_distance'] / (ddf['trip_duration'] / 60)

        # Save processed file
        output_name = self.processed_dir / f"processed_{filepath.stem}.parquet"
        ddf.to_parquet(output_name, overwrite=True)
        print(f"💾 Saved: {output_name}")

        return output_name


if __name__ == "__main__":
    scraper = TLCScraper()
    file = scraper.download_month(2025, 1, "yellow")
    if file:
        scraper.process_file(file)