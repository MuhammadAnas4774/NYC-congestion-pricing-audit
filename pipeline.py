#!/usr/bin/env python3
"""
NYC Congestion Pricing Audit - Main Pipeline
Processes 2025 taxi data for congestion pricing analysis
"""

import sys
import os
from pathlib import Path
import pandas as pd
import dask.dataframe as dd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from scraper import TLCScraper

# Congestion Zone: Manhattan South of 60th St
CONGESTION_ZONE_IDS = [
    4, 12, 13, 24, 41, 42, 43, 45, 48, 50, 68, 74, 75, 79, 87, 88, 90, 100, 103,
    107, 113, 114, 116, 120, 125, 127, 128, 137, 140, 141, 142, 143, 144, 148, 151,
    152, 153, 158, 161, 162, 163, 164, 166, 170, 186, 194, 202, 209, 211, 224, 229,
    230, 231, 232, 233, 234, 236, 237, 238, 239, 243, 244, 246, 249, 261, 262, 263
]


def detect_ghost_trips(df):
    """Detect suspicious trips"""
    print("\n👻 Detecting ghost trips...")

    ghost_count = 0

    # 1. Impossible speed (>65 MPH)
    mask_speed = df['avg_speed'] > 65
    speed_count = mask_speed.sum()

    # 2. Teleporter (<1 min, >$20 fare)
    mask_teleport = (df['trip_duration'] < 1) & (df['fare'] > 20)
    teleport_count = mask_teleport.sum()

    # 3. Stationary (0 distance, >$0 fare)
    mask_stationary = (df['trip_distance'] == 0) & (df['fare'] > 0)
    stationary_count = mask_stationary.sum()

    # Combine
    ghost_mask = mask_speed | mask_teleport | mask_stationary

    ghost_df = df[ghost_mask].copy()
    clean_df = df[~ghost_mask].copy()

    print(f"   Impossible speed (>65 MPH): {speed_count:,}")
    print(f"   Teleporter (<1 min, >$20): {teleport_count:,}")
    print(f"   Stationary (0 mi, >$0): {stationary_count:,}")
    print(f"   Total removed: {len(ghost_df):,} ({len(ghost_df) / len(df) * 100:.2f}%)")

    return clean_df, ghost_df


def analyze_congestion_zone(df):
    """Analyze congestion zone trips"""
    print("\n🚕 Analyzing congestion zone...")

    # Flag zone trips
    df['pickup_in_zone'] = df['pickup_loc'].isin(CONGESTION_ZONE_IDS)
    df['dropoff_in_zone'] = df['dropoff_loc'].isin(CONGESTION_ZONE_IDS)
    df['enters_zone'] = (~df['pickup_in_zone']) & (df['dropoff_in_zone'])

    # Post-toll analysis (after Jan 5, 2025)
    post_toll = df[df['pickup_time'] >= '2025-01-05']
    zone_entries = post_toll[post_toll['enters_zone']]

    if len(zone_entries) > 0:
        compliant = zone_entries[zone_entries['congestion_surcharge'] > 0]
        compliance_rate = len(compliant) / len(zone_entries)

        print(f"   Zone entries after Jan 5: {len(zone_entries):,}")
        print(f"   Compliant (with surcharge): {len(compliant):,}")
        print(f"   Compliance rate: {compliance_rate:.2%}")

        # Top leakage locations
        non_compliant = zone_entries[zone_entries['congestion_surcharge'] == 0]
        if len(non_compliant) > 0:
            top_leakage = non_compliant.groupby('pickup_loc').size().nlargest(3)
            print(f"\n   🔍 Top 3 leakage pickup locations:")
            for loc, count in top_leakage.items():
                print(f"      Location {loc}: {count:,} trips")

    return df


def create_summary_stats(df):
    """Create summary statistics"""
    print("\n📊 Summary Statistics:")
    print(f"   Total trips: {len(df):,}")
    print(f"   Average fare: ${df['fare'].mean():.2f}")
    print(f"   Average trip distance: {df['trip_distance'].mean():.2f} miles")
    print(f"   Average speed: {df['avg_speed'].mean():.1f} MPH")
    print(f"   Total revenue: ${df['total_amount'].sum():,.2f}")

    if 'congestion_surcharge' in df.columns:
        total_surcharge = df['congestion_surcharge'].sum()
        print(f"   Total congestion surcharge: ${total_surcharge:,.2f}")


def main():
    print("=" * 60)
    print("NYC Congestion Pricing Audit - Full Pipeline")
    print("=" * 60)

    scraper = TLCScraper()

    # Phase 1: Download and process data
    print("\n📥 Phase 1: Data Ingestion")

    # Download January 2025 (you have this)
    file = scraper.download_month(2025, 1, "yellow")

    if not file:
        print("❌ Download failed")
        return

    # Process the file
    processed = scraper.process_file(file)

    # Phase 2: Analysis
    print("\n🔍 Phase 2: Analysis")

    # Read processed data
    print("   Reading processed data...")
    df = pd.read_parquet(processed)
    print(f"   Loaded {len(df):,} rows")

    # Ghost trip detection
    clean_df, ghost_df = detect_ghost_trips(df)

    # Congestion zone analysis
    analyzed_df = analyze_congestion_zone(clean_df)

    # Summary
    create_summary_stats(analyzed_df)

    # Save results
    print("\n💾 Saving results...")
    clean_df.to_parquet("outputs/clean_data.parquet", index=False)
    ghost_df.to_parquet("outputs/ghost_trips.parquet", index=False)

    print("\n" + "=" * 60)
    print("✅ Pipeline Complete!")
    print("=" * 60)
    print("📁 Output files:")
    print("   - outputs/clean_data.parquet")
    print("   - outputs/ghost_trips.parquet")
    print("\n🚀 Next: Run 'python dashboard.py' to see visualizations")


if __name__ == "__main__":
    # Create outputs directory
    Path("outputs").mkdir(exist_ok=True)
    main()