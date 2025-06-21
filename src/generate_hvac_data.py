"""
Synthetic HVAC Data Generator

Generates realistic HVAC sensor data with labeled fault episodes for
anomaly detection testing and training.
"""

import numpy as np
import pandas as pd
import os


def generate_base_profile(start, end, freq="5min", n_zones=10, seed=42):
    """
    Generate base HVAC sensor data with realistic daily patterns.
    
    Args:
        start: Start datetime string
        end: End datetime string
        freq: Frequency of data points (default: "5min")
        n_zones: Number of zones to simulate (default: 10)
        seed: Random seed for reproducibility
        
    Returns:
        DataFrame with base HVAC sensor data
    """
    rng = np.random.default_rng(seed)
    timestamps = pd.date_range(start, end, freq=freq)
    rows = []
    zone_ids = [f"Z{i+1}" for i in range(n_zones)]
    
    for zone in zone_ids:
        # Each zone has a slightly different baseline temperature
        base_offset = rng.normal(0, 0.5)
        
        for ts in timestamps:
            # Calculate time-of-day factor for load
            hour = ts.hour + ts.minute / 60
            
            # Higher load during business hours (8 AM - 6 PM)
            if 8 <= hour <= 18:
                day_factor = 1.5
                mode = "cooling"
            else:
                day_factor = 0.5
                mode = "off"
            
            # Add weekend variation (lower load)
            if ts.dayofweek >= 5:  # Saturday=5, Sunday=6
                day_factor *= 0.6
            
            # Generate base sensor readings
            setpoint = 22.0 + base_offset
            
            # Zone temperature with some noise and load influence
            temp = setpoint + rng.normal(0, 0.3) + (day_factor - 1) * 1.0
            
            # Relative humidity
            rh = 45 + rng.normal(0, 5)
            rh = np.clip(rh, 30, 60)
            
            # Supply air temperature (cooler when cooling)
            supply = 14 + rng.normal(0, 0.5) if mode == "cooling" else 18 + rng.normal(0, 0.5)
            
            # Return air temperature (warmer than supply)
            ret = temp + rng.normal(1.0, 0.3)
            
            # Power consumption (higher during high load)
            power = max(0, 5 * day_factor + rng.normal(0, 0.3))
            
            # Fan speed (higher during high load)
            fan = np.clip(40 * day_factor + rng.normal(0, 5), 10, 90)
            
            rows.append({
                "timestamp": ts,
                "zone_id": zone,
                "ahu_id": "AHU1",
                "temp_zone_c": round(temp, 2),
                "rh_zone_pct": round(rh, 2),
                "supply_air_temp_c": round(supply, 2),
                "return_air_temp_c": round(ret, 2),
                "power_kw": round(power, 2),
                "fan_speed_pct": round(fan, 2),
                "setpoint_c": round(setpoint, 2),
                "mode": mode,
                "fault_type": "none",
            })
    
    return pd.DataFrame(rows)


def inject_faults(df):
    """
    Inject labeled fault episodes into the HVAC data.
    
    Implements 4 fault types:
    1. Clogged filter - gradual increase in fan speed and power
    2. Compressor failure - sudden drop in power, supply temp increases
    3. Temperature drift - zone temp deviates from setpoint
    4. Oscillating control - high frequency temperature oscillations
    
    Args:
        df: DataFrame with base HVAC data
        
    Returns:
        DataFrame with injected faults
    """
    df = df.copy()
    rng = np.random.default_rng(42)
    
    # Get unique zones and timestamps
    zones = df['zone_id'].unique()
    timestamps = pd.to_datetime(df['timestamp']).unique()
    
    # Inject 3-5 episodes of each fault type
    n_episodes_per_fault = 4
    
    # 1. CLOGGED FILTER
    # Gradual increase in fan_speed_pct and power_kw, increase in return_air_temp_c
    print("Injecting clogged_filter faults...")
    for episode in range(n_episodes_per_fault):
        zone = rng.choice(zones)
        # Choose a random day in the middle of the dataset
        day_start = 5 + episode * 6  # Spread episodes across the month
        
        # Get timestamps for this zone and day (use 8 hours during business hours)
        start_ts = timestamps[0] + pd.Timedelta(days=day_start, hours=9)
        end_ts = start_ts + pd.Timedelta(hours=8)
        
        mask = (df['zone_id'] == zone) & \
               (pd.to_datetime(df['timestamp']) >= start_ts) & \
               (pd.to_datetime(df['timestamp']) <= end_ts)
        
        n_points = mask.sum()
        if n_points > 0:
            # Gradual increase over the episode
            progression = np.linspace(0, 1, n_points)
            
            df.loc[mask, 'fan_speed_pct'] += progression * 20  # Increase up to 20%
            df.loc[mask, 'power_kw'] += progression * 2  # Increase up to 2 kW
            df.loc[mask, 'return_air_temp_c'] += progression * 1.5  # Increase up to 1.5°C
            df.loc[mask, 'fault_type'] = 'clogged_filter'
    
    # 2. COMPRESSOR FAILURE
    # Sudden drop in power_kw, supply_air_temp_c increases, zone temp drifts up
    print("Injecting compressor_failure faults...")
    for episode in range(n_episodes_per_fault):
        zone = rng.choice(zones)
        day_start = 3 + episode * 6
        
        start_ts = timestamps[0] + pd.Timedelta(days=day_start, hours=10)
        end_ts = start_ts + pd.Timedelta(hours=6)
        
        mask = (df['zone_id'] == zone) & \
               (pd.to_datetime(df['timestamp']) >= start_ts) & \
               (pd.to_datetime(df['timestamp']) <= end_ts) & \
               (df['mode'] == 'cooling')
        
        n_points = mask.sum()
        if n_points > 0:
            # Sudden failure - immediate impact
            df.loc[mask, 'power_kw'] *= 0.3  # Drop to 30% of normal
            df.loc[mask, 'supply_air_temp_c'] += 5  # Supply temp increases significantly
            
            # Zone temp gradually increases
            progression = np.linspace(0, 1, n_points)
            df.loc[mask, 'temp_zone_c'] += progression * 4  # Drift up to 4°C
            df.loc[mask, 'fault_type'] = 'compressor_failure'
    
    # 3. TEMPERATURE DRIFT
    # temp_zone_c deviates from setpoint_c by > 3°C for extended period
    print("Injecting temp_drift faults...")
    for episode in range(n_episodes_per_fault):
        zone = rng.choice(zones)
        day_start = 4 + episode * 6
        
        start_ts = timestamps[0] + pd.Timedelta(days=day_start, hours=11)
        end_ts = start_ts + pd.Timedelta(hours=10)
        
        mask = (df['zone_id'] == zone) & \
               (pd.to_datetime(df['timestamp']) >= start_ts) & \
               (pd.to_datetime(df['timestamp']) <= end_ts)
        
        if mask.sum() > 0:
            # Consistent drift from setpoint
            drift_amount = 3.5 + rng.uniform(0, 1.5)  # Drift by 3.5-5°C
            df.loc[mask, 'temp_zone_c'] += drift_amount
            df.loc[mask, 'fault_type'] = 'temp_drift'
    
    # 4. OSCILLATING CONTROL
    # Temperature oscillates around setpoint with high frequency
    print("Injecting oscillating_control faults...")
    for episode in range(n_episodes_per_fault):
        zone = rng.choice(zones)
        day_start = 2 + episode * 6
        
        start_ts = timestamps[0] + pd.Timedelta(days=day_start, hours=8)
        end_ts = start_ts + pd.Timedelta(hours=6)
        
        mask = (df['zone_id'] == zone) & \
               (pd.to_datetime(df['timestamp']) >= start_ts) & \
               (pd.to_datetime(df['timestamp']) <= end_ts)
        
        n_points = mask.sum()
        if n_points > 0:
            # Create oscillating pattern
            time_points = np.linspace(0, 4 * np.pi, n_points)
            oscillation = 2 * np.sin(time_points)  # Oscillate ±2°C
            
            df.loc[mask, 'temp_zone_c'] += oscillation
            df.loc[mask, 'fan_speed_pct'] += oscillation * 5  # Fan also oscillates
            df.loc[mask, 'fault_type'] = 'oscillating_control'
    
    # Ensure values stay within realistic bounds
    df['fan_speed_pct'] = df['fan_speed_pct'].clip(0, 100)
    df['power_kw'] = df['power_kw'].clip(0, None)
    df['temp_zone_c'] = df['temp_zone_c'].clip(15, 35)
    df['supply_air_temp_c'] = df['supply_air_temp_c'].clip(10, 25)
    
    # Round to 2 decimal places
    numeric_cols = ['temp_zone_c', 'rh_zone_pct', 'supply_air_temp_c', 
                   'return_air_temp_c', 'power_kw', 'fan_speed_pct', 'setpoint_c']
    df[numeric_cols] = df[numeric_cols].round(2)
    
    return df


def main():
    """
    Main function to generate HVAC data with faults.
    """
    print("=" * 60)
    print("HVAC Synthetic Data Generator")
    print("=" * 60)
    
    # Generate 30 days of data at 5-minute intervals for 10 zones
    print("\nGenerating base HVAC profile...")
    df = generate_base_profile(
        start="2024-01-01 00:00",
        end="2024-01-30 23:55",
        freq="5min",
        n_zones=10,
        seed=42
    )
    
    print(f"Generated {len(df)} base records")
    print(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Zones: {df['zone_id'].unique()}")
    
    # Inject faults
    print("\nInjecting fault episodes...")
    df = inject_faults(df)
    
    # Display fault statistics
    print("\n" + "=" * 60)
    print("Fault Statistics:")
    print("=" * 60)
    fault_counts = df['fault_type'].value_counts()
    print(fault_counts)
    print(f"\nTotal records with faults: {len(df[df['fault_type'] != 'none'])}")
    print(f"Fault percentage: {100 * len(df[df['fault_type'] != 'none']) / len(df):.2f}%")
    
    # Save to parquet
    output_dir = "data/raw"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "hvac_raw.parquet")
    
    df.to_parquet(output_path, index=False)
    print(f"\n✓ Data saved to: {output_path}")
    
    # Also save as CSV for easy inspection
    csv_path = os.path.join(output_dir, "hvac_raw.csv")
    df.to_csv(csv_path, index=False)
    print(f"✓ CSV saved to: {csv_path}")
    
    # Display sample
    print("\n" + "=" * 60)
    print("Sample of generated data:")
    print("=" * 60)
    print(df.head(10))
    
    # Display sample of each fault type
    print("\n" + "=" * 60)
    print("Sample records by fault type:")
    print("=" * 60)
    for fault_type in df['fault_type'].unique():
        if fault_type != 'none':
            sample = df[df['fault_type'] == fault_type].head(2)
            print(f"\n{fault_type}:")
            print(sample[['timestamp', 'zone_id', 'temp_zone_c', 'setpoint_c', 
                         'power_kw', 'fan_speed_pct', 'fault_type']])
    
    print("\n" + "=" * 60)
    print("Data generation complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
