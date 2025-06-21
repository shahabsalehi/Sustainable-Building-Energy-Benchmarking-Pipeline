"""
ETL Pipeline for HVAC Data

This module implements the Extract, Transform, Load pipeline for HVAC sensor data.
It includes data cleaning, feature engineering, and preprocessing for anomaly detection.
"""

import pandas as pd
import numpy as np
import os


def load_raw_data(filepath: str) -> pd.DataFrame:
    """
    Extract: Load raw HVAC data.
    
    Args:
        filepath: Path to raw data file (parquet or csv)
        
    Returns:
        DataFrame with raw HVAC data
    """
    print(f"Loading data from {filepath}...")
    
    if filepath.endswith('.parquet'):
        df = pd.read_parquet(filepath)
    elif filepath.endswith('.csv'):
        df = pd.read_csv(filepath)
    else:
        raise ValueError("File must be .parquet or .csv")
    
    print(f"Loaded {len(df)} records")
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform: Clean and validate HVAC data.
    
    Args:
        df: Raw DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    print("Cleaning data...")
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Sort by timestamp and zone
    df = df.sort_values(['zone_id', 'timestamp']).reset_index(drop=True)
    
    # Check for missing values
    missing = df.isnull().sum()
    if missing.any():
        print(f"Warning: Found missing values:\n{missing[missing > 0]}")
        # Forward fill within each zone
        df = df.groupby('zone_id', group_keys=False).ffill().reset_index(drop=True)
    
    # Remove any remaining nulls at the start of zones
    initial_nulls = df.isnull().sum().sum()
    if initial_nulls > 0:
        df = df.dropna()
        print(f"Dropped {initial_nulls} rows with remaining nulls")
    
    print(f"Cleaned data: {len(df)} records")
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform: Create features for anomaly detection.
    
    Creates:
    - temp_error_c: deviation from setpoint
    - Rolling statistics (15-min and 60-min windows)
    - Lag features (5-min back)
    - Delta features (return-supply temperature)
    
    Args:
        df: Cleaned DataFrame
        
    Returns:
        DataFrame with engineered features
    """
    print("Engineering features...")
    
    df = df.copy()
    
    # 1. Temperature error from setpoint
    df['temp_error_c'] = df['temp_zone_c'] - df['setpoint_c']
    
    # 2. Delta features
    df['delta_return_supply'] = df['return_air_temp_c'] - df['supply_air_temp_c']
    
    # Process each zone separately for time-series features
    feature_dfs = []
    
    for zone in df['zone_id'].unique():
        zone_df = df[df['zone_id'] == zone].copy()
        
        # 3. Rolling statistics (15-min = 3 periods, 60-min = 12 periods at 5-min freq)
        # Temperature error rolling stats
        zone_df['temp_error_rolling_mean_15min'] = zone_df['temp_error_c'].rolling(
            window=3, min_periods=1
        ).mean()
        zone_df['temp_error_rolling_std_15min'] = zone_df['temp_error_c'].rolling(
            window=3, min_periods=1
        ).std().fillna(0)
        
        zone_df['temp_error_rolling_mean_60min'] = zone_df['temp_error_c'].rolling(
            window=12, min_periods=1
        ).mean()
        zone_df['temp_error_rolling_std_60min'] = zone_df['temp_error_c'].rolling(
            window=12, min_periods=1
        ).std().fillna(0)
        
        # Power rolling stats
        zone_df['power_rolling_mean_15min'] = zone_df['power_kw'].rolling(
            window=3, min_periods=1
        ).mean()
        zone_df['power_rolling_std_15min'] = zone_df['power_kw'].rolling(
            window=3, min_periods=1
        ).std().fillna(0)
        
        zone_df['power_rolling_mean_60min'] = zone_df['power_kw'].rolling(
            window=12, min_periods=1
        ).mean()
        zone_df['power_rolling_std_60min'] = zone_df['power_kw'].rolling(
            window=12, min_periods=1
        ).std().fillna(0)
        
        # Fan speed rolling stats
        zone_df['fan_rolling_mean_15min'] = zone_df['fan_speed_pct'].rolling(
            window=3, min_periods=1
        ).mean()
        
        # 4. Lag features (1 period = 5 minutes back)
        zone_df['temp_zone_c_lag1'] = zone_df['temp_zone_c'].shift(1)
        zone_df['power_kw_lag1'] = zone_df['power_kw'].shift(1)
        zone_df['fan_speed_pct_lag1'] = zone_df['fan_speed_pct'].shift(1)
        
        # 5. Rate of change features
        zone_df['temp_change_rate'] = zone_df['temp_zone_c'].diff()
        zone_df['power_change_rate'] = zone_df['power_kw'].diff()
        
        # Fill NaN values created by lag and diff operations
        zone_df = zone_df.bfill().fillna(0)
        
        feature_dfs.append(zone_df)
    
    # Combine all zones
    df_features = pd.concat(feature_dfs, ignore_index=True)
    
    # Round numeric columns
    numeric_cols = df_features.select_dtypes(include=[np.number]).columns
    df_features[numeric_cols] = df_features[numeric_cols].round(4)
    
    print(f"Created features: {len(df_features.columns)} total columns")
    print(f"New features: {len(df_features.columns) - len(df.columns)} added")
    
    return df_features


def save_processed_data(df: pd.DataFrame, output_path: str) -> None:
    """
    Load: Save processed data.
    
    Args:
        df: Processed DataFrame
        output_path: Path to save processed data
    """
    print(f"Saving processed data to {output_path}...")
    
    # Create output directory if needed
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to parquet
    df.to_parquet(output_path, index=False)
    print(f"✓ Saved {len(df)} records to {output_path}")
    
    # Also save summary statistics
    summary_path = output_path.replace('.parquet', '_summary.csv')
    
    # Generate feature summary
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    summary = df[numeric_cols].describe().T
    summary.to_csv(summary_path)
    print(f"✓ Saved summary statistics to {summary_path}")


def run_etl_pipeline(
    input_path: str = "data/raw/hvac_raw.parquet",
    output_path: str = "data/processed/hvac_features.parquet"
) -> pd.DataFrame:
    """
    Execute the complete ETL pipeline.
    
    Args:
        input_path: Path to raw data
        output_path: Path to save processed data
        
    Returns:
        Processed DataFrame with features
    """
    print("=" * 60)
    print("HVAC Data ETL Pipeline")
    print("=" * 60)
    print()
    
    # Extract
    df = load_raw_data(input_path)
    
    # Transform - Clean
    df = clean_data(df)
    
    # Transform - Feature Engineering
    df = engineer_features(df)
    
    # Load
    save_processed_data(df, output_path)
    
    print()
    print("=" * 60)
    print("ETL Pipeline Complete!")
    print("=" * 60)
    print()
    print("Feature Summary:")
    print(f"  Total records: {len(df)}")
    print(f"  Total features: {len(df.columns)}")
    print(f"  Zones: {df['zone_id'].nunique()}")
    print(f"  Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"  Fault distribution:")
    print(df['fault_type'].value_counts().to_string())
    print()
    
    # Display sample
    print("Sample of processed data:")
    feature_cols = ['timestamp', 'zone_id', 'temp_zone_c', 'setpoint_c', 
                   'temp_error_c', 'temp_error_rolling_mean_60min',
                   'power_kw', 'power_rolling_mean_60min', 'fault_type']
    print(df[feature_cols].head(10))
    
    return df


def main():
    """Main function to run ETL pipeline."""
    df = run_etl_pipeline()
    return df


if __name__ == "__main__":
    main()
