"""
Tests for ETL pipeline module.
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os
import tempfile

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline_batch import (
    clean_data, engineer_features, 
    save_processed_data, run_etl_pipeline
)
from src.generate_hvac_data import generate_base_profile, inject_faults


@pytest.fixture
def sample_hvac_data():
    """Create sample HVAC data for testing."""
    df = generate_base_profile(
        start="2024-01-01 00:00",
        end="2024-01-01 23:55",
        freq="5min",
        n_zones=2,
        seed=42
    )
    return df


def test_clean_data(sample_hvac_data):
    """Test data cleaning functionality."""
    df = sample_hvac_data.copy()
    
    # Clean the data
    cleaned = clean_data(df)
    
    # Check that data is sorted
    assert cleaned['timestamp'].is_monotonic_increasing or \
           all(cleaned.groupby('zone_id')['timestamp'].apply(lambda x: x.is_monotonic_increasing))
    
    # Check no missing values
    assert cleaned.isnull().sum().sum() == 0
    
    # Check timestamp is datetime
    assert pd.api.types.is_datetime64_any_dtype(cleaned['timestamp'])


def test_engineer_features(sample_hvac_data):
    """Test feature engineering."""
    df = clean_data(sample_hvac_data)
    
    # Engineer features
    features = engineer_features(df)
    
    # Check new features were created
    assert 'temp_error_c' in features.columns
    assert 'delta_return_supply' in features.columns
    assert 'temp_error_rolling_mean_15min' in features.columns
    assert 'temp_error_rolling_mean_60min' in features.columns
    assert 'power_rolling_mean_15min' in features.columns
    assert 'power_rolling_mean_60min' in features.columns
    assert 'temp_zone_c_lag1' in features.columns
    assert 'power_kw_lag1' in features.columns
    assert 'temp_change_rate' in features.columns
    assert 'power_change_rate' in features.columns
    
    # Check no NaN values
    assert features.isnull().sum().sum() == 0
    
    # Check temp_error is calculated correctly
    expected_error = features['temp_zone_c'] - features['setpoint_c']
    assert np.allclose(features['temp_error_c'], expected_error, atol=0.01)
    
    # Check delta is calculated correctly
    expected_delta = features['return_air_temp_c'] - features['supply_air_temp_c']
    assert np.allclose(features['delta_return_supply'], expected_delta, atol=0.01)


def test_save_processed_data(sample_hvac_data):
    """Test saving processed data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, "test_features.parquet")
        
        df = clean_data(sample_hvac_data)
        features = engineer_features(df)
        
        # Save data
        save_processed_data(features, output_path)
        
        # Check file was created
        assert os.path.exists(output_path)
        
        # Check summary was created
        summary_path = output_path.replace('.parquet', '_summary.csv')
        assert os.path.exists(summary_path)
        
        # Load and verify
        loaded = pd.read_parquet(output_path)
        assert len(loaded) == len(features)
        assert list(loaded.columns) == list(features.columns)


def test_run_etl_pipeline():
    """Test complete ETL pipeline."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        raw_path = os.path.join(tmpdir, "raw.parquet")
        output_path = os.path.join(tmpdir, "features.parquet")
        
        # Generate and save test data
        df = generate_base_profile(
            start="2024-01-01 00:00",
            end="2024-01-02 23:55",
            freq="5min",
            n_zones=3,
            seed=42
        )
        df = inject_faults(df)
        df.to_parquet(raw_path, index=False)
        
        # Run pipeline
        result = run_etl_pipeline(raw_path, output_path)
        
        # Check result
        assert len(result) > 0
        assert 'temp_error_c' in result.columns
        assert 'temp_error_rolling_mean_60min' in result.columns
        
        # Check file was saved
        assert os.path.exists(output_path)


def test_feature_values_reasonable(sample_hvac_data):
    """Test that engineered features have reasonable values."""
    df = clean_data(sample_hvac_data)
    features = engineer_features(df)
    
    # Rolling means should be close to original values
    assert features['temp_error_rolling_mean_15min'].abs().max() < 10
    assert features['power_rolling_mean_15min'].min() >= 0
    
    # Lag features should be similar to current values
    # (excluding first row where lag is filled)
    temp_diff = (features['temp_zone_c'] - features['temp_zone_c_lag1']).abs()
    assert temp_diff[1:].mean() < 1.0  # Should be small changes


def test_zone_independence(sample_hvac_data):
    """Test that features are calculated independently for each zone."""
    df = clean_data(sample_hvac_data)
    features = engineer_features(df)
    
    # Check that each zone has its own feature calculations
    zones = features['zone_id'].unique()
    assert len(zones) == 2
    
    for zone in zones:
        zone_data = features[features['zone_id'] == zone]
        # Check that rolling features are calculated within zone
        assert zone_data['temp_error_rolling_mean_15min'].notna().all()
        # First lag should be 0 (filled) or reasonable
        assert zone_data['temp_zone_c_lag1'].iloc[0] == 0 or \
               abs(zone_data['temp_zone_c_lag1'].iloc[0] - zone_data['temp_zone_c'].iloc[0]) < 5
