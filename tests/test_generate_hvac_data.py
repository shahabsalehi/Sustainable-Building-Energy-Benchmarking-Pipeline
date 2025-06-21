"""
Tests for HVAC data generation module.
"""

import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.generate_hvac_data import generate_base_profile, inject_faults


def test_generate_base_profile():
    """Test basic HVAC profile generation."""
    df = generate_base_profile(
        start="2024-01-01 00:00",
        end="2024-01-01 23:55",
        freq="5min",
        n_zones=2,
        seed=42
    )
    
    # Check shape - 1 day * 24 hours * 12 intervals * 2 zones = 576 rows
    assert len(df) == 576
    
    # Check columns
    required_cols = [
        'timestamp', 'zone_id', 'ahu_id', 'temp_zone_c', 'rh_zone_pct',
        'supply_air_temp_c', 'return_air_temp_c', 'power_kw', 'fan_speed_pct',
        'setpoint_c', 'mode', 'fault_type'
    ]
    for col in required_cols:
        assert col in df.columns
    
    # Check zones
    assert set(df['zone_id'].unique()) == {'Z1', 'Z2'}
    
    # Check all start as no fault
    assert (df['fault_type'] == 'none').all()
    
    # Check reasonable value ranges
    assert df['temp_zone_c'].between(15, 35).all()
    assert df['rh_zone_pct'].between(30, 60).all()
    assert df['power_kw'].min() >= 0
    assert df['fan_speed_pct'].between(10, 90).all()


def test_inject_faults():
    """Test fault injection into HVAC data."""
    # Generate small dataset
    df = generate_base_profile(
        start="2024-01-01 00:00",
        end="2024-01-30 23:55",
        freq="5min",
        n_zones=10,
        seed=42
    )
    
    # Inject faults
    df_with_faults = inject_faults(df)
    
    # Check that some faults were injected
    fault_types = df_with_faults['fault_type'].unique()
    assert 'none' in fault_types
    assert len(fault_types) > 1  # Should have at least some faults
    
    # Check all expected fault types
    expected_faults = ['none', 'clogged_filter', 'compressor_failure', 
                      'temp_drift', 'oscillating_control']
    for fault in fault_types:
        assert fault in expected_faults
    
    # Check values still in reasonable bounds
    assert df_with_faults['temp_zone_c'].between(15, 35).all()
    assert df_with_faults['fan_speed_pct'].between(0, 100).all()
    assert df_with_faults['power_kw'].min() >= 0


def test_fault_characteristics():
    """Test that each fault type has expected characteristics."""
    df = generate_base_profile(
        start="2024-01-01 00:00",
        end="2024-01-30 23:55",
        freq="5min",
        n_zones=10,
        seed=42
    )
    
    df_with_faults = inject_faults(df)
    
    # Check clogged filter: should have increased fan speed
    clogged = df_with_faults[df_with_faults['fault_type'] == 'clogged_filter']
    if len(clogged) > 0:
        assert clogged['fan_speed_pct'].mean() > 50  # Should be elevated
    
    # Check compressor failure: should have low power
    compressor = df_with_faults[df_with_faults['fault_type'] == 'compressor_failure']
    if len(compressor) > 0:
        normal = df_with_faults[df_with_faults['fault_type'] == 'none']
        normal_power = normal[normal['mode'] == 'cooling']['power_kw'].mean()
        assert compressor['power_kw'].mean() < normal_power * 0.5
    
    # Check temp drift: should deviate from setpoint
    drift = df_with_faults[df_with_faults['fault_type'] == 'temp_drift']
    if len(drift) > 0:
        temp_errors = abs(drift['temp_zone_c'] - drift['setpoint_c'])
        assert temp_errors.mean() > 3.0  # Should be drifting significantly


def test_data_consistency():
    """Test that generated data maintains physical consistency."""
    df = generate_base_profile(
        start="2024-01-01 00:00",
        end="2024-01-02 23:55",
        freq="5min",
        n_zones=3,
        seed=42
    )
    
    # Return air should generally be warmer than supply air
    temp_diff = df['return_air_temp_c'] - df['supply_air_temp_c']
    assert temp_diff.mean() > 0
    
    # Zone temp should be close to setpoint (for normal operation)
    temp_error = abs(df['temp_zone_c'] - df['setpoint_c'])
    assert temp_error.mean() < 2.0  # Average error should be small
