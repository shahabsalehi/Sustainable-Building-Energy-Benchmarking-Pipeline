"""
Tests for anomaly detection models.
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os
import tempfile

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models import RulesBasedDetector, MLBasedDetector, run_anomaly_detection
from src.generate_hvac_data import generate_base_profile, inject_faults
from src.pipeline_batch import clean_data, engineer_features


@pytest.fixture
def sample_features():
    """Create sample HVAC features for testing."""
    df = generate_base_profile(
        start="2024-01-01 00:00",
        end="2024-01-05 23:55",
        freq="5min",
        n_zones=3,
        seed=42
    )
    df = inject_faults(df)
    df = clean_data(df)
    df = engineer_features(df)
    return df


def test_rules_detector_initialization():
    """Test rules-based detector initialization."""
    detector = RulesBasedDetector()
    assert detector.rules is not None
    assert len(detector.rules) == 4
    assert 'temp_drift' in detector.rules
    assert 'clogged_filter' in detector.rules
    assert 'compressor_failure' in detector.rules
    assert 'oscillating_control' in detector.rules


def test_rules_detector_basic(sample_features):
    """Test basic rules-based detection."""
    detector = RulesBasedDetector()
    anomalies = detector.detect_anomalies(sample_features)
    
    # Check that we got some anomalies
    assert len(anomalies) > 0
    
    # Check schema
    expected_cols = ['timestamp', 'zone_id', 'ahu_id', 'metric', 'score', 
                    'rule_name', 'severity', 'fault_type_label']
    for col in expected_cols:
        assert col in anomalies.columns
    
    # Check data types
    assert pd.api.types.is_datetime64_any_dtype(anomalies['timestamp'])
    assert anomalies['score'].dtype in [np.float64, np.float32]
    
    # Check severity values are valid
    assert anomalies['severity'].isin(['low', 'medium', 'high']).all()


def test_temp_drift_detection(sample_features):
    """Test temperature drift detection."""
    detector = RulesBasedDetector()
    
    # Create a scenario with clear temperature drift
    test_df = sample_features.copy()
    # Force a zone to have sustained high temp error
    mask = (test_df['zone_id'] == 'Z1') & (test_df.index < 10)
    test_df.loc[mask, 'temp_error_c'] = 4.5
    
    anomalies = detector._detect_temp_drift(test_df)
    
    # Should detect the drift
    assert len(anomalies) > 0
    assert all(a['rule_name'] == 'temp_drift' for a in anomalies)


def test_clogged_filter_detection(sample_features):
    """Test clogged filter detection."""
    detector = RulesBasedDetector()
    
    # Create a scenario with clogged filter symptoms
    test_df = sample_features.copy()
    mask = test_df['zone_id'] == 'Z1'
    test_df.loc[mask, 'fan_speed_pct'] = 75
    test_df.loc[mask, 'fan_rolling_mean_15min'] = 72
    
    anomalies = detector._detect_clogged_filter(test_df)
    
    # Should detect the clogged filter
    assert len(anomalies) > 0
    assert all(a['rule_name'] == 'clogged_filter' for a in anomalies)


def test_compressor_failure_detection(sample_features):
    """Test compressor failure detection."""
    detector = RulesBasedDetector()
    
    # Create a scenario with compressor failure
    test_df = sample_features.copy()
    mask = (test_df['zone_id'] == 'Z1') & (test_df['mode'] == 'cooling')
    test_df.loc[mask, 'power_kw'] = 2.0
    test_df.loc[mask, 'temp_error_c'] = 3.0
    test_df.loc[mask, 'power_rolling_mean_60min'] = 2.5
    
    anomalies = detector._detect_compressor_failure(test_df)
    
    # Should detect the failure
    assert len(anomalies) > 0
    assert all(a['rule_name'] == 'compressor_failure' for a in anomalies)


def test_ml_detector_initialization():
    """Test ML-based detector initialization."""
    detector = MLBasedDetector(contamination=0.02, random_state=42)
    assert detector.model is not None
    assert detector.scaler is not None
    assert not detector.is_trained


def test_ml_detector_training(sample_features):
    """Test ML detector training."""
    detector = MLBasedDetector(contamination=0.02, random_state=42)
    
    # Train on normal data
    detector.train(sample_features, normal_only=True)
    
    assert detector.is_trained
    assert detector.feature_cols is not None
    assert len(detector.feature_cols) > 0


def test_ml_detector_detection(sample_features):
    """Test ML-based anomaly detection."""
    detector = MLBasedDetector(contamination=0.02, random_state=42)
    
    # Train
    detector.train(sample_features, normal_only=True)
    
    # Detect
    anomalies = detector.detect_anomalies(sample_features)
    
    # Check that we got some anomalies
    assert len(anomalies) > 0
    
    # Check schema
    expected_cols = ['timestamp', 'zone_id', 'ahu_id', 'metric', 'score', 
                    'rule_name', 'severity', 'fault_type_label']
    for col in expected_cols:
        assert col in anomalies.columns
    
    # Check all are from isolation forest
    assert (anomalies['rule_name'] == 'isolation_forest').all()


def test_ml_detector_must_train_first(sample_features):
    """Test that ML detector requires training before detection."""
    detector = MLBasedDetector()
    
    # Should raise error if not trained
    with pytest.raises(ValueError, match="Model must be trained"):
        detector.detect_anomalies(sample_features)


def test_ml_detector_save_load(sample_features):
    """Test saving and loading ML model."""
    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = os.path.join(tmpdir, "test_model.pkl")
        
        # Train and save
        detector1 = MLBasedDetector(contamination=0.02, random_state=42)
        detector1.train(sample_features, normal_only=True)
        detector1.save_model(model_path)
        
        # Load and use
        detector2 = MLBasedDetector()
        detector2.load_model(model_path)
        
        assert detector2.is_trained
        assert detector2.feature_cols == detector1.feature_cols
        
        # Should be able to detect
        anomalies = detector2.detect_anomalies(sample_features)
        assert len(anomalies) > 0


def test_anomaly_detection_pipeline():
    """Test complete anomaly detection pipeline."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        features_path = os.path.join(tmpdir, "features.parquet")
        output_path = os.path.join(tmpdir, "anomalies.parquet")
        
        # Generate and save features
        df = generate_base_profile(
            start="2024-01-01 00:00",
            end="2024-01-03 23:55",
            freq="5min",
            n_zones=3,
            seed=42
        )
        df = inject_faults(df)
        df = clean_data(df)
        df = engineer_features(df)
        df.to_parquet(features_path, index=False)
        
        # Run pipeline
        anomalies = run_anomaly_detection(
            features_path=features_path,
            output_path=output_path,
            use_ml=True
        )
        
        # Check results
        assert len(anomalies) > 0
        assert os.path.exists(output_path)
        
        # Check that both rules and ML detected something
        rule_names = anomalies['rule_name'].unique()
        assert len(rule_names) > 1  # Should have multiple detection methods


def test_anomaly_severity_levels(sample_features):
    """Test that severity levels are assigned correctly."""
    detector = RulesBasedDetector()
    anomalies = detector.detect_anomalies(sample_features)
    
    if len(anomalies) > 0:
        # Check that severity is one of the valid values
        valid_severities = {'low', 'medium', 'high'}
        assert anomalies['severity'].isin(valid_severities).all()
        
        # Check that scores are positive
        assert (anomalies['score'] > 0).all()
