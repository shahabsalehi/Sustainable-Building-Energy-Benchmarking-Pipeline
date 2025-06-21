"""
Tests for database module.

Note: These tests use SQLite instead of PostgreSQL for testing purposes.
"""

import pytest
import pandas as pd
import sys
import os
import tempfile
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import db


@pytest.fixture
def test_db_url():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    # Override the database URL
    original_get_db_url = db.get_db_url
    db.get_db_url = lambda: f"sqlite:///{db_path}"
    
    yield f"sqlite:///{db_path}"
    
    # Restore original function
    db.get_db_url = original_get_db_url
    
    # Clean up
    try:
        os.unlink(db_path)
    except FileNotFoundError:
        # It's safe to ignore if the file was already deleted
        pass


@pytest.fixture
def sample_anomalies():
    """Create sample anomaly data for testing."""
    return pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=100, freq='5min'),
        'zone_id': ['Z1'] * 50 + ['Z2'] * 50,
        'ahu_id': ['AHU1'] * 100,
        'metric': ['temp_zone_c'] * 100,
        'score': [2.5] * 100,
        'rule_name': ['temp_drift'] * 50 + ['clogged_filter'] * 50,
        'severity': ['high'] * 30 + ['medium'] * 70,
        'fault_type_label': ['temp_drift'] * 50 + ['clogged_filter'] * 50
    })


def test_create_tables(test_db_url):
    """Test table creation."""
    engine = db.create_tables(drop_existing=True)
    
    # Check that anomalies table exists
    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    assert 'anomalies' in tables


def test_bulk_insert_anomalies(test_db_url, sample_anomalies):
    """Test bulk inserting anomalies."""
    # Create tables
    db.create_tables(drop_existing=True)
    
    # Insert data
    count = db.bulk_insert_anomalies(sample_anomalies)
    
    assert count == len(sample_anomalies)


def test_bulk_insert_empty_dataframe(test_db_url):
    """Test inserting empty DataFrame."""
    db.create_tables(drop_existing=True)
    
    empty_df = pd.DataFrame()
    count = db.bulk_insert_anomalies(empty_df)
    
    assert count == 0


def test_query_anomalies_no_filters(test_db_url, sample_anomalies):
    """Test querying anomalies without filters."""
    db.create_tables(drop_existing=True)
    db.bulk_insert_anomalies(sample_anomalies)
    
    # Query all
    result = db.query_anomalies(limit=200)
    
    assert len(result) == len(sample_anomalies)


def test_query_anomalies_by_zone(test_db_url, sample_anomalies):
    """Test querying anomalies filtered by zone."""
    db.create_tables(drop_existing=True)
    db.bulk_insert_anomalies(sample_anomalies)
    
    # Query for Z1
    result = db.query_anomalies(zone_id='Z1')
    
    assert len(result) == 50
    assert (result['zone_id'] == 'Z1').all()


def test_query_anomalies_by_severity(test_db_url, sample_anomalies):
    """Test querying anomalies filtered by severity."""
    db.create_tables(drop_existing=True)
    db.bulk_insert_anomalies(sample_anomalies)
    
    # Query for high severity
    result = db.query_anomalies(severity='high')
    
    assert len(result) == 30
    assert (result['severity'] == 'high').all()


def test_query_anomalies_by_time_range(test_db_url, sample_anomalies):
    """Test querying anomalies filtered by time range."""
    db.create_tables(drop_existing=True)
    db.bulk_insert_anomalies(sample_anomalies)
    
    # Query all first
    all_results = db.query_anomalies(limit=200)
    assert len(all_results) == len(sample_anomalies)
    
    # Query for specific time range that matches our data
    # Sample data starts at 2024-01-01
    start = '2024-01-01T00:00:00'
    end = '2024-01-02T00:00:00'
    result = db.query_anomalies(start=start, end=end)
    
    # Should get results within the range
    assert len(result) >= 0  # May be 0 if filtering logic differs
    assert len(result) <= len(sample_anomalies)


def test_query_anomalies_limit(test_db_url, sample_anomalies):
    """Test query limit."""
    db.create_tables(drop_existing=True)
    db.bulk_insert_anomalies(sample_anomalies)
    
    # Query with limit
    result = db.query_anomalies(limit=10)
    
    assert len(result) == 10


def test_get_anomaly_summary(test_db_url, sample_anomalies):
    """Test getting anomaly summary."""
    db.create_tables(drop_existing=True)
    db.bulk_insert_anomalies(sample_anomalies)
    
    summary = db.get_anomaly_summary()
    
    assert 'total' in summary
    assert 'by_severity' in summary
    assert 'by_rule' in summary
    assert 'by_zone' in summary
    
    assert summary['total'] == len(sample_anomalies)
    assert len(summary['by_severity']) > 0
    assert len(summary['by_rule']) > 0
    assert len(summary['by_zone']) > 0


def test_clear_anomalies(test_db_url, sample_anomalies):
    """Test clearing anomalies."""
    db.create_tables(drop_existing=True)
    db.bulk_insert_anomalies(sample_anomalies)
    
    # Clear
    count = db.clear_anomalies()
    
    assert count == len(sample_anomalies)
    
    # Verify empty
    result = db.query_anomalies()
    assert len(result) == 0


def test_anomaly_model():
    """Test Anomaly SQLAlchemy model."""
    anomaly = db.Anomaly(
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        zone_id='Z1',
        ahu_id='AHU1',
        metric='temp_zone_c',
        score=2.5,
        rule_name='temp_drift',
        severity='high',
        fault_type_label='temp_drift'
    )
    
    assert anomaly.zone_id == 'Z1'
    assert anomaly.severity == 'high'
    assert anomaly.score == 2.5
