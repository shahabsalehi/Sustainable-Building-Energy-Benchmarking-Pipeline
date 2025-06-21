"""
Tests for the benchmarking module.
"""

import pytest
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from benchmarking.model import benchmark_building


def test_benchmark_building_basic():
    """Test basic benchmarking functionality."""
    building_data = {
        'building_id': 'TEST001',
        'area': 1000,
        'energy_consumption': 50000,
        'building_type': 'office'
    }
    
    result = benchmark_building(building_data)
    
    assert result['building_id'] == 'TEST001'
    assert result['eui'] == 50.0
    assert 'performance_rating' in result
    assert 'recommendations' in result
    assert isinstance(result['recommendations'], list)


def test_benchmark_building_good_performance():
    """Test benchmarking for a high-performing building."""
    building_data = {
        'building_id': 'TEST002',
        'area': 2000,
        'energy_consumption': 100000,
        'building_type': 'office'
    }
    
    result = benchmark_building(building_data)
    
    assert result['eui'] == 50.0
    assert result['performance_rating'] == 'Good'


def test_benchmark_building_poor_performance():
    """Test benchmarking for a poor-performing building."""
    building_data = {
        'building_id': 'TEST003',
        'area': 1000,
        'energy_consumption': 250000,
        'building_type': 'office'
    }
    
    result = benchmark_building(building_data)
    
    assert result['eui'] == 250.0
    assert result['performance_rating'] == 'Poor'


def test_benchmark_building_zero_area():
    """Test benchmarking with zero area."""
    building_data = {
        'building_id': 'TEST004',
        'area': 0,
        'energy_consumption': 50000,
        'building_type': 'office'
    }
    
    result = benchmark_building(building_data)
    
    assert result['eui'] == 0
