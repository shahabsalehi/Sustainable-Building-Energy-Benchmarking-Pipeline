"""
Generate Sample Data for Building Energy Benchmarking Pipeline

This script generates sample building energy consumption data for testing
and demonstration purposes. It includes basic ETL (Extract, Transform, Load)
functionality.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta


def extract_data():
    """
    Extract phase: Generate raw building energy data.
    
    In a production system, this would extract data from various sources
    like building management systems, utility bills, or IoT sensors.
    
    Returns:
        DataFrame with raw building energy data
    """
    print("Extracting data...")
    
    # Generate sample data for 100 buildings
    np.random.seed(42)
    n_buildings = 100
    
    building_types = ['office', 'residential', 'retail', 'industrial', 'educational']
    
    data = {
        'building_id': [f'B{str(i).zfill(3)}' for i in range(1, n_buildings + 1)],
        'building_type': np.random.choice(building_types, n_buildings),
        'area': np.random.uniform(500, 10000, n_buildings),  # Square meters
        'year_built': np.random.randint(1970, 2023, n_buildings),
        'energy_consumption': np.random.uniform(10000, 500000, n_buildings),  # kWh per year
        'occupancy': np.random.randint(10, 500, n_buildings),
        'has_hvac': np.random.choice([True, False], n_buildings),
        'has_solar': np.random.choice([True, False], n_buildings, p=[0.3, 0.7]),
    }
    
    df = pd.DataFrame(data)
    return df


def transform_data(df):
    """
    Transform phase: Clean and enrich the data.
    
    Args:
        df: Raw building data DataFrame
        
    Returns:
        Transformed DataFrame with calculated metrics
    """
    print("Transforming data...")
    
    # Calculate Energy Use Intensity (EUI)
    df['eui'] = df['energy_consumption'] / df['area']
    
    # Calculate energy per occupant
    df['energy_per_occupant'] = df['energy_consumption'] / df['occupancy']
    
    # Add building age
    current_year = datetime.now().year
    df['building_age'] = current_year - df['year_built']
    
    # Categorize performance based on EUI
    def categorize_performance(eui):
        if eui < 100:
            return 'Excellent'
        elif eui < 150:
            return 'Good'
        elif eui < 200:
            return 'Average'
        else:
            return 'Poor'
    
    df['performance_category'] = df['eui'].apply(categorize_performance)
    
    # Round numeric columns
    numeric_columns = ['area', 'energy_consumption', 'eui', 'energy_per_occupant']
    df[numeric_columns] = df[numeric_columns].round(2)
    
    return df


def load_data(df, output_dir='sample_data'):
    """
    Load phase: Save processed data to files.
    
    Args:
        df: Transformed DataFrame
        output_dir: Directory to save output files
    """
    print(f"Loading data to {output_dir}...")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save to CSV
    csv_path = os.path.join(output_dir, 'buildings_sample.csv')
    df.to_csv(csv_path, index=False)
    print(f"Saved CSV: {csv_path}")
    
    # Save to JSON
    json_path = os.path.join(output_dir, 'buildings_sample.json')
    df.to_json(json_path, orient='records', indent=2)
    print(f"Saved JSON: {json_path}")
    
    # Generate summary statistics
    summary = df.groupby('building_type').agg({
        'eui': ['mean', 'median', 'std'],
        'energy_consumption': ['mean', 'sum'],
        'building_id': 'count'
    }).round(2)
    
    summary_path = os.path.join(output_dir, 'summary_statistics.csv')
    summary.to_csv(summary_path)
    print(f"Saved summary: {summary_path}")
    
    return df


def run_etl_pipeline():
    """
    Execute the complete ETL pipeline.
    
    This function orchestrates the Extract, Transform, and Load phases
    to generate sample building energy data.
    """
    print("=" * 50)
    print("Building Energy Data ETL Pipeline")
    print("=" * 50)
    
    # Extract
    raw_data = extract_data()
    print(f"Extracted {len(raw_data)} building records")
    
    # Transform
    transformed_data = transform_data(raw_data)
    print(f"Transformed data with {len(transformed_data.columns)} columns")
    
    # Load
    final_data = load_data(transformed_data)
    
    print("=" * 50)
    print("ETL Pipeline completed successfully!")
    print(f"Generated {len(final_data)} building energy records")
    print("=" * 50)
    
    # Display sample of the data
    print("\nSample of generated data:")
    print(final_data.head())
    print("\nPerformance distribution:")
    print(final_data['performance_category'].value_counts())
    
    return final_data


if __name__ == "__main__":
    run_etl_pipeline()
