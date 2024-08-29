import pandas as pd
import duckdb
from datetime import datetime

def load_building_data(filepath):
    df = pd.read_csv(filepath)
    return df

def calculate_eui(df):
    df['eui_kwh_m2'] = df['annual_energy_kwh'] / df['floor_area_m2']
    return df

def assign_rating(eui):
    if eui < 100:
        return 'A'
    elif eui < 150:
        return 'B'
    elif eui < 200:
        return 'C'
    elif eui < 250:
        return 'D'
    else:
        return 'E'

def benchmark_buildings(df):
    df = calculate_eui(df)
    df['energy_rating'] = df['eui_kwh_m2'].apply(assign_rating)
    return df

def save_to_duckdb(df, db_path, table_name):
    conn = duckdb.connect(db_path)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    conn.close()

if __name__ == "__main__":
    df = load_building_data("data/buildings.csv")
    df = benchmark_buildings(df)
    save_to_duckdb(df, "data/benchmarking.duckdb", "buildings")
    print(f"Processed {len(df)} buildings")
