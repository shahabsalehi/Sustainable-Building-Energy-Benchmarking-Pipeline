#!/usr/bin/env python3
"""
Export benchmarking data to canonical JSON for frontend consumption.

Outputs: artifacts/json/building_benchmarking.json

Canonical schema:
{
  "pipeline": "sustainable_building_benchmarking",
  "generated_at": "ISO8601 UTC timestamp",
  "portfolio_summary": { total_buildings, total_floor_area_m2, avg_energy_intensity_kwh_m2, ... },
  "benchmark_categories": { energy_intensity: { excellent, good, average, poor }, ... },
  "buildings": [ { building_id, name, location, floor_area_m2, energy_intensity_kwh_m2, co2_intensity_kg_m2, rating, certifications } ]
}
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def export_building_benchmarking(
    processed_dir: str = "data/processed",
    sample_dir: str = "sample_data",
    output_dir: str = "artifacts/json",
) -> str:
    """
    Export benchmarking data to canonical JSON schema.

    Includes portfolio_summary, benchmark_categories, buildings with CO₂ intensity and certifications.

    Args:
        processed_dir: Path to processed data files
        sample_dir: Fallback path to sample data
        output_dir: Output directory for JSON

    Returns:
        Path to exported JSON file
    """
    processed_path = Path(processed_dir)
    sample_path = Path(sample_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)

    # Initialize canonical structure
    benchmarking: dict[str, Any] = {
        "pipeline": "sustainable_building_benchmarking",
        "generated_at": now.isoformat(),
        "portfolio_summary": {},
        "benchmark_categories": {
            "energy_intensity": {
                "excellent": "< 70 kWh/m²",
                "good": "70-90 kWh/m²",
                "average": "90-110 kWh/m²",
                "poor": "> 110 kWh/m²",
            },
            "co2_intensity": {
                "excellent": "< 15 kg/m²",
                "good": "15-20 kg/m²",
                "average": "20-25 kg/m²",
                "poor": "> 25 kg/m²",
            },
        },
        "buildings": [],
    }

    # Try to load data
    df = None
    for path in [processed_path, sample_path]:
        if not path.exists():
            continue
        files = list(path.glob("*.parquet")) + list(path.glob("*.csv"))
        if files:
            try:
                if files[0].suffix == ".parquet":
                    df = pd.read_parquet(files[0])
                else:
                    df = pd.read_csv(files[0])
                print(f"Loaded {len(df)} building records from {files[0]}")
                break
            except Exception as e:
                print(f"Warning: Could not read {files[0]}: {e}")

    if df is not None and not df.empty:
        # Column mappings
        id_cols = ["building_id", "id", "property_id", "bldg_id"]
        name_cols = ["name", "building_name", "property_name"]
        location_cols = ["location", "city", "address"]
        area_cols = ["floor_area_m2", "area", "gross_floor_area", "gfa"]
        type_cols = ["building_type", "type", "use_type", "property_type"]
        year_cols = ["year_built", "construction_year", "year"]
        eui_cols = ["eui", "energy_use_intensity", "site_eui", "energy_intensity_kwh_m2"]
        co2_cols = ["co2_intensity", "carbon_intensity", "co2_kg_m2", "co2_intensity_kg_m2"]
        pct_cols = ["percentile", "percentile_rank", "energy_percentile"]
        rating_cols = ["rating", "grade", "energy_class", "performance_rating"]
        cert_cols = ["certifications", "certificates", "green_certifications"]

        def get_col(row, cols, default=None):
            for col in cols:
                if col in df.columns and pd.notna(row.get(col)):
                    return row[col]
            return default

        buildings = []
        for idx, row in df.iterrows():
            building_id = get_col(row, id_cols, f"BLD-{idx + 1:03d}")
            name = get_col(row, name_cols, f"Building {idx + 1}")
            location = get_col(row, location_cols, "Stockholm")
            floor_area = float(get_col(row, area_cols, 3000))
            building_type = get_col(row, type_cols, "Office")
            year_built = int(get_col(row, year_cols, 2015))
            eui = float(get_col(row, eui_cols, 85.0))
            co2 = float(get_col(row, co2_cols, eui * 0.22))  # Derive from EUI if missing
            percentile = int(get_col(row, pct_cols, 50))
            rating = get_col(row, rating_cols)
            certs = get_col(row, cert_cols, [])

            # Calculate rating from EUI if missing
            if rating is None:
                if eui < 70:
                    rating = "Excellent"
                elif eui < 90:
                    rating = "Good"
                elif eui < 110:
                    rating = "Average"
                else:
                    rating = "Poor"

            # Parse certifications
            if isinstance(certs, str):
                certs = [c.strip() for c in certs.split(",") if c.strip()]
            elif not isinstance(certs, list):
                certs = []

            buildings.append({
                "building_id": str(building_id),
                "name": str(name),
                "location": str(location),
                "floor_area_m2": round(floor_area, 0),
                "building_type": str(building_type),
                "year_built": year_built,
                "energy_intensity_kwh_m2": round(eui, 1),
                "co2_intensity_kg_m2": round(co2, 1),
                "energy_percentile": percentile,
                "rating": str(rating),
                "certifications": certs,
            })

        benchmarking["buildings"] = buildings

        # Compute portfolio summary
        total_area = sum(b["floor_area_m2"] for b in buildings)
        avg_eui = sum(b["energy_intensity_kwh_m2"] * b["floor_area_m2"] for b in buildings) / total_area if total_area > 0 else 0
        total_co2 = sum(b["co2_intensity_kg_m2"] * b["floor_area_m2"] / 1000 for b in buildings)
        top_performers = len([b for b in buildings if b["rating"] in ["Excellent", "Good"]])
        needs_improvement = len([b for b in buildings if b["rating"] in ["Poor"]])

        benchmarking["portfolio_summary"] = {
            "total_buildings": len(buildings),
            "total_floor_area_m2": round(total_area, 0),
            "avg_energy_intensity_kwh_m2": round(avg_eui, 1),
            "portfolio_co2_tons": round(total_co2, 1),
            "top_performer_pct": round(top_performers / len(buildings) * 100, 0) if buildings else 0,
            "needs_improvement_pct": round(needs_improvement / len(buildings) * 100, 0) if buildings else 0,
        }

    else:
        # Generate sample data
        print("No benchmarking data found. Generating sample portfolio data...")

        benchmarking["portfolio_summary"] = {
            "total_buildings": 12,
            "total_floor_area_m2": 45680,
            "avg_energy_intensity_kwh_m2": 98.4,
            "portfolio_co2_tons": 892.5,
            "top_performer_pct": 25,
            "needs_improvement_pct": 33,
        }

        benchmarking["buildings"] = [
            {
                "building_id": "BLD-001",
                "name": "Main Office Tower",
                "location": "Stockholm",
                "floor_area_m2": 5200,
                "building_type": "Office",
                "year_built": 2018,
                "energy_intensity_kwh_m2": 72.5,
                "co2_intensity_kg_m2": 16.2,
                "energy_percentile": 15,
                "rating": "Excellent",
                "certifications": ["LEED Gold", "BREEAM Excellent"],
            },
            {
                "building_id": "BLD-002",
                "name": "Research Center",
                "location": "Uppsala",
                "floor_area_m2": 3890,
                "building_type": "Laboratory",
                "year_built": 2015,
                "energy_intensity_kwh_m2": 145.8,
                "co2_intensity_kg_m2": 32.4,
                "energy_percentile": 88,
                "rating": "Poor",
                "certifications": [],
            },
            {
                "building_id": "BLD-003",
                "name": "Retail Complex",
                "location": "Gothenburg",
                "floor_area_m2": 4320,
                "building_type": "Retail",
                "year_built": 2020,
                "energy_intensity_kwh_m2": 88.3,
                "co2_intensity_kg_m2": 19.1,
                "energy_percentile": 42,
                "rating": "Good",
                "certifications": ["LEED Silver"],
            },
            {
                "building_id": "BLD-004",
                "name": "Hotel & Conference",
                "location": "Malmö",
                "floor_area_m2": 6100,
                "building_type": "Hotel",
                "year_built": 2012,
                "energy_intensity_kwh_m2": 112.6,
                "co2_intensity_kg_m2": 24.8,
                "energy_percentile": 72,
                "rating": "Average",
                "certifications": ["BREEAM Good"],
            },
            {
                "building_id": "BLD-005",
                "name": "Education Center",
                "location": "Lund",
                "floor_area_m2": 3200,
                "building_type": "Educational",
                "year_built": 2019,
                "energy_intensity_kwh_m2": 68.2,
                "co2_intensity_kg_m2": 14.6,
                "energy_percentile": 12,
                "rating": "Excellent",
                "certifications": ["LEED Platinum"],
            },
            {
                "building_id": "BLD-006",
                "name": "Healthcare Facility",
                "location": "Stockholm",
                "floor_area_m2": 5800,
                "building_type": "Healthcare",
                "year_built": 2010,
                "energy_intensity_kwh_m2": 156.4,
                "co2_intensity_kg_m2": 35.2,
                "energy_percentile": 92,
                "rating": "Poor",
                "certifications": [],
            },
        ]

    # Write canonical JSON
    output_file = output_path / "building_benchmarking.json"
    with open(output_file, "w") as f:
        json.dump(benchmarking, f, indent=2)

    print(f"✓ Exported canonical benchmarking to {output_file}")
    return str(output_file)


if __name__ == "__main__":
    export_building_benchmarking()
