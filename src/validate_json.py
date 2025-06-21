#!/usr/bin/env python3
"""
Validate exported JSON against schema for frontend consumption.

Usage:
    python src/validate_json.py [json_path]
    
If no path given, validates artifacts/json/building_benchmarking.json (canonical)
"""

import json
import sys
from datetime import datetime
from pathlib import Path


def validate_iso8601(value: str) -> bool:
    """Check if string is valid ISO 8601 timestamp."""
    try:
        if "+" in value or value.endswith("Z"):
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            datetime.fromisoformat(value)
        return True
    except ValueError:
        return False


def validate_building_benchmarking(data: dict) -> list[str]:
    """Validate canonical building_benchmarking.json schema."""
    errors = []
    
    # Required top-level fields
    required_top = ["pipeline", "generated_at", "portfolio_summary", "buildings"]
    for field in required_top:
        if field not in data:
            errors.append(f"Missing required field: {field}")
    
    # Validate generated_at timestamp
    if "generated_at" in data:
        if not validate_iso8601(data["generated_at"]):
            errors.append("Field 'generated_at' is not a valid ISO 8601 timestamp")
    
    # Validate portfolio_summary object
    if "portfolio_summary" in data:
        summary = data["portfolio_summary"]
        summary_fields = ["total_buildings", "total_floor_area_m2", "avg_energy_intensity_kwh_m2"]
        for field in summary_fields:
            if field not in summary:
                errors.append(f"Missing portfolio_summary.{field}")
    
    # Validate benchmark_categories if present
    if "benchmark_categories" in data:
        categories = data["benchmark_categories"]
        if "energy_intensity" not in categories:
            errors.append("Missing benchmark_categories.energy_intensity")
    
    # Validate buildings array
    if "buildings" in data:
        if not isinstance(data["buildings"], list):
            errors.append("buildings must be an array")
        elif len(data["buildings"]) > 0:
            sample = data["buildings"][0]
            required_building_fields = ["building_id", "name", "floor_area_m2", "energy_intensity_kwh_m2"]
            for field in required_building_fields:
                if field not in sample:
                    errors.append(f"buildings items must have '{field}' field")
    
    return errors


def main():
    json_path = sys.argv[1] if len(sys.argv) > 1 else "artifacts/json/building_benchmarking.json"
    path = Path(json_path)
    
    if not path.exists():
        print(f"✗ File not found: {path}")
        sys.exit(1)
    
    try:
        with open(path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON: {e}")
        sys.exit(1)
    
    errors = validate_building_benchmarking(data)
    
    if errors:
        print(f"✗ Validation failed for {path}:")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)
    else:
        print(f"✓ Validation passed: {path}")
        print(f"  pipeline: {data.get('pipeline', 'N/A')}")
        print(f"  generated_at: {data.get('generated_at', 'N/A')}")
        if "portfolio_summary" in data:
            ps = data["portfolio_summary"]
            print(f"  total_buildings: {ps.get('total_buildings', 'N/A')}")
            print(f"  avg_energy_intensity: {ps.get('avg_energy_intensity_kwh_m2', 'N/A')} kWh/m²")
        if "buildings" in data:
            print(f"  buildings: {len(data['buildings'])} records")
        sys.exit(0)


if __name__ == "__main__":
    main()
