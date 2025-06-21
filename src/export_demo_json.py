#!/usr/bin/env python3
"""
Export slim demo JSON for frontend static deployment.

Generates: artifacts/json/building_benchmarking.demo.json (~5 KB)

This creates a representative subset of the full benchmarking data
suitable for static hosting on Vercel/Netlify without performance issues.

The demo file includes:
- Full portfolio_summary and benchmark_categories (aggregated KPIs)
- 8 diverse representative buildings covering all rating categories
- Pagination metadata for API compatibility
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def export_demo_json(
    full_json_path: str = "artifacts/json/building_benchmarking.json",
    output_dir: str = "artifacts/json",
) -> str:
    """
    Generate a slim demo JSON from the full benchmarking export.

    If the full JSON doesn't exist, generates representative sample data.

    Args:
        full_json_path: Path to full building_benchmarking.json
        output_dir: Output directory for demo JSON

    Returns:
        Path to exported demo JSON file
    """
    full_path = Path(full_json_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)

    # Try to load full data
    if full_path.exists():
        with open(full_path) as f:
            full_data = json.load(f)
        buildings = full_data.get("buildings", [])
        print(f"Loaded full data with {len(buildings)} buildings")

        # Check if the data has sufficient diversity for a meaningful demo
        ratings = set(b.get("rating", "").lower() for b in buildings[:1000])  # Sample first 1000
        types = set(b.get("building_type", "") for b in buildings[:1000])
        has_diversity = len(ratings) >= 3 or len(types) >= 3

        if has_diversity:
            # Select diverse representative buildings from real data
            demo_buildings = select_representative_buildings(buildings, max_count=8)
            print(f"Selected {len(demo_buildings)} diverse buildings from real data")
        else:
            # Data is too uniform - use curated sample data for better demo
            print("Warning: Full data lacks diversity (all same rating/type). Using curated demo data.")
            sample_data = generate_sample_demo_data(now)
            demo_buildings = sample_data["buildings"]

        # Compute demo-specific portfolio summary from the demo buildings
        demo_summary = compute_portfolio_summary(demo_buildings)

        # Store full portfolio stats separately for reference
        full_portfolio = full_data.get("portfolio_summary", {})

        demo_data: dict[str, Any] = {
            "pipeline": full_data.get("pipeline", "sustainable_building_benchmarking"),
            "generated_at": now.isoformat(),
            "demo_mode": True,
            "portfolio_summary": demo_summary,  # KPIs for demo buildings only
            "full_portfolio_reference": {
                "total_buildings": full_portfolio.get("total_buildings", len(buildings)),
                "total_floor_area_m2": full_portfolio.get("total_floor_area_m2", 0),
                "note": "Full dataset available via paginated API",
            },
            "benchmark_categories": full_data.get("benchmark_categories", {}),
            "buildings": demo_buildings,
            "pagination": {
                "page": 1,
                "page_size": len(demo_buildings),
                "total_in_demo": len(demo_buildings),
                "full_dataset_size": len(buildings),
                "has_more": False,  # Demo is complete subset
            },
        }
    else:
        print("Full JSON not found. Generating representative demo data...")
        demo_data = generate_sample_demo_data(now)

    # Write demo JSON
    output_file = output_path / "building_benchmarking.demo.json"
    with open(output_file, "w") as f:
        json.dump(demo_data, f, indent=2)

    size_kb = output_file.stat().st_size / 1024
    print(f"Exported demo benchmarking to {output_file} ({size_kb:.1f} KB)")

    if size_kb > 10:
        print(f"Warning: Demo file is {size_kb:.1f} KB, target is <5 KB")

    return str(output_file)


def compute_portfolio_summary(buildings: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Compute portfolio summary KPIs from a list of buildings.
    """
    if not buildings:
        return {
            "total_buildings": 0,
            "total_floor_area_m2": 0,
            "avg_energy_intensity_kwh_m2": 0,
            "portfolio_co2_tons": 0,
            "top_performer_pct": 0,
            "needs_improvement_pct": 0,
        }

    total_area = sum(b.get("floor_area_m2", 0) for b in buildings)
    
    # Weighted average energy intensity
    if total_area > 0:
        avg_eui = sum(
            b.get("energy_intensity_kwh_m2", 0) * b.get("floor_area_m2", 0)
            for b in buildings
        ) / total_area
    else:
        avg_eui = 0

    # Total CO2 (intensity * area / 1000 to get tons)
    total_co2 = sum(
        b.get("co2_intensity_kg_m2", 0) * b.get("floor_area_m2", 0) / 1000
        for b in buildings
    )

    # Performance percentages
    n = len(buildings)
    top_performers = len([
        b for b in buildings 
        if b.get("rating", "").lower() in ["excellent", "good"]
    ])
    needs_improvement = len([
        b for b in buildings 
        if b.get("rating", "").lower() == "poor"
    ])

    return {
        "total_buildings": n,
        "total_floor_area_m2": round(total_area, 0),
        "avg_energy_intensity_kwh_m2": round(avg_eui, 1),
        "portfolio_co2_tons": round(total_co2, 1),
        "top_performer_pct": round(top_performers / n * 100, 0) if n > 0 else 0,
        "needs_improvement_pct": round(needs_improvement / n * 100, 0) if n > 0 else 0,
    }


def select_representative_buildings(
    buildings: list[dict[str, Any]], max_count: int = 8
) -> list[dict[str, Any]]:
    """
    Select a diverse subset of buildings representing all rating categories.

    Prioritizes:
    - Coverage of all rating categories (Excellent, Good, Average, Poor)
    - Variety of building types
    - Mix of locations
    - Range of certifications
    """
    if len(buildings) <= max_count:
        return buildings

    # Group by rating
    by_rating: dict[str, list[dict[str, Any]]] = {
        "Excellent": [],
        "Good": [],
        "Average": [],
        "Poor": [],
    }

    for b in buildings:
        rating = b.get("rating", "Average")
        # Normalize rating case
        rating_normalized = rating.capitalize() if isinstance(rating, str) else "Average"
        if rating_normalized in by_rating:
            by_rating[rating_normalized].append(b)
        else:
            by_rating["Average"].append(b)

    # Select 2 from each category, prioritizing diversity
    selected: list[dict[str, Any]] = []
    per_category = max(1, max_count // 4)

    for rating, group in by_rating.items():
        if not group:
            continue

        # Sort by building_type diversity
        seen_types: set[str] = set()
        for b in group:
            btype = b.get("building_type", "Office")
            if btype not in seen_types and len(selected) < max_count:
                selected.append(b)
                seen_types.add(btype)
                if len([s for s in selected if s.get("rating") == rating]) >= per_category:
                    break

    # Fill remaining slots if needed
    remaining = max_count - len(selected)
    if remaining > 0:
        selected_ids = {b.get("building_id") for b in selected}
        for b in buildings:
            if b.get("building_id") not in selected_ids:
                selected.append(b)
                if len(selected) >= max_count:
                    break

    return selected[:max_count]


def generate_sample_demo_data(now: datetime) -> dict[str, Any]:
    """Generate representative demo data when no full export exists."""
    buildings = [
        {
            "building_id": "BLD-001",
            "name": "Innovation Hub",
            "location": "Stockholm",
            "floor_area_m2": 12000,
            "building_type": "Office",
            "year_built": 2020,
            "energy_intensity_kwh_m2": 65.2,
            "co2_intensity_kg_m2": 14.3,
            "energy_percentile": 8,
            "rating": "Excellent",
            "certifications": ["LEED Platinum", "BREEAM Outstanding"],
        },
        {
            "building_id": "BLD-002",
            "name": "Green Data Center",
            "location": "Luleå",
            "floor_area_m2": 8500,
            "building_type": "Data Center",
            "year_built": 2022,
            "energy_intensity_kwh_m2": 68.8,
            "co2_intensity_kg_m2": 12.1,
            "energy_percentile": 11,
            "rating": "Excellent",
            "certifications": ["Nordic Swan", "ISO 50001"],
        },
        {
            "building_id": "BLD-003",
            "name": "Central Office Tower",
            "location": "Gothenburg",
            "floor_area_m2": 9200,
            "building_type": "Office",
            "year_built": 2017,
            "energy_intensity_kwh_m2": 82.4,
            "co2_intensity_kg_m2": 18.1,
            "energy_percentile": 32,
            "rating": "Good",
            "certifications": ["LEED Gold"],
        },
        {
            "building_id": "BLD-004",
            "name": "Campus Building A",
            "location": "Uppsala",
            "floor_area_m2": 6800,
            "building_type": "Education",
            "year_built": 2015,
            "energy_intensity_kwh_m2": 88.6,
            "co2_intensity_kg_m2": 19.5,
            "energy_percentile": 41,
            "rating": "Good",
            "certifications": ["Miljöbyggnad Silver"],
        },
        {
            "building_id": "BLD-005",
            "name": "Retail Complex South",
            "location": "Malmö",
            "floor_area_m2": 15000,
            "building_type": "Retail",
            "year_built": 2012,
            "energy_intensity_kwh_m2": 102.3,
            "co2_intensity_kg_m2": 22.5,
            "energy_percentile": 58,
            "rating": "Average",
            "certifications": ["BREEAM Good"],
        },
        {
            "building_id": "BLD-006",
            "name": "Healthcare Center",
            "location": "Stockholm",
            "floor_area_m2": 11200,
            "building_type": "Healthcare",
            "year_built": 2010,
            "energy_intensity_kwh_m2": 108.7,
            "co2_intensity_kg_m2": 23.9,
            "energy_percentile": 65,
            "rating": "Average",
            "certifications": [],
        },
        {
            "building_id": "BLD-007",
            "name": "Industrial Warehouse",
            "location": "Norrköping",
            "floor_area_m2": 18000,
            "building_type": "Industrial",
            "year_built": 2005,
            "energy_intensity_kwh_m2": 125.4,
            "co2_intensity_kg_m2": 27.6,
            "energy_percentile": 78,
            "rating": "Poor",
            "certifications": [],
        },
        {
            "building_id": "BLD-008",
            "name": "Legacy Office Block",
            "location": "Stockholm",
            "floor_area_m2": 4300,
            "building_type": "Office",
            "year_built": 1995,
            "energy_intensity_kwh_m2": 156.2,
            "co2_intensity_kg_m2": 34.4,
            "energy_percentile": 92,
            "rating": "Poor",
            "certifications": [],
        },
    ]

    # Compute summary from the demo buildings
    summary = compute_portfolio_summary(buildings)

    return {
        "pipeline": "sustainable_building_benchmarking",
        "generated_at": now.isoformat(),
        "demo_mode": True,
        "portfolio_summary": summary,
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
        "buildings": buildings,
        "pagination": {
            "page": 1,
            "page_size": len(buildings),
            "total_in_demo": len(buildings),
            "full_dataset_size": len(buildings),  # No full dataset in this case
            "has_more": False,
        },
    }


if __name__ == "__main__":
    export_demo_json()
