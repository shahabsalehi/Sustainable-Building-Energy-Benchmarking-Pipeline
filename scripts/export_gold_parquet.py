#!/usr/bin/env python3
"""
Export Gold summary JSON to Parquet format.

This script converts the Gold layer JSON summary to Parquet files for:
- Local analysis with Pandas, DuckDB, Polars
- Hugging Face Datasets upload
- Archival and backup

Usage:
    python scripts/export_gold_parquet.py \
        --json-path artifacts/json/gold_summary.json \
        --output-dir artifacts/parquet

    # With specific table export:
    python scripts/export_gold_parquet.py \
        --json-path artifacts/json/gold_summary.json \
        --output-dir artifacts/parquet \
        --tables portfolio_by_type performance_distribution
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas pyarrow")
    sys.exit(1)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow not installed. Run: pip install pyarrow")
    sys.exit(1)


# Default tables to export from Gold summary
DEFAULT_TABLES = [
    "by_building_type",
    "performance_distribution",
    "top_efficient_buildings",
]


def load_gold_summary(json_path: Path) -> dict:
    """Load and validate Gold summary JSON."""
    if not json_path.exists():
        raise FileNotFoundError(f"Gold summary not found: {json_path}")
    
    with open(json_path, "r") as f:
        data = json.load(f)
    
    # Validate required fields
    required = ["pipeline", "layer", "generated_at", "synthetic"]
    missing = [f for f in required if f not in data]
    if missing:
        raise ValueError(f"Missing required fields in Gold summary: {missing}")
    
    return data


def export_table_to_parquet(
    data: list[dict],
    output_path: Path,
    table_name: str,
    metadata: dict | None = None,
) -> Path:
    """
    Export a list of records to a Parquet file.
    
    Args:
        data: List of dictionaries (records)
        output_path: Output directory
        table_name: Name of the table (used for filename)
        metadata: Optional metadata to include in Parquet file
        
    Returns:
        Path to created Parquet file
    """
    if not data:
        print(f"  ⚠ Skipping {table_name}: no data")
        return None
    
    df = pd.DataFrame(data)
    
    # Create output path
    parquet_path = output_path / f"{table_name}.parquet"
    
    # Convert to Arrow table with optional metadata
    table = pa.Table.from_pandas(df)
    
    if metadata:
        # Add custom metadata
        existing_meta = table.schema.metadata or {}
        new_meta = {k.encode(): v.encode() for k, v in metadata.items()}
        existing_meta.update(new_meta)
        table = table.replace_schema_metadata(existing_meta)
    
    # Write Parquet with Snappy compression
    pq.write_table(
        table,
        parquet_path,
        compression="snappy",
        version="2.6",  # Parquet 2.6 for better compatibility
    )
    
    print(f"  ✓ {table_name}: {len(df)} rows → {parquet_path}")
    return parquet_path


def export_portfolio_summary_to_parquet(
    summary: dict,
    output_path: Path,
    metadata: dict | None = None,
) -> Path:
    """Export portfolio summary as a single-row Parquet file."""
    
    df = pd.DataFrame([summary])
    parquet_path = output_path / "portfolio_summary.parquet"
    
    table = pa.Table.from_pandas(df)
    if metadata:
        existing_meta = table.schema.metadata or {}
        new_meta = {k.encode(): v.encode() for k, v in metadata.items()}
        existing_meta.update(new_meta)
        table = table.replace_schema_metadata(existing_meta)
    
    pq.write_table(table, parquet_path, compression="snappy", version="2.6")
    print(f"  ✓ portfolio_summary: 1 row → {parquet_path}")
    return parquet_path


def export_gold_to_parquet(
    json_path: str,
    output_dir: str,
    tables: list[str] | None = None,
    include_summary: bool = True,
) -> list[Path]:
    """
    Convert Gold summary JSON to Parquet files.
    
    Args:
        json_path: Path to gold_summary.json
        output_dir: Output directory for Parquet files
        tables: List of table names to export (default: all)
        include_summary: Include portfolio_summary as Parquet
        
    Returns:
        List of created Parquet file paths
    """
    json_file = Path(json_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print(f"Loading Gold summary from {json_file}...")
    gold_data = load_gold_summary(json_file)
    
    # Prepare metadata
    metadata = {
        "pipeline": gold_data.get("pipeline", "unknown"),
        "layer": gold_data.get("layer", "gold"),
        "generated_at": gold_data.get("generated_at", ""),
        "synthetic": str(gold_data.get("synthetic", True)).lower(),
        "exported_at": datetime.now(timezone.utc).isoformat(),
    }
    
    print(f"Exporting to Parquet in {output_path}...")
    exported_files = []
    
    # Export portfolio summary
    if include_summary and "portfolio_summary" in gold_data:
        pq_path = export_portfolio_summary_to_parquet(
            gold_data["portfolio_summary"],
            output_path,
            metadata,
        )
        if pq_path:
            exported_files.append(pq_path)
    
    # Export tables
    tables_to_export = tables or DEFAULT_TABLES
    for table_name in tables_to_export:
        if table_name in gold_data:
            pq_path = export_table_to_parquet(
                gold_data[table_name],
                output_path,
                table_name,
                metadata,
            )
            if pq_path:
                exported_files.append(pq_path)
        else:
            print(f"  ⚠ Table '{table_name}' not found in Gold summary")
    
    print(f"\n✓ Exported {len(exported_files)} Parquet files to {output_path}")
    return exported_files


def main():
    parser = argparse.ArgumentParser(
        description="Export Gold summary JSON to Parquet files"
    )
    parser.add_argument(
        "--json-path",
        type=str,
        default="artifacts/json/gold_summary.json",
        help="Path to gold_summary.json file",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/parquet",
        help="Output directory for Parquet files",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Specific tables to export (default: all)",
    )
    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Skip exporting portfolio_summary table",
    )
    
    args = parser.parse_args()
    
    try:
        exported = export_gold_to_parquet(
            json_path=args.json_path,
            output_dir=args.output_dir,
            tables=args.tables,
            include_summary=not args.no_summary,
        )
        
        if not exported:
            print("No files exported!")
            sys.exit(1)
            
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
