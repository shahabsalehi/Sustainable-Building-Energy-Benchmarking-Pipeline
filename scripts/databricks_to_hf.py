#!/usr/bin/env python3
"""
Push Databricks Gold summaries to Hugging Face Datasets.

This script is designed to run after the Databricks medallion pipeline completes.
It uploads the Gold layer JSON summary to the HF Dataset, maintaining the same
contract as other pipelines in the ecosystem.

Usage:
    # From Databricks (after downloading gold_summary.json):
    python scripts/databricks_to_hf.py \
        --json-path /tmp/gold_summary.json \
        --dataset-name shahabsalehi/building-benchmarking \
        --commit-message "Databricks Gold update: 2025-11-29"

    # From GitHub Actions (with HF_TOKEN secret):
    python scripts/databricks_to_hf.py \
        --json-path artifacts/json/gold_summary.json \
        --dataset-name shahabsalehi/building-benchmarking

Environment Variables (or .env file):
    HF_TOKEN: Hugging Face API token with write access
    DATABRICKS_HOST: (optional) Databricks workspace URL
    DATABRICKS_TOKEN: (optional) Databricks PAT for direct export
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Look for .env in multiple locations (priority order)
    env_search_paths = [
        Path(".env"),                                    # Current directory
        Path(__file__).parent.parent / ".env",           # Repo root
        Path(__file__).parent.parent.parent / ".env",    # Workspace root
    ]
    for env_path in env_search_paths:
        if env_path.exists():
            load_dotenv(env_path)
            break
except ImportError:
    pass  # python-dotenv not installed, rely on environment variables

try:
    from huggingface_hub import HfApi, hf_hub_download, CommitOperationAdd
    from huggingface_hub.utils import RepositoryNotFoundError, EntryNotFoundError
except ImportError:
    print("ERROR: huggingface_hub not installed. Run: pip install huggingface_hub")
    sys.exit(1)


def compute_sha256(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()



def get_remote_sha(api: HfApi, dataset_name: str, sha_file: str = ".gold_sha256") -> str | None:
    """Get SHA256 hash from dataset metadata."""
    try:
        metadata_path = hf_hub_download(
            repo_id=dataset_name,
            filename=sha_file,
            repo_type="dataset",
        )
        with open(metadata_path, "r") as f:
            return f.read().strip()
    except (RepositoryNotFoundError, EntryNotFoundError):
        return None
    except Exception as e:
        print(f"Warning: Could not fetch remote SHA: {e}")
        return None


def validate_gold_schema(json_path: Path) -> bool:
    """
    Validate that the Gold JSON has the expected schema.
    
    Expected fields:
    - pipeline: str
    - layer: str (should be "gold")
    - generated_at: str (ISO timestamp)
    - synthetic: bool
    - portfolio_summary: dict
    - by_building_type: list
    - performance_distribution: list
    """
    required_fields = [
        "pipeline",
        "layer",
        "generated_at",
        "synthetic",
        "portfolio_summary",
        "by_building_type",
        "performance_distribution",
    ]
    
    try:
        with open(json_path, "r") as f:
            data = json.load(f)
        
        missing = [field for field in required_fields if field not in data]
        if missing:
            print(f"ERROR: Missing required fields: {missing}")
            return False
        
        if data.get("layer") != "gold":
            print(f"WARNING: layer is '{data.get('layer')}', expected 'gold'")
        
        return True
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON: {e}")
        return False


def push_gold_to_hf(
    json_path: str,
    dataset_name: str,
    commit_message: str | None = None,
    force: bool = False,
) -> bool:
    """
    Push Databricks Gold summary JSON to Hugging Face Dataset.
    
    Args:
        json_path: Path to gold_summary.json
        dataset_name: HF dataset repo (e.g., 'shahabsalehi/building-benchmarking')
        commit_message: Custom commit message (auto-generated if None)
        force: Skip SHA comparison and force upload
        
    Returns:
        bool: True if upload succeeded, False otherwise
    """
    json_file = Path(json_path)
    if not json_file.exists():
        print(f"ERROR: JSON file not found: {json_path}")
        return False
    
    # Validate schema
    if not validate_gold_schema(json_file):
        print("ERROR: Gold summary failed schema validation")
        return False
    
    hf_token = os.environ.get("HF_TOKEN")
    if not hf_token:
        print("ERROR: HF_TOKEN environment variable not set")
        return False
    
    api = HfApi(token=hf_token)
    local_sha = compute_sha256(json_file)
    print(f"Local SHA256: {local_sha[:16]}...")
    
    # Check if data has changed
    if not force:
        remote_sha = get_remote_sha(api, dataset_name)
        if remote_sha and remote_sha == local_sha:
            print(f"✓ Gold data unchanged, skipping push to {dataset_name}")
            return True
    
    # Prepare commit message
    if not commit_message:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        commit_message = f"Databricks Gold update: {timestamp}"
    
    # Upload files
    operations = [
        CommitOperationAdd(
            path_in_repo="gold_summary.json",
            path_or_fileobj=str(json_file),
        ),
        CommitOperationAdd(
            path_in_repo=".gold_sha256",
            path_or_fileobj=local_sha.encode("utf-8"),
        ),
    ]
    
    try:
        api.create_commit(
            repo_id=dataset_name,
            repo_type="dataset",
            operations=operations,
            commit_message=commit_message,
        )
        print(f"✓ Pushed Gold summary to {dataset_name}")
        print(f"  Commit: {commit_message}")
        return True
    except Exception as e:
        print(f"ERROR: Failed to push to HF: {e}")
        return False


def download_from_databricks(
    dbfs_path: str,
    local_path: str,
) -> bool:
    """
    Download Gold summary from Databricks DBFS.
    
    Requires DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.
    
    Args:
        dbfs_path: DBFS path (e.g., /FileStore/benchmarking/gold_summary.json)
        local_path: Local destination path
        
    Returns:
        bool: True if download succeeded
    """
    try:
        from databricks_cli.sdk.api_client import ApiClient
        from databricks_cli.dbfs.api import DbfsApi
    except ImportError:
        print("ERROR: databricks-cli not installed. Run: pip install databricks-cli")
        return False
    
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not host or not token:
        print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
        return False
    
    try:
        api_client = ApiClient(host=host, token=token)
        dbfs_api = DbfsApi(api_client)
        
        dbfs_api.get_file(dbfs_path, local_path, overwrite=True)
        print(f"✓ Downloaded {dbfs_path} to {local_path}")
        return True
    except Exception as e:
        print(f"ERROR: Failed to download from Databricks: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Push Databricks Gold summaries to Hugging Face Datasets"
    )
    parser.add_argument(
        "--json-path",
        type=str,
        help="Path to gold_summary.json file",
    )
    parser.add_argument(
        "--dataset-name",
        type=str,
        default="shahabsalehi/building-benchmarking",
        help="HF Dataset repository name",
    )
    parser.add_argument(
        "--commit-message",
        type=str,
        help="Custom commit message (auto-generated if omitted)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force upload even if data unchanged",
    )
    parser.add_argument(
        "--dbfs-path",
        type=str,
        help="Download from Databricks DBFS path first",
    )
    
    args = parser.parse_args()
    
    # If DBFS path provided, download first
    if args.dbfs_path:
        if not args.json_path:
            args.json_path = "/tmp/gold_summary.json"
        if not download_from_databricks(args.dbfs_path, args.json_path):
            sys.exit(1)
    
    if not args.json_path:
        print("ERROR: --json-path is required (or provide --dbfs-path)")
        parser.print_help()
        sys.exit(1)
    
    success = push_gold_to_hf(
        json_path=args.json_path,
        dataset_name=args.dataset_name,
        commit_message=args.commit_message,
        force=args.force,
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
