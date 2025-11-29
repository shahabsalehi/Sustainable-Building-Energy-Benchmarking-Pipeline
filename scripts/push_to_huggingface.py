#!/usr/bin/env python3
"""
Push JSON artifacts to Hugging Face Datasets.

Usage:
    python push_to_huggingface.py \
        --json-path artifacts/json/building_benchmarking.json \
        --dataset-name shahabsalehi/building-benchmarking \
        --commit-message "Auto-update: 2025-11-29"

Environment Variables:
    HF_TOKEN: Hugging Face API token with write access
"""

import argparse
import hashlib
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

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


def get_remote_sha(api: HfApi, dataset_name: str, filename: str) -> str | None:
    """Get SHA256 hash from dataset metadata."""
    try:
        metadata_path = hf_hub_download(
            repo_id=dataset_name,
            filename=".data_sha256",
            repo_type="dataset",
        )
        with open(metadata_path, "r") as f:
            return f.read().strip()
    except (RepositoryNotFoundError, EntryNotFoundError):
        return None
    except Exception as e:
        print(f"Warning: Could not fetch remote SHA: {e}")
        return None


def push_to_huggingface(
    json_path: str,
    dataset_name: str,
    commit_message: str | None = None,
    force: bool = False,
) -> bool:
    """Push JSON file to Hugging Face Dataset."""
    json_file = Path(json_path)
    if not json_file.exists():
        print(f"ERROR: JSON file not found: {json_path}")
        sys.exit(1)
    
    hf_token = os.environ.get("HF_TOKEN")
    if not hf_token:
        print("ERROR: HF_TOKEN environment variable not set")
        sys.exit(1)
    
    api = HfApi(token=hf_token)
    local_sha = compute_sha256(json_file)
    print(f"Local SHA256: {local_sha[:16]}...")
    
    if not force:
        remote_sha = get_remote_sha(api, dataset_name, json_file.name)
        if remote_sha and remote_sha == local_sha:
            print(f"✓ Data unchanged, skipping push to {dataset_name}")
            return False
        elif remote_sha:
            print(f"Remote SHA256: {remote_sha[:16]}... (changed)")
        else:
            print("No remote SHA found (first push)")
    
    if not commit_message:
        commit_message = f"Auto-update: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    
    target_filename = json_file.name
    sha_file = json_file.parent / ".data_sha256.tmp"
    sha_file.write_text(local_sha)
    
    try:
        try:
            api.repo_info(repo_id=dataset_name, repo_type="dataset")
            print(f"Dataset {dataset_name} exists")
        except RepositoryNotFoundError:
            print(f"Creating new dataset: {dataset_name}")
            api.create_repo(repo_id=dataset_name, repo_type="dataset", private=False)
        
        operations = [
            CommitOperationAdd(path_in_repo=target_filename, path_or_fileobj=str(json_file)),
            CommitOperationAdd(path_in_repo=".data_sha256", path_or_fileobj=str(sha_file)),
        ]
        
        api.create_commit(
            repo_id=dataset_name,
            repo_type="dataset",
            operations=operations,
            commit_message=commit_message,
        )
        
        print(f"✓ Successfully pushed to {dataset_name}")
        print(f"  File: {target_filename}")
        print(f"  URL: https://huggingface.co/datasets/{dataset_name}")
        return True
        
    finally:
        if sha_file.exists():
            sha_file.unlink()


def main():
    parser = argparse.ArgumentParser(description="Push JSON artifacts to Hugging Face Datasets")
    parser.add_argument("--json-path", required=True, help="Path to JSON file")
    parser.add_argument("--dataset-name", required=True, help="HF Dataset repo name")
    parser.add_argument("--commit-message", default=None, help="Commit message")
    parser.add_argument("--force", action="store_true", help="Force push")
    
    args = parser.parse_args()
    push_to_huggingface(
        json_path=args.json_path,
        dataset_name=args.dataset_name,
        commit_message=args.commit_message,
        force=args.force,
    )


if __name__ == "__main__":
    main()
