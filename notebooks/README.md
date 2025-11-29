# Notebooks

This directory contains Jupyter notebooks and Databricks notebooks for data exploration, analysis, and the medallion architecture ETL pipeline.

## Databricks Medallion Pipeline

| File | Description |
|------|-------------|
| `benchmarking_medallion.py` | Bronze → Silver → Gold ETL pipeline for Databricks |
| `git_sync.py` | Git helper for Databricks Repos (pull/push/status) |

### Running in Databricks

1. Import `benchmarking_medallion.py` into your Databricks workspace
2. Attach to a cluster (Community Edition works)
3. Run cells sequentially to execute the full pipeline
4. Gold summary is saved to `/FileStore/benchmarking/gold_summary.json`

### Automation (Databricks Pro/Enterprise)

For paid Databricks tiers with Jobs API access:

| File | Description |
|------|-------------|
| `databricks-gold-to-hf.yml.example` | GitHub Actions workflow for automated sync |

To enable automation:
1. Rename to `.github/workflows/databricks-gold-to-hf.yml`
2. Add required secrets (see file comments)
3. Create a Databricks Job to run the notebook

> **Note**: Databricks Community Edition doesn't support Jobs API. Use the manual workflow or rely on `generate-and-publish.yml` for daily HF updates.

## Getting Started

1. Install Jupyter: `pip install jupyter`
2. Launch Jupyter: `jupyter notebook`
3. Create and save your notebooks in this directory
