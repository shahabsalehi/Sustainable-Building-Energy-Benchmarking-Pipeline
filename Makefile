.PHONY: help install sample-data test test-cov clean run-api run-dashboard docker-up docker-down export-json validate-json push-gold-to-hf export-parquet venv

# Virtual environment
VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

help:
	@echo "Sustainable Building Energy Benchmarking Pipeline - Quick Commands"
	@echo ""
	@echo "Setup & Installation:"
	@echo "  make install        - Install all Python dependencies"
	@echo ""
	@echo "Sample Data Generation:"
	@echo "  make sample-data    - Generate all sample data (benchmarking + HVAC)"
	@echo ""
	@echo "Testing:"
	@echo "  make test          - Run all tests"
	@echo "  make test-cov      - Run tests with coverage report"
	@echo ""
	@echo "Running Services:"
	@echo "  make docker-up     - Start PostgreSQL database"
	@echo "  make docker-down   - Stop PostgreSQL database"
	@echo "  make run-api       - Start the API server (port 8000)"
	@echo "  make run-dashboard - Start the Streamlit dashboard (port 8501)"
	@echo ""
	@echo "Databricks Integration:"
	@echo "  make export-parquet  - Export Gold JSON to Parquet files"
	@echo "  make push-gold-to-hf - Push Gold summary to HF Dataset"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean         - Remove generated data and caches"

venv: $(VENV)/bin/activate

$(VENV)/bin/activate:
	@echo "Creating virtual environment..."
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@touch $(VENV)/bin/activate

install: venv
	@echo "Dependencies installed via venv target."

sample-data: venv
	@echo "Generating sample data for Building Energy Benchmarking..."
	$(PYTHON) generate_sample_data.py
	@echo ""
	@echo "Generating sample data for HVAC Fault Detection System..."
	$(PYTHON) src/generate_hvac_data.py
	$(PYTHON) src/pipeline_batch.py
	$(PYTHON) src/models.py
	@echo ""
	@echo "All sample data generated successfully."
	@echo "   - Benchmarking data: sample_data/"
	@echo "   - HVAC data: data/raw/ and data/processed/"

test: venv
	$(PYTHON) -m pytest tests/ -v

test-cov: venv
	$(PYTHON) -m pytest tests/ --cov=benchmarking --cov=api --cov=src --cov-report=html --cov-report=term

docker-up:
	docker-compose up -d
	@echo "PostgreSQL database started on localhost:5432"

docker-down:
	docker-compose down

run-api: venv
	@echo "Starting API server at http://localhost:8000"
	@echo "Interactive docs at http://localhost:8000/docs"
	$(VENV)/bin/uvicorn api.main:app --reload

run-dashboard: venv
	@echo "Starting Streamlit dashboard at http://localhost:8501"
	$(VENV)/bin/streamlit run src/dashboard_app.py

clean:
	rm -rf sample_data/*.csv sample_data/*.json
	rm -rf data/raw/* data/processed/*
	rm -rf .pytest_cache
	find . -type d -name "__pycache__" -delete 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "Cleanup complete."

export-json: venv
	@echo "Exporting building benchmarking data (canonical schema)..."
	@mkdir -p artifacts/json
	$(PYTHON) src/export_json.py
	@echo "Done! Output: artifacts/json/building_benchmarking.json"

export-demo-json: venv
	@echo "Exporting slim demo JSON for static frontend (~5 KB)..."
	@mkdir -p artifacts/json
	$(PYTHON) src/export_demo_json.py
	@echo "Done! Output: artifacts/json/building_benchmarking.demo.json"

validate-json: venv
	@echo "Validating JSON schema..."
	$(PYTHON) src/validate_json.py

export-parquet: venv
	@echo "Exporting Gold JSON to Parquet files..."
	@mkdir -p artifacts/parquet
	$(PYTHON) scripts/export_gold_parquet.py \
		--json-path artifacts/json/gold_summary.json \
		--output-dir artifacts/parquet
	@echo "Done! Output: artifacts/parquet/"

push-gold-to-hf: venv
	@echo "Pushing Databricks Gold summary to Hugging Face..."
	$(PYTHON) scripts/databricks_to_hf.py \
		--json-path artifacts/json/gold_summary.json \
		--dataset-name shahabsalehi/building-benchmarking
	@echo "Done!"
