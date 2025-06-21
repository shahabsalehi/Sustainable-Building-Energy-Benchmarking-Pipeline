# Sustainable Building Energy Benchmarking Pipeline

A comprehensive Python-based platform for building energy analysis, including:
1. **Building Energy Benchmarking** - Compare building performance against industry standards
2. **HVAC Fault Detection System** - Advanced anomaly detection for HVAC systems

## Key Features

### Building Energy Benchmarking
- **ETL Pipeline**: Extract, Transform, Load functionality for building energy data
- **Benchmarking Engine**: Calculate Energy Use Intensity (EUI) and compare buildings
- **RESTful API**: FastAPI-based web service for accessing benchmarking functionality
- **Sample Data Generation**: Generate synthetic building energy data for testing

### HVAC Fault Detection System (Lightweight Demo)
- **Synthetic Data Generation**: Realistic HVAC sensor data with labeled faults
- **Dual Anomaly Detection**: Rules-based + ML-based (Isolation Forest) detection
- **PostgreSQL Database**: Efficient storage and querying of detected anomalies
- **REST API**: FastAPI endpoints for programmatic access to alerts
- **Interactive Dashboard**: Real-time Streamlit dashboard with visualizations
- **4 Fault Types**: Clogged filter, compressor failure, temperature drift, oscillating control

This HVAC component is a slim demo aligned with the dedicated `HVAC-Fault-Detection-with-Anomaly-Pipeline` repository; use that project for deeper fault-detection workflows.

## Project Structure

```
├── api/                      # Original benchmarking API
│   ├── __init__.py
│   └── main.py              # API endpoints and server
├── benchmarking/            # Core benchmarking logic
│   ├── __init__.py
│   └── model.py             # Benchmarking functions and models
├── src/                     # HVAC Fault Detection System (NEW!)
│   ├── __init__.py
│   ├── generate_hvac_data.py    # Synthetic HVAC data generation
│   ├── pipeline_batch.py        # ETL pipeline with feature engineering
│   ├── models.py                # Anomaly detection (rules + ML)
│   ├── db.py                    # PostgreSQL database interface
│   ├── api.py                   # FastAPI service for alerts
│   └── dashboard_app.py         # Streamlit interactive dashboard
├── data/                    # Data storage
│   ├── raw/                 # Raw HVAC data (86,400 records)
│   └── processed/           # Processed features and anomalies
├── notebooks/               # Jupyter notebooks for analysis
├── sample_data/             # Sample benchmarking data
├── tests/                   # Test suite for benchmarking + HVAC components (run `make test`)
│   ├── test_benchmarking.py
│   ├── test_generate_hvac_data.py
│   ├── test_pipeline_batch.py
│   ├── test_models.py
│   └── test_db.py
├── docker-compose.yml       # PostgreSQL setup for HVAC system
├── generate_sample_data.py  # Original ETL script
├── requirements.txt         # Python dependencies
├── README.md               # This file
└── HVAC_README.md          # Detailed HVAC system documentation
```

## Quick Start

Get started with the project in 4 simple steps:

```bash
# 0. Create and activate virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate    # Linux/macOS
# .venv\Scripts\activate     # Windows

# 1. Install dependencies
make install
# Or: pip install -r requirements.txt

# 2. Generate sample data (one command for all systems!)
make sample-data

# 3. Run tests to verify everything works
make test
```

That's it! You now have all sample data generated and can start using the system.

### Demo Steps (Frontend Integration)

Run the full pipeline and export data for the frontend demo:

```bash
# 1. Setup and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
make install

# 3. Generate all sample data (benchmarking + HVAC)
make sample-data

# 4. Export canonical JSON for frontend
make export-json

# 5. Validate JSON schema
make validate-json
```

**Artifact paths:**
| Output | Path | Size |
|--------|------|------|
| Benchmarking data (demo slice) | `sample_data/buildings.csv` | ~5 KB (6 buildings) |
| HVAC raw data | `data/raw/hvac_raw.parquet` | ~5 MB |
| HVAC processed | `data/processed/hvac_features.parquet` | ~8 MB |
| Anomalies | `data/processed/anomalies.parquet` | ~500 KB |
| **Frontend JSON** | `artifacts/json/building_benchmarking.json` | ~5 KB |

Sync to frontend demo:
```bash
cd ../energy-pipeline-demo && ./sync-data.sh
```

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/shahabsalehi/Sustainable-Building-Energy-Benchmarking-Pipeline.git
   cd Sustainable-Building-Energy-Benchmarking-Pipeline
   ```

2. **Create virtual environment** (recommended):
   ```bash
   python -m venv .venv
   source .venv/bin/activate    # Linux/macOS
   # .venv\Scripts\activate     # Windows
   ```

3. **Install dependencies**:
   ```bash
   make install
   # Or: pip install -r requirements.txt
   ```

## Sample Data Generation

Generate all sample data with a **single command**:

```bash
make sample-data
```

This generates:
- **Benchmarking data**: a compact demo slice (6 buildings) in `sample_data/` by default; use generator flags to produce larger sets if needed
- **HVAC data**: 30 days of sensor data (86,400 records) with fault detection in `data/`

Alternatively, generate data for each system separately:

```bash
# Benchmarking data only
python generate_sample_data.py

# HVAC data only
python src/generate_hvac_data.py
python src/pipeline_batch.py
python src/models.py
```

## Usage

### Run the API Server

Start the FastAPI server using Make:

```bash
make run-api
```

Or directly:

```bash
python -m uvicorn api.main:app --reload
# Or: python api/main.py
```

The API will be available at `http://localhost:8000` with interactive docs at `http://localhost:8000/docs`

### API Endpoints

- **GET /** - API information and available endpoints
- **GET /health** - Health check endpoint
- **POST /benchmark** - Benchmark a building's energy performance

#### Example API Request

```bash
curl -X POST "http://localhost:8000/benchmark" \
  -H "Content-Type: application/json" \
  -d '{
    "building_id": "B001",
    "area": 1000,
    "energy_consumption": 50000,
    "building_type": "office"
  }'
```

#### Example Response

```json
{
  "building_id": "B001",
  "eui": 50.0,
  "performance_rating": "Good",
  "recommendations": [
    "Consider LED lighting upgrades",
    "Review HVAC system efficiency",
    "Implement building automation system"
  ]
}
```

### Using the Benchmarking Module

```python
from benchmarking.model import benchmark_building

building_data = {
    'building_id': 'B001',
    'area': 1000,  # square meters
    'energy_consumption': 50000,  # kWh per year
    'building_type': 'office'
}

result = benchmark_building(building_data)
print(f"EUI: {result['eui']} kWh/m²/year")
print(f"Rating: {result['performance_rating']}")
```

## Testing

Run all tests with a single command:

```bash
make test
```

Or with coverage report:

```bash
make test-cov
```

Or directly with pytest:

```bash
# All tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=benchmarking --cov=api --cov=src --cov-report=html

# Specific test modules
pytest tests/test_benchmarking.py -v
pytest tests/test_generate_hvac_data.py -v
pytest tests/test_pipeline_batch.py -v
pytest tests/test_models.py -v
pytest tests/test_db.py -v
```

## HVAC Fault Detection System

### Quick Start

The HVAC Fault Detection System is a complete anomaly detection pipeline for commercial building HVAC systems.

#### 1. Start PostgreSQL Database

```bash
make docker-up
# Or: docker-compose up -d
```

#### 2. Generate Sample Data (if not done already)

```bash
make sample-data
```

This automatically runs the complete HVAC pipeline (data generation, ETL, anomaly detection).

#### 3. Load Anomalies into Database

```bash
python src/db.py
```

#### 4. Start the API Server

```bash
python src/api.py
```

Access the API at http://localhost:8000 and interactive docs at http://localhost:8000/docs

#### 5. Launch the Dashboard

```bash
make run-dashboard
# Or: streamlit run src/dashboard_app.py
```

Access the dashboard at http://localhost:8501

### HVAC System Features

- **Synthetic Data**: 86,400 realistic HVAC sensor records with 4 fault types
- **ETL Pipeline**: 28 features including rolling statistics and lag features
- **Anomaly Detection**:
  - Rules-based: 4 detection rules (temp_drift, clogged_filter, compressor_failure, oscillating_control)
  - ML-based: Isolation Forest trained on normal data
  - Total: ~3,209 anomalies detected
- **PostgreSQL Database**: Efficient storage with indexed queries
- **REST API**: 3 endpoints for querying alerts and summaries
- **Interactive Dashboard**: Real-time monitoring with time-series plots and analytics

### API Examples

```bash
# Get high-severity alerts
curl "http://localhost:8000/alerts?severity=high&limit=100"

# Get alerts for specific zone
curl "http://localhost:8000/alerts?zone_id=Z1&start=2024-01-01T00:00:00&end=2024-01-31T23:59:59"

# Get summary statistics
curl "http://localhost:8000/alerts/summary"
```

### Dashboard Features

- **4 Interactive Tabs**:
  - Time Series: View sensor data with anomaly markers
  - Anomaly Table: Sortable, filterable table with CSV export
  - Analytics: Charts and distributions (severity, rules, zones, trends)
  - About: System documentation

- **Filters**: Time range, zone, severity, detection rule
- **Visualizations**: 6 interactive charts using Plotly
- **Real-time**: Auto-refresh with 60-second cache

For complete HVAC system documentation, see [HVAC_README.md](HVAC_README.md)

## Development

### Quick Commands with Make

For convenience, a Makefile is provided with common commands:

```bash
make help           # Show all available commands
make install        # Install dependencies
make sample-data    # Generate all sample data
make test           # Run all tests
make test-cov       # Run tests with coverage
make docker-up      # Start PostgreSQL
make docker-down    # Stop PostgreSQL
make run-api        # Start API server
make run-dashboard  # Start dashboard
make export-json    # Export benchmarking data to JSON
make clean          # Remove generated data and caches
```

## Frontend Export

Export benchmarking data to JSON for integration with the `energy-pipeline-demo` frontend:

### Environment Setup

```bash
# Create and activate virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate    # Linux/macOS
# .venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
# Or: make install
```

### Export Command

```bash
# Export benchmarking data to JSON
make export-json

# Output: artifacts/json/building_benchmarking.json (canonical)
```

### Canonical Schema (Recommended)

The **canonical** export `building_benchmarking.json` provides a rich, production-ready schema:

```json
{
  "pipeline": "sustainable_building_benchmarking",
  "generated_at": "2025-11-28T12:00:00Z",
  "portfolio_summary": {
    "total_buildings": 12,
    "total_floor_area_m2": 45680,
    "avg_energy_intensity_kwh_m2": 98.4,
    "portfolio_co2_tons": 892.5,
    "top_performer_pct": 25,
    "needs_improvement_pct": 33
  },
  "benchmark_categories": {
    "energy_intensity": {
      "excellent": "< 70 kWh/m²",
      "good": "70-90 kWh/m²",
      "average": "90-110 kWh/m²",
      "poor": "> 110 kWh/m²"
    }
  },
  "buildings": [
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
      "certifications": ["LEED Gold", "BREEAM Excellent"]
    }
  ]
}
```

| Section | Fields | Description |
|---------|--------|-------------|
| `pipeline` | string | Pipeline identifier |
| `generated_at` | ISO 8601 UTC | Export timestamp |
| `portfolio_summary` | object | Portfolio-wide aggregate metrics |
| `benchmark_categories` | object | Rating thresholds definition |
| `buildings` | array | Individual building records with metrics |

### Sync to Frontend

After export, copy to the frontend demo:

```bash
# From workspace root
cp Sustainable-Building-Energy-Benchmarking-Pipeline/artifacts/json/building_benchmarking.json \
   energy-pipeline-demo/public/data/

# Or use the sync script
cd energy-pipeline-demo && ./sync-data.sh
```

## Productionization Path

This pipeline is designed for easy transition to production databases:

### Database Integration

The canonical JSON schema maps directly to relational tables:

| JSON Section | Database Table | Notes |
|--------------|----------------|-------|
| `portfolio_summary` | `fact_portfolio_metrics` | Aggregate portfolio KPIs |
| `benchmark_categories` | `dim_benchmark_thresholds` | Rating definitions |
| `buildings` | `dim_buildings` + `fact_building_metrics` | Building data + metrics |

### PostgreSQL Example

```sql
-- Dimension table for buildings
CREATE TABLE dim_buildings (
    building_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    floor_area_m2 NUMERIC,
    building_type VARCHAR(100),
    year_built INT
);

-- Fact table for building metrics
CREATE TABLE fact_building_metrics (
    id SERIAL PRIMARY KEY,
    building_id VARCHAR(50) REFERENCES dim_buildings(building_id),
    energy_intensity_kwh_m2 NUMERIC,
    co2_intensity_kg_m2 NUMERIC,
    energy_percentile INT,
    rating VARCHAR(50),
    certifications JSONB,
    measured_at TIMESTAMPTZ DEFAULT NOW()
);

-- Portfolio summary view
CREATE VIEW vw_portfolio_summary AS
SELECT 
    COUNT(*) as total_buildings,
    SUM(floor_area_m2) as total_floor_area_m2,
    AVG(energy_intensity_kwh_m2) as avg_energy_intensity,
    SUM(co2_intensity_kg_m2 * floor_area_m2) / 1000 as portfolio_co2_tons
FROM dim_buildings b
JOIN fact_building_metrics m ON b.building_id = m.building_id;
```

### Migration Strategy

1. **Development**: Use JSON exports + `make export-json`
2. **Staging**: Add database loader, integrate with existing `docker-compose.yml` PostgreSQL
3. **Production**: Use same schema, add scheduled refresh via Airflow/Prefect

The benchmarking API (`api/main.py`) already supports database-backed queries and can be switched from file to DB mode.

### Project Components

1. **Benchmarking Engine** (`benchmarking/model.py`):
   - Calculate Energy Use Intensity (EUI)
   - Compare building performance against benchmarks
   - Generate energy efficiency recommendations

2. **FastAPI Application** (`api/main.py`):
   - RESTful API for benchmarking services
   - Input validation using Pydantic models
   - Health check and status endpoints

3. **ETL Pipeline** (`generate_sample_data.py`):
   - Extract: Generate/load building data
   - Transform: Calculate metrics and categorize performance
   - Load: Save processed data in multiple formats

### Adding New Features

- To add new benchmarking metrics, extend `benchmarking/model.py`
- To add new API endpoints, modify `api/main.py`
- To add data sources, update the ETL functions in `generate_sample_data.py`

## Technologies

- **Python 3.8+**
- **FastAPI** - Modern web framework for building APIs
- **Streamlit** - Interactive data dashboards
- **Pandas** - Data manipulation and analysis
- **NumPy** - Numerical computing
- **Scikit-learn** - Machine learning (Isolation Forest)
- **SQLAlchemy** - Database ORM
- **PostgreSQL** - Relational database (via Docker)
- **Plotly** - Interactive visualizations
- **Pytest** - Testing framework
- **Uvicorn** - ASGI server

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on:

- Setting up your development environment
- Running tests
- Coding standards and best practices
- Submitting pull requests

Quick start for contributors:
```bash
make install      # Set up environment
make sample-data  # Generate test data
make test         # Run test suite
```

## Maintenance Notes
- For larger benchmarking datasets, adjust generator parameters and switch API mode for pagination.
- For HVAC fault demos beyond the light slice here, use the dedicated `HVAC-Fault-Detection-with-Anomaly-Pipeline`.
