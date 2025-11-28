# HVAC Fault Detection System

A comprehensive anomaly detection service for commercial building HVAC systems. This system generates synthetic HVAC sensor data, detects faults using both rule-based and machine learning approaches, and provides an interactive dashboard for monitoring and analysis.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Components](#components)
- [API Documentation](#api-documentation)
- [Dashboard](#dashboard)
- [Testing](#testing)
- [Contributing](#contributing)

## Overview

The HVAC Fault Detection System simulates realistic HVAC sensor data with labeled faults and implements a complete ETL and anomaly detection pipeline. The system can detect four types of common HVAC faults:

1. **Clogged Filter** - Gradual increase in fan speed and power consumption
2. **Compressor Failure** - Sudden drop in power with rising temperatures
3. **Temperature Drift** - Sustained deviation from setpoint
4. **Oscillating Control** - High-frequency temperature oscillations

## Features

- Synthetic data generation for 30-day HVAC datasets
- ETL pipeline with rolling statistics and lag features
- Dual detection: rule-based plus Isolation Forest
- PostgreSQL storage for anomalies
- REST API built with FastAPI
- Interactive Streamlit dashboard
- Comprehensive tests across components

## Architecture

```
┌─────────────────────┐
│  Data Generation    │  Generate synthetic HVAC data with faults
└──────────┬──────────┘
           │
           v
┌─────────────────────┐
│   ETL Pipeline      │  Clean data, engineer features
└──────────┬──────────┘
           │
           v
┌─────────────────────┐
│ Anomaly Detection   │  Rules + ML (Isolation Forest)
└──────────┬──────────┘
           │
           v
┌─────────────────────┐
│  PostgreSQL DB      │  Store detected anomalies
└──────────┬──────────┘
           │
           ├──────────> FastAPI Service (REST API)
           │
           └──────────> Streamlit Dashboard (Web UI)
```

## Installation

### Prerequisites

- Python 3.8+
- Docker and Docker Compose (for PostgreSQL)
- Git

### Clone Repository

```bash
git clone <repository-url>
cd Sustainable-Building-Energy-Benchmarking-Pipeline
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Start PostgreSQL Database

```bash
docker-compose up -d
```

This will start PostgreSQL on `localhost:5432` with:
- Database: `hvac_faults`
- User: `user`
- Password: `password`

## Quick Start

Follow these steps to run the complete pipeline:

### 1. Generate Synthetic HVAC Data

```bash
python src/generate_hvac_data.py
```

**Output:**
- `data/raw/hvac_raw.parquet` - 86,400 records (30 days × 10 zones × 5-min intervals)
- `data/raw/hvac_raw.csv` - Same data in CSV format

**Statistics:**
- Total records: 86,400
- Faulty records: ~1,456 (1.69%)
- Fault types: 4 (clogged_filter, compressor_failure, temp_drift, oscillating_control)

### 2. Run ETL Pipeline

```bash
python src/pipeline_batch.py
```

**Output:**
- `data/processed/hvac_features.parquet` - 28 features including engineered features
- `data/processed/hvac_features_summary.csv` - Feature statistics

**Engineered Features:**
- Temperature error from setpoint
- Rolling statistics (15-min and 60-min windows)
- Lag features (5-min back)
- Rate of change features
- Delta return-supply temperature

### 3. Run Anomaly Detection

```bash
python src/models.py
```

**Output:**
- `data/processed/anomalies.parquet` - ~3,209 detected anomalies
- `data/processed/isolation_forest_model.pkl` - Trained ML model

**Detection Methods:**
- Rules-based: temp_drift, clogged_filter, compressor_failure, oscillating_control
- ML-based: Isolation Forest (trained on normal data only)

### 4. Load Data into Database

```bash
python src/db.py
```

**Output:**
- Creates `anomalies` table in PostgreSQL
- Loads all detected anomalies
- Displays summary statistics

### 5. Start the API Server

```bash
python src/api.py
```

or

```bash
uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload
```

**Access:**
- API: http://localhost:8000
- Interactive docs: http://localhost:8000/docs
- Alternative docs: http://localhost:8000/redoc

### 6. Launch the Dashboard

```bash
streamlit run src/dashboard_app.py
```

**Access:**
- Dashboard: http://localhost:8501

## Components

### 1. Data Generator (`src/generate_hvac_data.py`)

Generates realistic HVAC sensor data with:
- 10 zones (Z1-Z10)
- 5-minute intervals
- Daily temperature patterns
- Weekend variations
- Four injected fault types

**Key Functions:**
- `generate_base_profile()` - Generate normal HVAC behavior
- `inject_faults()` - Inject labeled fault episodes

### 2. ETL Pipeline (`src/pipeline_batch.py`)

Processes raw data and engineers features:
- Data cleaning and sorting
- Feature engineering
- Rolling statistics
- Lag features
- Rate of change calculations

**Key Functions:**
- `load_raw_data()` - Load from parquet/CSV
- `clean_data()` - Sort and handle missing values
- `engineer_features()` - Create 14 engineered features
- `run_etl_pipeline()` - Execute complete pipeline

### 3. Anomaly Detection (`src/models.py`)

Two detection approaches:

**A. Rules-Based Detection (`RulesBasedDetector`)**
- `_detect_temp_drift()` - Temperature >3°C from setpoint for >30 min
- `_detect_clogged_filter()` - High fan speed with elevated power
- `_detect_compressor_failure()` - Low power during cooling with temp rise
- `_detect_oscillating_control()` - >6 sign changes in temp within 1 hour

**B. ML-Based Detection (`MLBasedDetector`)**
- Isolation Forest algorithm
- Trained on normal data only (fault_type == 'none')
- Contamination = 2%
- 11 selected features

**Key Functions:**
- `RulesBasedDetector.detect_anomalies()` - Apply all rules
- `MLBasedDetector.train()` - Train on normal data
- `MLBasedDetector.detect_anomalies()` - Detect using trained model
- `run_anomaly_detection()` - Execute complete detection pipeline

### 4. Database Interface (`src/db.py`)

PostgreSQL interface with SQLAlchemy:

**Schema:**
```sql
CREATE TABLE anomalies (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    zone_id TEXT NOT NULL,
    ahu_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    score FLOAT NOT NULL,
    rule_name TEXT NOT NULL,
    severity TEXT NOT NULL,
    fault_type_label TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for fast querying
CREATE INDEX ON anomalies(timestamp);
CREATE INDEX ON anomalies(zone_id);
CREATE INDEX ON anomalies(rule_name);
CREATE INDEX ON anomalies(severity);
```

**Key Functions:**
- `create_tables()` - Create database schema
- `bulk_insert_anomalies()` - Efficient bulk insert
- `query_anomalies()` - Query with filters (time, zone, severity, rule)
- `get_anomaly_summary()` - Aggregate statistics

### 5. API Service (`src/api.py`)

FastAPI REST API with three main endpoints:

**Endpoints:**
- `GET /` - API information
- `GET /health` - Health check + DB connectivity
- `GET /alerts` - Query anomalies with filters
  - Filters: start, end, zone_id, severity, rule_name
  - Limit: 1-5000 records
  - Sorted by timestamp (descending)
- `GET /alerts/summary` - Aggregate statistics

**Example Requests:**
```bash
# Get all high-severity alerts
curl "http://localhost:8000/alerts?severity=high&limit=100"

# Get alerts for specific zone and time range
curl "http://localhost:8000/alerts?zone_id=Z1&start=2024-01-01T00:00:00&end=2024-01-31T23:59:59"

# Get summary statistics
curl "http://localhost:8000/alerts/summary"
```

### 6. Dashboard (`src/dashboard_app.py`)

Interactive Streamlit dashboard with four tabs:

**Time Series Tab**
- Interactive time-series plot with anomaly markers
- Select zone and metric
- Color-coded severity (high=red, medium=orange, low=yellow)
- Setpoint overlay

**Anomaly Table Tab**
- Sortable, filterable table of anomalies
- Color-coded by severity
- Download as CSV

**Analytics Tab**
- Severity distribution (pie chart)
- Detection rule distribution (bar chart)
- Zone distribution (bar chart)
- Daily anomaly trend (line chart)

**About Tab**
- System documentation
- Detection method descriptions
- Severity level definitions

**Filters (Sidebar):**
- Time range: Last 24h, 7d, 30d, All time, or Custom
- Zone: All or specific zone (Z1-Z10)
- Severity: All, high, medium, low
- Detection rule: All or specific rule

## API Documentation

### Anomaly Query Endpoint

**GET /alerts**

Query anomalies with optional filters.

**Parameters:**
- `start` (optional): Start timestamp in ISO 8601 format (e.g., 2024-01-01T00:00:00)
- `end` (optional): End timestamp in ISO 8601 format
- `zone_id` (optional): Filter by zone (e.g., Z1, Z2)
- `severity` (optional): Filter by severity (low, medium, high)
- `rule_name` (optional): Filter by detection rule
- `limit` (optional): Maximum records to return (1-5000, default 500)

**Response:**
```json
{
  "count": 100,
  "anomalies": [
    {
      "id": 1,
      "timestamp": "2024-01-15T10:30:00",
      "zone_id": "Z1",
      "ahu_id": "AHU1",
      "metric": "temp_zone_c",
      "score": 2.5,
      "rule_name": "temp_drift",
      "severity": "high",
      "fault_type_label": "temp_drift",
      "created_at": "2024-01-30T12:00:00"
    }
  ]
}
```

### Summary Endpoint

**GET /alerts/summary**

Get aggregate statistics.

**Parameters:**
- `start` (optional): Start timestamp
- `end` (optional): End timestamp

**Response:**
```json
{
  "total": 3209,
  "by_severity": [
    {"severity": "high", "count": 2344},
    {"severity": "medium", "count": 865}
  ],
  "by_rule": [
    {"rule_name": "isolation_forest", "count": 1937},
    {"rule_name": "temp_drift", "count": 562}
  ],
  "by_zone": [
    {"zone_id": "Z5", "count": 350},
    {"zone_id": "Z2", "count": 340}
  ]
}
```

## Dashboard

### Features

1. **Real-time Monitoring** - View current system status and anomalies
2. **Time Series Visualization** - Plot sensor data with anomaly markers
3. **Filtering** - Filter by time, zone, severity, and detection rule
4. **Analytics** - View trends and distributions
5. **Export** - Download anomaly data as CSV

### Screenshots

*Screenshots to be added after running the dashboard*

## Testing

The system includes comprehensive test coverage:

### Run All Tests

```bash
pytest tests/ -v
```

### Run Specific Test Modules

```bash
# Data generation tests
pytest tests/test_generate_hvac_data.py -v

# ETL pipeline tests
pytest tests/test_pipeline_batch.py -v

# Anomaly detection tests
pytest tests/test_models.py -v

# Database tests
pytest tests/test_db.py -v
```

### Test Coverage

```bash
pytest tests/ --cov=src --cov-report=html
```

**Current Coverage:**
- Total tests: 37
- All tests currently passing
- Modules covered: Data generation, ETL, Models, Database

## Project Structure

```
Sustainable-Building-Energy-Benchmarking-Pipeline/
├── src/                           # Source code
│   ├── __init__.py
│   ├── generate_hvac_data.py     # Synthetic data generation
│   ├── pipeline_batch.py         # ETL pipeline
│   ├── models.py                  # Anomaly detection models
│   ├── db.py                      # Database interface
│   ├── api.py                     # FastAPI service
│   └── dashboard_app.py           # Streamlit dashboard
├── data/                          # Data storage
│   ├── raw/                       # Raw HVAC data
│   │   ├── hvac_raw.parquet
│   │   └── hvac_raw.csv
│   └── processed/                 # Processed data
│       ├── hvac_features.parquet
│       ├── hvac_features_summary.csv
│       ├── anomalies.parquet
│       └── isolation_forest_model.pkl
├── tests/                         # Test suite
│   ├── test_generate_hvac_data.py
│   ├── test_pipeline_batch.py
│   ├── test_models.py
│   └── test_db.py
├── docker-compose.yml             # PostgreSQL setup
├── requirements.txt               # Python dependencies
├── HVAC_README.md                 # This file
└── README.md                      # Main project README
```

## Configuration

### Environment Variables

Create a `.env` file for custom configuration:

```bash
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/hvac_faults

# API
API_HOST=0.0.0.0
API_PORT=8000

# Dashboard
STREAMLIT_SERVER_PORT=8501
```

### Docker Compose Configuration

Edit `docker-compose.yml` to change PostgreSQL settings:

```yaml
environment:
  POSTGRES_DB: hvac_faults
  POSTGRES_USER: user
  POSTGRES_PASSWORD: password
ports:
  - "5432:5432"
```

## Troubleshooting

### Database Connection Issues

```bash
# Check if PostgreSQL is running
docker-compose ps

# Check logs
docker-compose logs postgres

# Restart PostgreSQL
docker-compose restart postgres
```

### API Not Starting

```bash
# Check if port 8000 is available
netstat -an | grep 8000

# Run with debug logging
uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload --log-level debug
```

### Dashboard Not Loading Data

```bash
# Verify database has data
python -c "from src.db import query_anomalies; print(len(query_anomalies()))"

# Clear Streamlit cache
# Click "Clear cache" in dashboard sidebar or restart the app
```

## Performance

### Data Volume
- Raw data: ~6.6 MB CSV, ~923 KB Parquet
- Processed features: ~2.5 MB Parquet
- Anomalies: ~250 KB Parquet

### Processing Time (approximate)
- Data generation: ~5 seconds
- ETL pipeline: ~10 seconds
- Anomaly detection: ~15 seconds (including ML training)
- Database load: ~2 seconds
- API response: <100ms per request
- Dashboard load: ~2 seconds

### Scalability
- Current: 30 days, 10 zones, 5-min intervals = 86,400 records
- Scalable to: 365 days, 100 zones, 1-min intervals = 52.6M records
- Recommended: Use batch processing and partitioned tables for large datasets

## Future Enhancements

- [ ] Real-time data ingestion from IoT sensors
- [ ] Advanced ML models (LSTM, Autoencoder)
- [ ] Automated alerting (email, Slack, SMS)
- [ ] Multi-building support
- [ ] Energy savings calculations
- [ ] Predictive maintenance scheduling
- [ ] Mobile dashboard app
- [ ] Integration with BMS systems

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Inspired by real-world HVAC fault detection challenges
- Built for the Sustainable Building Energy Benchmarking Pipeline project
- Uses industry-standard tools and libraries (FastAPI, Streamlit, scikit-learn, PostgreSQL)

## Support

For questions, issues, or suggestions:
- Open an issue on GitHub
- Contact the development team
- Check the documentation at `/docs`

---

**Last Updated:** November 2024  
**Version:** 1.0.0  
**Status:** Production-ready
