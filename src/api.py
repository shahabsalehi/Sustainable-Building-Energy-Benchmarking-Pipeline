"""
FastAPI service for HVAC Fault Detection System

This API provides endpoints for querying HVAC anomalies and system health.
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import sys
import os

# Add src directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.db import query_anomalies, get_anomaly_summary, get_db_url

app = FastAPI(
    title="HVAC Fault Detection API",
    description="API for querying HVAC system anomalies and faults",
    version="1.0.0"
)


class AnomalyResponse(BaseModel):
    """Response model for anomaly records."""
    id: int
    timestamp: str
    zone_id: str
    ahu_id: str
    metric: str
    score: float
    rule_name: str
    severity: str
    fault_type_label: Optional[str] = None
    created_at: str


class AnomalyListResponse(BaseModel):
    """Response model for list of anomalies."""
    count: int
    anomalies: List[Dict[str, Any]]


class SummaryResponse(BaseModel):
    """Response model for anomaly summary."""
    total: int
    by_severity: List[Dict[str, Any]]
    by_rule: List[Dict[str, Any]]
    by_zone: List[Dict[str, Any]]


class HealthResponse(BaseModel):
    """Response model for health check."""
    status: str
    timestamp: str
    database: str


@app.get("/", tags=["Info"])
async def root():
    """Root endpoint providing API information."""
    return {
        "message": "HVAC Fault Detection API",
        "version": "1.0.0",
        "endpoints": {
            "/health": "GET - Health check endpoint",
            "/alerts": "GET - Query anomaly alerts with filters",
            "/alerts/summary": "GET - Get anomaly summary statistics",
            "/docs": "GET - Interactive API documentation",
            "/redoc": "GET - Alternative API documentation"
        }
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint.
    
    Returns current system status and database connectivity.
    """
    try:
        # Test database connection
        db_url = get_db_url()
        db_status = "connected"
        
        # Try a simple query to verify connection
        from sqlalchemy import create_engine, text
        engine = create_engine(db_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return {
        "status": "healthy" if "connected" in db_status else "degraded",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database": db_status
    }


@app.get("/alerts", response_model=AnomalyListResponse, tags=["Alerts"])
async def get_alerts(
    start: Optional[str] = Query(
        None,
        description="Start timestamp (ISO 8601 format, e.g., 2024-01-01T00:00:00)"
    ),
    end: Optional[str] = Query(
        None,
        description="End timestamp (ISO 8601 format, e.g., 2024-01-31T23:59:59)"
    ),
    zone_id: Optional[str] = Query(
        None,
        description="Filter by zone ID (e.g., Z1, Z2)"
    ),
    severity: Optional[str] = Query(
        None,
        description="Filter by severity level (low, medium, high)"
    ),
    rule_name: Optional[str] = Query(
        None,
        description="Filter by detection rule (temp_drift, clogged_filter, compressor_failure, oscillating_control, isolation_forest)"
    ),
    limit: int = Query(
        500,
        ge=1,
        le=5000,
        description="Maximum number of records to return (1-5000)"
    )
):
    """
    Query HVAC anomaly alerts with optional filters.
    
    Returns a list of detected anomalies matching the specified criteria.
    Results are ordered by timestamp (most recent first).
    
    **Example queries:**
    - `/alerts?start=2024-01-01T00:00:00&end=2024-01-31T23:59:59`
    - `/alerts?zone_id=Z1&severity=high`
    - `/alerts?rule_name=temp_drift&limit=100`
    """
    try:
        # Validate timestamps if provided
        if start:
            try:
                datetime.fromisoformat(start.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid start timestamp format. Use ISO 8601 (e.g., 2024-01-01T00:00:00)"
                )
        
        if end:
            try:
                datetime.fromisoformat(end.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid end timestamp format. Use ISO 8601 (e.g., 2024-01-31T23:59:59)"
                )
        
        # Validate severity if provided
        if severity and severity not in ['low', 'medium', 'high']:
            raise HTTPException(
                status_code=400,
                detail="Invalid severity. Must be one of: low, medium, high"
            )
        
        # Query database
        df = query_anomalies(
            start=start,
            end=end,
            zone_id=zone_id,
            severity=severity,
            rule_name=rule_name,
            limit=limit
        )
        
        # Convert to list of dicts
        anomalies = df.to_dict('records')
        
        # Convert timestamps to ISO format strings
        for anomaly in anomalies:
            if 'timestamp' in anomaly:
                anomaly['timestamp'] = anomaly['timestamp'].isoformat()
            if 'created_at' in anomaly:
                anomaly['created_at'] = anomaly['created_at'].isoformat()
        
        return {
            "count": len(anomalies),
            "anomalies": anomalies
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to query anomalies: {str(e)}"
        )


@app.get("/alerts/summary", response_model=SummaryResponse, tags=["Alerts"])
async def get_alerts_summary(
    start: Optional[str] = Query(
        None,
        description="Start timestamp (ISO 8601 format)"
    ),
    end: Optional[str] = Query(
        None,
        description="End timestamp (ISO 8601 format)"
    )
):
    """
    Get summary statistics of HVAC anomalies.
    
    Returns aggregated statistics including:
    - Total count of anomalies
    - Breakdown by severity level
    - Breakdown by detection rule
    - Breakdown by zone (top 10)
    
    **Example queries:**
    - `/alerts/summary`
    - `/alerts/summary?start=2024-01-01T00:00:00&end=2024-01-31T23:59:59`
    """
    try:
        # Validate timestamps if provided
        if start:
            try:
                datetime.fromisoformat(start.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid start timestamp format"
                )
        
        if end:
            try:
                datetime.fromisoformat(end.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid end timestamp format"
                )
        
        # Get summary
        summary = get_anomaly_summary(start=start, end=end)
        
        return summary
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get summary: {str(e)}"
        )


@app.exception_handler(404)
async def not_found_handler(request, exc):
    """Custom 404 handler."""
    return JSONResponse(
        status_code=404,
        content={
            "detail": "Endpoint not found",
            "message": "Check /docs for available endpoints"
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
