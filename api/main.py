"""
FastAPI application for Building Energy Benchmarking Pipeline

This API provides endpoints for benchmarking building energy performance
and retrieving energy efficiency recommendations.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List
import sys
import os

# Add parent directory to path to import benchmarking module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from benchmarking.model import benchmark_building

app = FastAPI(
    title="Sustainable Building Energy Benchmarking API",
    description="API for benchmarking building energy performance",
    version="0.1.0"
)


class BuildingInput(BaseModel):
    """Input model for building data."""
    building_id: str
    area: float
    energy_consumption: float
    building_type: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "building_id": "B001",
                "area": 1000.0,
                "energy_consumption": 50000.0,
                "building_type": "office"
            }
        }


class BenchmarkResult(BaseModel):
    """Output model for benchmark results."""
    building_id: str
    eui: float
    performance_rating: str
    recommendations: List[str]


@app.get("/")
async def root():
    """Root endpoint providing API information."""
    return {
        "message": "Welcome to the Sustainable Building Energy Benchmarking API",
        "version": "0.1.0",
        "endpoints": {
            "/benchmark": "POST - Benchmark a building's energy performance",
            "/health": "GET - Health check endpoint"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/benchmark", response_model=BenchmarkResult)
async def benchmark(building: BuildingInput) -> Dict[str, Any]:
    """
    Benchmark a building's energy performance.
    
    Args:
        building: Building data including ID, area, energy consumption, and type
        
    Returns:
        Benchmark results including EUI, performance rating, and recommendations
        
    Raises:
        HTTPException: If benchmarking fails
    """
    try:
        building_dict = building.model_dump()
        result = benchmark_building(building_dict)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Benchmarking failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
