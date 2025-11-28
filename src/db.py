"""
Database interface for HVAC fault detection system.

This module provides functions to interact with PostgreSQL database
for storing and querying HVAC anomalies.
"""

import os
from datetime import datetime, timezone
from typing import Optional, Dict
import pandas as pd
from sqlalchemy import create_engine, text, Column, Integer, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base


# Database connection configuration
def get_db_url():
    """Get database URL from environment or use default."""
    return os.getenv(
        'DATABASE_URL',
        'postgresql://user:password@localhost:5432/hvac_faults'
    )


# SQLAlchemy setup
Base = declarative_base()


class Anomaly(Base):
    """SQLAlchemy model for anomalies table."""
    __tablename__ = 'anomalies'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    zone_id = Column(Text, nullable=False, index=True)
    ahu_id = Column(Text, nullable=False)
    metric = Column(Text, nullable=False)
    score = Column(Float, nullable=False)
    rule_name = Column(Text, nullable=False, index=True)
    severity = Column(Text, nullable=False, index=True)
    fault_type_label = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


def create_tables(drop_existing=False):
    """
    Create database tables.
    
    Args:
        drop_existing: If True, drop existing tables first
    """
    engine = create_engine(get_db_url())
    
    if drop_existing:
        print("Dropping existing tables...")
        Base.metadata.drop_all(engine)
    
    print("Creating tables...")
    Base.metadata.create_all(engine)
    print("✓ Tables created successfully")
    
    return engine


def bulk_insert_anomalies(df: pd.DataFrame, batch_size: int = 1000) -> int:
    """
    Bulk insert anomalies from a DataFrame.
    
    Args:
        df: DataFrame with anomaly records
        batch_size: Number of records to insert per batch
        
    Returns:
        Number of records inserted
    """
    if df.empty:
        print("No anomalies to insert")
        return 0
    
    # Ensure required columns exist
    required_cols = ['timestamp', 'zone_id', 'ahu_id', 'metric', 'score', 
                    'rule_name', 'severity']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
    
    # Add fault_type_label if not present
    if 'fault_type_label' not in df.columns:
        df['fault_type_label'] = None
    
    # Add created_at if not present
    if 'created_at' not in df.columns:
        df['created_at'] = datetime.now(timezone.utc)
    
    engine = create_engine(get_db_url())
    
    # Convert timestamp to datetime if it's not already
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Insert in batches
    total_inserted = 0
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        batch.to_sql('anomalies', engine, if_exists='append', index=False)
        total_inserted += len(batch)
        print(f"  Inserted batch {i // batch_size + 1}: {total_inserted} / {len(df)} records")
    
    print(f"✓ Successfully inserted {total_inserted} anomaly records")
    return total_inserted


def query_anomalies(
    start: Optional[str] = None,
    end: Optional[str] = None,
    zone_id: Optional[str] = None,
    severity: Optional[str] = None,
    rule_name: Optional[str] = None,
    limit: int = 500
) -> pd.DataFrame:
    """
    Query anomalies with optional filters.
    
    Args:
        start: Start timestamp (ISO format)
        end: End timestamp (ISO format)
        zone_id: Filter by zone ID
        severity: Filter by severity level
        rule_name: Filter by detection rule
        limit: Maximum number of records to return
        
    Returns:
        DataFrame with matching anomaly records
    """
    engine = create_engine(get_db_url())
    
    # Build query
    query = "SELECT * FROM anomalies WHERE 1=1"
    params = {}
    
    if start:
        query += " AND timestamp >= :start"
        params['start'] = start
    
    if end:
        query += " AND timestamp <= :end"
        params['end'] = end
    
    if zone_id:
        query += " AND zone_id = :zone_id"
        params['zone_id'] = zone_id
    
    if severity:
        query += " AND severity = :severity"
        params['severity'] = severity
    
    if rule_name:
        query += " AND rule_name = :rule_name"
        params['rule_name'] = rule_name
    
    query += " ORDER BY timestamp DESC"
    query += " LIMIT :limit"
    params['limit'] = limit
    
    # Execute query
    df = pd.read_sql(text(query), engine, params=params)
    
    return df


def get_anomaly_summary(
    start: Optional[str] = None,
    end: Optional[str] = None
) -> Dict:
    """
    Get summary statistics of anomalies.
    
    Args:
        start: Start timestamp (ISO format)
        end: End timestamp (ISO format)
        
    Returns:
        Dictionary with summary statistics
    """
    engine = create_engine(get_db_url())
    
    # Build base query
    where_clause = "WHERE 1=1"
    params = {}
    
    if start:
        where_clause += " AND timestamp >= :start"
        params['start'] = start
    
    if end:
        where_clause += " AND timestamp <= :end"
        params['end'] = end
    
    # Total count
    count_query = f"SELECT COUNT(*) as total FROM anomalies {where_clause}"
    total = pd.read_sql(text(count_query), engine, params=params)['total'][0]
    
    # By severity
    severity_query = f"""
        SELECT severity, COUNT(*) as count 
        FROM anomalies {where_clause}
        GROUP BY severity
        ORDER BY count DESC
    """
    by_severity = pd.read_sql(text(severity_query), engine, params=params).to_dict('records')
    
    # By rule
    rule_query = f"""
        SELECT rule_name, COUNT(*) as count 
        FROM anomalies {where_clause}
        GROUP BY rule_name
        ORDER BY count DESC
    """
    by_rule = pd.read_sql(text(rule_query), engine, params=params).to_dict('records')
    
    # By zone
    zone_query = f"""
        SELECT zone_id, COUNT(*) as count 
        FROM anomalies {where_clause}
        GROUP BY zone_id
        ORDER BY count DESC
        LIMIT 10
    """
    by_zone = pd.read_sql(text(zone_query), engine, params=params).to_dict('records')
    
    return {
        'total': int(total),
        'by_severity': by_severity,
        'by_rule': by_rule,
        'by_zone': by_zone
    }


def clear_anomalies():
    """Clear all anomalies from the database."""
    engine = create_engine(get_db_url())
    with engine.connect() as conn:
        result = conn.execute(text("DELETE FROM anomalies"))
        conn.commit()
        count = result.rowcount
    print(f"✓ Cleared {count} anomaly records")
    return count


def load_anomalies_from_file(filepath: str = "data/processed/anomalies.parquet") -> int:
    """
    Load anomalies from file and insert into database.
    
    Args:
        filepath: Path to anomalies file (parquet or csv)
        
    Returns:
        Number of records inserted
    """
    print(f"Loading anomalies from {filepath}...")
    
    if filepath.endswith('.parquet'):
        df = pd.read_parquet(filepath)
    elif filepath.endswith('.csv'):
        df = pd.read_csv(filepath)
    else:
        raise ValueError("File must be .parquet or .csv")
    
    print(f"Loaded {len(df)} anomaly records")
    
    # Insert into database
    return bulk_insert_anomalies(df)


def main():
    """Main function to set up database and load data."""
    print("=" * 60)
    print("HVAC Database Setup")
    print("=" * 60)
    print()
    
    # Create tables
    create_tables(drop_existing=True)
    print()
    
    # Load anomalies
    try:
        load_anomalies_from_file()
        print()
        
        # Show summary
        print("=" * 60)
        print("Database Summary")
        print("=" * 60)
        summary = get_anomaly_summary()
        print(f"Total anomalies: {summary['total']}")
        print()
        print("By severity:")
        for item in summary['by_severity']:
            print(f"  {item['severity']}: {item['count']}")
        print()
        print("By detection rule:")
        for item in summary['by_rule']:
            print(f"  {item['rule_name']}: {item['count']}")
        print()
        print("Top zones:")
        for item in summary['by_zone']:
            print(f"  {item['zone_id']}: {item['count']}")
        print()
        
    except FileNotFoundError:
        print("Warning: Anomalies file not found. Run models.py first to generate anomalies.")
    
    print("=" * 60)
    print("Database setup complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
