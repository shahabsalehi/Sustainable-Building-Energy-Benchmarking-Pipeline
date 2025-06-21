"""
Anomaly Detection Models for HVAC Fault Detection

This module implements both rules-based and ML-based anomaly detection
for HVAC systems.
"""

import pandas as pd
import numpy as np
from typing import List, Dict
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import pickle
import os


class RulesBasedDetector:
    """
    Rules-based anomaly detector for HVAC faults.
    
    Implements detection rules for:
    - Temperature drift
    - Clogged filter
    - Compressor failure
    - Oscillating control
    """
    
    def __init__(self):
        """Initialize the rules-based detector."""
        self.rules = {
            'temp_drift': self._detect_temp_drift,
            'clogged_filter': self._detect_clogged_filter,
            'compressor_failure': self._detect_compressor_failure,
            'oscillating_control': self._detect_oscillating_control
        }
    
    def detect_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect anomalies using rule-based logic.
        
        Args:
            df: DataFrame with HVAC features
            
        Returns:
            DataFrame with anomaly records
        """
        print("Running rules-based anomaly detection...")
        
        anomalies = []
        
        # Apply each rule
        for rule_name, rule_func in self.rules.items():
            rule_anomalies = rule_func(df)
            anomalies.extend(rule_anomalies)
            print(f"  {rule_name}: {len(rule_anomalies)} anomalies detected")
        
        if not anomalies:
            # Return empty DataFrame with correct schema
            return pd.DataFrame(columns=[
                'timestamp', 'zone_id', 'ahu_id', 'metric', 'score', 
                'rule_name', 'severity', 'fault_type_label'
            ])
        
        return pd.DataFrame(anomalies)
    
    def _detect_temp_drift(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect temperature drift from setpoint.
        
        Rule: temp_error_c > +3°C for > 30 minutes (6+ consecutive readings)
        """
        anomalies = []
        
        for zone in df['zone_id'].unique():
            zone_df = df[df['zone_id'] == zone].reset_index(drop=True)
            
            # Find periods where temp error exceeds threshold
            threshold = 3.0
            min_duration = 6  # 30 minutes at 5-min intervals
            
            # Mark points that exceed threshold
            exceeds = zone_df['temp_error_c'] > threshold
            
            # Find consecutive sequences
            start_idx = None
            for i, exceeds_now in enumerate(exceeds):
                if exceeds_now and start_idx is None:
                    start_idx = i
                elif not exceeds_now and start_idx is not None:
                    # End of sequence
                    duration = i - start_idx
                    if duration >= min_duration:
                        # Record anomalies for this period
                        for idx in range(start_idx, i):
                            row = zone_df.iloc[idx]
                            severity = 'high' if row['temp_error_c'] > 5 else 'medium'
                            anomalies.append({
                                'timestamp': row['timestamp'],
                                'zone_id': row['zone_id'],
                                'ahu_id': row['ahu_id'],
                                'metric': 'temp_zone_c',
                                'score': min(3.0, abs(row['temp_error_c']) / threshold),
                                'rule_name': 'temp_drift',
                                'severity': severity,
                                'fault_type_label': row.get('fault_type', 'unknown')
                            })
                    start_idx = None
            
            # Handle sequence that goes to end
            if start_idx is not None:
                duration = len(zone_df) - start_idx
                if duration >= min_duration:
                    for idx in range(start_idx, len(zone_df)):
                        row = zone_df.iloc[idx]
                        severity = 'high' if row['temp_error_c'] > 5 else 'medium'
                        anomalies.append({
                            'timestamp': row['timestamp'],
                            'zone_id': row['zone_id'],
                            'ahu_id': row['ahu_id'],
                            'metric': 'temp_zone_c',
                            'score': min(3.0, abs(row['temp_error_c']) / threshold),
                            'rule_name': 'temp_drift',
                            'severity': severity,
                            'fault_type_label': row.get('fault_type', 'unknown')
                        })
        
        return anomalies
    
    def _detect_clogged_filter(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect clogged filter condition.
        
        Rule: fan_speed_pct > 70 AND power_rolling_mean_60min increased significantly
        """
        anomalies = []
        
        # Apply the rule - clogged filter causes high fan speed with elevated power
        mask = (df['fan_speed_pct'] > 70) & (df['fan_rolling_mean_15min'] > 65)
        
        for idx in df[mask].index:
            row = df.loc[idx]
            # Severity based on how extreme the fan speed is
            severity = 'high' if row['fan_speed_pct'] > 80 else 'medium'
            
            anomalies.append({
                'timestamp': row['timestamp'],
                'zone_id': row['zone_id'],
                'ahu_id': row['ahu_id'],
                'metric': 'fan_speed_pct',
                'score': 2.0,
                'rule_name': 'clogged_filter',
                'severity': severity,
                'fault_type_label': row.get('fault_type', 'unknown')
            })
        
        return anomalies
    
    def _detect_compressor_failure(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect compressor failure.
        
        Rule: power_kw < 2.5 AND temp_error_c > +1.5°C AND mode == "cooling"
        (adjusted for actual fault characteristics)
        """
        anomalies = []
        
        # Apply the rule (only during cooling mode)
        # Also check that it's during business hours when cooling is expected
        mask = (df['power_kw'] < 2.5) & \
               (df['temp_error_c'] > 1.5) & \
               (df['mode'] == 'cooling') & \
               (df['power_rolling_mean_60min'] < 3.0)
        
        for idx in df[mask].index:
            row = df.loc[idx]
            # High severity as compressor failure is critical
            severity = 'high'
            
            anomalies.append({
                'timestamp': row['timestamp'],
                'zone_id': row['zone_id'],
                'ahu_id': row['ahu_id'],
                'metric': 'power_kw',
                'score': 3.0,
                'rule_name': 'compressor_failure',
                'severity': severity,
                'fault_type_label': row.get('fault_type', 'unknown')
            })
        
        return anomalies
    
    def _detect_oscillating_control(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detect oscillating control behavior.
        
        Rule: High-frequency sign changes in temp_change_rate within 1 hour
        """
        anomalies = []
        
        for zone in df['zone_id'].unique():
            zone_df = df[df['zone_id'] == zone].reset_index(drop=True)
            
            # Look at rolling window of 12 points (1 hour)
            window_size = 12
            i = 0
            
            while i < len(zone_df) - window_size + 1:
                window = zone_df.iloc[i:i + window_size]
                
                # Count sign changes in temperature change rate
                temp_changes = window['temp_change_rate'].values
                sign_changes = np.sum(np.diff(np.sign(temp_changes)) != 0)
                
                # If more than 6 sign changes in 1 hour, it's oscillating
                if sign_changes > 6:
                    # Mark all points in this window as anomalous
                    for idx in range(i, i + window_size):
                        row = zone_df.iloc[idx]
                        anomalies.append({
                            'timestamp': row['timestamp'],
                            'zone_id': row['zone_id'],
                            'ahu_id': row['ahu_id'],
                            'metric': 'temp_zone_c',
                            'score': 2.0,
                            'rule_name': 'oscillating_control',
                            'severity': 'medium',
                            'fault_type_label': row.get('fault_type', 'unknown')
                        })
                    # Skip ahead to avoid duplicate detection in overlapping windows
                    i += window_size
                else:
                    i += 1
        
        return anomalies


class MLBasedDetector:
    """
    Machine Learning-based anomaly detector using Isolation Forest.
    """
    
    def __init__(self, contamination=0.02, random_state=42):
        """
        Initialize ML-based detector.
        
        Args:
            contamination: Expected proportion of anomalies in the data
            random_state: Random seed for reproducibility
        """
        self.model = IsolationForest(
            contamination=contamination,
            random_state=random_state,
            n_estimators=100
        )
        self.scaler = StandardScaler()
        self.feature_cols = None
        self.is_trained = False
    
    def train(self, df: pd.DataFrame, normal_only=True):
        """
        Train the Isolation Forest model.
        
        Args:
            df: DataFrame with HVAC features
            normal_only: If True, train only on normal data (fault_type == 'none')
        """
        print("Training ML-based anomaly detector...")
        
        # Select numeric features for training
        self.feature_cols = [
            'temp_zone_c', 'temp_error_c', 'power_kw', 'fan_speed_pct',
            'delta_return_supply', 'temp_error_rolling_mean_60min',
            'temp_error_rolling_std_60min', 'power_rolling_mean_60min',
            'power_rolling_std_60min', 'temp_change_rate', 'power_change_rate'
        ]
        
        # Filter to normal data if requested
        if normal_only:
            train_df = df[df['fault_type'] == 'none']
            print(f"  Training on {len(train_df)} normal records")
        else:
            train_df = df
            print(f"  Training on {len(train_df)} total records")
        
        # Prepare features
        X = train_df[self.feature_cols].values
        
        # Fit scaler and transform
        X_scaled = self.scaler.fit_transform(X)
        
        # Train model
        self.model.fit(X_scaled)
        self.is_trained = True
        
        print("  Training complete!")
    
    def detect_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect anomalies using the trained model.
        
        Args:
            df: DataFrame with HVAC features
            
        Returns:
            DataFrame with anomaly records
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before detection")
        
        print("Running ML-based anomaly detection...")
        
        # Prepare features
        X = df[self.feature_cols].values
        X_scaled = self.scaler.transform(X)
        
        # Predict anomalies
        predictions = self.model.predict(X_scaled)
        scores = self.model.score_samples(X_scaled)
        
        # Convert predictions (-1 for anomaly, 1 for normal)
        is_anomaly = predictions == -1
        
        print(f"  Detected {is_anomaly.sum()} anomalies")
        
        # Create anomaly records
        anomalies = []
        for idx in np.where(is_anomaly)[0]:
            row = df.iloc[idx]
            # Convert anomaly score (more negative = more anomalous)
            anomaly_score = abs(scores[idx])
            
            # Determine severity based on score
            if anomaly_score > 0.5:
                severity = 'high'
            elif anomaly_score > 0.3:
                severity = 'medium'
            else:
                severity = 'low'
            
            anomalies.append({
                'timestamp': row['timestamp'],
                'zone_id': row['zone_id'],
                'ahu_id': row['ahu_id'],
                'metric': 'multiple',
                'score': round(anomaly_score, 4),
                'rule_name': 'isolation_forest',
                'severity': severity,
                'fault_type_label': row.get('fault_type', 'unknown')
            })
        
        if not anomalies:
            return pd.DataFrame(columns=[
                'timestamp', 'zone_id', 'ahu_id', 'metric', 'score', 
                'rule_name', 'severity', 'fault_type_label'
            ])
        
        return pd.DataFrame(anomalies)
    
    def save_model(self, filepath: str):
        """Save trained model to disk."""
        if not self.is_trained:
            raise ValueError("Cannot save untrained model")
        
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'feature_cols': self.feature_cols
        }
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"Model saved to {filepath}")
    
    def load_model(self, filepath: str):
        """Load trained model from disk."""
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model = model_data['model']
        self.scaler = model_data['scaler']
        self.feature_cols = model_data['feature_cols']
        self.is_trained = True
        
        print(f"Model loaded from {filepath}")


def run_anomaly_detection(
    features_path: str = "data/processed/hvac_features.parquet",
    output_path: str = "data/processed/anomalies.parquet",
    use_ml: bool = True
) -> pd.DataFrame:
    """
    Run complete anomaly detection pipeline.
    
    Args:
        features_path: Path to processed features
        output_path: Path to save anomalies
        use_ml: Whether to include ML-based detection
        
    Returns:
        DataFrame with all detected anomalies
    """
    print("=" * 60)
    print("HVAC Anomaly Detection Pipeline")
    print("=" * 60)
    print()
    
    # Load features
    print(f"Loading features from {features_path}...")
    df = pd.read_parquet(features_path)
    print(f"Loaded {len(df)} records")
    print()
    
    all_anomalies = []
    
    # 1. Rules-based detection
    rules_detector = RulesBasedDetector()
    rules_anomalies = rules_detector.detect_anomalies(df)
    all_anomalies.append(rules_anomalies)
    print()
    
    # 2. ML-based detection (optional)
    if use_ml:
        ml_detector = MLBasedDetector(contamination=0.02)
        ml_detector.train(df, normal_only=True)
        ml_anomalies = ml_detector.detect_anomalies(df)
        all_anomalies.append(ml_anomalies)
        
        # Save model
        model_path = "data/processed/isolation_forest_model.pkl"
        ml_detector.save_model(model_path)
        print()
    
    # Combine all anomalies
    if all_anomalies:
        combined_anomalies = pd.concat(all_anomalies, ignore_index=True)
    else:
        combined_anomalies = pd.DataFrame()
    
    # Save anomalies
    print(f"Saving {len(combined_anomalies)} anomalies to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    combined_anomalies.to_parquet(output_path, index=False)
    print()
    
    # Print summary
    print("=" * 60)
    print("Anomaly Detection Summary")
    print("=" * 60)
    print(f"Total anomalies detected: {len(combined_anomalies)}")
    print()
    print("By detection method:")
    print(combined_anomalies['rule_name'].value_counts())
    print()
    print("By severity:")
    print(combined_anomalies['severity'].value_counts())
    print()
    print("By actual fault type:")
    print(combined_anomalies['fault_type_label'].value_counts())
    print()
    
    return combined_anomalies


def main():
    """Main function to run anomaly detection."""
    anomalies = run_anomaly_detection(use_ml=True)
    return anomalies


if __name__ == "__main__":
    main()
