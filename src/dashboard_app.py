"""
Streamlit Dashboard for HVAC Fault Detection System

This dashboard provides an interactive interface for monitoring and analyzing
HVAC system anomalies detected by the fault detection pipeline.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import sys
import os

# Add src directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.db import query_anomalies, get_anomaly_summary


# Page configuration
st.set_page_config(
    page_title="HVAC Fault Detection Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)


# Custom CSS
st.markdown("""
<style>
.metric-card {
    background-color: #f0f2f6;
    padding: 15px;
    border-radius: 5px;
    margin: 5px 0;
}
.severity-high {
    color: #d32f2f;
    font-weight: bold;
}
.severity-medium {
    color: #ff9800;
    font-weight: bold;
}
.severity-low {
    color: #388e3c;
    font-weight: bold;
}
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=60)
def load_anomalies(start=None, end=None, zone_id=None, severity=None, rule_name=None, limit=500):
    """Load anomalies from database with caching."""
    try:
        df = query_anomalies(
            start=start,
            end=end,
            zone_id=zone_id,
            severity=severity,
            rule_name=rule_name,
            limit=limit
        )
        return df
    except Exception as e:
        st.error(f"Error loading anomalies: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_summary(start=None, end=None):
    """Load summary statistics with caching."""
    try:
        summary = get_anomaly_summary(start=start, end=end)
        return summary
    except Exception as e:
        st.error(f"Error loading summary: {str(e)}")
        return None


@st.cache_data(ttl=300)
def load_features_data():
    """Load HVAC features data for time-series plotting."""
    try:
        features_path = "data/processed/hvac_features.parquet"
        if os.path.exists(features_path):
            df = pd.read_parquet(features_path)
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading features: {str(e)}")
        return pd.DataFrame()


def plot_timeseries_with_anomalies(features_df, anomalies_df, zone_id, metric='temp_zone_c'):
    """Create time-series plot with anomaly markers."""
    # Filter data for the selected zone
    zone_features = features_df[features_df['zone_id'] == zone_id].copy()
    zone_anomalies = anomalies_df[anomalies_df['zone_id'] == zone_id].copy()
    
    if zone_features.empty:
        st.warning(f"No data available for zone {zone_id}")
        return
    
    # Create figure
    fig = go.Figure()
    
    # Add temperature trace
    fig.add_trace(go.Scatter(
        x=zone_features['timestamp'],
        y=zone_features[metric],
        mode='lines',
        name=metric,
        line=dict(color='blue', width=1)
    ))
    
    # Add setpoint trace
    if 'setpoint_c' in zone_features.columns and metric == 'temp_zone_c':
        fig.add_trace(go.Scatter(
            x=zone_features['timestamp'],
            y=zone_features['setpoint_c'],
            mode='lines',
            name='Setpoint',
            line=dict(color='green', width=1, dash='dash')
        ))
    
    # Add anomaly markers
    if not zone_anomalies.empty:
        # Convert timestamps to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(zone_anomalies['timestamp']):
            zone_anomalies['timestamp'] = pd.to_datetime(zone_anomalies['timestamp'])
        
        # Map severity to colors
        severity_colors = {
            'high': 'red',
            'medium': 'orange',
            'low': 'yellow'
        }
        
        for severity in zone_anomalies['severity'].unique():
            severity_anomalies = zone_anomalies[zone_anomalies['severity'] == severity]
            
            # Find corresponding values from features
            merged = pd.merge(
                severity_anomalies[['timestamp', 'severity', 'rule_name']],
                zone_features[['timestamp', metric]],
                on='timestamp',
                how='left'
            )
            
            fig.add_trace(go.Scatter(
                x=merged['timestamp'],
                y=merged[metric],
                mode='markers',
                name=f'{severity.capitalize()} Anomaly',
                marker=dict(
                    size=8,
                    color=severity_colors.get(severity, 'gray'),
                    symbol='x',
                    line=dict(width=1, color='black')
                ),
                hovertemplate='<b>Anomaly</b><br>' +
                             'Time: %{x}<br>' +
                             'Value: %{y:.2f}<br>' +
                             '<extra></extra>'
            ))
    
    # Update layout
    fig.update_layout(
        title=f"Time Series for {zone_id} - {metric}",
        xaxis_title="Timestamp",
        yaxis_title=metric.replace('_', ' ').title(),
        hovermode='x unified',
        height=500,
        showlegend=True
    )
    
    st.plotly_chart(fig, use_container_width=True)


def main():
    """Main dashboard application."""
    
    # Title and header
    st.title("HVAC Fault Detection Dashboard")
    st.markdown("Monitor and analyze HVAC system anomalies in real-time")
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Time range selection
    st.sidebar.subheader("Time Range")
    time_range = st.sidebar.selectbox(
        "Quick Select",
        ["Last 24 Hours", "Last 7 Days", "Last 30 Days", "All Time", "Custom"]
    )
    
    if time_range == "Custom":
        # Get the actual data date range for default values
        default_start = datetime(2024, 1, 1).date()
        default_end = datetime(2024, 1, 31).date()
        start_date = st.sidebar.date_input("Start Date", default_start)
        end_date = st.sidebar.date_input("End Date", default_end)
        start_time = datetime.combine(start_date, datetime.min.time()).isoformat()
        end_time = datetime.combine(end_date, datetime.max.time()).isoformat()
    else:
        # Use datetime.now() or the actual data end date for time ranges
        # For synthetic data spanning 2024-01-01 to 2024-01-30, use that range
        data_end = datetime(2024, 1, 30, 23, 59, 59)
        if time_range == "Last 24 Hours":
            start_time = (data_end - timedelta(days=1)).isoformat()
            end_time = data_end.isoformat()
        elif time_range == "Last 7 Days":
            start_time = (data_end - timedelta(days=7)).isoformat()
            end_time = data_end.isoformat()
        elif time_range == "Last 30 Days":
            start_time = datetime(2024, 1, 1).isoformat()
            end_time = data_end.isoformat()
        else:  # All Time
            start_time = None
            end_time = None
    
    # Zone filter
    zone_options = ["All Zones"] + [f"Z{i}" for i in range(1, 11)]
    selected_zone = st.sidebar.selectbox("Zone", zone_options)
    zone_filter = None if selected_zone == "All Zones" else selected_zone
    
    # Severity filter
    severity_options = ["All Severities", "high", "medium", "low"]
    selected_severity = st.sidebar.selectbox("Severity", severity_options)
    severity_filter = None if selected_severity == "All Severities" else selected_severity
    
    # Rule filter
    rule_options = [
        "All Rules",
        "temp_drift",
        "clogged_filter",
        "compressor_failure",
        "oscillating_control",
        "isolation_forest"
    ]
    selected_rule = st.sidebar.selectbox("Detection Rule", rule_options)
    rule_filter = None if selected_rule == "All Rules" else selected_rule
    
    # Refresh button
    if st.sidebar.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    # Load data
    with st.spinner("Loading data..."):
        anomalies = load_anomalies(
            start=start_time,
            end=end_time,
            zone_id=zone_filter,
            severity=severity_filter,
            rule_name=rule_filter,
            limit=5000
        )
        summary = load_summary(start=start_time, end=end_time)
    
    # Display summary metrics
    if summary:
        st.header("Summary Statistics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Anomalies", summary['total'])
        
        with col2:
            if summary['by_severity']:
                high_count = next((x['count'] for x in summary['by_severity'] if x['severity'] == 'high'), 0)
                st.metric("High Severity", high_count, delta_color="inverse")
            else:
                st.metric("High Severity", 0)
        
        with col3:
            if summary['by_rule']:
                top_rule = summary['by_rule'][0]
                st.metric("Top Detection Rule", f"{top_rule['rule_name']}", 
                         f"{top_rule['count']} alerts")
            else:
                st.metric("Top Detection Rule", "N/A")
        
        with col4:
            if summary['by_zone']:
                top_zone = summary['by_zone'][0]
                st.metric("Most Affected Zone", f"{top_zone['zone_id']}", 
                         f"{top_zone['count']} alerts")
            else:
                st.metric("Most Affected Zone", "N/A")
    
    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["Time Series", "Anomaly Table", "Analytics", "About"])
    
    with tab1:
        st.header("Time Series View")
        
        # Load features data
        features_df = load_features_data()
        
        if not features_df.empty and not anomalies.empty:
            # Zone selector for time series
            ts_zone = st.selectbox("Select Zone for Time Series", [f"Z{i}" for i in range(1, 11)], key="ts_zone")
            
            # Metric selector
            metric = st.selectbox(
                "Select Metric",
                ["temp_zone_c", "power_kw", "fan_speed_pct", "rh_zone_pct"],
                key="ts_metric"
            )
            
            # Plot
            plot_timeseries_with_anomalies(features_df, anomalies, ts_zone, metric)
        else:
            st.info("Load anomaly data to view time series")
    
    with tab2:
        st.header("Anomaly Records")
        
        if not anomalies.empty:
            st.write(f"Showing {len(anomalies)} anomalies")
            
            # Display anomalies in a table
            display_df = anomalies[[
                'timestamp', 'zone_id', 'severity', 'rule_name', 
                'metric', 'score', 'fault_type_label'
            ]].copy()
            
            # Format timestamp
            display_df['timestamp'] = pd.to_datetime(display_df['timestamp']).dt.strftime('%Y-%m-%d %H:%M')
            
            # Round score
            display_df['score'] = display_df['score'].round(2)
            
            # Color-code severity
            def highlight_severity(row):
                if row['severity'] == 'high':
                    return ['background-color: #ffcdd2'] * len(row)
                elif row['severity'] == 'medium':
                    return ['background-color: #fff3e0'] * len(row)
                else:
                    return ['background-color: #f1f8e9'] * len(row)
            
            st.dataframe(
                display_df.style.apply(highlight_severity, axis=1),
                use_container_width=True,
                height=600
            )
            
            # Download button
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="Download as CSV",
                data=csv,
                file_name=f"hvac_anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.info("No anomalies found for the selected filters")
    
    with tab3:
        st.header("Analytics")
        
        if not anomalies.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Severity distribution
                severity_counts = anomalies['severity'].value_counts()
                fig = px.pie(
                    values=severity_counts.values,
                    names=severity_counts.index,
                    title="Anomalies by Severity",
                    color=severity_counts.index,
                    color_discrete_map={'high': '#d32f2f', 'medium': '#ff9800', 'low': '#388e3c'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Rule distribution
                rule_counts = anomalies['rule_name'].value_counts()
                fig = px.bar(
                    x=rule_counts.values,
                    y=rule_counts.index,
                    orientation='h',
                    title="Anomalies by Detection Rule",
                    labels={'x': 'Count', 'y': 'Rule'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Zone distribution
            zone_counts = anomalies['zone_id'].value_counts()
            fig = px.bar(
                x=zone_counts.index,
                y=zone_counts.values,
                title="Anomalies by Zone",
                labels={'x': 'Zone', 'y': 'Count'}
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Daily trend
            anomalies['date'] = pd.to_datetime(anomalies['timestamp']).dt.date
            daily_counts = anomalies.groupby('date').size().reset_index(name='count')
            fig = px.line(
                daily_counts,
                x='date',
                y='count',
                title="Daily Anomaly Trend",
                labels={'date': 'Date', 'count': 'Number of Anomalies'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available for analytics")
    
    with tab4:
        st.header("About This Dashboard")
        st.markdown("""
        ### HVAC Fault Detection System
        
        This dashboard provides real-time monitoring and analysis of HVAC system anomalies 
        detected by our fault detection pipeline.
        
        #### Detection Methods:
        - **Rules-based Detection**: Expert-defined rules for common faults
          - Temperature drift
          - Clogged filter
          - Compressor failure
          - Oscillating control
        - **ML-based Detection**: Isolation Forest algorithm for anomaly detection
        
        #### Severity Levels:
        - **High**: Critical issues requiring immediate attention
        - **Medium**: Moderate issues that should be addressed soon
        - **Low**: Minor deviations from normal operation
        
        #### Features:
        - Interactive time-series visualization
        - Real-time anomaly monitoring
        - Detailed anomaly records
        - Statistical analytics and trends
        - Export capabilities
        
        #### Data Source:
        - Database: PostgreSQL
        - Update Frequency: Real-time
        - Data Retention: Configurable
        """)


if __name__ == "__main__":
    main()
