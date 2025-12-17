"""
================================================================================
STREAMLIT DASHBOARD - CRYPTO PRICE FORECASTING
================================================================================
Main entry point for the dashboard
Run: streamlit run app.py
================================================================================
"""

import streamlit as st
import pandas as pd
import os
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Crypto Forecasting Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.title("🚀 Navigation")
st.sidebar.markdown("---")

# Project info
st.sidebar.markdown("""
### 📊 Project Info
- **Name:** Crypto Price Forecasting
- **Author:** Đoàn Thế Tín
- **MSSV:** 4551190056
- **Date:** 17/12/2025
""")

st.sidebar.markdown("---")

# Data status
st.sidebar.markdown("### 📁 Data Status")

# Check if data exists
base_dir = "data_analysis"
metrics_path = os.path.join(base_dir, "prophet_metrics", "metrics.csv")
forecasts_dir = os.path.join(base_dir, "prophet_forecasts")
daily_filled = os.path.join(base_dir, "daily_filled")

if os.path.exists(metrics_path):
    st.sidebar.success("✅ Metrics available")
else:
    st.sidebar.error("❌ Metrics not found")

if os.path.exists(forecasts_dir):
    st.sidebar.success("✅ Forecasts available")
else:
    st.sidebar.error("❌ Forecasts not found")

if os.path.exists(daily_filled):
    st.sidebar.success("✅ Daily data available")
else:
    st.sidebar.error("❌ Daily data not found")

st.sidebar.markdown("---")
st.sidebar.markdown(f"**Last updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Main page
st.markdown('<p class="main-header">🚀 Cryptocurrency Price Forecasting Dashboard</p>', unsafe_allow_html=True)

st.markdown("""
## Welcome to Lambda Architecture Demo

This dashboard visualizes the results of a **Big Data project** using:
- **Lambda Architecture** (Batch + Speed + Serving layers)
- **Apache Spark** for distributed processing
- **Apache Kafka** for real-time streaming
- **Prophet** (Facebook) for time series forecasting

### 📋 Available Pages

Navigate using the sidebar to explore:

1. **📊 Metrics** - View model performance metrics (MAPE, MSE, CV scores)
2. **📈 Forecasts** - Interactive forecast visualizations (actual vs predicted)
3. **📁 Data Info** - Dataset statistics and information

---

### 🎯 Quick Stats
""")

# Load quick stats
try:
    if os.path.exists(metrics_path):
        df_metrics = pd.read_csv(metrics_path)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Symbols", len(df_metrics), help="Number of cryptocurrencies")
        
        with col2:
            avg_mape = df_metrics['mape'].mean()
            st.metric("Avg MAPE", f"{avg_mape:.2f}%", help="Average prediction error")
        
        with col3:
            best_symbol = df_metrics.loc[df_metrics['mape'].idxmin(), 'symbol']
            st.metric("Best Model", best_symbol, help="Lowest MAPE")
        
        with col4:
            st.metric("Model", "Prophet", help="Time series forecasting model")
    else:
        st.warning("⚠️ Metrics data not found. Please run `prophet_train.py` first.")
        
except Exception as e:
    st.error(f"Error loading metrics: {str(e)}")

st.markdown("---")

# Project description
st.markdown("""
### 📖 Project Overview

**Lambda Architecture Pipeline:**

1. **Batch Layer** 
   - Historical data from Kaggle (2012-2025)
   - Binance API backfill for gaps
   - Daily OHLC aggregation

2. **Speed Layer**
   - Kafka streaming (real-time)
   - Producer polls API every 1 second
   - Consumer aggregates to daily

3. **Serving Layer**
   - Merges Batch + Speed data
   - Deduplication and MA computation
   - Prophet-ready format

4. **ML Layer**
   - Prophet time series forecasting
   - Grid search hyperparameter tuning
   - Cross-validation
   - MAPE < 5% accuracy

---

### 🚀 How to Use

1. **View Metrics**: Check model performance on different symbols
2. **Explore Forecasts**: See interactive predictions with confidence intervals
3. **Check Data**: Review dataset statistics and timeline

**Note:** Data is updated by running Python scripts in `scripts/` directory.

---

### 📚 Documentation

Full documentation available in:
- `BAO_CAO_BIG_DATA_PROJECT.md` - Complete report
- `docs/` folder - Detailed code explanations

""")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p>Built with ❤️ using Streamlit | © 2025 Đoàn Thế Tín</p>
</div>
""", unsafe_allow_html=True)
