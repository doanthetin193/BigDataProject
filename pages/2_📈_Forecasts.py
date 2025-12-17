"""
================================================================================
FORECASTS PAGE - Interactive Forecast Visualization
================================================================================
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import os

st.set_page_config(page_title="Forecasts", page_icon="📈", layout="wide")

st.title("📈 Prophet Forecasts")
st.markdown("---")

# Paths
results_dir = "data_analysis/prophet_results"
forecasts_dir = "data_analysis/prophet_forecasts"

# Check directories
if not os.path.exists(results_dir):
    st.error(f"❌ Results directory not found: {results_dir}")
    st.info("💡 Please run `prophet_train.py` to generate forecasts.")
    st.stop()

# Symbol selection
symbols = ["BTCUSDT", "ETHUSDT"]
selected_symbol = st.selectbox("🔍 Select Symbol", symbols, index=0)

st.markdown("---")

# Load actual vs predicted
actual_vs_pred_path = os.path.join(results_dir, f"{selected_symbol}_actual_vs_pred.csv")

if not os.path.exists(actual_vs_pred_path):
    st.error(f"❌ Results file not found: {actual_vs_pred_path}")
    st.stop()

try:
    # Load data
    df = pd.read_csv(actual_vs_pred_path)
    df['ds'] = pd.to_datetime(df['ds'])
    
    st.success(f"✅ Loaded {len(df)} predictions for {selected_symbol}")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_error = df['pct_error'].mean()
        st.metric("Avg Error", f"{avg_error:.2f}%", help="Mean Absolute Percentage Error")
    
    with col2:
        max_error = df['pct_error'].max()
        st.metric("Max Error", f"{max_error:.2f}%", help="Largest prediction error")
    
    with col3:
        min_price = df['y'].min()
        st.metric("Min Price", f"${min_price:,.2f}", help="Minimum actual price")
    
    with col4:
        max_price = df['y'].max()
        st.metric("Max Price", f"${max_price:,.2f}", help="Maximum actual price")
    
    st.markdown("---")
    
    # Main chart: Actual vs Predicted
    st.markdown("### 📊 Actual vs Predicted Prices")
    
    fig = go.Figure()
    
    # Actual prices
    fig.add_trace(go.Scatter(
        x=df['ds'],
        y=df['y'],
        mode='lines',
        name='Actual',
        line=dict(color='#1f77b4', width=2),
        hovertemplate='<b>Actual</b><br>Date: %{x}<br>Price: $%{y:,.2f}<extra></extra>'
    ))
    
    # Predicted prices
    fig.add_trace(go.Scatter(
        x=df['ds'],
        y=df['yhat'],
        mode='lines',
        name='Predicted',
        line=dict(color='#ff7f0e', width=2, dash='dash'),
        hovertemplate='<b>Predicted</b><br>Date: %{x}<br>Price: $%{y:,.2f}<extra></extra>'
    ))
    
    fig.update_layout(
        title=f'{selected_symbol} - Actual vs Predicted Prices',
        xaxis_title='Date',
        yaxis_title='Price (USD)',
        hovermode='x unified',
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Error distribution
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📉 Error Distribution")
        
        fig_error = px.histogram(
            df,
            x='pct_error',
            nbins=30,
            title='Percentage Error Distribution',
            labels={'pct_error': 'Error (%)'},
            color_discrete_sequence=['#2ca02c']
        )
        
        fig_error.update_layout(
            showlegend=False,
            height=400,
            xaxis_title="Error (%)",
            yaxis_title="Frequency"
        )
        
        st.plotly_chart(fig_error, use_container_width=True)
    
    with col2:
        st.markdown("### 📈 Error Over Time")
        
        fig_error_time = px.line(
            df,
            x='ds',
            y='pct_error',
            title='Prediction Error Over Time',
            labels={'ds': 'Date', 'pct_error': 'Error (%)'},
            color_discrete_sequence=['#d62728']
        )
        
        fig_error_time.add_hline(
            y=0,
            line_dash="dash",
            line_color="gray",
            annotation_text="Perfect Prediction"
        )
        
        fig_error_time.update_layout(
            height=400,
            xaxis_title="Date",
            yaxis_title="Error (%)"
        )
        
        st.plotly_chart(fig_error_time, use_container_width=True)
    
    st.markdown("---")
    
    # Recent predictions table
    st.markdown("### 📋 Recent Predictions")
    
    n_recent = st.slider("Number of recent days", 5, 30, 10)
    
    df_recent = df.tail(n_recent).copy()
    df_recent['y'] = df_recent['y'].apply(lambda x: f"${x:,.2f}")
    df_recent['yhat'] = df_recent['yhat'].apply(lambda x: f"${x:,.2f}")
    df_recent['error'] = df_recent['error'].apply(lambda x: f"${x:,.2f}")
    df_recent['pct_error'] = df_recent['pct_error'].apply(lambda x: f"{x:.2f}%")
    
    st.dataframe(
        df_recent[['ds', 'y', 'yhat', 'error', 'pct_error']],
        use_container_width=True,
        hide_index=True,
        column_config={
            'ds': 'Date',
            'y': 'Actual Price',
            'yhat': 'Predicted Price',
            'error': 'Error ($)',
            'pct_error': 'Error (%)'
        }
    )
    
    st.markdown("---")
    
    # Download button
    st.markdown("### 💾 Download Data")
    
    csv = df.to_csv(index=False)
    st.download_button(
        label=f"📥 Download {selected_symbol} Predictions",
        data=csv,
        file_name=f"{selected_symbol}_predictions.csv",
        mime="text/csv"
    )
    
except Exception as e:
    st.error(f"❌ Error loading forecast data: {str(e)}")
    st.exception(e)

# Footer
st.markdown("---")
st.markdown("""
**Chart Explanation:**
- **Blue line**: Actual historical prices
- **Orange dashed line**: Prophet model predictions
- **Error**: Difference between actual and predicted ($ and %)
- **Hover** over chart for detailed values
""")
