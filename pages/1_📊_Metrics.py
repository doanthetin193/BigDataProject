"""
================================================================================
TRANG METRICS - Hiển thị Hiệu suất Model
================================================================================
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

st.set_page_config(page_title="Metrics", page_icon="📊", layout="wide")

st.title("📊 Chỉ số Hiệu suất Model")
st.markdown("---")

# Path
metrics_path = "data_analysis/prophet_metrics/metrics.csv"

# Check if file exists
if not os.path.exists(metrics_path):
    st.error(f"❌ Không tìm thấy file metrics: {metrics_path}")
    st.info("💡 Vui lòng chạy `prophet_train.py` để tạo metrics.")
    st.stop()

# Load data
try:
    df = pd.read_csv(metrics_path)
    
    st.success(f"✅ Đã tải metrics cho {len(df)} symbols")
    
    # Display table
    st.markdown("### 📋 Tổng hợp Hiệu suất")
    
    # Format table
    df_display = df.copy()
    df_display['mape'] = df_display['mape'].apply(lambda x: f"{x:.2f}%")
    df_display['cv_mape'] = df_display['cv_mape'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A")
    df_display['mse'] = df_display['mse'].apply(lambda x: f"{x:,.2f}")
    
    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            'symbol': 'Symbol',
            'mape': 'Test MAPE',
            'cv_mape': 'CV MAPE',
            'mse': 'MSE',
            'mode': 'Seasonality Mode',
            'prior': 'Changepoint Prior'
        }
    )
    
    st.markdown("---")
    
    # Visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📉 So sánh MAPE")
        
        fig_mape = px.bar(
            df,
            x='symbol',
            y='mape',
            title='Mean Absolute Percentage Error (MAPE)',
            labels={'mape': 'MAPE (%)', 'symbol': 'Symbol'},
            color='mape',
            color_continuous_scale='RdYlGn_r',
            text='mape'
        )
        
        fig_mape.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
        fig_mape.update_layout(
            showlegend=False,
            height=400,
            yaxis_title="MAPE (%)",
            xaxis_title="Symbol"
        )
        
        st.plotly_chart(fig_mape, use_container_width=True)
        
        # MAPE interpretation
        avg_mape = df['mape'].mean()
        if avg_mape < 5:
            st.success(f"✅ Xuất sắc! MAPE trung bình: {avg_mape:.2f}% (< 5%)")
        elif avg_mape < 10:
            st.info(f"ℹ️ Tốt! MAPE trung bình: {avg_mape:.2f}% (< 10%)")
        else:
            st.warning(f"⚠️ MAPE trung bình: {avg_mape:.2f}% (> 10%)")
    
    with col2:
        st.markdown("### 🔄 Cross-Validation vs Test MAPE")
        
        # Prepare data for comparison
        df_compare = df.melt(
            id_vars=['symbol'],
            value_vars=['mape', 'cv_mape'],
            var_name='Metric',
            value_name='Value'
        )
        df_compare['Metric'] = df_compare['Metric'].replace({
            'mape': 'Test MAPE',
            'cv_mape': 'CV MAPE'
        })
        
        fig_compare = px.bar(
            df_compare,
            x='symbol',
            y='Value',
            color='Metric',
            barmode='group',
            title='Test MAPE vs Cross-Validation MAPE',
            labels={'Value': 'MAPE (%)', 'symbol': 'Symbol'},
            color_discrete_sequence=['#1f77b4', '#ff7f0e']
        )
        
        fig_compare.update_layout(
            height=400,
            yaxis_title="MAPE (%)",
            xaxis_title="Symbol"
        )
        
        st.plotly_chart(fig_compare, use_container_width=True)
        
        # CV interpretation
        st.info("""
        **Cross-Validation (Đánh giá chéo):**
        - Kiểm tra model trên nhiều khoảng thời gian
        - Đáng tin cậy hơn so với chỉ test 1 lần
        - CV MAPE ≈ Test MAPE → Model không overfitting
        """)
    
    st.markdown("---")
    
    # MSE visualization
    st.markdown("### 📐 Mean Squared Error (MSE)")
    
    fig_mse = px.bar(
        df,
        x='symbol',
        y='mse',
        title='Mean Squared Error theo Symbol',
        labels={'mse': 'MSE', 'symbol': 'Symbol'},
        color='symbol',
        text='mse'
    )
    
    fig_mse.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
    fig_mse.update_layout(
        showlegend=False,
        height=400
    )
    
    st.plotly_chart(fig_mse, use_container_width=True)
    
    st.markdown("---")
    
    # Hyperparameters
    st.markdown("### ⚙️ Hyperparameters Tốt nhất")
    
    col1, col2 = st.columns(2)
    
    for idx, row in df.iterrows():
        with col1 if idx % 2 == 0 else col2:
            st.markdown(f"""
            **{row['symbol']}**
            - Seasonality Mode: `{row['mode']}`
            - Changepoint Prior: `{row['prior']}`
            - Test MAPE: `{row['mape']:.2f}%`
            - CV MAPE: `{row['cv_mape']:.2f}%`
            """)
    
    st.markdown("---")
    
    # Download button
    st.markdown("### 💾 Tải xuống Dữ liệu")
    
    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Tải Metrics CSV",
        data=csv,
        file_name="prophet_metrics.csv",
        mime="text/csv"
    )
    
except Exception as e:
    st.error(f"❌ Lỗi khi tải metrics: {str(e)}")
    st.exception(e)

# Footer
st.markdown("---")
st.markdown("""
**Giải thích các Chỉ số:**
- **MAPE**: Mean Absolute Percentage Error - Sai số % trung bình (càng thấp càng tốt, < 5% là xuất sắc)
- **MSE**: Mean Squared Error - Sai số bình phương trung bình (phạt nặng sai số lớn)
- **CV MAPE**: Cross-validation MAPE - Kiểm tra độ ổn định model
- **Mode**: Seasonality mode (additive/multiplicative) - Cách tính mùa vụ
- **Prior**: Changepoint prior scale - Độ nhạy với thay đổi xu hướng
""")
