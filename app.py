"""
================================================================================
STREAMLIT DASHBOARD - DỰ ĐOÁN GIÁ TIỀN MÃ HÓA
================================================================================
Giao diện chính của dashboard
Chạy: streamlit run app.py
================================================================================
"""

import streamlit as st
import pandas as pd
import os
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Dự đoán Giá Crypto",
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
st.sidebar.title("🚀 Điều hướng")
st.sidebar.markdown("---")

# Project info
st.sidebar.markdown("""
### 📊 Thông tin Đồ án
- **Tên:** Dự đoán Giá Tiền Mã Hóa
- **Tác giả:** Đoàn Thế Tín
- **MSSV:** 4551190056
- **Ngày:** 24/12/2025
""")

st.sidebar.markdown("---")

# Data status
st.sidebar.markdown("### 📁 Trạng thái Dữ liệu")

# Check if data exists
base_dir = "data_analysis"
metrics_path = os.path.join(base_dir, "prophet_metrics", "metrics.csv")
forecasts_dir = os.path.join(base_dir, "prophet_forecasts")
daily_filled = os.path.join(base_dir, "daily_filled")

if os.path.exists(metrics_path):
    st.sidebar.success("✅ Metrics có sẵn")
else:
    st.sidebar.error("❌ Chưa có Metrics")

if os.path.exists(forecasts_dir):
    st.sidebar.success("✅ Forecasts có sẵn")
else:
    st.sidebar.error("❌ Chưa có Forecasts")

if os.path.exists(daily_filled):
    st.sidebar.success("✅ Daily data có sẵn")
else:
    st.sidebar.error("❌ Chưa có Daily data")

st.sidebar.markdown("---")
st.sidebar.markdown(f"**Cập nhật lần cuối:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Main page
st.markdown('<p class="main-header">🚀 Dashboard Dự đoán Giá Tiền Mã Hóa</p>', unsafe_allow_html=True)

st.markdown("""
## Chào mừng đến với Demo Lambda Architecture

Dashboard này hiển thị kết quả của **Đồ án Big Data** sử dụng:
- **Lambda Architecture** (Batch + Speed + Serving Layer)
- **Apache Spark** để xử lý dữ liệu phân tán
- **Apache Kafka** để streaming real-time
- **Prophet** (Facebook) để dự đoán chuỗi thời gian

### 📋 Các Trang Có Sẵn

Sử dụng sidebar để điều hướng:

1. **📊 Metrics** - Xem các chỉ số hiệu suất model (MAPE, MSE, Cross-validation)
2. **📈 Forecasts** - Biểu đồ dự đoán tương tác (Thực tế vs Dự đoán)
3. **📁 Data Info** - Thống kê và thông tin dataset

---

### 🎯 Thống kê Nhanh
""")

# Load quick stats
try:
    if os.path.exists(metrics_path):
        df_metrics = pd.read_csv(metrics_path)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Số Symbols", len(df_metrics), help="Số lượng tiền mã hóa")
        
        with col2:
            avg_mape = df_metrics['mape'].mean()
            st.metric("MAPE Trung bình", f"{avg_mape:.2f}%", help="Sai số dự đoán trung bình")
        
        with col3:
            best_symbol = df_metrics.loc[df_metrics['mape'].idxmin(), 'symbol']
            st.metric("Model Tốt nhất", best_symbol, help="MAPE thấp nhất")
        
        with col4:
            st.metric("Thuật toán", "Prophet", help="Mô hình dự đoán chuỗi thời gian")
    else:
        st.warning("⚠️ Chưa có dữ liệu Metrics. Vui lòng chạy `prophet_train.py` trước.")
        
except Exception as e:
    st.error(f"Lỗi khi tải metrics: {str(e)}")

st.markdown("---")

# Project description
st.markdown("""
### 📖 Tổng quan Đồ án

**Pipeline Lambda Architecture:**

1. **Batch Layer** (Xử lý theo lô)
   - Dữ liệu lịch sử từ Kaggle (2012-2025)
   - Backfill từ Binance API cho các ngày thiếu
   - Aggregate dữ liệu 1-phút thành Daily OHLC

2. **Speed Layer** (Xử lý thời gian thực)
   - Kafka streaming (real-time)
   - Producer lấy data từ API mỗi 1 giây
   - Consumer aggregate thành dữ liệu ngày

3. **Serving Layer** (Phục vụ dữ liệu)
   - Merge dữ liệu Batch + Speed
   - Loại bỏ trùng lặp và tính Moving Average
   - Format dữ liệu cho Prophet

4. **ML Layer** (Học máy)
   - Prophet time series forecasting
   - Grid Search để tìm hyperparameters tốt nhất
   - Cross-validation để đánh giá độ ổn định
   - Đạt độ chính xác MAPE < 5%

---

### 🚀 Hướng dẫn Sử dụng

1. **Xem Metrics**: Kiểm tra hiệu suất model cho từng coin
2. **Khám phá Forecasts**: Xem dự đoán tương tác với khoảng tin cậy
3. **Kiểm tra Data**: Xem thống kê và timeline dữ liệu

**Lưu ý:** Dữ liệu được cập nhật bằng cách chạy các script Python trong thư mục `scripts/`.

---

### 📚 Tài liệu

Tài liệu đầy đủ có trong:
- `BAO_CAO_CHINH_THUC.md` - Báo cáo đồ án chính thức
- `docs/` - Thư mục chứa giải thích code chi tiết
- `MERMAID_DIAGRAMS.md` - Sơ đồ Mermaid cho báo cáo

""")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p>Xây dựng với ❤️ sử dụng Streamlit | © 2025 Đoàn Thế Tín - KTPM45</p>
</div>
""", unsafe_allow_html=True)
