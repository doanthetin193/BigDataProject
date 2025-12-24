"""
================================================================================
TRANG DATA INFO - Thống kê và Thông tin Dataset
================================================================================
"""

import streamlit as st
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, count

st.set_page_config(page_title="Data Info", page_icon="📁", layout="wide")

st.title("📁 Thông tin Dataset")
st.markdown("---")

# Initialize Spark (with error handling)
@st.cache_resource
def get_spark():
    try:
        spark = SparkSession.builder \
            .appName("StreamlitDashboard") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        st.error(f"Không thể khởi tạo Spark: {str(e)}")
        return None

spark = get_spark()

if spark is None:
    st.error("❌ Spark session không khả dụng. Không thể tải thông tin dữ liệu.")
    st.stop()

# Paths
daily_filled_path = "data_analysis/daily_filled"
daily_raw_path = "data_analysis/daily_raw"
prophet_input_path = "data_analysis/prophet_input"

# Tabs
tab1, tab2, tab3 = st.tabs(["📊 Daily Filled", "📈 Daily Raw", "🔮 Prophet Input"])

# Tab 1: Daily Filled
with tab1:
    st.markdown("### 📊 Dataset Daily Filled")
    st.markdown("**Dataset hoàn chỉnh với MA7/MA30, dùng để train Prophet**")
    
    if os.path.exists(daily_filled_path):
        try:
            with st.spinner("Đang tải dữ liệu..."):
                df = spark.read.parquet(daily_filled_path)
                
                # Statistics
                stats = df.groupBy("symbol").agg(
                    min("date").alias("first_date"),
                    max("date").alias("last_date"),
                    count("*").alias("rows")
                ).toPandas()
                
                st.success("✅ Dữ liệu đã tải thành công!")
                
                # Display stats
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    total_rows = stats['rows'].sum()
                    st.metric("Tổng số Rows", f"{total_rows:,}")
                
                with col2:
                    symbols_count = len(stats)
                    st.metric("Số Symbols", symbols_count)
                
                with col3:
                    # Date range
                    min_date = stats['first_date'].min()
                    max_date = stats['last_date'].max()
                    st.metric("Khoảng thời gian", f"{min_date} → {max_date}")
                
                st.markdown("---")
                
                # Stats table
                st.dataframe(
                    stats,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'symbol': 'Symbol',
                        'first_date': 'Ngày đầu',
                        'last_date': 'Ngày cuối',
                        'rows': st.column_config.NumberColumn('Số Rows', format="%d")
                    }
                )
                
                st.markdown("---")
                
                # Schema
                st.markdown("### 📋 Schema")
                schema_df = pd.DataFrame([
                    (field.name, str(field.dataType), field.nullable)
                    for field in df.schema.fields
                ], columns=['Tên cột', 'Kiểu dữ liệu', 'Cho phép NULL'])
                
                st.dataframe(schema_df, use_container_width=True, hide_index=True)
                
                # Sample data
                st.markdown("### 🔍 Dữ liệu Mẫu")
                sample = df.limit(10).toPandas()
                st.dataframe(sample, use_container_width=True, hide_index=True)
                
        except Exception as e:
            st.error(f"❌ Lỗi khi tải daily_filled: {str(e)}")
    else:
        st.warning(f"⚠️ Không tìm thấy: {daily_filled_path}")

# Tab 2: Daily Raw
with tab2:
    st.markdown("### 📈 Dataset Daily Raw")
    st.markdown("**Dữ liệu OHLC hàng ngày đã aggregate (trước khi forward fill và tính MA)**")
    
    if os.path.exists(daily_raw_path):
        try:
            with st.spinner("Đang tải dữ liệu..."):
                df = spark.read.parquet(daily_raw_path)
                
                # Statistics
                stats = df.groupBy("symbol").agg(
                    min("date").alias("first_date"),
                    max("date").alias("last_date"),
                    count("*").alias("rows")
                ).toPandas()
                
                st.success("✅ Dữ liệu đã tải thành công!")
                
                # Display stats
                col1, col2 = st.columns(2)
                
                with col1:
                    st.dataframe(
                        stats,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            'symbol': 'Symbol',
                            'first_date': 'Ngày đầu',
                            'last_date': 'Ngày cuối',
                            'rows': st.column_config.NumberColumn('Số Rows', format="%d")
                        }
                    )
                
                with col2:
                    st.info("""
                    **daily_raw vs daily_filled:**
                    - `daily_raw`: Dữ liệu aggregate gốc (có thể có gaps)
                    - `daily_filled`: Đã forward-fill và tính MA7/MA30
                    - Số rows tương tự (gaps đã được điền)
                    """)
                
                st.markdown("---")
                
                # Schema
                st.markdown("### 📋 Schema")
                schema_df = pd.DataFrame([
                    (field.name, str(field.dataType))
                    for field in df.schema.fields
                ], columns=['Tên cột', 'Kiểu dữ liệu'])
                
                st.dataframe(schema_df, use_container_width=True, hide_index=True)
                
        except Exception as e:
            st.error(f"❌ Lỗi khi tải daily_raw: {str(e)}")
    else:
        st.warning(f"⚠️ Không tìm thấy: {daily_raw_path}")

# Tab 3: Prophet Input
with tab3:
    st.markdown("### 🔮 Dataset Prophet Input")
    st.markdown("**Schema tối giản cho Prophet training (ds, y, symbol)**")
    
    if os.path.exists(prophet_input_path):
        try:
            with st.spinner("Đang tải dữ liệu..."):
                df = spark.read.parquet(prophet_input_path)
                
                # Statistics
                stats = df.groupBy("symbol").agg(
                    min("ds").alias("first_date"),
                    max("ds").alias("last_date"),
                    count("*").alias("rows")
                ).toPandas()
                
                st.success("✅ Dữ liệu đã tải thành công!")
                
                st.dataframe(
                    stats,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'symbol': 'Symbol',
                        'first_date': 'Ngày đầu',
                        'last_date': 'Ngày cuối',
                        'rows': st.column_config.NumberColumn('Số Rows', format="%d")
                    }
                )
                
                st.markdown("---")
                
                st.info("""
                **Prophet Schema:**
                - `ds`: Ngày (theo quy ước của Prophet)
                - `y`: Biến mục tiêu (giá daily_close)
                - `symbol`: Partition key
                
                **Lưu ý:** MA7/MA30 được thêm làm regressors trong quá trình training (join từ daily_filled)
                """)
                
                # Sample data
                st.markdown("### 🔍 Dữ liệu Mẫu")
                sample = df.limit(10).toPandas()
                st.dataframe(sample, use_container_width=True, hide_index=True)
                
        except Exception as e:
            st.error(f"❌ Lỗi khi tải prophet_input: {str(e)}")
    else:
        st.warning(f"⚠️ Không tìm thấy: {prophet_input_path}")

# Footer
st.markdown("---")
st.markdown("""
**Data Pipeline:**
1. **CSV** (Kaggle) → `convert_to_parquet.py` → **Parquet** (11.5M rows)
2. **Parquet** → `preprocess_step1.py` → **daily_raw** (~8,000 rows)
3. **daily_raw** → `preprocess_step2.py` → **daily_filled** (+ MA7/MA30)
4. **daily_filled** → extract → **prophet_input** (schema tối giản)
5. **prophet_input** → `prophet_train.py` → **Forecasts**

📌 **Data Snapshot:** 01/01/2012 → 14/12/2025 (BTC + ETH)
""")
