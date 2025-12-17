"""
================================================================================
DATA INFO PAGE - Dataset Statistics and Information
================================================================================
"""

import streamlit as st
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, count

st.set_page_config(page_title="Data Info", page_icon="📁", layout="wide")

st.title("📁 Dataset Information")
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
        st.error(f"Failed to initialize Spark: {str(e)}")
        return None

spark = get_spark()

if spark is None:
    st.error("❌ Spark session not available. Data info cannot be loaded.")
    st.stop()

# Paths
daily_filled_path = "data_analysis/daily_filled"
daily_raw_path = "data_analysis/daily_raw"
prophet_input_path = "data_analysis/prophet_input"

# Tabs
tab1, tab2, tab3 = st.tabs(["📊 Daily Filled", "📈 Daily Raw", "🔮 Prophet Input"])

# Tab 1: Daily Filled
with tab1:
    st.markdown("### 📊 Daily Filled Dataset")
    st.markdown("**Complete dataset with MA7/MA30, used for Prophet training**")
    
    if os.path.exists(daily_filled_path):
        try:
            with st.spinner("Loading data..."):
                df = spark.read.parquet(daily_filled_path)
                
                # Statistics
                stats = df.groupBy("symbol").agg(
                    min("date").alias("first_date"),
                    max("date").alias("last_date"),
                    count("*").alias("rows")
                ).toPandas()
                
                st.success("✅ Data loaded successfully!")
                
                # Display stats
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    total_rows = stats['rows'].sum()
                    st.metric("Total Rows", f"{total_rows:,}")
                
                with col2:
                    symbols_count = len(stats)
                    st.metric("Symbols", symbols_count)
                
                with col3:
                    # Date range
                    min_date = stats['first_date'].min()
                    max_date = stats['last_date'].max()
                    st.metric("Date Range", f"{min_date} to {max_date}")
                
                st.markdown("---")
                
                # Stats table
                st.dataframe(
                    stats,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'symbol': 'Symbol',
                        'first_date': 'First Date',
                        'last_date': 'Last Date',
                        'rows': st.column_config.NumberColumn('Rows', format="%d")
                    }
                )
                
                st.markdown("---")
                
                # Schema
                st.markdown("### 📋 Schema")
                schema_df = pd.DataFrame([
                    (field.name, str(field.dataType), field.nullable)
                    for field in df.schema.fields
                ], columns=['Column', 'Type', 'Nullable'])
                
                st.dataframe(schema_df, use_container_width=True, hide_index=True)
                
                # Sample data
                st.markdown("### 🔍 Sample Data")
                sample = df.limit(10).toPandas()
                st.dataframe(sample, use_container_width=True, hide_index=True)
                
        except Exception as e:
            st.error(f"❌ Error loading daily_filled: {str(e)}")
    else:
        st.warning(f"⚠️ Path not found: {daily_filled_path}")

# Tab 2: Daily Raw
with tab2:
    st.markdown("### 📈 Daily Raw Dataset")
    st.markdown("**Aggregated daily OHLC (before forward fill and MA computation)**")
    
    if os.path.exists(daily_raw_path):
        try:
            with st.spinner("Loading data..."):
                df = spark.read.parquet(daily_raw_path)
                
                # Statistics
                stats = df.groupBy("symbol").agg(
                    min("date").alias("first_date"),
                    max("date").alias("last_date"),
                    count("*").alias("rows")
                ).toPandas()
                
                st.success("✅ Data loaded successfully!")
                
                # Display stats
                col1, col2 = st.columns(2)
                
                with col1:
                    st.dataframe(
                        stats,
                        use_container_width=True,
                        hide_index=True
                    )
                
                with col2:
                    st.info("""
                    **daily_raw vs daily_filled:**
                    - daily_raw: Original aggregated data (may have gaps)
                    - daily_filled: Forward-filled + MA computed
                    - Row count should be similar (gaps filled)
                    """)
                
                st.markdown("---")
                
                # Schema
                st.markdown("### 📋 Schema")
                schema_df = pd.DataFrame([
                    (field.name, str(field.dataType))
                    for field in df.schema.fields
                ], columns=['Column', 'Type'])
                
                st.dataframe(schema_df, use_container_width=True, hide_index=True)
                
        except Exception as e:
            st.error(f"❌ Error loading daily_raw: {str(e)}")
    else:
        st.warning(f"⚠️ Path not found: {daily_raw_path}")

# Tab 3: Prophet Input
with tab3:
    st.markdown("### 🔮 Prophet Input Dataset")
    st.markdown("**Minimal schema for Prophet training (ds, y, symbol)**")
    
    if os.path.exists(prophet_input_path):
        try:
            with st.spinner("Loading data..."):
                df = spark.read.parquet(prophet_input_path)
                
                # Statistics
                stats = df.groupBy("symbol").agg(
                    min("ds").alias("first_date"),
                    max("ds").alias("last_date"),
                    count("*").alias("rows")
                ).toPandas()
                
                st.success("✅ Data loaded successfully!")
                
                st.dataframe(
                    stats,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'symbol': 'Symbol',
                        'first_date': 'First Date',
                        'last_date': 'Last Date',
                        'rows': st.column_config.NumberColumn('Rows', format="%d")
                    }
                )
                
                st.markdown("---")
                
                st.info("""
                **Prophet Schema:**
                - `ds`: Date (Prophet naming convention)
                - `y`: Target variable (daily_close price)
                - `symbol`: Partition key
                
                **Note:** MA7/MA30 are added as regressors during training (joined from daily_filled)
                """)
                
                # Sample data
                st.markdown("### 🔍 Sample Data")
                sample = df.limit(10).toPandas()
                st.dataframe(sample, use_container_width=True, hide_index=True)
                
        except Exception as e:
            st.error(f"❌ Error loading prophet_input: {str(e)}")
    else:
        st.warning(f"⚠️ Path not found: {prophet_input_path}")

# Footer
st.markdown("---")
st.markdown("""
**Data Pipeline:**
1. **CSV** (Kaggle) → convert_to_parquet.py → **Parquet** (8M rows)
2. **Parquet** → preprocess_step1.py → **daily_raw** (7,980 rows)
3. **daily_raw** → preprocess_step2.py → **daily_filled** (7,980 rows + MA)
4. **daily_filled** → extract → **prophet_input** (minimal schema)
5. **prophet_input** → prophet_train.py → **Forecasts**
""")
