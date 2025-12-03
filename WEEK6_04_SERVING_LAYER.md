# 📘 WEEK 6 - PHẦN 4: SERVING LAYER (Merge + Forecast)

## 📑 Mục lục
1. [Mục đích của Serving Layer](#1-mục-đích-của-serving-layer)
2. [Kiến trúc Serving Layer](#2-kiến-trúc-serving-layer)
3. [week6_merge.py - Giải thích chi tiết](#3-week6_mergepy---giải-thích-chi-tiết)
4. [prophet_train.py - Giải thích chi tiết](#4-prophet_trainpy---giải-thích-chi-tiết)
5. [Kết nối 3 Layers](#5-kết-nối-3-layers)
6. [Output và Kết quả](#6-output-và-kết-quả)
7. [Câu hỏi thường gặp](#7-câu-hỏi-thường-gặp)
8. [Tổng kết Week 6](#8-tổng-kết-week-6)

---

## 1. Mục đích của Serving Layer

### 1.1. Vai trò trong Lambda Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    LAMBDA ARCHITECTURE                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│        BATCH LAYER              SPEED LAYER                         │
│        ┌─────────┐              ┌─────────┐                        │
│        │Historical│              │Real-time│                        │
│        │  Data    │              │  Data   │                        │
│        │ (chính   │              │ (nhanh) │                        │
│        │  xác)    │              │         │                        │
│        └────┬─────┘              └────┬────┘                        │
│             │                         │                             │
│             │                         │                             │
│             └──────────┬──────────────┘                             │
│                        │                                            │
│                        ▼                                            │
│               ┌────────────────┐                                   │
│               │ SERVING LAYER  │ ← ĐÂY LÀ SERVING LAYER            │
│               │                │                                   │
│               │ • MERGE data   │                                   │
│               │ • DEDUPLICATE  │                                   │
│               │ • RECOMPUTE    │                                   │
│               │ • FORECAST     │                                   │
│               │ • VISUALIZE    │                                   │
│               │                │                                   │
│               └────────────────┘                                   │
│                        │                                            │
│                        ▼                                            │
│               ┌────────────────┐                                   │
│               │    OUTPUT      │                                   │
│               │  • Forecasts   │                                   │
│               │  • Charts      │                                   │
│               │  • Metrics     │                                   │
│               └────────────────┘                                   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.2. Đặc điểm của Serving Layer

```
Serving Layer chịu trách nhiệm:

1. MERGE dữ liệu từ Batch + Speed Layer
   └── Union → Deduplicate → Sort

2. DEDUPLICATE loại bỏ trùng lặp
   └── Cùng (symbol, date) → Giữ 1 bản

3. RECOMPUTE các metrics
   └── Tính lại MA7, MA30 cho timeline mới

4. SERVE cho downstream applications
   └── Prophet forecasting
   └── Visualization
   └── Analytics

5. ANSWER queries
   └── Latest data always available
```

### 1.3. Tại sao cần Serving Layer?

```
Vấn đề: Batch Layer và Speed Layer có data riêng biệt

┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  BATCH LAYER (daily_filled/)                                       │
│  ├── 2012-01-01 → 2025-11-23                                       │
│  └── Chính xác, đầy đủ                                             │
│                                                                     │
│  SPEED LAYER (streaming_output_spark/daily/)                       │
│  ├── 2025-11-24 → 2025-12-03 (today)                               │
│  └── Real-time, có thể trùng                                       │
│                                                                     │
│  Để forecast, cần:                                                  │
│  ├── Timeline liên tục từ 2012 → 2025-12-03                        │
│  ├── Không có gaps                                                 │
│  └── Không có duplicates                                           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

Giải pháp: Serving Layer merge tất cả

┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  SERVING LAYER                                                      │
│                                                                     │
│  Input 1: Batch Layer data                                         │
│  Input 2: Speed Layer data                                         │
│                                                                     │
│  Output: Unified timeline                                           │
│  ├── 2012-01-01 → 2025-12-03                                       │
│  ├── No gaps                                                        │
│  ├── No duplicates                                                  │
│  └── MA7/MA30 recomputed                                           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Kiến trúc Serving Layer

### 2.1. Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SERVING LAYER DATA FLOW                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│      BATCH LAYER                    SPEED LAYER                     │
│  ┌─────────────────┐            ┌─────────────────┐                │
│  │ daily_filled/   │            │streaming_output_│                │
│  │                 │            │  spark/daily/   │                │
│  │ BTCUSDT:        │            │                 │                │
│  │ 2012 → 2025-11  │            │ BTCUSDT:        │                │
│  │ ~4700 days      │            │ 2025-11 → 12    │                │
│  │                 │            │ ~10 days        │                │
│  │ ETHUSDT:        │            │                 │                │
│  │ 2017 → 2025-11  │            │ ETHUSDT:        │                │
│  │ ~2900 days      │            │ ~10 days        │                │
│  └────────┬────────┘            └────────┬────────┘                │
│           │                              │                          │
│           │     ┌────────────────────┐   │                          │
│           │     │                    │   │                          │
│           └────►│  week6_merge.py    │◄──┘                          │
│                 │                    │                              │
│                 │  1. Read both      │                              │
│                 │  2. Align schema   │                              │
│                 │  3. Union          │                              │
│                 │  4. Deduplicate    │                              │
│                 │  5. Recompute MA   │                              │
│                 │  6. Save           │                              │
│                 │                    │                              │
│                 └─────────┬──────────┘                              │
│                           │                                         │
│                           ▼                                         │
│                 ┌────────────────────┐                              │
│                 │    MERGED DATA     │                              │
│                 │  daily_filled/     │                              │
│                 │                    │                              │
│                 │  BTCUSDT:          │                              │
│                 │  2012 → 2025-12-03 │                              │
│                 │  ~4710 days        │                              │
│                 │                    │                              │
│                 │  ETHUSDT:          │                              │
│                 │  2017 → 2025-12-03 │                              │
│                 │  ~2910 days        │                              │
│                 └─────────┬──────────┘                              │
│                           │                                         │
│                           ▼                                         │
│                 ┌────────────────────┐                              │
│                 │  prophet_train.py  │                              │
│                 │                    │                              │
│                 │  • Load data       │                              │
│                 │  • Train Prophet   │                              │
│                 │  • Forecast 30d    │                              │
│                 │  • Visualize       │                              │
│                 └─────────┬──────────┘                              │
│                           │                                         │
│                           ▼                                         │
│                 ┌────────────────────┐                              │
│                 │      OUTPUTS       │                              │
│                 │                    │                              │
│                 │  week4_forecasts/  │                              │
│                 │  week4_metrics/    │                              │
│                 │  week4_results/    │                              │
│                 │  week4_visualizations/│                           │
│                 └────────────────────┘                              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2. Files trong Serving Layer

```
Project/
├── week6_merge.py       # Merge Batch + Speed Layer
├── prophet_train.py     # Train Prophet, forecast
│
├── data_analysis/
│   ├── daily_filled/    # Input/Output của merge
│   │   ├── symbol=BTCUSDT/
│   │   └── symbol=ETHUSDT/
│   │
│   ├── prophet_input/   # Input cho Prophet
│   │   ├── symbol=BTCUSDT/
│   │   └── symbol=ETHUSDT/
│   │
│   ├── week4_forecasts/ # Prophet forecasts
│   ├── week4_metrics/   # MAPE, RMSE
│   ├── week4_results/   # Actual vs Predicted
│   └── week4_visualizations/ # Charts
│
└── streaming_output_spark/  # Speed Layer output
    └── daily/
```

---

## 3. week6_merge.py - Giải thích chi tiết

### 3.1. Tổng quan

```python
"""
week6_merge.py

Nhiệm vụ:
1. Đọc Batch Layer data (daily_filled)
2. Đọc Speed Layer data (streaming_output_spark/daily)
3. Merge + Deduplicate
4. Recompute MA7/MA30
5. Save unified dataset
"""
```

### 3.2. Step 1: Read Batch Layer

```python
# ============================================================================
# STEP 1: READ BATCH LAYER DATA
# ============================================================================
print("\n[STEP 1] Reading Batch Layer (backfill data)...")

# Đọc từ daily_filled
# Đây là output của week6_backfill.py
df_batch = spark.read.parquet("data_analysis/daily_filled")

batch_count = df_batch.count()
print(f"  ✅ Batch data loaded: {batch_count:,} rows")

# Thống kê
df_batch.groupBy("symbol").agg(
    count("*").alias("days"),        # Số ngày
    min("date").alias("first_date"), # Ngày đầu
    max("date").alias("last_date")   # Ngày cuối
).show(truncate=False)

# Output:
# +-------+-----+----------+----------+
# |symbol |days |first_date|last_date |
# +-------+-----+----------+----------+
# |BTCUSDT|4711 |2012-01-01|2025-11-23|
# |ETHUSDT|2912 |2017-08-17|2025-11-23|
# +-------+-----+----------+----------+
```

### 3.3. Step 2: Read Speed Layer

```python
# ============================================================================
# STEP 2: READ SPEED LAYER DATA (STREAMING)
# ============================================================================
print("\n[STEP 2] Reading Speed Layer (streaming data)...")

streaming_path = "streaming_output_spark/daily"

# Kiểm tra folder tồn tại
if not os.path.exists(streaming_path):
    print(f"  ⚠️  No streaming data found at {streaming_path}")
    # Hướng dẫn user start streaming
    exit(0)

# Đọc streaming data
df_streaming = spark.read.parquet(streaming_path)

streaming_count = df_streaming.count()
print(f"  ✅ Streaming data loaded: {streaming_count:,} rows")

# Output:
# +-------+-----+----------+----------+
# |symbol |days |first_date|last_date |
# +-------+-----+----------+----------+
# |BTCUSDT|10   |2025-11-24|2025-12-03|
# |ETHUSDT|10   |2025-11-24|2025-12-03|
# +-------+-----+----------+----------+
```

### 3.4. Step 3: Align Schema và Merge

```python
# ============================================================================
# STEP 3: ALIGN SCHEMAS AND MERGE
# ============================================================================
print("\n[STEP 3] Merging batch + streaming data...")

# Batch columns vs Streaming columns
batch_cols = set(df_batch.columns)
# {'symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'MA7', 'MA30', 'year'}

streaming_cols = set(df_streaming.columns)
# {'symbol', 'date', 'daily_open', 'daily_high', 'daily_low', 'daily_close', 
#  'daily_volume', 'window'}

# Common columns (rename nếu cần)
common_cols = ["symbol", "date", "open", "high", "low", "close", "volume"]

# Align batch
df_batch_aligned = df_batch.select(*common_cols)

# Align streaming (rename daily_* → *)
df_streaming_aligned = df_streaming.select(
    col("symbol"),
    col("date"),
    col("daily_open").alias("open"),
    col("daily_high").alias("high"),
    col("daily_low").alias("low"),
    col("daily_close").alias("close"),
    col("daily_volume").alias("volume")
)

# Add null MA7/MA30 cho streaming (sẽ tính lại sau)
from pyspark.sql.functions import lit
df_streaming_aligned = df_streaming_aligned \
    .withColumn("MA7", lit(None).cast("double")) \
    .withColumn("MA30", lit(None).cast("double"))

# UNION: Gộp 2 DataFrames
df_merged = df_batch_aligned.union(df_streaming_aligned)

# DEDUPLICATE: Loại bỏ trùng lặp
# Giữ 1 row cho mỗi (symbol, date)
df_merged = df_merged.dropDuplicates(["symbol", "date"])

# SORT: Sắp xếp theo symbol, date
df_merged = df_merged.orderBy("symbol", "date")

merged_count = df_merged.count()
print(f"  ✅ Merged data: {merged_count:,} rows")
```

### 3.5. Minh họa Union và Deduplicate

```
UNION:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  BATCH (4711 rows):                                                │
│  ┌────────────────────────────────────────────────────┐            │
│  │ BTCUSDT | 2012-01-01 | open | high | low | close   │            │
│  │ BTCUSDT | 2012-01-02 | ...                         │            │
│  │ ...                                                 │            │
│  │ BTCUSDT | 2025-11-23 | ...                         │            │
│  └────────────────────────────────────────────────────┘            │
│                                                                     │
│  STREAMING (10 rows):                                              │
│  ┌────────────────────────────────────────────────────┐            │
│  │ BTCUSDT | 2025-11-23 | open | high | low | close   │ ← TRÙNG!  │
│  │ BTCUSDT | 2025-11-24 | ...                         │            │
│  │ ...                                                 │            │
│  │ BTCUSDT | 2025-12-03 | ...                         │            │
│  └────────────────────────────────────────────────────┘            │
│                                                                     │
│  AFTER UNION (4721 rows):                                          │
│  ┌────────────────────────────────────────────────────┐            │
│  │ BTCUSDT | 2012-01-01 | ...                         │            │
│  │ ...                                                 │            │
│  │ BTCUSDT | 2025-11-23 | ... (from batch)            │            │
│  │ BTCUSDT | 2025-11-23 | ... (from streaming) ← TRÙNG!│           │
│  │ BTCUSDT | 2025-11-24 | ...                         │            │
│  │ ...                                                 │            │
│  │ BTCUSDT | 2025-12-03 | ...                         │            │
│  └────────────────────────────────────────────────────┘            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

DEDUPLICATE:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  dropDuplicates(["symbol", "date"])                                │
│                                                                     │
│  BEFORE: 4721 rows (có trùng 2025-11-23)                          │
│  AFTER:  4720 rows (không trùng)                                   │
│                                                                     │
│  ┌────────────────────────────────────────────────────┐            │
│  │ BTCUSDT | 2012-01-01 | ...                         │            │
│  │ ...                                                 │            │
│  │ BTCUSDT | 2025-11-23 | ... (giữ 1 bản)             │            │
│  │ BTCUSDT | 2025-11-24 | ...                         │            │
│  │ ...                                                 │            │
│  │ BTCUSDT | 2025-12-03 | ...                         │            │
│  └────────────────────────────────────────────────────┘            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.6. Step 4: Recompute MA7/MA30

```python
# ============================================================================
# STEP 4: RECOMPUTE MA7/MA30 FOR ENTIRE TIMELINE
# ============================================================================
print("\n[STEP 4] Recomputing MA7/MA30 for merged timeline...")

# Window function cho MA7
# Lấy trung bình 7 ngày gần nhất (bao gồm ngày hiện tại)
window_ma7 = Window \
    .partitionBy("symbol")     # Tính riêng cho mỗi symbol
    .orderBy("date")           # Sắp xếp theo ngày
    .rowsBetween(-6, 0)        # 6 ngày trước + ngày hiện tại = 7 ngày

# Window function cho MA30
window_ma30 = Window \
    .partitionBy("symbol") \
    .orderBy("date") \
    .rowsBetween(-29, 0)       # 29 ngày trước + ngày hiện tại = 30 ngày

# Compute MA
df_merged = df_merged.withColumn("MA7", avg("close").over(window_ma7))
df_merged = df_merged.withColumn("MA30", avg("close").over(window_ma30))

print(f"  ✅ MA7 and MA30 recomputed")
```

### 3.7. Tại sao phải tính lại MA?

```
Vấn đề: MA7/MA30 cần timeline liên tục

┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Trước merge:                                                       │
│                                                                     │
│  BATCH: [..., 11/21, 11/22, 11/23] ← MA30 tính đến 11/23           │
│                                                                     │
│  STREAMING: [11/24, 11/25, ..., 12/03] ← MA30 = NULL               │
│             (chỉ có 10 ngày, không đủ 30 ngày để tính MA30)        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

Sau merge + recompute:

┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  MERGED: [..., 11/21, 11/22, 11/23, 11/24, 11/25, ..., 12/03]     │
│                                                                     │
│  MA30 cho ngày 12/03:                                              │
│  = avg(close của 30 ngày: 11/04 → 12/03)                           │
│  = Giá trị chính xác ✓                                             │
│                                                                     │
│  Không có gaps, timeline liên tục                                  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.8. Step 5: Save Unified Dataset

```python
# ============================================================================
# STEP 5: SAVE UNIFIED DATASET
# ============================================================================
print("\n[STEP 5] Saving unified dataset...")

# Add year column cho partitioning
df_merged = df_merged.withColumn("year", year("date"))

# Save với partitioning
output_path = "data_analysis/daily_filled"
df_merged.write \
    .mode("overwrite") \         # Ghi đè data cũ
    .partitionBy("symbol", "year") \
    .parquet(output_path)

print(f"  ✅ Saved to {output_path}")

# Update prophet_input (format cho Prophet)
df_prophet = df_merged.select(
    col("date").alias("ds"),     # Prophet cần cột "ds"
    col("close").alias("y"),     # Prophet cần cột "y"
    "symbol",
    "MA7",
    "MA30"
).orderBy("symbol", "ds")

df_prophet.write \
    .mode("overwrite") \
    .partitionBy("symbol") \
    .parquet("data_analysis/prophet_input")

print(f"  ✅ Prophet input updated")
```

---

## 4. prophet_train.py - Giải thích chi tiết

### 4.1. Prophet là gì?

```
Facebook Prophet là time series forecasting library:

┌─────────────────────────────────────────────────────────────────────┐
│                         PROPHET MODEL                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  y(t) = g(t) + s(t) + h(t) + ε(t)                                 │
│                                                                     │
│  g(t) = TREND component                                            │
│         └── Linear hoặc Logistic growth                            │
│         └── Changepoints (điểm thay đổi trend)                    │
│                                                                     │
│  s(t) = SEASONALITY component                                      │
│         └── Yearly seasonality                                     │
│         └── Weekly seasonality                                     │
│         └── Custom seasonality                                     │
│                                                                     │
│  h(t) = HOLIDAY effects                                            │
│         └── Events đặc biệt                                        │
│                                                                     │
│  ε(t) = ERROR (noise)                                              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

Ưu điểm:
✅ Dễ sử dụng (chỉ cần df với "ds" và "y")
✅ Xử lý missing data tốt
✅ Robust với outliers
✅ Tự động detect changepoints
✅ Interpretable (giải thích được)
```

### 4.2. Tổng quan prophet_train.py

```python
"""
prophet_train.py

Flow:
1. Load data từ prophet_input
2. Train Prophet model cho mỗi symbol
3. Forecast 30 ngày tương lai
4. Evaluate với MAPE, RMSE
5. Visualize với Plotly
6. Save kết quả
"""
```

### 4.3. Load Data

```python
# Load data
spark = SparkSession.builder \
    .appName("ProphetTraining") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

df = spark.read.parquet("data_analysis/prophet_input")

# Filter by symbol
df_btc = df.filter(col("symbol") == "BTCUSDT").toPandas()
df_eth = df.filter(col("symbol") == "ETHUSDT").toPandas()

# Prophet cần DataFrame với columns: ds, y
# ds = date, y = value to predict (close price)
print(f"BTC: {len(df_btc)} days")
print(f"ETH: {len(df_eth)} days")
```

### 4.4. Train Prophet

```python
def train_and_forecast(df, symbol, periods=30):
    """
    Train Prophet và forecast
    
    Parameters:
    - df: DataFrame với columns (ds, y)
    - symbol: "BTCUSDT" hoặc "ETHUSDT"
    - periods: Số ngày forecast (default 30)
    """
    
    # ============================================
    # TRAIN PROPHET
    # ============================================
    model = Prophet(
        # SEASONALITY
        yearly_seasonality=True,    # Có yearly pattern
        weekly_seasonality=True,    # Có weekly pattern
        daily_seasonality=False,    # Không daily (data là daily)
        
        # CHANGEPOINTS
        changepoint_prior_scale=0.05,  # Flexibility của trend
        # 0.5 = rất flexible (có thể overfit)
        # 0.001 = rất rigid (có thể underfit)
        
        # SEASONALITY STRENGTH
        seasonality_prior_scale=10,
        
        # UNCERTAINTY
        interval_width=0.95  # 95% confidence interval
    )
    
    # Fit model (huấn luyện)
    model.fit(df)
    
    # ============================================
    # FORECAST
    # ============================================
    # Tạo DataFrame cho future dates
    future = model.make_future_dataframe(periods=periods)
    
    # Forecast
    forecast = model.predict(future)
    
    # forecast DataFrame có nhiều columns:
    # - ds: date
    # - yhat: predicted value
    # - yhat_lower: lower bound (95% CI)
    # - yhat_upper: upper bound (95% CI)
    # - trend: trend component
    # - yearly: yearly seasonality
    # - weekly: weekly seasonality
    
    return model, forecast
```

### 4.5. Evaluate Model

```python
def evaluate_model(actual, predicted):
    """
    Tính metrics đánh giá model
    
    MAPE = Mean Absolute Percentage Error
    RMSE = Root Mean Square Error
    MAE = Mean Absolute Error
    """
    
    # ============================================
    # MAPE (Mean Absolute Percentage Error)
    # ============================================
    # MAPE = (1/n) * Σ |actual - predicted| / |actual| * 100%
    #
    # Ý nghĩa:
    # - MAPE 5% nghĩa là trung bình sai 5% so với actual
    # - Càng thấp càng tốt
    # - < 10% = tốt, < 20% = chấp nhận được
    
    mape = np.mean(np.abs((actual - predicted) / actual)) * 100
    
    # ============================================
    # RMSE (Root Mean Square Error)
    # ============================================
    # RMSE = sqrt((1/n) * Σ (actual - predicted)²)
    #
    # Ý nghĩa:
    # - RMSE = $1000 nghĩa là error trung bình ~$1000
    # - Penalize large errors nhiều hơn
    
    rmse = np.sqrt(np.mean((actual - predicted) ** 2))
    
    # ============================================
    # MAE (Mean Absolute Error)
    # ============================================
    # MAE = (1/n) * Σ |actual - predicted|
    
    mae = np.mean(np.abs(actual - predicted))
    
    return {"MAPE": mape, "RMSE": rmse, "MAE": mae}

# Trong project, kết quả:
# BTC: MAPE ≈ 4.5%, RMSE ≈ $4000
# ETH: MAPE ≈ 4.5%, RMSE ≈ $150
```

### 4.6. Visualization

```python
def create_visualization(model, forecast, df, symbol):
    """
    Tạo interactive chart với Plotly
    """
    
    import plotly.graph_objects as go
    
    fig = go.Figure()
    
    # ============================================
    # ACTUAL DATA (đường xanh)
    # ============================================
    fig.add_trace(go.Scatter(
        x=df['ds'],
        y=df['y'],
        mode='lines',
        name='Actual',
        line=dict(color='blue', width=1)
    ))
    
    # ============================================
    # FORECAST (đường đỏ)
    # ============================================
    fig.add_trace(go.Scatter(
        x=forecast['ds'],
        y=forecast['yhat'],
        mode='lines',
        name='Forecast',
        line=dict(color='red', width=2)
    ))
    
    # ============================================
    # CONFIDENCE INTERVAL (vùng xám)
    # ============================================
    fig.add_trace(go.Scatter(
        x=forecast['ds'].tolist() + forecast['ds'].tolist()[::-1],
        y=forecast['yhat_upper'].tolist() + forecast['yhat_lower'].tolist()[::-1],
        fill='toself',
        fillcolor='rgba(128,128,128,0.2)',
        line=dict(color='rgba(255,255,255,0)'),
        name='95% Confidence Interval'
    ))
    
    # ============================================
    # LAYOUT
    # ============================================
    fig.update_layout(
        title=f'{symbol} Price Forecast',
        xaxis_title='Date',
        yaxis_title='Price (USD)',
        hovermode='x unified',
        showlegend=True
    )
    
    # Save as HTML (interactive)
    fig.write_html(f"week4_visualizations/{symbol}_forecast_interactive.html")
```

### 4.7. Output Files

```
Sau khi chạy prophet_train.py:

data_analysis/
├── week4_forecasts/
│   ├── BTCUSDT_forecast.parquet
│   └── ETHUSDT_forecast.parquet
│
├── week4_metrics/
│   └── metrics.csv
│   # symbol,MAPE,RMSE,MAE
│   # BTCUSDT,4.52,4123.45,3567.89
│   # ETHUSDT,4.61,156.78,134.56
│
├── week4_results/
│   ├── BTCUSDT_actual_vs_pred.csv
│   └── ETHUSDT_actual_vs_pred.csv
│   # date,actual,predicted,error,error_percent
│
└── week4_visualizations/
    ├── BTCUSDT_forecast_interactive.html
    └── ETHUSDT_forecast_interactive.html
```

---

## 5. Kết nối 3 Layers

### 5.1. Full Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    COMPLETE LAMBDA ARCHITECTURE                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │                    DATA SOURCE                              │    │
│  │                                                             │    │
│  │  Binance API: https://api.binance.com/api/v3/              │    │
│  │  ├── /klines (historical OHLCV)                            │    │
│  │  └── /ticker/24hr (real-time price)                        │    │
│  └────────────────────────────────────────────────────────────┘    │
│                               │                                     │
│              ┌────────────────┴────────────────┐                   │
│              │                                 │                   │
│              ▼                                 ▼                   │
│  ┌──────────────────┐              ┌──────────────────┐           │
│  │   BATCH LAYER    │              │   SPEED LAYER    │           │
│  │                  │              │                  │           │
│  │ week6_backfill.py│              │ websocket_       │           │
│  │                  │              │ producer.py      │           │
│  │ • Detect gap     │              │ • Poll API 1s    │           │
│  │ • Fetch klines   │              │ • Send to Kafka  │           │
│  │ • Daily agg      │              │                  │           │
│  │ • MA7/MA30       │              │ spark_streaming_ │           │
│  │ • Forward fill   │              │ consumer.py      │           │
│  │                  │              │ • Read Kafka     │           │
│  │ Output:          │              │ • Window agg     │           │
│  │ daily_filled/    │              │ • Watermark      │           │
│  │                  │              │                  │           │
│  │ 2012 → 2025-11   │              │ Output:          │           │
│  │                  │              │ streaming_output_│           │
│  │                  │              │ spark/daily/     │           │
│  │                  │              │                  │           │
│  │                  │              │ 2025-11 → now    │           │
│  └────────┬─────────┘              └────────┬─────────┘           │
│           │                                 │                      │
│           └───────────────┬─────────────────┘                      │
│                           │                                        │
│                           ▼                                        │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │                     SERVING LAYER                          │   │
│  │                                                            │   │
│  │  week6_merge.py                                           │   │
│  │  ├── Read Batch data                                      │   │
│  │  ├── Read Speed data                                      │   │
│  │  ├── Union + Deduplicate                                  │   │
│  │  ├── Recompute MA7/MA30                                   │   │
│  │  └── Save unified dataset                                 │   │
│  │                                                            │   │
│  │  prophet_train.py                                         │   │
│  │  ├── Load merged data                                     │   │
│  │  ├── Train Prophet model                                  │   │
│  │  ├── Forecast 30 days                                     │   │
│  │  ├── Evaluate (MAPE, RMSE)                               │   │
│  │  └── Visualize (Plotly HTML)                             │   │
│  │                                                            │   │
│  │  Output:                                                   │   │
│  │  ├── daily_filled/ (unified)                              │   │
│  │  ├── prophet_input/                                       │   │
│  │  ├── week4_forecasts/                                     │   │
│  │  ├── week4_metrics/                                       │   │
│  │  ├── week4_results/                                       │   │
│  │  └── week4_visualizations/                                │   │
│  │                                                            │   │
│  └────────────────────────────────────────────────────────────┘   │
│                           │                                        │
│                           ▼                                        │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │                      END USER                              │   │
│  │                                                            │   │
│  │  • View forecasts                                         │   │
│  │  • Analyze trends                                         │   │
│  │  • Make decisions                                         │   │
│  │                                                            │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2. Thứ tự chạy

```
STEP 1: Start Docker (Kafka)
┌─────────────────────────────────────────────────────────────────────┐
│ cd week6_streaming                                                  │
│ docker-compose up -d                                                │
│ # Wait 15 seconds                                                   │
└─────────────────────────────────────────────────────────────────────┘

STEP 2: Run Backfill (Batch Layer)
┌─────────────────────────────────────────────────────────────────────┐
│ python week6_backfill.py                                           │
│ # Fills gap from last data to today                                │
│ # Output: data_analysis/daily_filled/                              │
└─────────────────────────────────────────────────────────────────────┘

STEP 3: Start Producer (Terminal 1)
┌─────────────────────────────────────────────────────────────────────┐
│ cd week6_streaming                                                  │
│ python websocket_producer.py                                        │
│ # Sends data to Kafka every second                                 │
│ # Let it run for 60+ seconds                                       │
└─────────────────────────────────────────────────────────────────────┘

STEP 4: Start Consumer (Terminal 2)
┌─────────────────────────────────────────────────────────────────────┐
│ cd week6_streaming                                                  │
│ python spark_streaming_consumer.py                                  │
│ # Reads from Kafka, aggregates, saves to Parquet                   │
│ # Let it run for 60+ seconds                                       │
└─────────────────────────────────────────────────────────────────────┘

STEP 5: Stop Producer & Consumer
┌─────────────────────────────────────────────────────────────────────┐
│ Ctrl+C in both terminals                                           │
└─────────────────────────────────────────────────────────────────────┘

STEP 6: Merge (Serving Layer)
┌─────────────────────────────────────────────────────────────────────┐
│ python week6_merge.py                                              │
│ # Merges Batch + Speed data                                        │
│ # Output: unified daily_filled/                                    │
└─────────────────────────────────────────────────────────────────────┘

STEP 7: Forecast (Serving Layer)
┌─────────────────────────────────────────────────────────────────────┐
│ python prophet_train.py                                            │
│ # Trains Prophet, forecasts 30 days                                │
│ # Output: week4_forecasts/, week4_visualizations/                  │
└─────────────────────────────────────────────────────────────────────┘

STEP 8: Cleanup
┌─────────────────────────────────────────────────────────────────────┐
│ cd week6_streaming                                                  │
│ docker-compose down -v                                              │
│ # Stop and remove Docker containers                                │
│                                                                     │
│ Remove-Item -Recurse -Force checkpoint_spark                       │
│ Remove-Item -Recurse -Force streaming_output_spark                 │
│ # Clean up streaming artifacts                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 6. Output và Kết quả

### 6.1. Forecast Results

```
Kết quả forecast ngày 03/12/2025:

┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  BTCUSDT:                                                          │
│  ├── Current Price: $92,817.92                                     │
│  ├── 30-day Forecast: $95,000 - $100,000 (trend up)               │
│  ├── MAPE: 4.52%                                                   │
│  └── Interpretation: Model khá chính xác                          │
│                                                                     │
│  ETHUSDT:                                                          │
│  ├── Current Price: $3,031.32                                      │
│  ├── 30-day Forecast: $3,100 - $3,500 (trend up)                  │
│  ├── MAPE: 4.61%                                                   │
│  └── Interpretation: Model khá chính xác                          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

Lưu ý:
- MAPE < 10% = Model tốt
- Crypto volatile nên MAPE 4-5% là rất tốt
- Forecast chỉ mang tính tham khảo, không phải financial advice
```

### 6.2. Visualization

```
File: week4_visualizations/BTCUSDT_forecast_interactive.html

┌─────────────────────────────────────────────────────────────────────┐
│  [Interactive Chart]                                               │
│                                                                     │
│  Price                                                              │
│  $100k │                                          ┌─── Forecast    │
│        │                                      ╱───┘    (red)       │
│   $90k │                                 ╱────╱                    │
│        │                            ╱────╱                         │
│   $80k │                       ╱────╱                              │
│        │                  ╱────╱                                   │
│   $70k │             ╱────╱                                        │
│        │        ╱────╱                                             │
│   $60k │   ╱────╱   Actual (blue)                                  │
│        │╱──╱                                                       │
│   $50k └───────────────────────────────────────────────────────    │
│        2024-01    2024-06    2024-12    2025-06    2025-12         │
│                                                                     │
│  [Hover để xem giá chi tiết]                                       │
│  [Zoom in/out với scroll]                                          │
│  [Pan với drag]                                                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 7. Câu hỏi thường gặp

### Q1: Tại sao dùng MAPE thay vì accuracy?

```
A: MAPE phù hợp hơn cho time series regression:

Accuracy dùng cho classification:
- Đúng/Sai
- Ví dụ: 95% predictions đúng

MAPE dùng cho regression:
- Đo % sai lệch
- Ví dụ: Trung bình sai 5% so với actual

Trong crypto:
- Actual = $92,000
- Predicted = $88,000
- Error = $4,000
- MAPE = 4.3%

→ MAPE cho biết model sai trung bình bao nhiêu %
```

### Q2: Prophet có thể forecast bao xa?

```
A: Về mặt kỹ thuật, bao xa cũng được. Nhưng:

Ngắn hạn (1-7 ngày):
├── Độ chính xác cao
├── Trend ổn định
└── Recommended

Trung hạn (7-30 ngày):
├── Độ chính xác trung bình
├── Trend có thể thay đổi
└── Cần cẩn thận

Dài hạn (30+ ngày):
├── Độ chính xác thấp
├── Nhiều yếu tố không dự đoán được
└── Chỉ mang tính tham khảo

Trong project:
- Forecast 30 ngày là reasonable
- Crypto volatile nên không nên forecast quá xa
```

### Q3: Serving Layer có thể query real-time không?

```
A: Có, nhưng cần thiết lập:

Hiện tại (Batch serving):
├── Chạy merge.py + prophet_train.py
├── Kết quả lưu vào files
├── Không real-time

Real-time serving (cải tiến):
├── Memory table trong Spark
├── REST API (Flask/FastAPI)
├── Dashboard (Streamlit/Dash)
├── Query bất cứ lúc nào

Ví dụ với Streamlit:
import streamlit as st

df = pd.read_parquet("week4_forecasts/BTCUSDT_forecast.parquet")
st.line_chart(df[['ds', 'yhat']])
```

### Q4: Merge có mất data không?

```
A: Không, merge được thiết kế để giữ toàn bộ data:

1. UNION: Gộp tất cả rows từ cả 2 sources
   Không mất row nào

2. DEDUPLICATE: Chỉ loại duplicates
   Nếu (symbol, date) trùng → giữ 1 bản
   Đây là expected behavior

3. Verify:
   Batch: 4711 rows
   Streaming: 10 rows (có 1 ngày overlap)
   After merge: 4720 rows = 4711 + 10 - 1 ✓
```

---

## 8. Tổng kết Week 6

### 8.1. Những gì đã học

```
┌─────────────────────────────────────────────────────────────────────┐
│                    WEEK 6 - LAMBDA ARCHITECTURE                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. BATCH LAYER                                                     │
│     ├── Xử lý dữ liệu lịch sử lớn                                  │
│     ├── PySpark distributed processing                             │
│     ├── Binance API (klines endpoint)                              │
│     ├── Forward fill missing days                                  │
│     └── MA7/MA30 computation                                       │
│                                                                     │
│  2. SPEED LAYER                                                     │
│     ├── Apache Kafka message broker                                │
│     ├── Docker containerization                                    │
│     ├── Kafka Producer (Python)                                    │
│     ├── Spark Structured Streaming                                 │
│     ├── Watermark (late data handling)                            │
│     └── Window aggregation                                         │
│                                                                     │
│  3. SERVING LAYER                                                   │
│     ├── Data merging (Union + Deduplicate)                        │
│     ├── Schema alignment                                           │
│     ├── Facebook Prophet forecasting                               │
│     ├── Model evaluation (MAPE, RMSE)                             │
│     └── Interactive visualization (Plotly)                         │
│                                                                     │
│  4. TECHNOLOGIES                                                    │
│     ├── PySpark 3.5.3                                              │
│     ├── Apache Kafka 7.5.0 (via Docker)                            │
│     ├── Facebook Prophet 1.1.5                                     │
│     ├── Plotly (visualization)                                     │
│     └── Docker Compose                                             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 8.2. Key Takeaways

```
1. LAMBDA ARCHITECTURE giải quyết tradeoff:
   - Batch: Chính xác nhưng chậm
   - Speed: Nhanh nhưng có thể không chính xác
   - Serving: Kết hợp cả hai

2. KAFKA là message broker:
   - Decoupling producer/consumer
   - Durability (không mất data)
   - Scalability (nhiều consumers)

3. SPARK STRUCTURED STREAMING:
   - DataFrame/SQL API cho streaming
   - Watermark xử lý late data
   - Window aggregation

4. PROPHET:
   - Dễ sử dụng cho time series
   - Tự động detect trend/seasonality
   - Interpretable results
```

### 8.3. Điểm cần trình bày với giáo sư

```
1. ARCHITECTURE:
   "Em đã implement Lambda Architecture với 3 layers:
   Batch Layer cho historical data, Speed Layer cho real-time,
   và Serving Layer để merge và serve kết quả."

2. TECHNOLOGIES:
   "Em sử dụng PySpark cho distributed processing,
   Apache Kafka cho message streaming,
   và Facebook Prophet cho time series forecasting."

3. RESULTS:
   "Model Prophet đạt MAPE khoảng 4.5%,
   nghĩa là trung bình sai khoảng 4.5% so với giá thực.
   Đây là kết quả tốt cho volatile asset như crypto."

4. CHALLENGES:
   "Khó khăn chính là:
   - Setup Kafka với Docker trên Windows
   - Handle late data với watermark
   - Align schema giữa batch và streaming data"

5. IMPROVEMENTS:
   "Có thể cải tiến bằng cách:
   - Thêm real-time dashboard
   - Sử dụng multiple models (ensemble)
   - Add more features (sentiment analysis, etc.)"
```

---

## 📚 Tài liệu tham khảo

1. **Lambda Architecture:**
   - Nathan Marz - "Big Data: Principles and best practices"
   - https://lambda-architecture.net/

2. **Apache Kafka:**
   - https://kafka.apache.org/documentation/
   - Confluent Documentation

3. **Spark Structured Streaming:**
   - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

4. **Facebook Prophet:**
   - https://facebook.github.io/prophet/
   - https://facebook.github.io/prophet/docs/quick_start.html

---

*Tạo bởi: Big Data Project - Week 6 Documentation*
*Cập nhật: 03/12/2025*
*Phiên bản: Complete (4/4 files)*
