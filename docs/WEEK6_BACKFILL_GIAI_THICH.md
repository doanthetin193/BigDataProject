# Giải thích chi tiết: week6_backfill.py

**File:** `scripts/lambda_batch/week6_backfill.py`  
**Chức năng:** Backfill missing dates từ Binance API cho Batch Layer  
**Tác giả:** Đoàn Thế Tín  
**Ngày:** Week 6 - Lambda Architecture

---

## 📋 Mục lục
1. [Import và khởi tạo](#1-import-và-khởi-tạo)
2. [Step 1: Detect Last Date](#2-step-1-detect-last-date)
3. [Step 2: Fetch từ Binance API](#3-step-2-fetch-từ-binance-api)
4. [Step 3: Clean Data](#4-step-3-clean-data)
5. [Step 4: Aggregate Daily OHLC](#5-step-4-aggregate-daily-ohlc)
6. [Step 5: Forward Fill Missing Dates](#6-step-5-forward-fill-missing-dates)
7. [Step 6: Prepare Backfill Data](#7-step-6-prepare-backfill-data)
8. [Step 7: Merge & Recalculate MA](#8-step-7-merge--recalculate-ma)
9. [Step 8: Extract Prophet Input](#9-step-8-extract-prophet-input)
10. [Tóm tắt](#tóm-tắt-tổng-quan)

---

## 1. Import và Khởi tạo

### Dòng 1-5: Docstring
```python
"""
Week 6 - Batch Layer (Lambda Architecture)
Backfill missing dates from Binance API
"""
```
**Giải thích:** Header mô tả mục đích của file - backfill gaps (khoảng trống) trong dữ liệu bằng cách lấy từ Binance API.

---

### Dòng 6-17: Import PySpark
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, from_unixtime, to_date, year, month, first, last, max, min, 
    sum, count, avg, lit, sequence, explode, lag, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, DoubleType
```
**Giải thích:**
- `SparkSession`: Điểm vào chính của Spark, dùng để tạo DataFrame
- `functions as F`: Alias cho các hàm Spark SQL
- `col, from_unixtime, to_date...`: Các hàm xử lý dữ liệu:
  - `col()`: Truy cập cột
  - `from_unixtime()`: Chuyển Unix timestamp → datetime
  - `to_date()`: Extract date từ datetime
  - `year(), month()`: Extract year/month
  - `max(), min(), sum(), count(), avg()`: Aggregation functions
  - `sequence()`: Tạo dãy số/ngày
  - `explode()`: Chuyển array → rows
  - `lag()`: Lấy giá trị row trước đó (window function)
  - `coalesce()`: Lấy giá trị non-null đầu tiên
- `Window`: Dùng cho window functions (xử lý theo cửa sổ dữ liệu)
- `LongType, DoubleType`: Kiểu dữ liệu Spark

---

### Dòng 18-21: Import Python Libraries
```python
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
```
**Giải thích:**
- `requests`: Call HTTP API (Binance)
- `pandas`: Xử lý data dạng DataFrame (Python, không phải Spark)
- `datetime, timedelta`: Xử lý ngày tháng
- `time`: Sleep/delay giữa các API calls
- `os`: Xử lý đường dẫn file

---

### Dòng 23-24: Base Directory
```python
base_dir = r"D:\BigDataProject"
```
**Giải thích:** 
- `r"..."`: Raw string (không escape `\`)
- Đường dẫn gốc của project

---

### Dòng 26-30: Khởi tạo Spark
```python
spark = SparkSession.builder \
    .appName("Week6_Backfill_BatchLayer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```
**Giải thích:**
- `SparkSession.builder`: Builder pattern
- `.appName(...)`: Đặt tên application (hiển thị trong Spark UI)
- `.config("spark.sql.adaptive.enabled", "true")`: Bật Adaptive Query Execution (AQE)
  - AQE tự động tối ưu query plan khi runtime
  - Cải thiện performance cho join, aggregate
- `.getOrCreate()`: Lấy session hiện tại hoặc tạo mới nếu chưa có

---

### Dòng 32-36: Header
```python
print("=" * 80)
print("WEEK 6 - BATCH LAYER (Lambda Architecture)")
print("Backfill missing dates from Binance API")
print("=" * 80)
```
**Giải thích:** In header đẹp cho console output.

---

## 2. Step 1: Detect Last Date

### Dòng 38-41: Bắt đầu Step 1
```python
print("\n[STEP 1] Detecting last date in existing data...")

daily_filled_path = os.path.join(base_dir, "data_analysis", "daily_filled")
```
**Giải thích:**
- `os.path.join()`: Nối đường dẫn một cách an toàn (tự động xử lý `/` hoặc `\`)
- `daily_filled_path`: `D:\BigDataProject\data_analysis\daily_filled`

---

### Dòng 43-55: Detect Last Date từ Daily Filled
```python
try:
    df_existing = spark.read.parquet(daily_filled_path)
    last_date_existing = df_existing.agg(F.max("date")).first()[0]
    data_source = "daily_filled"
except:
    try:
        prophet_path = os.path.join(base_dir, "data_analysis", "prophet_input")
        df_existing = spark.read.parquet(prophet_path)
        last_date_existing = df_existing.agg(F.max("ds")).first()[0]
        data_source = "prophet_input"
    except:
        print("  [WARN] No existing data found!")
        spark.stop()
        exit(1)
```
**Giải thích:**
- **Try 1:** Đọc từ `daily_filled/`
  - `spark.read.parquet()`: Đọc Parquet file
  - `.agg(F.max("date"))`: Lấy ngày lớn nhất (latest)
  - `.first()[0]`: Lấy giá trị đầu tiên (row đầu, cột đầu)
- **Try 2:** Nếu không có daily_filled, thử đọc `prophet_input/`
  - Tương tự nhưng cột tên `ds` (không phải `date`)
- **Except:** Nếu cả 2 không có → Cảnh báo và thoát
  - `spark.stop()`: Dừng Spark session
  - `exit(1)`: Thoát với mã lỗi 1

---

### Dòng 57-64: Tính Gap
```python
print(f"  [OK] Last date found in {data_source}: {last_date_existing}")

today = datetime.now().strftime('%Y-%m-%d')
fetch_start_date = (datetime.strptime(str(last_date_existing), '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
gap_days = (datetime.strptime(today, '%Y-%m-%d') - datetime.strptime(fetch_start_date, '%Y-%m-%d')).days

print(f"  [DATE] Today: {today}")
print(f"  [DATA] Gap: {gap_days} days")
```
**Giải thích:**
- `datetime.now().strftime('%Y-%m-%d')`: Ngày hôm nay (format YYYY-MM-DD)
- `fetch_start_date`: Ngày sau last_date_existing
  - `datetime.strptime()`: Parse string → datetime object
  - `+ timedelta(days=1)`: Cộng 1 ngày
  - `.strftime()`: Format lại thành string
- `gap_days`: Số ngày cần backfill
  - Tính: today - fetch_start_date

---

### Dòng 66-76: Kiểm tra cần Backfill không
```python
if gap_days <= 0:
    print("\n[OK] Data is already up to date! No backfill needed.")
    print("  You can start streaming for real-time data:")
    print("    cd week6_streaming")
    print("    docker-compose up -d")
    print("    python websocket_producer.py")
    print("    python spark_streaming_consumer.py")
    spark.stop()
    exit(0)

print(f"\n  [TARGET] Will backfill: {fetch_start_date} -> {today} ({gap_days} days)")
```
**Giải thích:**
- Nếu `gap_days <= 0`: Dữ liệu đã up-to-date
  - In hướng dẫn chạy Speed Layer
  - Thoát với mã 0 (success)
- Nếu có gap: In thông tin sẽ backfill

---

## 3. Step 2: Fetch từ Binance API

### Dòng 78-81: Bắt đầu Step 2
```python
print("\n[STEP 2] Fetching data from Binance API...")

def fetch_binance_klines(symbol, start_date, end_date, interval='1m'):
    """Fetch klines from Binance API with pagination"""
```
**Giải thích:** Định nghĩa hàm fetch dữ liệu từ Binance API.

---

### Dòng 82-84: Chuyển đổi Timestamp
```python
    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
    end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000) + 86399999
```
**Giải thích:**
- Binance API dùng Unix timestamp (milliseconds)
- `datetime.strptime()`: Parse string → datetime
- `.timestamp()`: Chuyển → Unix timestamp (seconds)
- `* 1000`: Chuyển seconds → milliseconds
- `+ 86399999`: Thêm 23:59:59.999 (cuối ngày)
  - 86399 giây = 23h 59m 59s
  - 999 ms = 0.999s

---

### Dòng 86-88: Khởi tạo Variables
```python
    all_data = []
    current_start = start_ts
```
**Giải thích:**
- `all_data`: List chứa tất cả klines
- `current_start`: Timestamp bắt đầu (để pagination)

---

### Dòng 90-100: Loop Pagination
```python
    while current_start < end_ts:
        url = f"https://api.binance.com/api/v3/klines"
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': current_start,
            'endTime': end_ts,
            'limit': 1000
        }
        
        max_retries = 3
```
**Giải thích:**
- `while current_start < end_ts`: Lặp cho đến khi lấy hết data
- **Tại sao cần loop?** Binance API limit 1000 rows/request → Cần nhiều request
- `url`: Endpoint Binance Klines API
- `params`: Query parameters:
  - `symbol`: BTCUSDT hoặc ETHUSDT
  - `interval`: '1m' (1 phút)
  - `startTime`: Timestamp bắt đầu
  - `endTime`: Timestamp kết thúc
  - `limit`: 1000 rows/request (max)
- `max_retries = 3`: Thử lại tối đa 3 lần nếu lỗi

---

### Dòng 101-120: Retry Logic
```python
        for retry in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                
                all_data.extend(data)
                current_start = int(data[-1][0]) + 60000
                time.sleep(0.1)
                break
            except Exception as e:
                if retry < max_retries - 1:
                    print(f"    Retry after error: {e}")
                    time.sleep(2)
                else:
                    print(f"    [WARN] Failed after {max_retries} retries")
                    break
```
**Giải thích:**
- `for retry in range(max_retries)`: Loop 3 lần
- **Try block:**
  - `requests.get()`: Call HTTP GET
    - `timeout=30`: Timeout 30 giây
  - `raise_for_status()`: Raise exception nếu HTTP error (4xx, 5xx)
  - `.json()`: Parse JSON response
  - `if not data: break`: Nếu không có data → Thoát loop
  - `all_data.extend(data)`: Thêm data vào list
  - `current_start = int(data[-1][0]) + 60000`: Update start time
    - `data[-1][0]`: Timestamp của row cuối
    - `+ 60000`: Thêm 1 phút (60,000 ms)
  - `time.sleep(0.1)`: Đợi 0.1s tránh rate limit
  - `break`: Thoát retry loop (thành công)
- **Except block:**
  - Nếu retry < 2: Print error, đợi 2s, thử lại
  - Nếu retry = 2 (lần cuối): Print warning, thoát

---

### Dòng 122: Return
```python
    return all_data
```
**Giải thích:** Trả về list chứa tất cả klines.

---

### Dòng 124-142: Fetch cho BTC và ETH
```python
all_klines = []
for symbol in ["BTCUSDT", "ETHUSDT"]:
    print(f"\n  Fetching {symbol}...")
    klines = fetch_binance_klines(symbol, fetch_start_date, today)
    
    if klines:
        for k in klines:
            all_klines.append({
                'timestamp': int(k[0]) // 1000,
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4]),
                'volume': float(k[5]),
                'symbol': symbol
            })
        print(f"    [OK] Fetched {len(klines):,} rows")
    else:
        print(f"    [WARN] No data fetched")
```
**Giải thích:**
- Loop qua 2 symbols: BTCUSDT, ETHUSDT
- Call `fetch_binance_klines()` cho mỗi symbol
- **Parse klines:**
  - Binance API trả về array: `[timestamp, open, high, low, close, volume, ...]`
  - `k[0]`: Timestamp (ms) → Chia 1000 → seconds
  - `k[1]`: Open price
  - `k[2]`: High price
  - `k[3]`: Low price
  - `k[4]`: Close price
  - `k[5]`: Volume
- Chuyển thành dictionary và append vào `all_klines`

---

### Dòng 144-149: Kiểm tra Data
```python
if not all_klines:
    print("\n[ERROR] No data fetched. Exiting.")
    spark.stop()
    exit(1)

print(f"\n  Total rows fetched: {len(all_klines):,}")
```
**Giải thích:**
- Nếu không fetch được data nào → Thoát
- In tổng số rows fetched

---

## 4. Step 3: Clean Data

### Dòng 151-156: Tạo DataFrame
```python
print("\n[STEP 3] Cleaning data...")

df_raw = spark.createDataFrame(all_klines)
df_raw = df_raw.withColumn("datetime", from_unixtime(col("timestamp")))
df_raw = df_raw.withColumn("date", to_date("datetime"))
```
**Giải thích:**
- `spark.createDataFrame()`: Chuyển Python list → Spark DataFrame
- `.withColumn("datetime", ...)`: Thêm cột mới
  - `from_unixtime(col("timestamp"))`: Unix timestamp → datetime string
- `.withColumn("date", ...)`: Extract date từ datetime
  - `to_date("datetime")`: '2025-12-16 10:30:00' → '2025-12-16'

---

### Dòng 158-161: Remove Duplicates
```python
df_clean = df_raw.dropDuplicates(["symbol", "timestamp"])
clean_rows = df_clean.count()
print(f"  [OK] After deduplication: {clean_rows:,} rows")
```
**Giải thích:**
- `dropDuplicates(["symbol", "timestamp"])`: Xóa duplicate dựa trên (symbol, timestamp)
  - Ví dụ: Nếu có 2 rows với (BTCUSDT, 1734134400) → Giữ 1, xóa 1
- `.count()`: Đếm số rows
- `{clean_rows:,}`: Format số với dấu phẩy (1,000)

---

## 5. Step 4: Aggregate Daily OHLC

### Dòng 163-165: Bắt đầu Step 4
```python
print("\n[STEP 4] Aggregating to daily OHLC...")

minmax = df_clean.groupBy("symbol", "date").agg(
```
**Giải thích:** Tính min/max timestamp cho mỗi ngày (để lấy open/close).

---

### Dòng 166-169: Min/Max Timestamp
```python
    F.min("timestamp").alias("min_ts"),
    F.max("timestamp").alias("max_ts")
)
```
**Giải thích:**
- `groupBy("symbol", "date")`: Nhóm theo symbol và ngày
- `F.min("timestamp")`: Timestamp nhỏ nhất (đầu ngày)
- `F.max("timestamp")`: Timestamp lớn nhất (cuối ngày)
- `.alias()`: Đặt tên cột mới

---

### Dòng 171-174: Daily Open
```python
opens = df_clean.join(minmax, on=["symbol", "date"]) \
    .filter(col("timestamp") == col("min_ts")) \
    .select("symbol", "date", col("open").alias("daily_open"))
```
**Giải thích:**
- **Join:** df_clean với minmax
- **Filter:** Chỉ lấy rows có timestamp = min_ts (đầu ngày)
- **Select:** Lấy giá `open` → Đặt tên `daily_open`
- **Kết quả:** DataFrame với (symbol, date, daily_open)

---

### Dòng 176-179: Daily Close
```python
closes = df_clean.join(minmax, on=["symbol", "date"]) \
    .filter(col("timestamp") == col("max_ts")) \
    .select("symbol", "date", col("close").alias("daily_close"))
```
**Giải thích:** Tương tự `opens` nhưng lấy `close` ở timestamp cuối ngày.

---

### Dòng 181-186: Daily High/Low/Volume
```python
basic = df_clean.groupBy("symbol", "date").agg(
    F.max("high").alias("daily_high"),
    F.min("low").alias("daily_low"),
    F.sum("volume").alias("daily_volume")
)
```
**Giải thích:**
- `F.max("high")`: Giá cao nhất trong ngày
- `F.min("low")`: Giá thấp nhất trong ngày
- `F.sum("volume")`: Tổng volume trong ngày

---

### Dòng 188-191: Join All
```python
df_daily = basic.join(opens, ["symbol", "date"], "left") \
                .join(closes, ["symbol", "date"], "left") \
                .orderBy("symbol", "date")
```
**Giải thích:**
- Join 3 DataFrames: `basic`, `opens`, `closes`
- Left join: Giữ tất cả rows từ `basic`
- `orderBy()`: Sắp xếp theo symbol, date

---

### Dòng 193-199: Print Statistics
```python
daily_count = df_daily.count()
print(f"  [OK] Daily aggregation: {daily_count} rows")
df_daily.groupBy("symbol").agg(
    count("*").alias("days"),
    min("date").alias("first_date"),
    max("date").alias("last_date")
).show(truncate=False)
```
**Giải thích:**
- In số rows
- Group by symbol và show statistics
- `.show(truncate=False)`: Hiển thị full text (không cắt)

---

## 6. Step 5: Forward Fill Missing Dates

### Dòng 201-209: Tạo Date Range
```python
print("\n[STEP 5] Forward filling missing dates...")

date_range_df = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{fetch_start_date}'),
        to_date('{today}'),
        interval 1 day
    )) as date
""")
```
**Giải thích:**
- **Spark SQL:** Tạo DataFrame với tất cả ngày từ start → end
- `sequence(start, end, interval 1 day)`: Tạo array ngày
  - Ví dụ: ['2025-12-15', '2025-12-16']
- `explode()`: Chuyển array → rows
  - ['2025-12-15', '2025-12-16'] → 2 rows

---

### Dòng 211-213: Loop qua Symbols
```python
df_filled_list = []

for symbol in ["BTCUSDT", "ETHUSDT"]:
```
**Giải thích:** Xử lý từng symbol riêng.

---

### Dòng 214-217: Create Complete Date Range
```python
    df_symbol = df_daily.filter(col("symbol") == symbol)
    df_complete = date_range_df.crossJoin(df_symbol.select("symbol").distinct())
    df_with_gaps = df_complete.join(df_symbol, ["symbol", "date"], "left")
```
**Giải thích:**
- `df_symbol`: Chỉ lấy 1 symbol
- `df_complete`: CrossJoin date_range với symbol
  - Ví dụ: 2 ngày × 1 symbol = 2 rows
- `df_with_gaps`: Left join với data thật
  - Rows không có data → NULL

---

### Dòng 219: Window Specification
```python
    window_spec = Window.partitionBy("symbol").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
```
**Giải thích:**
- **Window Function:** Xử lý theo cửa sổ dữ liệu
- `partitionBy("symbol")`: Phân vùng theo symbol (mỗi symbol tính riêng)
- `orderBy("date")`: Sắp xếp theo ngày
- `rowsBetween(unboundedPreceding, 0)`: Cửa sổ từ đầu đến row hiện tại
  - Dùng cho forward fill: Lấy giá trị gần nhất trước đó

---

### Dòng 221-227: Forward Fill
```python
    for col_name in ["daily_open", "daily_high", "daily_low", "daily_close", "daily_volume"]:
        df_with_gaps = df_with_gaps.withColumn(
            col_name,
            F.last(col(col_name), ignorenulls=True).over(window_spec)
        )
    
    df_filled_list.append(df_with_gaps)
```
**Giải thích:**
- Loop qua 5 cột OHLCV
- `F.last(..., ignorenulls=True).over(window_spec)`:
  - Lấy giá trị **cuối cùng non-null** trong cửa sổ
  - Ví dụ: [100, NULL, NULL] → [100, 100, 100]
  - Đây là **forward fill**: Điền giá trị trước đó vào NULL
- Append vào list

---

### Dòng 229-234: Union All Symbols
```python
df_filled = df_filled_list[0]
if len(df_filled_list) > 1:
    df_filled = df_filled.union(df_filled_list[1])

df_filled = df_filled.filter(col("daily_close").isNotNull())
filled_count = df_filled.count()
print(f"  [OK] After forward fill: {filled_count} rows")
```
**Giải thích:**
- Union 2 DataFrames (BTCUSDT + ETHUSDT)
- Filter bỏ rows không có data (daily_close NULL)
  - Vì có thể có ngày chưa có data gốc → Không thể forward fill
- In số rows

---

## 7. Step 6: Prepare Backfill Data

### Dòng 236-246: Select Columns
```python
print("\n[STEP 6] Preparing daily OHLCV data...")

df_backfill = df_filled.select("symbol", "date", "daily_open", "daily_high", "daily_low", "daily_close", "daily_volume")

print(f"  [OK] Backfill data prepared: {df_backfill.count()} rows")
df_backfill.groupBy("symbol").agg(
    count("*").alias("rows"),
    min("date").alias("first"),
    max("date").alias("last")
).show(truncate=False)
```
**Giải thích:**
- Select 7 cột cần thiết (symbol, date, OHLCV)
- In statistics

---

## 8. Step 7: Merge & Recalculate MA

### Dòng 248-256: Read Existing Data
```python
print("\n[STEP 7] Merging with existing daily_filled...")

try:
    df_old_filled = spark.read.parquet(daily_filled_path)
    old_count = df_old_filled.count()
    print(f"  [FOUND] Found existing daily_filled: {old_count:,} rows")
    
    # Select only OHLCV columns (drop old MA7/MA30)
    df_old_filled = df_old_filled.select("symbol", "date", "daily_open", "daily_high", "daily_low", "daily_close", "daily_volume")
```
**Giải thích:**
- Try đọc daily_filled cũ
- **Quan trọng:** Chỉ select OHLCV, **bỏ MA7/MA30 cũ**
  - Vì sẽ tính lại MA sau khi merge

---

### Dòng 258-264: Union & Deduplicate
```python
    # Union and remove duplicates
    df_merged = df_old_filled.union(df_backfill).dropDuplicates(["symbol", "date"]).orderBy("symbol", "date")
    merged_count = df_merged.count()
    print(f"  [MERGE] After merge: {merged_count:,} rows (added {merged_count - old_count} new)")
except Exception as e:
    print(f"  [INFO] No existing daily_filled, creating new ({type(e).__name__})")
    df_merged = df_backfill
```
**Giải thích:**
- **Union:** Kết hợp old + new data
- **dropDuplicates:** Xóa duplicate theo (symbol, date)
  - Nếu trùng → Giữ row đầu tiên (old data)
- **Except:** Nếu không có old data → Dùng backfill data

---

### Dòng 266-272: Recalculate MA7/MA30
```python
print("\n  [CALC] Recalculating MA7/MA30 for entire dataset...")

window_ma7 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-6, 0)
window_ma30 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0)

df_merged = df_merged \
    .withColumn("ma7", avg("daily_close").over(window_ma7)) \
```
**Giải thích:**
- **Window MA7:** 7 ngày gần nhất
  - `rowsBetween(-6, 0)`: 6 rows trước + row hiện tại = 7 rows
- **Window MA30:** 30 ngày gần nhất
  - `rowsBetween(-29, 0)`: 29 rows trước + row hiện tại = 30 rows
- `avg("daily_close").over(window_ma7)`: Tính average trong cửa sổ

---

### Dòng 273-275: Add Year/Month
```python
    .withColumn("ma30", avg("daily_close").over(window_ma30)) \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date"))
```
**Giải thích:**
- Thêm cột `year`, `month` để partition khi save Parquet

---

### Dòng 277-281: Save Daily Filled
```python
df_merged.write.mode("overwrite").partitionBy("symbol", "year", "month").parquet(daily_filled_path)

print(f"  [OK] Saved daily_filled: {df_merged.count():,} rows")
print(f"  [SAVE] Path: {daily_filled_path}")
```
**Giải thích:**
- `.write.mode("overwrite")`: Ghi đè file cũ
- `.partitionBy("symbol", "year", "month")`: Tạo folders:
  - `symbol=BTCUSDT/year=2025/month=12/part-xxx.parquet`
- `.parquet()`: Save dạng Parquet format

---

## 9. Step 8: Extract Prophet Input

### Dòng 283-294: Extract Prophet Input
```python
print("\n[STEP 8] Extracting prophet_input from daily_filled...")

prophet_path = os.path.join(base_dir, "data_analysis", "prophet_input")

# Extract minimal schema: (ds, y, symbol) - NO MA columns
df_prophet = df_merged.select(
    col("date").alias("ds"),
    col("daily_close").alias("y"),
    "symbol"
)

# Save (overwrite - always sync with daily_filled)
df_prophet.write.mode("overwrite").partitionBy("symbol").parquet(prophet_path)
```
**Giải thích:**
- **Prophet cần 3 cột:**
  - `ds`: Date (datetime)
  - `y`: Target variable (giá close)
  - `symbol`: Partition key
- **Không include MA7/MA30** ở đây
  - Sẽ join từ daily_filled khi train Prophet
- `partitionBy("symbol")`: 2 partitions (BTCUSDT, ETHUSDT)

---

### Dòng 296-298: Print Info
```python
print(f"  [OK] Prophet input extracted: {df_prophet.count():,} rows")
print(f"  [INFO] Schema: (ds, y, symbol) - MA7/MA30 will be joined from daily_filled")
print(f"  [SAVE] Path: {prophet_path}")
```
**Giải thích:** In thông tin output.

---

## 10. Summary & Next Steps

### Dòng 300-308: Final Statistics
```python
print("\n" + "=" * 80)
print("[OK] BACKFILL COMPLETE (BATCH LAYER)")
print("=" * 80)

print("\n[DATA] Final Statistics:")
df_merged.groupBy("symbol").agg(
    count("*").alias("total_days"),
    min("date").alias("first_date"),
    max("date").alias("last_date")
).show(truncate=False)
```
**Giải thích:** In statistics tổng kết.

---

### Dòng 310-314: Output Paths
```python
print("\n[DATA] Output:")
print(f"  [OK] data_analysis/daily_filled/   (Backfilled data with MA7/MA30)")
print(f"  [OK] data_analysis/prophet_input/  (Ready for Prophet)")

print("\n[TARGET] Next Steps:")
```
**Giải thích:** Liệt kê output files.

---

### Dòng 315-324: Next Steps
```python
print("  1. Start Streaming (Speed Layer):")
print("     cd week6_streaming")
print("     docker-compose up -d")
print("     python websocket_producer.py  # Terminal 1")
print("     python spark_streaming_consumer.py  # Terminal 2")
print("")
print("  2. Let streaming run to collect real-time data")
print("")
print("  3. Merge batch + streaming data:")
print("     python week6_merge.py")
```
**Giải thích:** Hướng dẫn bước tiếp theo.

---

### Dòng 326-327: Cleanup
```python
spark.stop()
print("\n[DONE] Week 6 Backfill process finished!")
```
**Giải thích:**
- `spark.stop()`: Dừng Spark session (giải phóng tài nguyên)
- In message hoàn thành

---

---

# Tóm tắt Tổng quan

## 🎯 Mục đích File
File `week6_backfill.py` thực hiện **Batch Layer backfill** trong Lambda Architecture - Điền gaps (khoảng trống) trong dữ liệu bằng cách lấy historical data từ Binance API.

---

## 📊 Workflow (8 Steps)

### **1. Detect Last Date**
- Đọc `daily_filled/` hoặc `prophet_input/`
- Tìm ngày cuối cùng có data
- Tính gap: Hôm nay - Last date

### **2. Fetch từ Binance API**
- Call Binance Klines API cho BTC và ETH
- Lấy 1-minute candles từ last_date+1 đến hôm nay
- Pagination: 1000 rows/request
- Retry logic: 3 lần nếu timeout

### **3. Clean Data**
- Chuyển Unix timestamp → Date
- Remove duplicates (symbol, timestamp)

### **4. Aggregate Daily OHLC**
- Group by (symbol, date)
- Daily Open: Giá đầu ngày (min timestamp)
- Daily High: Max high
- Daily Low: Min low
- Daily Close: Giá cuối ngày (max timestamp)
- Daily Volume: Sum volume

### **5. Forward Fill Missing Dates**
- Tạo complete date range (tất cả ngày trong khoảng)
- Forward fill NULL values (điền giá trị gần nhất trước đó)
- Xử lý gaps do Binance API thiếu data

### **6. Prepare Backfill Data**
- Select columns: symbol, date, OHLCV
- Ready to merge

### **7. Merge & Recalculate MA**
- Union old data + new backfill data
- Deduplicate (giữ old data nếu trùng)
- **Recalculate MA7/MA30** cho toàn bộ dataset
  - MA7: Average 7 ngày
  - MA30: Average 30 ngày
- Save → `data_analysis/daily_filled/`

### **8. Extract Prophet Input**
- Select (date→ds, daily_close→y, symbol)
- **Không include MA** (sẽ join từ daily_filled)
- Save → `data_analysis/prophet_input/`

---

## 🔑 Điểm Quan Trọng

### **1. Tại sao Recalculate MA?**
- MA phụ thuộc vào **toàn bộ dataset**
- Khi thêm data mới → MA của ngày cũ thay đổi
- **Phải tính lại toàn bộ** để đảm bảo chính xác

### **2. Forward Fill**
- Điền gaps do Binance API thiếu data (nghỉ lễ, downtime)
- Dùng giá trị ngày trước đó
- Tránh NULL trong dataset

### **3. Deduplication Priority**
- Old data > New backfill data
- Tránh ghi đè data đã clean/verified

### **4. Partition Strategy**
- `partitionBy("symbol", "year", "month")` 
- Tối ưu query performance
- Dễ dàng quản lý/delete theo thời gian

---

## 📁 Input/Output

| Stage | Input | Output |
|-------|-------|--------|
| **Detect** | `daily_filled/` hoặc `prophet_input/` | Last date |
| **Fetch** | Binance API | Raw 1-min klines |
| **Clean** | Raw klines | Deduplicated DataFrame |
| **Aggregate** | 1-min data | Daily OHLCV |
| **Forward Fill** | Daily OHLCV | Complete date range |
| **Merge** | Old + New data | `daily_filled/` with MA |
| **Extract** | `daily_filled/` | `prophet_input/` |

---

## 💡 Use Cases

### **Khi nào chạy file này?**
1. ✅ Lần đầu setup project (no data)
2. ✅ Sau khi pause project vài ngày (có gap)
3. ✅ Trước khi train Prophet (cần data mới nhất)
4. ✅ Sau khi fix data corruption (rebuild)

### **Khi nào KHÔNG chạy?**
- ❌ Data đã up-to-date (gap ≤ 0 days)
- ❌ Binance API đang down/rate limit
- ❌ Đang có Speed Layer running (chờ merge)

---

## 🚀 Next Steps (Sau khi Backfill)

1. **Start Speed Layer:**
   ```bash
   cd week6_streaming
   docker-compose up -d
   python websocket_producer.py
   python kafka_batch_reader.py
   ```

2. **Merge Batch + Speed:**
   ```bash
   python scripts/lambda_batch/week6_merge.py
   ```

3. **Train Prophet:**
   ```bash
   python scripts/ml_models/prophet_train.py
   ```

---

## 🎓 Key Technologies

- **PySpark:** DataFrame API, SparkSQL
- **Window Functions:** MA calculation, Forward fill
- **Binance API:** Historical klines endpoint
- **Parquet:** Columnar storage, Partitioning
- **Lambda Architecture:** Batch Layer component

---

**Tác giả:** Đoàn Thế Tín  
**MSSV:** 4551190056  
**File:** `scripts/lambda_batch/week6_backfill.py`  
**Lines:** 327 dòng code  
**Mục đích:** Backfill missing dates cho Batch Layer trong Lambda Architecture

---
