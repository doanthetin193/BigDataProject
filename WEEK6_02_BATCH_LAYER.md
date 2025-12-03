# 📘 WEEK 6 - PHẦN 2: BATCH LAYER (week6_backfill.py)

## 📑 Mục lục
1. [Mục đích của Batch Layer](#1-mục-đích-của-batch-layer)
2. [Tổng quan file week6_backfill.py](#2-tổng-quan-file-week6_backfillpy)
3. [Giải thích chi tiết từng phần code](#3-giải-thích-chi-tiết-từng-phần-code)
4. [Binance API - Cách hoạt động](#4-binance-api---cách-hoạt-động)
5. [Xử lý dữ liệu với PySpark](#5-xử-lý-dữ-liệu-với-pyspark)
6. [Forward Fill - Điền dữ liệu thiếu](#6-forward-fill---điền-dữ-liệu-thiếu)
7. [Moving Average (MA7, MA30)](#7-moving-average-ma7-ma30)
8. [Output và cấu trúc dữ liệu](#8-output-và-cấu-trúc-dữ-liệu)
9. [Câu hỏi thường gặp](#9-câu-hỏi-thường-gặp)

---

## 1. Mục đích của Batch Layer

### 1.1. Vấn đề cần giải quyết

Trong Lambda Architecture, Batch Layer có nhiệm vụ:
- Xử lý **dữ liệu lịch sử** (historical data)
- **Backfill** (lấp đầy) những ngày bị thiếu
- Đảm bảo **tính chính xác** của dữ liệu

### 1.2. Tình huống thực tế

```
Ví dụ:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Ngày 24/11: Bạn chạy project, dữ liệu đến 24/11                   │
│                                                                     │
│  Ngày 25/11 - 02/12: Máy tính tắt, không chạy streaming           │
│                                                                     │
│  Ngày 03/12: Bạn mở lại project                                    │
│              → GAP: 9 ngày thiếu dữ liệu (25/11 - 03/12)           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.3. Giải pháp của Batch Layer

```
week6_backfill.py sẽ:

1. DETECT: Phát hiện ngày cuối cùng có dữ liệu (24/11)
2. CALCULATE: Tính số ngày gap (9 ngày)
3. FETCH: Gọi Binance API lấy dữ liệu 25/11 - 03/12
4. PROCESS: Clean, aggregate, compute MA
5. SAVE: Lưu vào Parquet để dùng cho forecast
```

---

## 2. Tổng quan file week6_backfill.py

### 2.1. Thông tin file

| Thuộc tính | Giá trị |
|------------|---------|
| Tên file | `week6_backfill.py` |
| Vị trí | `D:\BigDataProject\week6_backfill.py` |
| Số dòng | ~335 dòng |
| Ngôn ngữ | Python 3.10 |
| Framework | PySpark 3.5.3 |
| API | Binance API v3 |

### 2.2. Cấu trúc file

```python
# ============================================================
# week6_backfill.py - Cấu trúc tổng quan
# ============================================================

# PHẦN 1: Import thư viện (dòng 1-35)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import requests
import pandas as pd
...

# PHẦN 2: Khởi tạo Spark (dòng 37-47)
spark = SparkSession.builder...

# PHẦN 3: STEP 1 - Detect last date (dòng 49-95)
# Đọc dữ liệu hiện có, tìm ngày cuối

# PHẦN 4: STEP 2 - Fetch from Binance (dòng 97-170)
# Gọi API lấy dữ liệu mới

# PHẦN 5: STEP 3 - Clean data (dòng 172-185)
# Loại bỏ duplicate

# PHẦN 6: STEP 4 - Daily aggregation (dòng 187-210)
# Tổng hợp từ 1-minute thành daily

# PHẦN 7: STEP 5 - Forward fill (dòng 212-260)
# Điền các ngày thiếu

# PHẦN 8: STEP 6 - Compute MA (dòng 262-280)
# Tính Moving Average

# PHẦN 9: STEP 7-8 - Save output (dòng 282-320)
# Lưu kết quả

# PHẦN 10: Summary (dòng 322-335)
# Hiển thị tổng kết
```

### 2.3. Luồng xử lý (Pipeline)

```
┌─────────────────────────────────────────────────────────────────────┐
│                    BATCH LAYER PIPELINE                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐         │
│   │ STEP 1  │    │ STEP 2  │    │ STEP 3  │    │ STEP 4  │         │
│   │ Detect  │───►│ Fetch   │───►│ Clean   │───►│ Daily   │         │
│   │ Last    │    │ Binance │    │ Data    │    │ OHLC    │         │
│   │ Date    │    │ API     │    │         │    │         │         │
│   └─────────┘    └─────────┘    └─────────┘    └─────────┘         │
│                                                      │              │
│                                                      ▼              │
│   ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐         │
│   │ STEP 8  │    │ STEP 7  │    │ STEP 6  │    │ STEP 5  │         │
│   │ Update  │◄───│ Save    │◄───│ Compute │◄───│ Forward │         │
│   │ Prophet │    │ Parquet │    │ MA7/30  │    │ Fill    │         │
│   │ Input   │    │         │    │         │    │         │         │
│   └─────────┘    └─────────┘    └─────────┘    └─────────┘         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Giải thích chi tiết từng phần code

### 3.1. Import thư viện

```python
# ==================================================================
# PHẦN 1: IMPORT THƯ VIỆN
# ==================================================================

# PySpark - Framework xử lý Big Data
from pyspark.sql import SparkSession          # Tạo session làm việc với Spark
from pyspark.sql import functions as F        # Các hàm xử lý dữ liệu

# Các hàm cụ thể của PySpark
from pyspark.sql.functions import (
    col,            # Truy cập cột: col("price")
    from_unixtime,  # Convert timestamp unix -> datetime
    to_date,        # Convert datetime -> date only
    year,           # Lấy năm từ date
    month,          # Lấy tháng từ date
    dayofmonth,     # Lấy ngày từ date
    first,          # Lấy giá trị đầu tiên (cho Open price)
    last,           # Lấy giá trị cuối cùng (cho Close price)
    max,            # Giá trị lớn nhất (cho High price)
    min,            # Giá trị nhỏ nhất (cho Low price)
    sum,            # Tổng (cho Volume)
    count,          # Đếm số lượng
    avg,            # Trung bình (cho Moving Average)
    when,           # Điều kiện if-else
    lit,            # Tạo cột constant: lit("BTCUSDT")
    expr,           # Viết SQL expression
    sequence,       # Tạo dãy số/ngày
    explode,        # Mở rộng array thành nhiều rows
    lag,            # Lấy giá trị row trước
    coalesce        # Lấy giá trị không null đầu tiên
)

from pyspark.sql.window import Window  # Window functions (cho MA, forward fill)
from pyspark.sql.types import LongType, DoubleType  # Kiểu dữ liệu

# Python standard libraries
import requests      # Gọi HTTP API
import pandas as pd  # Xử lý DataFrame (chuyển đổi)
from datetime import datetime, timedelta  # Xử lý ngày tháng
import time          # Sleep (rate limiting)
import os            # Thao tác file system
```

**Giải thích:**

| Thư viện | Mục đích |
|----------|----------|
| `pyspark` | Xử lý dữ liệu lớn, distributed computing |
| `requests` | Gọi REST API (Binance) |
| `pandas` | Chuyển đổi dữ liệu trung gian |
| `datetime` | Xử lý ngày tháng |

### 3.2. Khởi tạo Spark Session

```python
# ==================================================================
# PHẦN 2: KHỞI TẠO SPARK SESSION
# ==================================================================

spark = SparkSession.builder \
    .appName("Week6_Backfill_BatchLayer") \    # Tên ứng dụng (hiển thị trong Spark UI)
    .config("spark.sql.adaptive.enabled", "true") \  # Bật Adaptive Query Execution
    .getOrCreate()  # Tạo session mới hoặc lấy session có sẵn
```

**Giải thích chi tiết:**

```
SparkSession là gì?
├── Entry point để làm việc với Spark
├── Quản lý context, configuration
├── Cung cấp API để đọc/ghi dữ liệu
└── Cho phép chạy SQL queries

appName:
├── Tên hiển thị trong Spark Web UI (localhost:4040)
└── Giúp identify ứng dụng khi có nhiều jobs

spark.sql.adaptive.enabled:
├── Adaptive Query Execution (AQE)
├── Tự động tối ưu query plan
├── Điều chỉnh số partitions
└── Xử lý data skew
```

### 3.3. STEP 1: Detect Last Date

```python
# ==================================================================
# STEP 1: DETECT LAST DATE IN EXISTING DATA
# ==================================================================
print("\n[STEP 1] Detecting last date in existing data...")

try:
    # Thử đọc từ daily_filled trước (nếu đã chạy backfill trước đó)
    df_existing = spark.read.parquet("data_analysis/daily_filled")
    
    # Lấy ngày lớn nhất trong dữ liệu
    # agg(max("date")): Aggregate function lấy max của cột "date"
    # collect()[0][0]: Lấy giá trị từ DataFrame về Python
    last_date_existing = df_existing.agg(max("date")).collect()[0][0]
    data_source = "daily_filled"
    
except:
    try:
        # Nếu không có daily_filled, đọc từ prophet_input (Week 4)
        df_existing = spark.read.parquet("data_analysis/prophet_input")
        last_date_existing = df_existing.agg(max("ds")).collect()[0][0]
        data_source = "prophet_input"
        
    except:
        # Không có dữ liệu nào → yêu cầu chạy Week 1-5 trước
        print("  ⚠️  No existing data found!")
        print("  Please run Week 1-5 pipeline first:")
        print("    python convert_to_parquet.py")
        print("    python clean_parquet.py")
        print("    python preprocess_step1.py")
        print("    python preprocess_step2.py")
        spark.stop()
        exit(1)

print(f"  ✅ Last date found in {data_source}: {last_date_existing}")
```

**Giải thích logic:**

```
Tại sao thử đọc nhiều nguồn?

1. daily_filled (ưu tiên cao):
   - Đây là output của Batch Layer
   - Nếu đã chạy week6_backfill.py trước đó, data ở đây mới nhất
   
2. prophet_input (backup):
   - Đây là output của Week 4
   - Nếu lần đầu chạy Week 6, chỉ có data này

3. Không có data:
   - Chưa chạy pipeline Week 1-5
   - Cần chạy pipeline trước

Luồng xử lý:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   Try daily_filled ──► Success? ──► Use it                         │
│         │                                                           │
│         │ Fail                                                      │
│         ▼                                                           │
│   Try prophet_input ──► Success? ──► Use it                        │
│         │                                                           │
│         │ Fail                                                      │
│         ▼                                                           │
│   Exit with error message                                          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.4. Tính Gap Days

```python
# Tính số ngày gap
today = datetime.now().date()  # Ngày hôm nay
gap_days = (today - last_date_existing).days  # Số ngày chênh lệch

print(f"  📅 Today: {today}")
print(f"  📊 Gap: {gap_days} days")

# Nếu không có gap, không cần backfill
if gap_days <= 0:
    print("\n✅ Data is already up to date! No backfill needed.")
    print("  You can start streaming for real-time data:")
    print("    cd week6_streaming")
    print("    docker-compose up -d")
    print("    python websocket_producer.py")
    print("    python spark_streaming_consumer.py")
    spark.stop()
    exit(0)

# Ngày bắt đầu fetch = ngày sau ngày cuối
fetch_start_date = last_date_existing + timedelta(days=1)
print(f"\n  🎯 Will backfill: {fetch_start_date} → {today} ({gap_days} days)")
```

**Ví dụ minh họa:**

```
Ví dụ 1: Có gap
  last_date_existing = 2025-11-24
  today             = 2025-12-03
  gap_days          = 9
  fetch_start_date  = 2025-11-25
  → Backfill: 25/11 → 03/12

Ví dụ 2: Không có gap (chạy cùng ngày)
  last_date_existing = 2025-12-03
  today             = 2025-12-03
  gap_days          = 0
  → Không cần backfill, chạy streaming

Ví dụ 3: Data tương lai (?)
  last_date_existing = 2025-12-05
  today             = 2025-12-03
  gap_days          = -2
  → Không cần backfill (trường hợp hiếm)
```

---

## 4. Binance API - Cách hoạt động

### 4.1. Binance Klines API

```python
# ==================================================================
# STEP 2: FETCH DATA FROM BINANCE API
# ==================================================================

def fetch_binance_klines(symbol, interval, start_time, end_time):
    """
    Fetch historical klines (candlestick data) from Binance API.
    
    Parameters:
    -----------
    symbol : str
        Trading pair, e.g., "BTCUSDT", "ETHUSDT"
    interval : str
        Candlestick interval: "1m", "5m", "1h", "1d"
    start_time : int
        Start time in milliseconds
    end_time : int
        End time in milliseconds
    
    Returns:
    --------
    list : List of klines (candlesticks)
    """
    
    url = "https://api.binance.com/api/v3/klines"
    all_klines = []
    current_start = start_time
    
    while current_start < end_time:
        # Tạo parameters cho request
        params = {
            "symbol": symbol,        # VD: "BTCUSDT"
            "interval": interval,    # VD: "1m" (1 phút)
            "startTime": current_start,  # Thời gian bắt đầu (ms)
            "endTime": end_time,         # Thời gian kết thúc (ms)
            "limit": 1000            # Max 1000 records per request
        }
        
        try:
            # Gọi API
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()  # Raise exception nếu lỗi HTTP
            klines = response.json()
            
            if not klines:
                break  # Không còn data
            
            all_klines.extend(klines)  # Thêm vào list kết quả
            
            # Cập nhật start time cho lần gọi tiếp theo
            # close_time của kline cuối + 1ms
            current_start = klines[-1][6] + 1
            
            time.sleep(0.1)  # Rate limiting: tránh bị block
            
        except Exception as e:
            print(f"    Retry after error: {e}")
            time.sleep(5)  # Chờ 5s rồi thử lại
            continue
    
    return all_klines
```

### 4.2. Cấu trúc dữ liệu Klines

```
Binance API trả về mỗi kline là một array:

[
  1499040000000,      // [0]  Open time (timestamp ms)
  "0.01634000",       // [1]  Open price
  "0.80000000",       // [2]  High price
  "0.01575800",       // [3]  Low price
  "0.01577100",       // [4]  Close price
  "148976.11427815",  // [5]  Volume
  1499644799999,      // [6]  Close time (timestamp ms)
  "2434.19055334",    // [7]  Quote asset volume
  308,                // [8]  Number of trades
  "1756.87402397",    // [9]  Taker buy base asset volume
  "28.46694368",      // [10] Taker buy quote asset volume
  "0"                 // [11] Ignore
]

OHLC là gì?
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│    O = Open   : Giá mở cửa (đầu khoảng thời gian)                  │
│    H = High   : Giá cao nhất trong khoảng thời gian                │
│    L = Low    : Giá thấp nhất trong khoảng thời gian               │
│    C = Close  : Giá đóng cửa (cuối khoảng thời gian)               │
│                                                                     │
│    Ví dụ 1 nến 1-minute:                                           │
│                                                                     │
│         │                                                           │
│     ────┼──── High: $93,000                                        │
│         │                                                           │
│      ┌──┴──┐                                                        │
│      │     │  Close: $92,800                                       │
│      │     │                                                        │
│      │     │  Body (thân nến)                                      │
│      │     │                                                        │
│      │     │  Open: $92,500                                        │
│      └──┬──┘                                                        │
│         │                                                           │
│     ────┼──── Low: $92,200                                         │
│         │                                                           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.3. Rate Limiting và Pagination

```python
# Rate Limiting: Tránh bị Binance block
time.sleep(0.1)  # Nghỉ 100ms giữa các request

# Tại sao cần?
# - Binance giới hạn 1200 requests/phút
# - Nếu gọi quá nhanh sẽ bị block IP
# - 0.1s delay = max 600 requests/phút (an toàn)

# Pagination: Lấy nhiều data hơn limit
while current_start < end_time:
    # Mỗi request lấy max 1000 klines
    # Cần loop để lấy hết
    
    # Ví dụ: 9 ngày × 24 giờ × 60 phút = 12,960 klines
    # Cần 13 requests (12,960 / 1000)
    
    current_start = klines[-1][6] + 1  # Tiếp tục từ điểm cuối
```

### 4.4. Xử lý kết quả từ Binance

```python
# Convert timestamp sang datetime
start_ms = int(datetime.combine(fetch_start_date, datetime.min.time()).timestamp() * 1000)
end_ms = int(datetime.combine(today, datetime.max.time()).timestamp() * 1000)

# Giải thích:
# datetime.combine(date, time): Kết hợp date và time
# datetime.min.time() = 00:00:00.000000
# datetime.max.time() = 23:59:59.999999
# .timestamp() → Unix timestamp (seconds)
# × 1000 → milliseconds (Binance yêu cầu)

# Fetch data cho cả BTC và ETH
for symbol in ["BTCUSDT", "ETHUSDT"]:
    print(f"\n  Fetching {symbol}...")
    
    klines = fetch_binance_klines(symbol, "1m", start_ms, end_ms)
    
    if not klines:
        print(f"    ⚠️  No data fetched")
        continue
    
    # Convert sang Pandas DataFrame
    df_klines = pd.DataFrame(klines, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_volume', 'trades', 'taker_buy_base',
        'taker_buy_quote', 'ignore'
    ])
    
    # Convert timestamp to datetime
    df_klines['open_time'] = pd.to_datetime(df_klines['open_time'], unit='ms')
    
    # Convert string prices to float
    for col_name in ['open', 'high', 'low', 'close', 'volume']:
        df_klines[col_name] = df_klines[col_name].astype(float)
    
    # Convert Pandas → Spark DataFrame
    df_spark = spark.createDataFrame(
        df_klines[['open_time', 'open', 'high', 'low', 'close', 'volume']]
    )
    
    # Thêm cột symbol và date
    df_spark = df_spark.withColumn("symbol", lit(symbol))
    df_spark = df_spark.withColumn("date", to_date(col("open_time")))
```

---

## 5. Xử lý dữ liệu với PySpark

### 5.1. STEP 3: Clean Data

```python
# ==================================================================
# STEP 3: CLEAN DATA
# ==================================================================
print("\n[STEP 3] Cleaning data...")

# dropDuplicates: Loại bỏ các dòng trùng lặp
# Dựa trên cột "symbol" và "open_time"
df_new_clean = df_new_raw.dropDuplicates(["symbol", "open_time"])

clean_rows = df_new_clean.count()
print(f"  ✅ After deduplication: {clean_rows:,} rows")
```

**Tại sao cần deduplication?**

```
Có thể có duplicate vì:
1. Binance API trả về overlap data ở biên pagination
2. Network issues gây retry → duplicate requests
3. Data issues từ phía Binance

Ví dụ:
Before dedup:
| symbol   | open_time           | close  |
|----------|---------------------|--------|
| BTCUSDT  | 2025-12-03 00:00:00 | 92000  |
| BTCUSDT  | 2025-12-03 00:00:00 | 92000  |  ← Duplicate!
| BTCUSDT  | 2025-12-03 00:01:00 | 92050  |

After dedup:
| symbol   | open_time           | close  |
|----------|---------------------|--------|
| BTCUSDT  | 2025-12-03 00:00:00 | 92000  |
| BTCUSDT  | 2025-12-03 00:01:00 | 92050  |
```

### 5.2. STEP 4: Daily Aggregation

```python
# ==================================================================
# STEP 4: AGGREGATE TO DAILY OHLC
# ==================================================================
print("\n[STEP 4] Aggregating to daily OHLC...")

df_daily = df_new_clean.groupBy("symbol", "date").agg(
    first("open").alias("open"),     # Giá đầu ngày
    max("high").alias("high"),       # Giá cao nhất trong ngày
    min("low").alias("low"),         # Giá thấp nhất trong ngày
    last("close").alias("close"),    # Giá cuối ngày
    sum("volume").alias("volume")    # Tổng volume trong ngày
)

daily_count = df_daily.count()
print(f"  ✅ Daily aggregation: {daily_count} rows")
```

**Giải thích aggregation:**

```
Input: 1-minute data (1440 rows/ngày)
┌───────────────────────────────────────────────────────────────┐
│ symbol  │ open_time            │ open   │ high   │ low    │ close  │
├─────────┼──────────────────────┼────────┼────────┼────────┼────────┤
│ BTCUSDT │ 2025-12-03 00:00:00  │ 92000  │ 92050  │ 91980  │ 92030  │
│ BTCUSDT │ 2025-12-03 00:01:00  │ 92030  │ 92100  │ 92020  │ 92080  │
│ BTCUSDT │ 2025-12-03 00:02:00  │ 92080  │ 92120  │ 92050  │ 92100  │
│ ...     │ ...                  │ ...    │ ...    │ ...    │ ...    │
│ BTCUSDT │ 2025-12-03 23:59:00  │ 93000  │ 93050  │ 92980  │ 93020  │
└───────────────────────────────────────────────────────────────┘

Output: Daily OHLC (1 row/ngày)
┌───────────────────────────────────────────────────────────────┐
│ symbol  │ date       │ open   │ high   │ low    │ close  │ volume │
├─────────┼────────────┼────────┼────────┼────────┼────────┼────────┤
│ BTCUSDT │ 2025-12-03 │ 92000  │ 93200  │ 91800  │ 93020  │ 50000  │
└───────────────────────────────────────────────────────────────┘

Aggregation logic:
  open  = first("open")  → Giá mở cửa của phút đầu tiên
  high  = max("high")    → Giá cao nhất của tất cả phút
  low   = min("low")     → Giá thấp nhất của tất cả phút
  close = last("close")  → Giá đóng cửa của phút cuối cùng
  volume = sum("volume") → Tổng volume cả ngày
```

---

## 6. Forward Fill - Điền dữ liệu thiếu

### 6.1. Tại sao cần Forward Fill?

```
Vấn đề: Có thể có ngày không có giao dịch

Ví dụ:
| date       | close  |
|------------|--------|
| 2025-12-01 | 92000  |
| 2025-12-02 | NULL   |  ← Không có data (sàn nghỉ?)
| 2025-12-03 | 93000  |

Giải pháp: Forward Fill - Điền giá trị từ ngày trước

| date       | close  |
|------------|--------|
| 2025-12-01 | 92000  |
| 2025-12-02 | 92000  |  ← Filled từ 01/12
| 2025-12-03 | 93000  |
```

### 6.2. Code Forward Fill

```python
# ==================================================================
# STEP 5: FORWARD FILL MISSING DATES
# ==================================================================
print("\n[STEP 5] Forward filling missing dates...")

# Bước 1: Tạo dãy ngày đầy đủ
date_range_df = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{fetch_start_date}'),
        to_date('{today}'),
        interval 1 day
    )) as date
""")

# Giải thích:
# sequence(start, end, step): Tạo array các ngày
# explode: Biến array thành nhiều rows
#
# Ví dụ:
# sequence('2025-12-01', '2025-12-03', 1 day) 
#   → ['2025-12-01', '2025-12-02', '2025-12-03']
# explode → 3 rows

df_filled_list = []

for symbol in ["BTCUSDT", "ETHUSDT"]:
    df_symbol = df_daily.filter(col("symbol") == symbol)
    
    # Bước 2: Cross join với date range
    # Tạo tất cả kết hợp (symbol, date)
    df_complete = date_range_df.crossJoin(
        df_symbol.select("symbol").distinct()
    )
    
    # Bước 3: Left join với data thực
    # Những ngày có data → có giá trị
    # Những ngày không có data → NULL
    df_with_gaps = df_complete.join(df_symbol, ["symbol", "date"], "left")
    
    # Bước 4: Forward fill với Window function
    window_spec = Window.partitionBy("symbol") \
                        .orderBy("date") \
                        .rowsBetween(Window.unboundedPreceding, 0)
    
    # F.last(col, ignorenulls=True): Lấy giá trị không null gần nhất
    for col_name in ["open", "high", "low", "close", "volume"]:
        df_with_gaps = df_with_gaps.withColumn(
            col_name,
            F.last(col(col_name), ignorenulls=True).over(window_spec)
        )
    
    df_filled_list.append(df_with_gaps)
```

### 6.3. Giải thích Window Function

```
Window.partitionBy("symbol").orderBy("date").rowsBetween(unboundedPreceding, 0)

Giải thích từng phần:

1. partitionBy("symbol"):
   - Chia data thành groups theo symbol
   - BTC xử lý riêng, ETH xử lý riêng

2. orderBy("date"):
   - Sắp xếp theo ngày tăng dần

3. rowsBetween(unboundedPreceding, 0):
   - unboundedPreceding = từ row đầu tiên
   - 0 = đến row hiện tại
   - Tức là xem xét tất cả rows từ đầu đến hiện tại

4. F.last(col, ignorenulls=True):
   - Lấy giá trị cuối cùng trong window
   - ignorenulls=True: bỏ qua các NULL
   - → Lấy giá trị gần nhất không phải NULL

Ví dụ minh họa:
┌────────┬────────────┬────────┬──────────────────────────────────────┐
│ symbol │ date       │ close  │ Window (unboundedPreceding, 0)       │
├────────┼────────────┼────────┼──────────────────────────────────────┤
│ BTC    │ 2025-12-01 │ 92000  │ [92000]          → last = 92000     │
│ BTC    │ 2025-12-02 │ NULL   │ [92000, NULL]    → last = 92000     │
│ BTC    │ 2025-12-03 │ 93000  │ [92000, NULL, 93000] → last = 93000 │
└────────┴────────────┴────────┴──────────────────────────────────────┘

Kết quả sau forward fill:
│ BTC    │ 2025-12-01 │ 92000  │
│ BTC    │ 2025-12-02 │ 92000  │  ← Filled!
│ BTC    │ 2025-12-03 │ 93000  │
```

---

## 7. Moving Average (MA7, MA30)

### 7.1. Moving Average là gì?

```
Moving Average (Trung bình động):
- Trung bình của N ngày gần nhất
- Giúp làm mượt (smooth) đường giá
- Loại bỏ noise, thấy trend rõ hơn

MA7: Trung bình 7 ngày gần nhất
MA30: Trung bình 30 ngày gần nhất

Ví dụ MA7:
| date       | close  | MA7                          |
|------------|--------|------------------------------|
| 2025-11-27 | 91000  | (91000)/1 = 91000           |
| 2025-11-28 | 91500  | (91000+91500)/2 = 91250     |
| 2025-11-29 | 92000  | (91000+91500+92000)/3       |
| 2025-11-30 | 91800  | ...                          |
| 2025-12-01 | 92500  | ...                          |
| 2025-12-02 | 93000  | ...                          |
| 2025-12-03 | 92800  | (91000+...+92800)/7 = 92086 |
```

### 7.2. Code tính MA

```python
# ==================================================================
# STEP 6: COMPUTE MA7 AND MA30
# ==================================================================
print("\n[STEP 6] Computing MA7 and MA30...")

# Window cho MA7: 6 rows trước + row hiện tại = 7 rows
window_ma7 = Window.partitionBy("symbol") \
                   .orderBy("date") \
                   .rowsBetween(-6, 0)

# Window cho MA30: 29 rows trước + row hiện tại = 30 rows
window_ma30 = Window.partitionBy("symbol") \
                    .orderBy("date") \
                    .rowsBetween(-29, 0)

# Tính trung bình
df_filled = df_filled.withColumn("MA7", avg("close").over(window_ma7))
df_filled = df_filled.withColumn("MA30", avg("close").over(window_ma30))

print(f"  ✅ MA7 and MA30 computed")
```

### 7.3. Giải thích Window cho MA

```
rowsBetween(-6, 0):
  -6 = 6 rows trước row hiện tại
   0 = row hiện tại
  → Tổng cộng 7 rows (MA7)

Ví dụ MA7:
┌────────┬────────────┬────────┬─────────────────────────────────────┐
│ symbol │ date       │ close  │ Window (-6, 0)                      │
├────────┼────────────┼────────┼─────────────────────────────────────┤
│ BTC    │ 2025-11-27 │ 91000  │ [91000]                → avg=91000 │
│ BTC    │ 2025-11-28 │ 91500  │ [91000,91500]          → avg=91250 │
│ BTC    │ 2025-11-29 │ 92000  │ [91000,91500,92000]    → avg=91500 │
│ BTC    │ 2025-11-30 │ 91800  │ [91000,91500,92000,91800]          │
│ BTC    │ 2025-12-01 │ 92500  │ [91000,91500,92000,91800,92500]    │
│ BTC    │ 2025-12-02 │ 93000  │ [91000,91500,92000,91800,92500,    │
│        │            │        │  93000]                             │
│ BTC    │ 2025-12-03 │ 92800  │ [91000,91500,92000,91800,92500,    │
│        │            │        │  93000,92800] → avg=92086          │
│ BTC    │ 2025-12-04 │ 93200  │ [91500,92000,91800,92500,93000,    │
│        │            │        │  92800,93200] → Slide window!      │
└────────┴────────────┴────────┴─────────────────────────────────────┘

Lưu ý: Những ngày đầu có ít hơn 7 rows, MA7 tính trên số rows có sẵn.
```

---

## 8. Output và cấu trúc dữ liệu

### 8.1. STEP 7: Save to Parquet

```python
# ==================================================================
# STEP 7: SAVE TO daily_filled
# ==================================================================
print("\n[STEP 7] Saving backfill data...")

# Thêm cột year để partition
df_filled = df_filled.withColumn("year", year("date"))

# Lưu với partitioning
output_path = "data_analysis/daily_filled"
df_filled.write \
    .mode("overwrite") \        # Ghi đè nếu đã tồn tại
    .partitionBy("symbol", "year") \  # Partition theo symbol và năm
    .parquet(output_path)       # Format Parquet

print(f"  ✅ Saved to {output_path}")
```

### 8.2. Cấu trúc thư mục output

```
data_analysis/
└── daily_filled/
    ├── _SUCCESS                    # Marker file (ghi thành công)
    ├── symbol=BTCUSDT/
    │   ├── year=2025/
    │   │   ├── part-00000-xxx.snappy.parquet
    │   │   ├── part-00001-xxx.snappy.parquet
    │   │   └── ...
    │   └── year=2024/
    │       └── ...
    └── symbol=ETHUSDT/
        └── year=2025/
            └── ...

Tại sao partition?
1. Query nhanh hơn:
   - Query BTC only → chỉ đọc folder BTCUSDT
   - Query 2025 only → chỉ đọc folder year=2025

2. Parallel processing:
   - Mỗi partition xử lý trên 1 executor

3. Dễ quản lý:
   - Xóa data năm cũ dễ dàng
   - Biết ngay có data symbol nào
```

### 8.3. STEP 8: Update Prophet Input

```python
# ==================================================================
# STEP 8: UPDATE prophet_input
# ==================================================================
print("\n[STEP 8] Updating prophet_input...")

# Prophet yêu cầu format: ds (date), y (value), symbol
df_prophet = df_filled.select(
    col("date").alias("ds"),    # Đổi tên date → ds (Prophet convention)
    col("close").alias("y"),    # Đổi tên close → y (target variable)
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

### 8.4. Schema của output

```
daily_filled schema:
root
 |-- date: date
 |-- symbol: string
 |-- open: double
 |-- high: double
 |-- low: double
 |-- close: double
 |-- volume: double
 |-- MA7: double
 |-- MA30: double
 |-- year: integer (partition column)

prophet_input schema:
root
 |-- ds: date
 |-- y: double
 |-- symbol: string
 |-- MA7: double
 |-- MA30: double
```

---

## 9. Câu hỏi thường gặp

### Q1: Tại sao dùng Binance API thay vì WebSocket?

```
A: Vì mục đích khác nhau:

Binance REST API (dùng trong Batch Layer):
├── Lấy dữ liệu LỊCH SỬ
├── Có thể lấy data từ quá khứ
├── Synchronous (gọi - đợi - nhận)
└── Phù hợp cho backfill

Binance WebSocket (dùng trong Speed Layer):
├── Lấy dữ liệu REAL-TIME
├── Chỉ lấy data từ thời điểm connect
├── Asynchronous (stream liên tục)
└── Phù hợp cho streaming

Batch Layer cần lấy data QUÁ KHỨ (những ngày máy tắt)
→ Phải dùng REST API
```

### Q2: Limit 1000 records có đủ không?

```
A: Đủ, vì ta loop cho đến khi hết:

Ví dụ: 9 ngày × 1440 phút = 12,960 records

Loop 1: Lấy record 1-1000
Loop 2: Lấy record 1001-2000
...
Loop 13: Lấy record 12001-12960

Mỗi loop cập nhật current_start:
  current_start = klines[-1][6] + 1
  (close_time của record cuối + 1ms)
```

### Q3: Rate limiting 0.1s có đủ an toàn?

```
A: Có, thậm chí có thể nhanh hơn:

Binance limit:
├── 1200 requests/phút cho IP
├── 10 requests/giây

Với 0.1s delay:
├── Max 10 requests/giây
├── = 600 requests/phút
└── Chỉ dùng 50% quota → An toàn

Nếu cần nhanh hơn, có thể giảm xuống 0.05s
Nhưng 0.1s đủ ổn định và tránh bị block
```

### Q4: Forward fill có chính xác không?

```
A: Forward fill là BEST PRACTICE cho time series:

Tại sao không dùng:
├── Backward fill: Dùng data tương lai → sai logic
├── Interpolation: Giả định linear → không phù hợp với giá
├── Mean fill: Làm mất pattern
└── Zero fill: Sai hoàn toàn

Forward fill:
├── Giữ nguyên giá cuối cùng biết được
├── Assumption: "Không có thay đổi = giá giữ nguyên"
├── Phổ biến trong finance
└── Prophet và các model time series đều chấp nhận
```

### Q5: Tại sao tính MA trong Batch Layer?

```
A: Để consistency và efficiency:

1. Consistency:
   - MA tính trên TOÀN BỘ timeline
   - Nếu tính trong Prophet, chỉ tính trên training data
   
2. Efficiency:
   - Tính 1 lần, dùng nhiều lần
   - Không cần tính lại mỗi khi train

3. Feature engineering:
   - MA7, MA30 là features quan trọng
   - Có thể thêm nhiều features khác (RSI, MACD...)
```

---

## 📚 Tài liệu tiếp theo

Sau khi hiểu Batch Layer, tiếp tục với:

**WEEK6_03_SPEED_LAYER.md** - Giải thích Kafka + Spark Streaming

---

*Tạo bởi: Big Data Project - Week 6 Documentation*
*Cập nhật: 03/12/2025*
