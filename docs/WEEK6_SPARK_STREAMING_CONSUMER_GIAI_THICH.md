# Giải thích chi tiết: spark_streaming_consumer.py

**File:** `week6_streaming/spark_streaming_consumer.py`  
**Chức năng:** Speed Layer Consumer - Spark Structured Streaming real-time processing  
**Tác giả:** Đoàn Thế Tín  
**Ngày:** Week 6 - Lambda Architecture

---

## 📋 Mục lục
1. [Configuration và Spark Session](#1-configuration-và-spark-session)
2. [Schema Definition](#2-schema-definition)
3. [Read Stream from Kafka](#3-read-stream-from-kafka)
4. [Parse JSON Data](#4-parse-json-data)
5. [Data Transformation](#5-data-transformation)
6. [Watermarking](#6-watermarking)
7. [Window Aggregation](#7-window-aggregation)
8. [Write Streams](#8-write-streams)
9. [Monitoring và Termination](#9-monitoring-và-termination)
10. [Tóm tắt](#tóm-tắt-tổng-quan)

---

## 1. Configuration và Spark Session

### Dòng 1-11: Docstring
```python
"""
spark_streaming_consumer.py - Spark Structured Streaming Consumer

ĐÂY LÀ STRUCTURED STREAMING THẬT SỰ:
- Đọc continuous stream từ Kafka
- Micro-batch processing (trigger 10s)
- Watermarking (xử lý late data)
- Window aggregation (1 day)
- Stateful operations
- Checkpoint (fault tolerance)
"""
```
**Giải thích:**
- **Structured Streaming:** Spark API cho real-time processing
- **Micro-batch:** Chia stream thành batches nhỏ (10s/batch)
- **Watermarking:** Xử lý data đến muộn (late arrival)
- **Window aggregation:** Tính toán theo cửa sổ thời gian (1 ngày)
- **Stateful:** Lưu trạng thái giữa các batches
- **Checkpoint:** Lưu progress để recovery khi fail

---

### Dòng 12-15: Import
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
```
**Giải thích:**
- `SparkSession`: Entry point của Spark
- `functions`: Tất cả functions (col, max, min, sum, count, ...)
- `types`: Data types (StringType, LongType, DoubleType, ...)
- `os`: Xử lý đường dẫn file

---

### Dòng 17-23: Configuration
```python
# ============================================================================
# CONFIGURATION
# ============================================================================
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto-prices"
OUTPUT_PATH = "streaming_output_spark"
CHECKPOINT_PATH = "checkpoint_spark"
```
**Giải thích:**

| Variable | Value | Ý nghĩa |
|----------|-------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `crypto-prices` | Topic để subscribe |
| `OUTPUT_PATH` | `streaming_output_spark` | Thư mục lưu output Parquet |
| `CHECKPOINT_PATH` | `checkpoint_spark` | Thư mục checkpoint (fault tolerance) |

**Checkpoint là gì?**
- Lưu **offset Kafka** đã process
- Lưu **state của aggregations**
- Khi restart → Continue từ checkpoint (không duplicate)

---

### Dòng 25-32: Banner
```python
# ============================================================================
# SPARK SESSION
# ============================================================================
print("=" * 80)
print("SPARK STRUCTURED STREAMING - Crypto Price Analysis")
print("=" * 80)
```
**Giải thích:** In header cho console.

---

### Dòng 34-39: Spark Session Configuration
```python
spark = SparkSession.builder \
    .appName("CryptoPriceStructuredStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```
**Giải thích từng config:**

#### `.appName("CryptoPriceStructuredStreaming")`
- Tên application (hiển thị trong Spark UI)

#### `.config("spark.jars.packages", "...")`
- **Download Kafka connector dependency**
- `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`
  - `spark-sql-kafka-0-10`: Connector cho Kafka
  - `_2.12`: Scala version 2.12
  - `3.5.3`: Spark version 3.5.3
- Tự động download từ Maven Central khi start

#### `.config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)`
- Đặt checkpoint location mặc định
- Nếu không set → Mỗi query phải specify riêng

#### `.config("spark.sql.adaptive.enabled", "true")`
- Bật **Adaptive Query Execution (AQE)**
- Tự động tối ưu query plan runtime

#### `.config("spark.driver.memory", "4g")`
- Cấp phát 4GB RAM cho driver
- **Tại sao cần 4GB?**
  - Spark Streaming giữ state trong memory
  - Aggregations cần memory
  - Default 1GB thường không đủ

---

### Dòng 41-44: Log Level & Info
```python
spark.sparkContext.setLogLevel("WARN")

print(f"\n✓ Spark {spark.version} initialized")
print(f"✓ Checkpoint location: {CHECKPOINT_PATH}")
print(f"✓ Output location: {OUTPUT_PATH}\n")
```
**Giải thích:**
- `setLogLevel("WARN")`: Chỉ log WARNING và ERROR
  - Ẩn INFO logs (quá nhiều)
- Print thông tin initialization

---

## 2. Schema Definition

### Dòng 46-62: Message Schema
```python
# ============================================================================
# SCHEMA DEFINITION
# ============================================================================
# Schema cho JSON message từ Kafka
message_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("event_time", LongType(), True),
    StructField("price", DoubleType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("quote_volume", DoubleType(), True),
    StructField("number_trades", IntegerType(), True),
    StructField("price_change", DoubleType(), True),
    StructField("price_change_percent", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])
```
**Giải thích:**

#### StructType & StructField
- `StructType`: Container cho schema
- `StructField(name, dataType, nullable)`:
  - `name`: Tên field
  - `dataType`: Kiểu dữ liệu
  - `nullable`: True = Cho phép NULL

#### Tại sao cần schema?
- **from_json()** cần schema để parse JSON
- Không có schema → Không parse được
- **Ví dụ:**
  ```python
  # Kafka message (bytes):
  b'{"symbol":"BTCUSDT","price":42000.0,...}'
  
  # Parse với schema:
  Row(symbol='BTCUSDT', price=42000.0, ...)
  ```

#### Schema Fields
| Field | Type | Nullable | Source |
|-------|------|----------|--------|
| `symbol` | String | True | Producer |
| `event_time` | Long | True | Binance API (ms) |
| `price` | Double | True | Current price |
| `open` | Double | True | 24h open |
| `high` | Double | True | 24h high |
| `low` | Double | True | 24h low |
| `volume` | Double | True | 24h volume (BTC/ETH) |
| `quote_volume` | Double | True | 24h volume (USDT) |
| `number_trades` | Int | True | 24h trades count |
| `price_change` | Double | True | 24h change ($) |
| `price_change_percent` | Double | True | 24h change (%) |
| `timestamp` | String | True | Producer timestamp (ISO) |

---

## 3. Read Stream from Kafka

### Dòng 64-67: Step 1 Header
```python
# ============================================================================
# STEP 1: READ STREAM FROM KAFKA
# ============================================================================
print("STEP 1: Reading stream from Kafka...")
```
**Giải thích:** Bắt đầu Step 1.

---

### Dòng 69-78: Kafka Stream Reader
```python
kafkaDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .option("kafka.session.timeout.ms", "30000") \
    .option("kafka.request.timeout.ms", "40000") \
    .load()
```
**Giải thích từng option:**

#### `.format("kafka")`
- Dùng Kafka source connector

#### `.option("kafka.bootstrap.servers", ...)`
- Kafka broker address: `localhost:9092`

#### `.option("subscribe", KAFKA_TOPIC)`
- Subscribe topic `crypto-prices`

#### `.option("startingOffsets", "earliest")`
- Đọc từ đầu topic (offset đầu tiên)
- **Alternatives:**
  - `"latest"`: Chỉ đọc messages mới
  - `{"crypto-prices": {"0": 100}}`: Từ offset cụ thể

#### `.option("failOnDataLoss", "false")`
- **Không fail** nếu data bị mất (Kafka retention)
- `true` → Exception nếu offset không tồn tại
- **Use case:** Kafka có retention 7 ngày, checkpoint > 7 ngày → Data loss

#### `.option("maxOffsetsPerTrigger", 1000)`
- **Rate limiting:** Tối đa 1000 messages/trigger
- Tránh overload nếu có backlog lớn
- Ví dụ: Có 10,000 messages → Process 10 batches (1000/batch)

#### `.option("kafka.session.timeout.ms", "30000")`
- **Session timeout:** 30 giây
- Kafka broker sẽ đợi 30s trước khi coi consumer chết
- Default 10s (quá ngắn cho slow network)

#### `.option("kafka.request.timeout.ms", "40000")`
- **Request timeout:** 40 giây
- Timeout cho mỗi Kafka request
- Phải > session.timeout.ms

---

### Dòng 80-81: Info
```python
print(f"✓ Connected to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"✓ Subscribed to topic: {KAFKA_TOPIC}\n")
```
**Giải thích:** Print success message.

---

### Kafka DataFrame Schema
```
kafkaDF schema:
├── key: binary (null)
├── value: binary (JSON message)
├── topic: string (crypto-prices)
├── partition: int (0)
├── offset: long (0, 1, 2, ...)
├── timestamp: timestamp (Kafka timestamp)
└── timestampType: int (0=CreateTime, 1=LogAppendTime)
```

---

## 4. Parse JSON Data

### Dòng 83-86: Step 2 Header
```python
# ============================================================================
# STEP 2: PARSE JSON DATA
# ============================================================================
print("STEP 2: Parsing JSON messages...")
```
**Giải thích:** Bắt đầu Step 2 - Parse JSON.

---

### Dòng 88-92: JSON Parsing
```python
parsedDF = kafkaDF.select(
    from_json(col("value").cast("string"), message_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")
```
**Giải thích từng bước:**

#### `.select(from_json(...), col("timestamp"))`
- Select 2 cột:
  1. Parsed JSON (alias "data")
  2. Kafka timestamp (alias "kafka_timestamp")

#### `from_json(col("value").cast("string"), message_schema)`
- **Step 1:** `col("value")` → Lấy column value (binary)
- **Step 2:** `.cast("string")` → Chuyển binary → string
  - `b'{"symbol":"BTCUSDT",...}'` → `'{"symbol":"BTCUSDT",...}'`
- **Step 3:** `from_json(..., message_schema)` → Parse JSON
  - String → StructType (nested columns)
- **Step 4:** `.alias("data")` → Đặt tên column "data"

#### `.select("data.*", "kafka_timestamp")`
- `data.*`: Unpack nested struct
  - `data.symbol`, `data.price`, ... → `symbol`, `price`, ...
- `kafka_timestamp`: Giữ Kafka timestamp

**Ví dụ transformation:**
```
Before:
├── value: b'{"symbol":"BTCUSDT","price":42000.0}'
└── timestamp: 2025-12-16 10:30:45

After from_json():
├── data: {symbol: "BTCUSDT", price: 42000.0, ...}
└── kafka_timestamp: 2025-12-16 10:30:45

After select("data.*"):
├── symbol: "BTCUSDT"
├── price: 42000.0
├── ...
└── kafka_timestamp: 2025-12-16 10:30:45
```

---

### Dòng 94: Info
```python
print("✓ JSON parsed successfully\n")
```
**Giải thích:** Print success.

---

## 5. Data Transformation

### Dòng 96-99: Step 3 Header
```python
# ============================================================================
# STEP 3: DATA TRANSFORMATION
# ============================================================================
print("STEP 3: Transforming data...")
```

---

### Dòng 101-105: Timestamp Conversion
```python
streamDF = parsedDF \
    .withColumn("event_timestamp", (col("event_time") / 1000).cast("timestamp")) \
    .withColumn("date", to_date(col("event_timestamp"))) \
    .withColumn("hour", hour(col("event_timestamp")))
```
**Giải thích từng transformation:**

#### `.withColumn("event_timestamp", ...)`
- **Input:** `event_time` = 1734134400000 (milliseconds)
- **Transform:**
  1. `col("event_time") / 1000` → 1734134400.0 (seconds)
  2. `.cast("timestamp")` → 2025-12-16 10:30:00 (datetime)
- **Output:** Cột `event_timestamp` kiểu timestamp

#### `.withColumn("date", to_date(col("event_timestamp")))`
- **Input:** `event_timestamp` = 2025-12-16 10:30:00
- **Transform:** `to_date()` → Extract date
- **Output:** `date` = 2025-12-16 (date only)

#### `.withColumn("hour", hour(col("event_timestamp")))`
- **Input:** `event_timestamp` = 2025-12-16 10:30:00
- **Transform:** `hour()` → Extract hour
- **Output:** `hour` = 10 (integer)

**Tại sao cần 3 cột timestamp?**
- `event_time`: Original (ms) - Lưu trữ
- `event_timestamp`: Datetime - **Watermarking** và **Window aggregation**
- `date`: Date only - Partitioning, grouping
- `hour`: Hour - Hourly aggregations (nếu cần)

---

### Dòng 107-108: Info
```python
print("✓ Timestamps converted")
print("✓ Date and hour extracted\n")
```

---

## 6. Watermarking

### Dòng 110-113: Step 4 Header
```python
# ============================================================================
# STEP 4: WATERMARKING (Handle Late Data)
# ============================================================================
print("STEP 4: Applying watermark...")
```

---

### Dòng 115: Watermark
```python
watermarkedDF = streamDF.withWatermark("event_timestamp", "1 hour")
```
**Giải thích Watermarking:**

#### Watermark là gì?
- **Định nghĩa:** Threshold để drop late data
- **Công thức:** `watermark = max_event_time - threshold`
- **Ví dụ:**
  ```
  Max event_time seen: 10:30:00
  Threshold: 1 hour
  → Watermark: 09:30:00
  → Drop data có event_time < 09:30:00
  ```

#### Tại sao cần Watermarking?
- **Vấn đề:** Data có thể đến muộn (late arrival)
  - Network delay
  - Producer restart
  - Kafka retention
- **Không có watermark:**
  - Phải giữ state vô hạn (memory leak)
  - Window không bao giờ đóng
- **Có watermark:**
  - Sau 1 giờ → Đóng window, emit result
  - Drop data đến muộn > 1 giờ

#### Watermark Flow
```
Time 10:00 - Message arrives (event_time: 10:00)
  → max_event_time = 10:00
  → watermark = 10:00 - 1h = 09:00
  → Accept data >= 09:00

Time 10:30 - Message arrives (event_time: 10:30)
  → max_event_time = 10:30
  → watermark = 10:30 - 1h = 09:30
  → Accept data >= 09:30
  → Drop data < 09:30

Time 11:00 - Late message (event_time: 08:50)
  → watermark = 10:00
  → 08:50 < 10:00 → DROPPED
```

---

### Dòng 117: Info
```python
print("✓ Watermark: 1 hour (late data tolerance)\n")
```

---

## 7. Window Aggregation

### Dòng 119-122: Step 5 Header
```python
# ============================================================================
# STEP 5: WINDOW AGGREGATION - DAILY
# ============================================================================
print("STEP 5: Daily aggregation...")
```

---

### Dòng 124-127: Group By Window
```python
dailyDF = watermarkedDF \
    .groupBy(
        window(col("event_timestamp"), "1 day"),
        col("symbol")
    ) \
```
**Giải thích:**

#### `.groupBy(window(...), col("symbol"))`
- **Group by 2 keys:**
  1. `window(col("event_timestamp"), "1 day")`: Tumbling window 1 ngày
  2. `col("symbol")`: Symbol (BTCUSDT/ETHUSDT)

#### `window(col("event_timestamp"), "1 day")`
- **Tumbling Window:** Không overlap
- **Size:** 1 ngày (24 giờ)
- **Ví dụ windows:**
  ```
  Window 1: 2025-12-16 00:00:00 → 2025-12-17 00:00:00
  Window 2: 2025-12-17 00:00:00 → 2025-12-18 00:00:00
  Window 3: 2025-12-18 00:00:00 → 2025-12-19 00:00:00
  ```

#### Window Types
| Type | Description | Example |
|------|-------------|---------|
| **Tumbling** | Không overlap | [0-1h], [1-2h], [2-3h] |
| **Sliding** | Có overlap | [0-1h], [0.5-1.5h], [1-2h] |
| **Session** | Dynamic size | Dựa vào gaps |

- **File này dùng:** Tumbling window

---

### Dòng 128-138: Aggregations
```python
    .agg(
        first("open").alias("daily_open"),
        max("high").alias("daily_high"),
        min("low").alias("daily_low"),
        last("price").alias("daily_close"),  # Use 'price' (lastPrice from Binance)
        sum("volume").alias("daily_volume"),
        sum("quote_volume").alias("daily_quote_volume"),
        sum("number_trades").alias("total_trades"),
        count("*").alias("tick_count"),
        avg("price").alias("avg_price")
    ) \
```
**Giải thích từng aggregation:**

| Function | Input | Output | Ý nghĩa |
|----------|-------|--------|---------|
| `first("open")` | `open` | `daily_open` | Giá open **đầu tiên** trong window |
| `max("high")` | `high` | `daily_high` | Giá **cao nhất** trong window |
| `min("low")` | `low` | `daily_low` | Giá **thấp nhất** trong window |
| `last("price")` | `price` | `daily_close` | Giá **cuối cùng** trong window |
| `sum("volume")` | `volume` | `daily_volume` | **Tổng** volume BTC/ETH |
| `sum("quote_volume")` | `quote_volume` | `daily_quote_volume` | **Tổng** volume USDT |
| `sum("number_trades")` | `number_trades` | `total_trades` | **Tổng** số trades |
| `count("*")` | All rows | `tick_count` | **Số messages** trong window |
| `avg("price")` | `price` | `avg_price` | **Giá trung bình** |

**Chú ý:**
- `first("open")`: Lấy open của **message đầu tiên** (theo event_timestamp)
- `last("price")`: Lấy price của **message cuối cùng** (theo event_timestamp)
- **Tại sao dùng `price` thay vì `close`?**
  - `close` là close của 24h window (Binance API)
  - `price` là current price (lastPrice)
  - Muốn close của daily window → Dùng `price` cuối cùng

---

### Dòng 139-151: Select Columns
```python
    .select(
        col("window.start").alias("date"),
        col("symbol"),
        col("daily_open"),
        col("daily_high"),
        col("daily_low"),
        col("daily_close"),
        col("daily_volume"),
        col("daily_quote_volume"),
        col("total_trades"),
        col("tick_count"),
        col("avg_price")
    )
```
**Giải thích:**

#### `col("window.start").alias("date")`
- `window` là StructType với 2 fields:
  - `window.start`: Timestamp bắt đầu window
  - `window.end`: Timestamp kết thúc window
- Lấy `start` và đặt alias `date`
- **Ví dụ:**
  ```
  window.start: 2025-12-16 00:00:00
  window.end: 2025-12-17 00:00:00
  → date: 2025-12-16 00:00:00
  ```

#### Select 11 cột
- `date`: Window start (timestamp)
- `symbol`: BTCUSDT/ETHUSDT
- `daily_open/high/low/close`: OHLC
- `daily_volume/quote_volume`: Volume
- `total_trades`: Số trades
- `tick_count`: Số messages
- `avg_price`: Giá trung bình

---

### Dòng 153-154: Info
```python
print("✓ Window: 1 day")
print("✓ Aggregations: OHLC, Volume, Trades\n")
```

---

## 8. Write Streams

### Dòng 156-159: Step 6 Header
```python
# ============================================================================
# STEP 6: WRITE STREAMS
# ============================================================================
print("STEP 6: Starting streaming queries...\n")
```

---

### Dòng 161-170: Query 1 - Daily Parquet
```python
# Query 1: Daily data to Parquet
daily_query = dailyDF.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"{OUTPUT_PATH}/daily") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/daily") \
    .partitionBy("symbol") \
    .trigger(processingTime="10 seconds") \
    .start()
```
**Giải thích từng option:**

#### `.outputMode("append")`
- **Append mode:** Chỉ ghi rows mới
- **Alternatives:**
  - `"complete"`: Ghi lại toàn bộ result table (cho aggregations)
  - `"update"`: Chỉ ghi rows changed
- **Tại sao append?**
  - Window đóng → Emit 1 lần → Không update
  - Append mode hiệu quả nhất

#### `.format("parquet")`
- Ghi output dạng Parquet file

#### `.option("path", f"{OUTPUT_PATH}/daily")`
- Đường dẫn: `streaming_output_spark/daily/`

#### `.option("checkpointLocation", f"{CHECKPOINT_PATH}/daily")`
- Checkpoint path: `checkpoint_spark/daily/`
- Lưu offset Kafka, state aggregations

#### `.partitionBy("symbol")`
- Partition theo symbol
- Folder structure:
  ```
  streaming_output_spark/daily/
  ├── symbol=BTCUSDT/
  │   ├── part-00000-xxx.parquet
  │   └── part-00001-xxx.parquet
  └── symbol=ETHUSDT/
      ├── part-00000-xxx.parquet
      └── part-00001-xxx.parquet
  ```

#### `.trigger(processingTime="10 seconds")`
- **Micro-batch trigger:** Mỗi 10 giây
- Process messages accumulated trong 10s
- **Alternatives:**
  - `trigger(once=True)`: Chạy 1 lần rồi dừng
  - `trigger(continuous="1 second")`: Continuous mode (experimental)

#### `.start()`
- Start streaming query (async)
- Return `StreamingQuery` object

---

### Dòng 172-175: Info
```python
print("✓ Query 1: Daily aggregates → Parquet")
print(f"  Output: {OUTPUT_PATH}/daily")
print(f"  Trigger: 10 seconds")
```

---

### Dòng 177-189: Query 2 - Console Monitoring
```python
# Query 2: Raw stream to Console (monitoring)
console_query = streamDF \
    .select(
        col("symbol"),
        col("price"),
        col("volume"),
        col("price_change_percent"),
        col("event_timestamp")
    ) \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", "10") \
    .trigger(processingTime="30 seconds") \
    .start()
```
**Giải thích:**

#### Select 5 cột
- `symbol`, `price`, `volume`, `price_change_percent`, `event_timestamp`
- Chỉ select columns quan trọng (tránh spam console)

#### `.format("console")`
- Ghi output ra console (terminal)

#### `.option("truncate", "false")`
- Không cắt text (hiển thị full)
- Default `true`: Cắt strings > 20 chars

#### `.option("numRows", "10")`
- Chỉ show 10 rows/batch
- Tránh spam console

#### `.trigger(processingTime="30 seconds")`
- Mỗi 30 giây (ít hơn daily query)
- Tránh log quá nhiều

**Output ví dụ:**
```
-------------------------------------------
Batch: 1
-------------------------------------------
+--------+--------+----------+-------------------+-------------------+
|symbol  |price   |volume    |price_change_percent|event_timestamp    |
+--------+--------+----------+-------------------+-------------------+
|BTCUSDT |42000.50|12345.67  |-2.38              |2025-12-16 10:30:00|
|ETHUSDT |3200.00 |45678.90  |1.25               |2025-12-16 10:30:01|
+--------+--------+----------+-------------------+-------------------+
```

---

### Dòng 191-192: Info
```python
print("✓ Query 3: Raw data → Console (monitoring)")
print(f"  Trigger: 30 seconds")
```

---

### Dòng 194-200: Query 3 - Memory Table
```python
# Query 4: Real-time stats to Memory (for queries)
stats_query = dailyDF.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("crypto_daily_stats") \
    .trigger(processingTime="10 seconds") \
    .start()
```
**Giải thích:**

#### `.outputMode("complete")`
- **Complete mode:** Ghi lại **toàn bộ** result table
- Cần cho in-memory table (query được full data)

#### `.format("memory")`
- Ghi vào **in-memory table** (không ra file)
- Store trong Spark catalog

#### `.queryName("crypto_daily_stats")`
- Tên table: `crypto_daily_stats`
- Query được bằng SQL:
  ```python
  spark.sql("SELECT * FROM crypto_daily_stats").show()
  ```

#### Use case
- **Real-time monitoring:** Query data đang stream
- **Dashboard:** Power BI, Tableau connect vào Spark
- **Ad-hoc queries:** Debug, explore data

**Ví dụ query:**
```python
# Terminal 2 (while streaming đang chạy):
spark.sql("""
  SELECT symbol, date, daily_close, daily_volume
  FROM crypto_daily_stats
  ORDER BY date DESC
  LIMIT 5
""").show()
```

---

### Dòng 202-203: Info
```python
print("✓ Query 4: Daily stats → Memory table")
print(f"  Table name: crypto_daily_stats")
```

---

## 9. Monitoring và Termination

### Dòng 205-218: Monitoring Info
```python
# ============================================================================
# MONITORING
# ============================================================================
print("\n" + "=" * 80)
print("STREAMING QUERIES ACTIVE")
print("=" * 80)
print("\nActive queries:")
for query in spark.streams.active:
    print(f"  - {query.name if query.name else query.id}")

print("\n📊 Monitor status:")
print("  - Console output will show every 30 seconds")
print("  - Parquet files updated every 10 seconds")
print("  - Check checkpoint/ for progress")
print("  - Check streaming_output_spark/ for results")
```
**Giải thích:**

#### `spark.streams.active`
- List tất cả streaming queries đang chạy
- Mỗi query có:
  - `id`: UUID duy nhất
  - `name`: Tên (nếu set bằng `.queryName()`)

#### Monitoring Tips
- **Console:** Show raw data mỗi 30s
- **Parquet:** Write mỗi 10s
- **Checkpoint:** Lưu progress (offsets, state)
- **Output:** Kết quả cuối cùng

---

### Dòng 220-221: Query Instructions
```python
print("\n💡 To query in-memory stats, open another terminal:")
print("  spark.sql('SELECT * FROM crypto_daily_stats').show()")
```

---

### Dòng 223-225: Stop Instructions
```python
print("\nPress Ctrl+C to stop all queries\n")
print("=" * 80)
```

---

### Dòng 227-232: Wait for Termination
```python
# ============================================================================
# WAIT FOR TERMINATION
# ============================================================================
try:
    # Wait for all queries
    spark.streams.awaitAnyTermination()
```
**Giải thích:**

#### `spark.streams.awaitAnyTermination()`
- **Block main thread** cho đến khi 1 query terminate
- Nếu không có → Program exit ngay (queries sẽ stop)
- **Tại sao cần?**
  - Streaming queries chạy async (background threads)
  - Main thread phải chờ để keep program alive

---

### Dòng 234-243: Graceful Shutdown
```python
except KeyboardInterrupt:
    print("\n\n⏹ Stopping all streaming queries...")
    
    # Stop all queries gracefully
    for query in spark.streams.active:
        print(f"  Stopping: {query.name if query.name else query.id}")
        query.stop()
    
    print("\n✓ All queries stopped")
    print("✓ Checkpoints saved")
```
**Giải thích:**

#### `except KeyboardInterrupt`
- Bắt Ctrl+C

#### `query.stop()`
- **Graceful stop:**
  1. Finish current micro-batch
  2. Save checkpoint
  3. Close resources
- **Không dùng `kill -9`** (mất checkpoint!)

#### Tại sao quan trọng?
- **Checkpoint saved:** Resume được từ đúng offset
- **No data loss:** Finish processing batch hiện tại
- **Clean state:** Không corrupt files

---

### Dòng 245-258: Final Statistics
```python
    # Show final statistics
    print("\n" + "=" * 80)
    print("FINAL STATISTICS")
    print("=" * 80)
    
    try:
        daily_stats = spark.sql("SELECT * FROM crypto_daily_stats")
        print("\nDaily aggregates:")
        daily_stats.show(10, truncate=False)
    except:
        print("No daily stats available yet")
    
    print("\n" + "=" * 80)
    
    spark.stop()
    print("\n✓ Spark session closed")
```
**Giải thích:**

#### Query Final Stats
- Lấy data từ in-memory table
- Show 10 rows cuối
- Nếu chưa có data (window chưa đóng) → Print warning

#### `spark.stop()`
- Dừng Spark session
- Giải phóng resources (memory, threads)

---

---

# Tóm tắt Tổng quan

## 🎯 Mục đích File
File `spark_streaming_consumer.py` là **Speed Layer Consumer (Production)** trong Lambda Architecture - Xử lý real-time stream từ Kafka bằng Spark Structured Streaming với watermarking và window aggregation.

---

## 📊 Workflow (6 Steps)

### **1. Read Stream from Kafka**
- Connect to Kafka broker (`localhost:9092`)
- Subscribe topic `crypto-prices`
- Read from earliest offset
- Rate limiting: 1000 messages/trigger

### **2. Parse JSON Data**
- Convert Kafka binary → JSON string
- Parse với predefined schema
- Unpack nested struct → flat columns

### **3. Data Transformation**
- Convert Unix timestamp (ms) → Datetime
- Extract date và hour
- Prepare for windowing

### **4. Watermarking**
- Apply 1-hour watermark
- Drop late data > 1 hour
- Enable window closing

### **5. Window Aggregation**
- Tumbling window: 1 day
- Group by: (window, symbol)
- Aggregations: OHLC, Volume, Trades

### **6. Write Streams**
- **Query 1:** Daily → Parquet (append, 10s trigger)
- **Query 2:** Raw → Console (append, 30s trigger)
- **Query 3:** Daily → Memory (complete, 10s trigger)

---

## 🔑 Điểm Quan Trọng

### **1. Structured Streaming vs Batch**
| Aspect | Batch | Streaming |
|--------|-------|-----------|
| Processing | Full dataset at once | Incremental micro-batches |
| Latency | Minutes to hours | Seconds |
| State | Stateless | Stateful (aggregations) |
| Fault Tolerance | Rerun entire job | Checkpoint + replay |

### **2. Watermarking**
```
Purpose: Handle late data và close windows
Threshold: 1 hour
Formula: watermark = max_event_time - 1h
Effect: Drop data có event_time < watermark
```

**Tại sao cần watermark?**
- Không có → Window không bao giờ đóng (memory leak)
- Có → Window đóng sau 1h, emit result

### **3. Window Types**
File này dùng **Tumbling Window:**
- Size: 1 day (24h)
- No overlap
- Windows: [00:00-24:00], [24:00-48:00], ...

**Khi nào window đóng?**
- Watermark vượt qua `window.end`
- **Ví dụ:**
  ```
  Window: 2025-12-16 00:00 → 2025-12-17 00:00
  Window.end: 2025-12-17 00:00
  Watermark: 2025-12-17 01:00 (max_event_time: 2025-12-17 02:00 - 1h)
  → Window đóng, emit result
  ```

### **4. Output Modes**
| Mode | Description | Use Case |
|------|-------------|----------|
| **Append** | Chỉ ghi rows mới | Daily Parquet (window đóng → emit 1 lần) |
| **Update** | Ghi rows changed | Real-time updates |
| **Complete** | Ghi lại toàn bộ | Memory table (query full data) |

### **5. Checkpoint**
**Lưu gì?**
- Kafka offsets (đã process đến đâu)
- Aggregation state (window states)
- Metadata (query config)

**Tại sao quan trọng?**
- **Exactly-once semantics:** Không duplicate/loss data
- **Fault tolerance:** Restart từ checkpoint (không reprocess)
- **State recovery:** Giữ aggregations khi restart

**Cách hoạt động:**
```
Batch 1: Process offset 0-99 → Save checkpoint (offset=100)
Batch 2: Process offset 100-199 → Save checkpoint (offset=200)
[CRASH]
Restart: Read checkpoint → Resume from offset 200
```

---

## 📁 Output Structure

### **1. Parquet Output**
```
streaming_output_spark/daily/
├── symbol=BTCUSDT/
│   ├── part-00000-xxx.parquet
│   ├── part-00001-xxx.parquet
│   └── _spark_metadata/
└── symbol=ETHUSDT/
    ├── part-00000-xxx.parquet
    └── _spark_metadata/
```

### **2. Checkpoint**
```
checkpoint_spark/daily/
├── commits/
│   ├── 0
│   ├── 1
│   └── 2
├── offsets/
│   ├── 0
│   ├── 1
│   └── 2
├── metadata
└── state/
    └── 0/
        └── 0/
```

### **3. Schema Output**
| Column | Type | Example |
|--------|------|---------|
| `date` | timestamp | 2025-12-16 00:00:00 |
| `symbol` | string | BTCUSDT |
| `daily_open` | double | 43000.0 |
| `daily_high` | double | 43500.0 |
| `daily_low` | double | 41800.0 |
| `daily_close` | double | 42000.0 |
| `daily_volume` | double | 12345.67 |
| `daily_quote_volume` | double | 520000000.0 |
| `total_trades` | long | 123456 |
| `tick_count` | long | 1008 |
| `avg_price` | double | 42250.0 |

---

## 💡 Use Cases

### **Khi nào chạy file này?**
1. ✅ **Production 24/7:** Continuous real-time processing
2. ✅ Sau khi start Kafka và Producer
3. ✅ Muốn exactly-once semantics
4. ✅ Cần fault tolerance (checkpoint recovery)

### **Khi nào KHÔNG chạy?**
- ❌ **Demo nhanh:** Dùng `kafka_batch_reader.py` thay thế
  - Lý do: Window 1 ngày cần đợi 24h để đóng
- ❌ Kafka chưa start
- ❌ Producer chưa gửi data (sẽ đợi mãi)
- ❌ Không muốn đợi lâu (window closure)

---

## 🚀 Cách Sử Dụng

### **1. Start Infrastructure**
```bash
cd week6_streaming
docker-compose up -d
```

### **2. Start Producer**
```bash
python websocket_producer.py
# Để chạy ít nhất 24 giờ
```

### **3. Start Consumer (Terminal 2)**
```bash
python spark_streaming_consumer.py
# Đợi 24-25 giờ để window đóng
```

### **4. Expected Output**
```
================================================================================
SPARK STRUCTURED STREAMING - Crypto Price Analysis
================================================================================

✓ Spark 3.5.3 initialized
✓ Checkpoint location: checkpoint_spark
✓ Output location: streaming_output_spark

...

================================================================================
STREAMING QUERIES ACTIVE
================================================================================

Active queries:
  - None
  - None
  - crypto_daily_stats

📊 Monitor status:
  - Console output will show every 30 seconds
  - Parquet files updated every 10 seconds
```

### **5. Monitor Console Output (Mỗi 30s)**
```
-------------------------------------------
Batch: 5
-------------------------------------------
+--------+--------+----------+-------------------+-------------------+
|symbol  |price   |volume    |price_change_percent|event_timestamp    |
+--------+--------+----------+-------------------+-------------------+
|BTCUSDT |42000.50|12345.67  |-2.38              |2025-12-16 10:30:00|
|ETHUSDT |3200.00 |45678.90  |1.25               |2025-12-16 10:30:01|
+--------+--------+----------+-------------------+-------------------+
```

### **6. Query Memory Table (Terminal 3)**
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Query in-memory stats
spark.sql("""
  SELECT symbol, date, daily_close, daily_volume
  FROM crypto_daily_stats
  ORDER BY date DESC
""").show(truncate=False)
```

### **7. Stop Gracefully**
```bash
# Press Ctrl+C in Terminal 2
⏹ Stopping all streaming queries...
  Stopping: None
  Stopping: None
  Stopping: crypto_daily_stats

✓ All queries stopped
✓ Checkpoints saved
```

---

## 🔧 Troubleshooting

### **1. No Output Files**
**Triệu chứng:** `streaming_output_spark/daily/` trống  
**Nguyên nhân:** Window chưa đóng (chưa đủ 24h + 1h watermark)  
**Giải pháp:**
- Đợi ít nhất 25 giờ
- Hoặc dùng `kafka_batch_reader.py` (instant output)

### **2. Kafka Connection Timeout**
**Error:** `TimeoutException: Failed to get records for crypto-prices`  
**Giải pháp:**
```bash
# Check Kafka running
docker ps | grep kafka

# Check topic exists
docker exec -it kafka_container kafka-topics --list --bootstrap-server localhost:9092

# Increase timeout
.option("kafka.session.timeout.ms", "60000")
```

### **3. Out of Memory**
**Error:** `java.lang.OutOfMemoryError: Java heap space`  
**Giải pháp:**
```python
# Tăng driver memory
.config("spark.driver.memory", "8g")

# Hoặc giảm maxOffsetsPerTrigger
.option("maxOffsetsPerTrigger", 500)
```

### **4. Checkpoint Corruption**
**Error:** `IllegalStateException: Checkpoint directory corrupted`  
**Giải pháp:**
```bash
# Xóa checkpoint và restart từ đầu
rm -rf checkpoint_spark
python spark_streaming_consumer.py
```

### **5. Late Data Dropped**
**Triệu chứng:** Thiếu rows trong output  
**Nguyên nhân:** Watermark drop data > 1h  
**Giải pháp:**
```python
# Tăng watermark threshold
.withWatermark("event_timestamp", "2 hours")
```

---

## 📈 Performance

### **Throughput**
- **Input:** 2 msg/s từ Producer
- **Trigger:** 10s (process 20 messages/batch)
- **Window:** 1 day (accumulate 172,800 messages)

### **Latency**
- **Micro-batch:** 10s (trigger interval)
- **Window closure:** 24h (window size) + 1h (watermark) = **25 giờ**
- **End-to-end:** Kafka → Spark → Parquet ~ 10-20s

### **Resource Usage**
- **Memory:** 4GB driver + 2GB executor
- **CPU:** 2-4 cores (parallel processing)
- **Disk:** ~100 KB/day/symbol (Parquet compressed)

---

## 🎓 Key Technologies

- **Spark Structured Streaming:** Real-time processing framework
- **Kafka Consumer:** Spark Kafka connector
- **Watermarking:** Late data handling
- **Tumbling Windows:** Non-overlapping time windows
- **Checkpoint:** Fault tolerance + exactly-once
- **Micro-batching:** Batches every 10s
- **Parquet:** Columnar storage format

---

## 🔗 Integration

### **Lambda Architecture Flow**
```
Binance API
  ↓
websocket_producer.py (Speed Layer Producer)
  ↓ (Kafka: crypto-prices)
spark_streaming_consumer.py (Speed Layer Consumer)
  ↓ (Parquet: streaming_output_spark/daily/)
week6_merge.py (Serving Layer)
  ↓ (Merge Batch + Speed)
prophet_train.py (ML Layer)
```

### **Alternative Flow (Demo)**
```
websocket_producer.py
  ↓ (Kafka: crypto-prices)
kafka_batch_reader.py (Batch mode - Instant output)
  ↓ (Parquet: streaming_output_spark_BATCH/)
week6_merge.py
```

---

## ⚠️ Production vs Demo

| Aspect | Production (File này) | Demo (kafka_batch_reader.py) |
|--------|----------------------|------------------------------|
| **Processing** | Streaming (continuous) | Batch (one-time) |
| **Window** | 1 day (24h) | Batch entire topic |
| **Watermark** | 1 hour | N/A |
| **Output Time** | 25 hours | 1-2 seconds |
| **Use Case** | 24/7 real-time | Quick demo (5-10 min) |
| **Checkpoint** | Yes (recovery) | No |

**Recommendation cho demo:**
- ✅ Dùng `kafka_batch_reader.py` để show kết quả nhanh
- ✅ Giải thích file này là production version (24h window)
- ✅ Show code và architecture (không cần chạy thật)

---

**Tác giả:** Đoàn Thế Tín  
**MSSV:** 4551190056  
**File:** `week6_streaming/spark_streaming_consumer.py`  
**Lines:** 263 dòng code  
**Mục đích:** Speed Layer Consumer (Production) - Real-time processing với Spark Structured Streaming

---
