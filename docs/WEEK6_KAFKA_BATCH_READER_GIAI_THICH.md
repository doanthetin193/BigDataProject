# Giải thích chi tiết: kafka_batch_reader.py

**File:** `week6_streaming/kafka_batch_reader.py`  
**Chức năng:** Speed Layer Demo - Batch mode reader để test nhanh Kafka → Aggregation → Parquet  
**Tác giả:** Đoàn Thế Tín  
**Ngày:** Week 6 - Lambda Architecture

---

## 📋 Mục lục
1. [Import và Configuration](#1-import-và-configuration)
2. [Spark Session Setup](#2-spark-session-setup)
3. [Schema Definition](#3-schema-definition)
4. [Read Batch from Kafka](#4-read-batch-from-kafka)
5. [Parse JSON Data](#5-parse-json-data)
6. [Data Transformation](#6-data-transformation)
7. [Daily Aggregation](#7-daily-aggregation)
8. [Save to Parquet](#8-save-to-parquet)
9. [Tóm tắt](#tóm-tắt-tổng-quan)

---

## 1. Import và Configuration

### Dòng 1-6: Docstring
```python
"""
kafka_batch_reader.py - Doc du lieu tu Kafka bang batch mode

Muc dich: Chung minh Speed Layer hoat dong (doc tu Kafka -> aggregation -> Parquet)
Su dung: python kafka_batch_reader.py
"""
```
**Giải thích:**
- **Batch mode:** Đọc tất cả messages từ Kafka **1 lần** (không continuous stream)
- **Mục đích:** Demo nhanh Speed Layer (không cần đợi 24h như streaming)
- **Use case:** Presentation, testing, quick results

---

### Dòng 7-10: Import
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
```
**Giải thích:**
- `SparkSession`: Entry point Spark
- `functions`: Các hàm SQL (col, from_json, to_date, ...)
- `types`: Data types (StringType, LongType, DoubleType, ...)
- `os`: Xử lý đường dẫn file

---

### Dòng 12-14: Banner
```python
print("="*80)
print("KAFKA BATCH READER - Speed Layer Test")
print("="*80)
```
**Giải thích:** In header cho console.

---

## 2. Spark Session Setup

### Dòng 16-19: Spark Session
```python
# Spark Session
spark = SparkSession.builder \
    .appName("KafkaBatchReader") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()
```
**Giải thích:**

#### `.appName("KafkaBatchReader")`
- Tên application
- Hiển thị trong Spark UI

#### `.config("spark.jars.packages", "...")`
- Download Kafka connector dependency
- `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`
  - Tương tự như `spark_streaming_consumer.py`
  - Cần để đọc từ Kafka

#### `.getOrCreate()`
- Lấy session hiện có hoặc tạo mới

---

### Dòng 21-22: Log Level
```python
spark.sparkContext.setLogLevel("WARN")
print("\n✓ Spark initialized\n")
```
**Giải thích:**
- `WARN`: Chỉ log WARNING và ERROR
- Ẩn INFO logs (giảm noise)

---

## 3. Schema Definition

### Dòng 24-38: Message Schema
```python
# Schema
message_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("event_time", LongType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("quote_volume", DoubleType(), True),
    StructField("number_trades", IntegerType(), True),
    StructField("price_change", DoubleType(), True),
    StructField("price_change_percent", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])
```
**Giải thích:**

### Schema giống với `spark_streaming_consumer.py`
- **Tại sao?** Cùng đọc từ Kafka topic `crypto-prices`
- **Producer** (`websocket_producer.py`) gửi JSON với schema này
- **Consumer** (cả streaming và batch) dùng cùng schema để parse

### So sánh với Streaming Consumer
| Aspect | Batch Reader (File này) | Streaming Consumer |
|--------|------------------------|-------------------|
| Schema | Giống hệt | Giống hệt |
| Kafka Topic | crypto-prices | crypto-prices |
| Read Mode | **Batch (1 lần)** | **Streaming (continuous)** |

---

## 4. Read Batch from Kafka

### Dòng 40-48: Batch Read
```python
# Doc BATCH tu Kafka (khong phai streaming)
print("Reading ALL messages from Kafka (batch mode)...")
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-prices") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()
```
**Giải thích:**

### `spark.read` vs `spark.readStream`
| API | Mode | Behavior |
|-----|------|----------|
| `spark.read` | **Batch** | Đọc tất cả data 1 lần, return DataFrame |
| `spark.readStream` | **Streaming** | Đọc continuous, return StreamingDataFrame |

### Điểm khác biệt chính
```python
# Batch (File này):
df = spark.read.format("kafka")...

# Streaming (spark_streaming_consumer.py):
df = spark.readStream.format("kafka")...
```

---

### Dòng 43-47: Kafka Options

#### `.format("kafka")`
- Dùng Kafka source connector

#### `.option("kafka.bootstrap.servers", "localhost:9092")`
- Kafka broker address

#### `.option("subscribe", "crypto-prices")`
- Subscribe topic `crypto-prices`

#### `.option("startingOffsets", "earliest")`
- **Bắt đầu từ offset đầu tiên** trong topic
- Đọc tất cả messages từ đầu đến cuối
- **Alternatives:**
  - `"latest"`: Chỉ đọc messages mới (không đọc gì nếu topic đã có data)
  - `{"crypto-prices": {"0": 100}}`: Từ offset 100

#### `.option("endingOffsets", "latest")`
- **Kết thúc ở offset cuối cùng** hiện tại
- **Tại sao cần?**
  - Batch mode cần biết điểm dừng
  - Streaming không có `endingOffsets` (continuous)

---

### Kafka Offset Flow
```
Topic: crypto-prices
├── Offset 0: {"symbol": "BTCUSDT", "price": 42000.0, ...}
├── Offset 1: {"symbol": "ETHUSDT", "price": 3200.0, ...}
├── Offset 2: {"symbol": "BTCUSDT", "price": 42010.0, ...}
...
└── Offset 1007: {"symbol": "ETHUSDT", "price": 3210.0, ...}

Batch Read:
  startingOffsets = "earliest" → Bắt đầu từ 0
  endingOffsets = "latest" → Kết thúc ở 1007
  → Đọc 1,008 messages
```

---

### Dòng 50-51: Info
```python
print(f"✓ Read from Kafka topic: crypto-prices")
print(f"✓ Total messages: {df.count()}\n")
```
**Giải thích:**
- `df.count()`: Đếm số messages đọc được
- **Action:** Trigger Spark job (đọc Kafka)

**Ví dụ output:**
```
✓ Read from Kafka topic: crypto-prices
✓ Total messages: 1,008
```

---

## 5. Parse JSON Data

### Dòng 53-57: JSON Parsing
```python
# Parse JSON
parsed_df = df.select(
    from_json(col("value").cast("string"), message_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")
```
**Giải thích:**

### Logic giống `spark_streaming_consumer.py`
- **Step 1:** Lấy column `value` (binary)
- **Step 2:** Cast sang string
- **Step 3:** Parse JSON với schema
- **Step 4:** Unpack struct → flat columns

### Transformation
```
Input (Kafka DataFrame):
├── key: null
├── value: b'{"symbol":"BTCUSDT","price":42000.0,...}'
├── topic: "crypto-prices"
├── partition: 0
├── offset: 0
└── timestamp: 2025-12-16 10:30:00

After parsing:
├── symbol: "BTCUSDT"
├── event_time: 1734134400000
├── price: 42000.0
├── open: 43000.0
├── high: 43500.0
├── low: 41800.0
├── volume: 12345.67
├── quote_volume: 520000000.0
├── number_trades: 123456
├── price_change: -1000.0
├── price_change_percent: -2.38
├── timestamp: "2025-12-16T10:30:45.123456"
└── kafka_timestamp: 2025-12-16 10:30:00
```

---

## 6. Data Transformation

### Dòng 59-63: Timestamp Conversion
```python
# Transform
transformed_df = parsed_df \
    .withColumn("event_timestamp", (col("event_time") / 1000).cast("timestamp")) \
    .withColumn("date", to_date(col("event_timestamp"))) \
    .withColumn("hour", hour(col("event_timestamp")))
```
**Giải thích:**

### Logic giống `spark_streaming_consumer.py`

#### `.withColumn("event_timestamp", ...)`
- Convert milliseconds → timestamp
- `1734134400000 / 1000` → `1734134400` (seconds)
- `.cast("timestamp")` → `2025-12-16 10:30:00`

#### `.withColumn("date", ...)`
- Extract date từ timestamp
- `2025-12-16 10:30:00` → `2025-12-16`

#### `.withColumn("hour", ...)`
- Extract hour
- `2025-12-16 10:30:00` → `10`

---

### Dòng 65-66: Sample Data
```python
print("Sample data:")
transformed_df.select("symbol", "price", "volume", "event_timestamp").show(10, False)
```
**Giải thích:**
- Show 10 rows mẫu
- `.show(10, False)`: 10 rows, không truncate

**Ví dụ output:**
```
Sample data:
+--------+--------+----------+-------------------+
|symbol  |price   |volume    |event_timestamp    |
+--------+--------+----------+-------------------+
|BTCUSDT |42000.50|12345.67  |2025-12-16 10:30:00|
|ETHUSDT |3200.00 |45678.90  |2025-12-16 10:30:01|
|BTCUSDT |42010.00|12346.78  |2025-12-16 10:30:02|
|ETHUSDT |3201.50 |45680.12  |2025-12-16 10:30:03|
...
```

---

## 7. Daily Aggregation

### Dòng 68-82: Aggregation
```python
# Daily Aggregation (khong can watermark cho batch)
daily_df = transformed_df \
    .groupBy("date", "symbol") \
    .agg(
        first("open").alias("daily_open"),
        max("high").alias("daily_high"),
        min("low").alias("daily_low"),
        last("price").alias("daily_close"),
        sum("volume").alias("daily_volume"),
        sum("quote_volume").alias("daily_quote_volume"),
        sum("number_trades").alias("total_trades"),
        count("*").alias("tick_count"),
        avg("price").alias("avg_price")
    ) \
    .orderBy("date", "symbol")
```
**Giải thích:**

### Khác biệt với Streaming Consumer

| Aspect | Batch Reader (File này) | Streaming Consumer |
|--------|------------------------|-------------------|
| **Group By** | `groupBy("date", "symbol")` | `groupBy(window(...), "symbol")` |
| **Watermark** | **KHÔNG CẦN** | Cần (1 hour) |
| **Window** | **KHÔNG CẦN** | Tumbling 1 day |
| **Processing** | 1 lần | Continuous micro-batches |

---

### Tại sao không cần Watermark?
- **Batch mode:** Đọc tất cả data sẵn có (không có late data)
- **Streaming mode:** Data đến liên tục → Cần watermark để drop late data

### Tại sao không dùng window()?
- **Batch:** Dùng `groupBy("date")` đơn giản
  - `date` đã extract từ timestamp
  - Group theo date thẳng luôn
- **Streaming:** Dùng `window(col("event_timestamp"), "1 day")`
  - Cần window object để quản lý state
  - Window có `start` và `end` timestamps

---

### Aggregation Functions

#### `first("open")`
- Giá `open` của message **đầu tiên** trong ngày
- **Sắp xếp:** Theo thứ tự Spark read (offset order)
- **Assumption:** Producer gửi theo thứ tự thời gian

#### `max("high")`
- Giá cao nhất trong ngày

#### `min("low")`
- Giá thấp nhất trong ngày

#### `last("price")`
- Giá `price` của message **cuối cùng** trong ngày
- Đây là **daily close price**

#### `sum("volume")`
- Tổng volume BTC/ETH trong ngày

#### `sum("quote_volume")`
- Tổng volume USDT trong ngày

#### `sum("number_trades")`
- Tổng số trades trong ngày

#### `count("*")`
- Số messages (ticks) trong ngày
- **Ví dụ:** Producer chạy 1h, gửi 2 msg/s → 7,200 ticks

#### `avg("price")`
- Giá trung bình trong ngày

---

### Dòng 84-85: Show Results
```python
print("\nDaily aggregation:")
daily_df.show(20, False)
```
**Giải thích:**
- Show 20 rows kết quả
- `False`: Không truncate

**Ví dụ output:**
```
Daily aggregation:
+----------+--------+-----------+-----------+-----------+------------+-------------+-------------------+------------+----------+-----------+
|date      |symbol  |daily_open |daily_high |daily_low  |daily_close |daily_volume |daily_quote_volume |total_trades|tick_count|avg_price  |
+----------+--------+-----------+-----------+-----------+------------+-------------+-------------------+------------+----------+-----------+
|2025-12-16|BTCUSDT |43000.0    |43500.0    |41800.0    |42000.0     |12345.67     |520000000.0        |123456      |504       |42250.5    |
|2025-12-16|ETHUSDT |3150.0     |3220.0     |3100.0     |3200.0      |45678.90     |145000000.0        |89012       |504       |3160.8     |
+----------+--------+-----------+-----------+-----------+------------+-------------+-------------------+------------+----------+-----------+
```

---

## 8. Save to Parquet

### Dòng 87-89: Output Path
```python
# Luu Parquet
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(BASE_DIR, "streaming_output_spark_BATCH")
```
**Giải thích:**

#### `os.path.dirname(os.path.abspath(__file__))`
- `__file__`: Path của file hiện tại
  - `/week6_streaming/kafka_batch_reader.py`
- `os.path.abspath()`: Convert → absolute path
  - `D:\BigDataProject\week6_streaming\kafka_batch_reader.py`
- `os.path.dirname()`: Lấy thư mục cha
  - `D:\BigDataProject\week6_streaming\`

#### Output Path
- `streaming_output_spark_BATCH`
- **Khác với streaming consumer:** `streaming_output_spark` (không có `_BATCH`)
- **Tại sao tách riêng?**
  - Tránh conflict với streaming output
  - Dễ phân biệt batch vs streaming results

---

### Dòng 91-94: Write Parquet
```python
daily_df.write \
    .mode("overwrite") \
    .partitionBy("symbol") \
    .parquet(OUTPUT_PATH)
```
**Giải thích:**

#### `.write`
- Batch write API (không phải `.writeStream`)

#### `.mode("overwrite")`
- **Overwrite mode:** Xóa data cũ, ghi mới
- **Alternatives:**
  - `"append"`: Thêm vào cuối
  - `"ignore"`: Skip nếu đã tồn tại
  - `"error"`: Raise exception nếu tồn tại

#### `.partitionBy("symbol")`
- Partition theo symbol
- Folder structure:
  ```
  streaming_output_spark_BATCH/
  ├── symbol=BTCUSDT/
  │   └── part-00000-xxx.parquet
  └── symbol=ETHUSDT/
      └── part-00000-xxx.parquet
  ```

#### `.parquet(OUTPUT_PATH)`
- Save dạng Parquet format
- Path: `week6_streaming/streaming_output_spark_BATCH/`

---

### Dòng 96-99: Success Message
```python
print(f"\n✓ Saved to: {OUTPUT_PATH}")
print(f"✓ Total rows: {daily_df.count()}")
print(f"\n{'='*80}")
print("SUCCESS! Speed Layer data processed from Kafka")
```
**Giải thích:**
- Print output path
- Print số rows saved
- Success banner

**Ví dụ output:**
```
✓ Saved to: D:\BigDataProject\week6_streaming\streaming_output_spark_BATCH
✓ Total rows: 2

================================================================================
SUCCESS! Speed Layer data processed from Kafka
================================================================================
```

---

### Dòng 101-103: Cleanup
```python
print(f"{'='*80}\n")

spark.stop()
```
**Giải thích:**
- `spark.stop()`: Dừng Spark session
- Giải phóng resources (memory, threads)

---

---

# Tóm tắt Tổng quan

## 🎯 Mục đích File
File `kafka_batch_reader.py` là **Speed Layer Demo Reader** - Đọc tất cả messages từ Kafka bằng batch mode (1 lần) để test nhanh và demo kết quả mà không cần đợi 24h như streaming consumer.

---

## 📊 Workflow (5 Steps)

### **1. Read Batch from Kafka**
- Batch mode: `spark.read` (không phải `spark.readStream`)
- Đọc từ `earliest` → `latest` offsets
- 1 lần xong (không continuous)

### **2. Parse JSON Data**
- Convert binary → string → JSON
- Parse với predefined schema
- Unpack nested struct

### **3. Transform Data**
- Convert Unix timestamp → Datetime
- Extract date và hour

### **4. Aggregate Daily**
- Group by (date, symbol)
- **KHÔNG cần watermark** (batch mode)
- **KHÔNG dùng window()** (dùng date thẳng)
- Aggregations: OHLC, Volume, Trades

### **5. Save Parquet**
- Write mode: Overwrite
- Partition by symbol
- Output: `streaming_output_spark_BATCH/`

---

## 🔑 Điểm Quan Trọng

### **1. Batch vs Streaming Mode**

| Aspect | Batch Mode (File này) | Streaming Mode |
|--------|----------------------|----------------|
| **API** | `spark.read` | `spark.readStream` |
| **Read** | 1 lần (earliest → latest) | Continuous |
| **Offsets** | `startingOffsets` + `endingOffsets` | `startingOffsets` only |
| **Processing** | 1 job | Micro-batches (10s) |
| **Watermark** | **KHÔNG CẦN** | Cần (1h) |
| **Window** | **KHÔNG CẦN** | Tumbling 1 day |
| **Output** | `.write` | `.writeStream` |
| **Time to Result** | **1-2 giây** | **25 giờ** |
| **Use Case** | Demo, Testing | Production 24/7 |

---

### **2. Khi nào dùng Batch Reader?**

✅ **Demo cho giảng viên** (5-10 phút)
- Chạy Producer 10 phút
- Chạy Batch Reader → Kết quả ngay
- Show Parquet output

✅ **Testing Speed Layer logic**
- Verify aggregation logic đúng
- Check schema matching
- Debug nhanh

✅ **One-time processing**
- Process backlog messages
- Reprocess data với logic mới

---

### **3. Khi nào dùng Streaming Consumer?**

✅ **Production 24/7**
- Real-time continuous processing
- Exactly-once semantics
- Fault tolerance với checkpoint

❌ **KHÔNG dùng cho demo**
- Window 1 ngày cần đợi 24h
- Watermark 1h thêm delay
- Tổng: 25 giờ để có kết quả

---

### **4. Aggregation Logic**

#### Group By
```python
# Batch (đơn giản):
.groupBy("date", "symbol")

# Streaming (phức tạp):
.groupBy(window(col("event_timestamp"), "1 day"), col("symbol"))
```

#### Tại sao khác?
- **Batch:** Data đã có sẵn, group theo `date` column
- **Streaming:** Cần window object để quản lý state

#### Output giống nhau
- Cả 2 đều cho OHLC daily
- Schema giống hệt nhau

---

## 📁 Output Structure

### **Folder Structure**
```
week6_streaming/
├── streaming_output_spark/          # Streaming consumer output
│   └── daily/
│       ├── symbol=BTCUSDT/
│       └── symbol=ETHUSDT/
└── streaming_output_spark_BATCH/    # Batch reader output (File này)
    ├── symbol=BTCUSDT/
    │   └── part-00000-xxx.parquet
    └── symbol=ETHUSDT/
        └── part-00000-xxx.parquet
```

### **Schema Output**
| Column | Type | Example | Source |
|--------|------|---------|--------|
| `date` | date | 2025-12-16 | Extract từ event_timestamp |
| `symbol` | string | BTCUSDT | Producer |
| `daily_open` | double | 43000.0 | first("open") |
| `daily_high` | double | 43500.0 | max("high") |
| `daily_low` | double | 41800.0 | min("low") |
| `daily_close` | double | 42000.0 | last("price") |
| `daily_volume` | double | 12345.67 | sum("volume") |
| `daily_quote_volume` | double | 520000000.0 | sum("quote_volume") |
| `total_trades` | long | 123456 | sum("number_trades") |
| `tick_count` | long | 504 | count(*) |
| `avg_price` | double | 42250.5 | avg("price") |

---

## 💡 Use Cases

### **1. Quick Demo (5-10 phút)**
```bash
# Step 1: Start Kafka
cd week6_streaming
docker-compose up -d

# Step 2: Run Producer (10 phút)
python websocket_producer.py
# Ctrl+C sau 10 phút (~1,200 messages)

# Step 3: Run Batch Reader
python kafka_batch_reader.py
# ✓ Kết quả ngay lập tức!

# Step 4: Check output
ls streaming_output_spark_BATCH/
```

### **2. Test Aggregation Logic**
```bash
# Run producer với test data
python websocket_producer.py
# Ctrl+C sau 1 phút

# Run batch reader và verify
python kafka_batch_reader.py
# Check console output
```

### **3. Reprocess Messages**
```bash
# Kafka có 10,000 messages
# Muốn reprocess với logic mới

# Edit kafka_batch_reader.py (update aggregation)
# Run lại
python kafka_batch_reader.py
# Process all 10,000 messages
```

---

## 🚀 Cách Sử Dụng

### **Prerequisites**
```bash
# Kafka phải running
docker ps | grep kafka

# Topic phải có data
# (Chạy websocket_producer.py trước)
```

---

### **Run Command**
```bash
cd week6_streaming
python kafka_batch_reader.py
```

---

### **Expected Output**
```
================================================================================
KAFKA BATCH READER - Speed Layer Test
================================================================================

✓ Spark initialized

Reading ALL messages from Kafka (batch mode)...
✓ Read from Kafka topic: crypto-prices
✓ Total messages: 1,008

Sample data:
+--------+--------+----------+-------------------+
|symbol  |price   |volume    |event_timestamp    |
+--------+--------+----------+-------------------+
|BTCUSDT |42000.50|12345.67  |2025-12-16 10:30:00|
|ETHUSDT |3200.00 |45678.90  |2025-12-16 10:30:01|
|BTCUSDT |42010.00|12346.78  |2025-12-16 10:30:02|
|ETHUSDT |3201.50 |45680.12  |2025-12-16 10:30:03|
...
+--------+--------+----------+-------------------+

Daily aggregation:
+----------+--------+-----------+-----------+-----------+------------+-------------+-------------------+------------+----------+-----------+
|date      |symbol  |daily_open |daily_high |daily_low  |daily_close |daily_volume |daily_quote_volume |total_trades|tick_count|avg_price  |
+----------+--------+-----------+-----------+-----------+------------+-------------+-------------------+------------+----------+-----------+
|2025-12-16|BTCUSDT |43000.0    |43500.0    |41800.0    |42000.0     |12345.67     |520000000.0        |123456      |504       |42250.5    |
|2025-12-16|ETHUSDT |3150.0     |3220.0     |3100.0     |3200.0      |45678.90     |145000000.0        |89012       |504       |3160.8     |
+----------+--------+-----------+-----------+-----------+------------+-------------+-------------------+------------+----------+-----------+

✓ Saved to: D:\BigDataProject\week6_streaming\streaming_output_spark_BATCH
✓ Total rows: 2

================================================================================
SUCCESS! Speed Layer data processed from Kafka
================================================================================
```

---

### **Verify Output**
```bash
# Check Parquet files
ls streaming_output_spark_BATCH/

# Output:
# symbol=BTCUSDT/
# symbol=ETHUSDT/

# Read Parquet với PySpark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("streaming_output_spark_BATCH")
df.show()
```

---

## 🔧 Troubleshooting

### **1. No Messages Read**
**Triệu chứng:** `Total messages: 0`  
**Nguyên nhân:**
- Kafka topic trống (chưa chạy producer)
- Topic không tồn tại

**Giải pháp:**
```bash
# Check topic exists
docker exec -it kafka_container kafka-topics --list --bootstrap-server localhost:9092

# Check topic có data
docker exec -it kafka_container kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto-prices \
  --from-beginning \
  --max-messages 10

# Run producer trước
python websocket_producer.py
# Ctrl+C sau vài phút
```

---

### **2. Kafka Connection Failed**
**Error:** `Failed to get records for crypto-prices`  
**Giải pháp:**
```bash
# Check Kafka running
docker ps | grep kafka

# Start if not running
docker-compose up -d

# Check port 9092
netstat -an | grep 9092
```

---

### **3. JSON Parse Error**
**Error:** `Could not parse JSON`  
**Nguyên nhân:** Schema không khớp với data  
**Giải pháp:**
```python
# Check sample message từ Kafka
df.select(col("value").cast("string")).show(1, False)

# Verify schema match
# Update message_schema nếu cần
```

---

### **4. Empty Output**
**Triệu chứng:** `Total rows: 0` sau aggregation  
**Nguyên nhân:**
- All rows filtered out (NULL values)
- Aggregation logic sai

**Giải pháp:**
```python
# Check intermediate results
print(f"Parsed rows: {parsed_df.count()}")
print(f"Transformed rows: {transformed_df.count()}")
print(f"Daily rows: {daily_df.count()}")

# Show nulls
transformed_df.filter(col("date").isNull()).show()
```

---

### **5. File Already Exists**
**Error:** `Path already exists`  
**Nguyên nhân:** Output folder đã tồn tại  
**Giải pháp:**
```bash
# Xóa output cũ
rm -rf streaming_output_spark_BATCH

# Hoặc dùng append mode
.mode("append")

# Hoặc ignore mode
.mode("ignore")
```

---

## 📈 Performance

### **Processing Time**
| Stage | Time | Note |
|-------|------|------|
| Read Kafka | 1-2s | 1,000 messages |
| Parse JSON | 0.5s | Schema apply |
| Aggregation | 0.5s | Group by 2 symbols |
| Write Parquet | 0.5s | 2 rows |
| **Total** | **2-3s** | Very fast! |

### **Throughput**
- **Input:** 1,008 messages (504 BTC + 504 ETH)
- **Output:** 2 rows (1 BTC + 1 ETH daily)
- **Compression:** 504:1 ratio

### **Resource Usage**
- **Memory:** 1-2 GB (default Spark driver)
- **CPU:** 1-2 cores
- **Disk:** ~10 KB output (Parquet compressed)

---

## 🎓 Key Technologies

- **Spark Batch Read:** `spark.read.format("kafka")`
- **Kafka Consumer:** Read from earliest to latest offsets
- **JSON Parsing:** `from_json()` with schema
- **Group By Aggregation:** Daily OHLC
- **Parquet:** Columnar storage with partitioning

---

## 🔗 Integration

### **Lambda Architecture Flow**
```
Binance API
  ↓
websocket_producer.py
  ↓ (Kafka: crypto-prices)
kafka_batch_reader.py (DEMO - Instant)
  ↓ (Parquet: streaming_output_spark_BATCH/)
week6_merge.py
  ↓ (Merge Batch + Speed)
prophet_train.py
```

### **Alternative: Streaming Flow**
```
websocket_producer.py
  ↓ (Kafka: crypto-prices)
spark_streaming_consumer.py (PRODUCTION - 25h)
  ↓ (Parquet: streaming_output_spark/)
week6_merge.py
```

---

## ⚙️ Demo Strategy

### **Giảng viên hỏi: "Speed Layer thật sự hoạt động không?"**

**Trả lời:**
> "Dạ có ạ! Em chạy demo cho thầy xem ngay:
> 1. Producer đã gửi 1,008 messages vào Kafka
> 2. Batch Reader đọc tất cả messages trong 2 giây
> 3. Kết quả: 2 rows daily OHLC đã save vào Parquet
> 4. Schema giống hệt Batch Layer (ready to merge)
> 
> Production thì dùng spark_streaming_consumer.py với:
> - Continuous streaming (micro-batches 10s)
> - Watermark 1h (handle late data)
> - Window 1 day (tumbling window)
> - Checkpoint (fault tolerance)
> - Cần đợi 25h để window đóng
> 
> Nên em dùng batch reader để demo nhanh cho thầy!"

---

## 📊 Comparison Table

| Feature | Batch Reader | Streaming Consumer |
|---------|-------------|-------------------|
| **Read API** | `spark.read` | `spark.readStream` |
| **Processing** | 1 lần | Continuous |
| **Offsets** | earliest → latest | earliest → ongoing |
| **Watermark** | ❌ Không cần | ✅ 1 hour |
| **Window** | ❌ Không cần | ✅ 1 day tumbling |
| **Write API** | `.write` | `.writeStream` |
| **Output Mode** | overwrite/append | append/complete |
| **Time** | **2-3 giây** | **25 giờ** |
| **Checkpoint** | ❌ Không có | ✅ Fault tolerance |
| **State** | ❌ Stateless | ✅ Stateful |
| **Exactly-once** | ❌ No | ✅ Yes |
| **Use Case** | Demo, Testing | Production 24/7 |

---

**Tác giả:** Đoàn Thế Tín  
**MSSV:** 4551190056  
**File:** `week6_streaming/kafka_batch_reader.py`  
**Lines:** 104 dòng code  
**Mục đích:** Speed Layer Demo Reader - Batch mode processing cho presentation và testing

---
