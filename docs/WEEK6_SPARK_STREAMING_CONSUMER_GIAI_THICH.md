# WEEK 6 - SPARK STREAMING CONSUMER - GIẢI THÍCH CHI TIẾT

## 📌 TỔNG QUAN

**File:** `week6_streaming/spark_streaming_consumer.py` (308 dòng)

**Vai trò:** Speed Layer Consumer - Nhận real-time data từ Kafka, xử lý streaming với PySpark

**Công nghệ:**

- Apache Spark Structured Streaming
- Kafka Consumer
- Window Aggregation (Daily)
- Watermarking (xử lý late data)
- Checkpoint (fault tolerance)

**Input:** Kafka topic `crypto-prices` (từ websocket_producer.py)

**Output:**

- Parquet files: `streaming_output_spark/daily/`
- Console monitoring
- In-memory table: `crypto_daily_stats`

---

## 🔧 CẤU TRÚC FILE

```
┌─────────────────────────────────────┐
│ PHẦN 1: Import & Configuration     │
│ PHẦN 2: Spark Session              │
│ PHẦN 3: Schema Definition          │
├─────────────────────────────────────┤
│ STEP 1: Read Stream from Kafka     │
│ STEP 2: Parse JSON Data            │
│ STEP 3: Data Transformation        │
│ STEP 4: Watermarking               │
│ STEP 5: Daily Aggregation          │
│ STEP 6: Write Streams (3 queries)  │
├─────────────────────────────────────┤
│ PHẦN 8: Monitoring                 │
│ PHẦN 9: Graceful Shutdown          │
└─────────────────────────────────────┘
```

---

## 📖 GIẢI THÍCH CHI TIẾT

### **PHẦN 1: IMPORT & CONFIGURATION (Dòng 1-23)**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
```

**Import:**

- `SparkSession`: Tạo Spark application
- `functions`: Các hàm như `from_json`, `window`, `col`, `to_date`, `hour`
- `types`: Định nghĩa schema (StructType, StructField, StringType, DoubleType...)
- `os`: Xử lý đường dẫn file (không dùng trong code này)

```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto-prices"
OUTPUT_PATH = "streaming_output_spark"
CHECKPOINT_PATH = "checkpoint_spark"
```

**Configuration:**

- **Kafka Server:** `localhost:9092` - nơi Kafka đang chạy
- **Topic:** `crypto-prices` - producer gửi data vào topic này
- **Output Path:** Thư mục lưu kết quả Parquet
- **Checkpoint Path:** Lưu tiến trình xử lý (fault tolerance)

**Tại sao cần Checkpoint?**

```
Scenario:
1. Spark đang xử lý window 2025-12-10
2. Crash giữa chừng
3. Restart lại → đọc checkpoint → tiếp tục từ window 2025-12-10
4. Không bị mất data, không xử lý trùng
```

---

### **PHẦN 2: SPARK SESSION (Dòng 24-42)**

```python
spark = SparkSession.builder \
    .appName("CryptoPriceStructuredStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

**Từng config:**

**1. `.appName("CryptoPriceStructuredStreaming")`**

- Tên application hiển thị trong Spark UI

**2. `.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")`**

- **QUAN TRỌNG NHẤT!**
- Tải thư viện Kafka Connector từ Maven
- Spark version 3.5.3, Scala 2.12
- Không có config này → không kết nối được Kafka

**3. `.config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)`**

- Checkpoint location mặc định
- Mỗi query có thể override bằng checkpoint riêng

**4. `.config("spark.sql.adaptive.enabled", "true")`**

- Adaptive Query Execution (AQE)
- Spark tự động tối ưu query execution plan
- Cải thiện performance

**5. `.config("spark.driver.memory", "4g")`**

- Driver process dùng 4GB RAM
- Đủ cho streaming nhỏ/vừa (2 symbols)

```python
spark.sparkContext.setLogLevel("WARN")
```

- Chỉ hiển thị log level WARN/ERROR
- Không spam màn hình với INFO/DEBUG logs

---

### **PHẦN 3: SCHEMA DEFINITION (Dòng 43-60)**

```python
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

**Tại sao cần schema?**

Kafka message là **JSON string**:

```json
{
  "symbol": "BTCUSDT",
  "event_time": 1733856000123,
  "price": 42156.78,
  ...
}
```

Spark cần **parse JSON** → DataFrame với datatypes chính xác:

- `symbol`: String
- `event_time`: Long (milliseconds)
- `price`: Double
- `number_trades`: Integer

**Mapping với Producer:**

Producer (`websocket_producer.py`) gửi:

```python
message = {
    "symbol": ticker["symbol"],
    "event_time": ticker["closeTime"],  # Long
    "price": float(ticker["lastPrice"]),  # Double
    ...
}
```

Consumer parse theo schema → DataFrame với đúng datatype.

---

### **STEP 1: READ STREAM FROM KAFKA (Dòng 61-77)**

```python
kafkaDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()
```

**Từng option:**

**1. `.readStream` (không phải `.read`)**

- Streaming mode (continuous processing)
- Batch mode dùng `.read`

**2. `.format("kafka")`**

- Sử dụng Kafka connector (từ jars.packages)

**3. `.option("kafka.bootstrap.servers", "localhost:9092")`**

- Kafka server address
- Có thể có nhiều servers: `"server1:9092,server2:9092"`

**4. `.option("subscribe", "crypto-prices")`**

- Subscribe vào topic `crypto-prices`
- Có thể subscribe nhiều topics: `"topic1,topic2"`

**5. `.option("startingOffsets", "earliest")`**

- **Lần đầu chạy:** đọc từ message đầu tiên trong topic
- **Lần sau:** dùng checkpoint → đọc từ offset cuối cùng đã xử lý

```
Timeline:
[Msg1] [Msg2] [Msg3] [Msg4] [Msg5] ... [MsgN]
  ↑
earliest - Bắt đầu từ đây

Checkpoint lưu: "Đã xử lý đến Msg100"
Restart → Tiếp tục từ Msg101
```

**6. `.option("failOnDataLoss", "false")`**

- Kafka có retention time → message cũ bị xóa
- `false`: Không crash khi message bị mất
- `true`: Crash nếu detect data loss (strict mode)

**DataFrame schema sau khi load:**

```
kafkaDF:
├─ key: binary
├─ value: binary  ← JSON data (bytes)
├─ topic: string
├─ partition: int
├─ offset: long
├─ timestamp: timestamp
└─ timestampType: int
```

---

### **STEP 2: PARSE JSON DATA (Dòng 78-86)**

```python
parsedDF = kafkaDF.select(
    from_json(col("value").cast("string"), message_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")
```

**Breakdown:**

**Bước 1: `.cast("string")`**

```python
col("value").cast("string")
```

- `value` column là **binary** (bytes)
- Cast sang **string** để parse JSON

```
Binary: b'{"symbol":"BTCUSDT","price":42156.78,...}'
String: '{"symbol":"BTCUSDT","price":42156.78,...}'
```

**Bước 2: `from_json(..., message_schema)`**

```python
from_json(col("value").cast("string"), message_schema).alias("data")
```

- Parse JSON string theo schema
- Tạo **struct column** tên `data`

```
Before:
value (string): '{"symbol":"BTCUSDT","price":42156.78}'

After:
data (struct):
  ├─ symbol: "BTCUSDT"
  ├─ price: 42156.78
  └─ ...
```

**Bước 3: `.select("data.*", "kafka_timestamp")`**

```python
.select("data.*", "kafka_timestamp")
```

- `data.*`: Expand struct → separate columns
- Giữ lại `kafka_timestamp` (timestamp Kafka nhận message)

**Kết quả cuối cùng:**

```
parsedDF:
├─ symbol: string
├─ event_time: long
├─ price: double
├─ open: double
├─ high: double
├─ low: double
├─ volume: double
├─ quote_volume: double
├─ number_trades: int
├─ price_change: double
├─ price_change_percent: double
├─ timestamp: string
└─ kafka_timestamp: timestamp
```

---

### **STEP 3: DATA TRANSFORMATION (Dòng 87-99)**

```python
streamDF = parsedDF \
    .withColumn("event_timestamp", (col("event_time") / 1000).cast("timestamp")) \
    .withColumn("date", to_date(col("event_timestamp"))) \
    .withColumn("hour", hour(col("event_timestamp")))
```

**Transformation 1: Convert milliseconds → timestamp**

```python
.withColumn("event_timestamp", (col("event_time") / 1000).cast("timestamp"))
```

Producer gửi `event_time` dạng **milliseconds**:

```
event_time: 1733856000123  (milliseconds)
```

Convert sang **timestamp**:

```
Step 1: 1733856000123 / 1000 = 1733856000.123 (seconds)
Step 2: cast("timestamp") → 2025-12-10 12:00:00.123
```

**Transformation 2: Extract date**

```python
.withColumn("date", to_date(col("event_timestamp")))
```

```
event_timestamp: 2025-12-10 12:34:56
         ↓
date: 2025-12-10
```

**Transformation 3: Extract hour**

```python
.withColumn("hour", hour(col("event_timestamp")))
```

```
event_timestamp: 2025-12-10 12:34:56
         ↓
hour: 12
```

**Tại sao cần convert timestamp?**

Window aggregation cần **timestamp datatype**:

```python
window(col("event_timestamp"), "1 day")  # ✅ OK
window(col("event_time"), "1 day")        # ❌ FAIL (Long type)
```

---

### **STEP 4: WATERMARKING (Dòng 100-106)**

```python
watermarkedDF = streamDF.withWatermark("event_timestamp", "1 hour")
```

**Watermark là gì?**

Streaming data **không đồng bộ** - có thể đến muộn:

```
Timeline thực tế:
12:00:00 - Event A xảy ra
12:00:01 - Event B xảy ra
12:00:02 - Event C xảy ra

Data đến Consumer:
12:00:01 - Nhận Event A ✅
12:00:02 - Nhận Event C ✅
12:00:05 - Nhận Event B ❌ (Late 4 giây!)
```

**Watermark = "Chờ late data bao lâu?"**

```python
.withWatermark("event_timestamp", "1 hour")
```

Ý nghĩa:

- Chờ late data tối đa **1 giờ**
- Late > 1 giờ → BỎ QUA (quá muộn)

**Ví dụ thực tế:**

```
Current time: 13:05
Watermark: 13:05 - 1 hour = 12:05

┌────────────────────────────────────────┐
│ Data nhận được:                        │
├────────────────────────────────────────┤
│ event_time=13:04 ✅ OK                 │
│ event_time=13:00 ✅ OK                 │
│ event_time=12:30 ✅ OK (late 35 min)   │
│ event_time=12:05 ✅ OK (late 1 hour)   │
│ event_time=12:04 ❌ BỎ QUA (late > 1h) │
│ event_time=11:00 ❌ BỎ QUA (late > 1h) │
└────────────────────────────────────────┘
```

**Watermark ảnh hưởng đến Window:**

```
Window: 2025-12-10 00:00 → 2025-12-10 24:00
Watermark: 1 hour

Scenario:
- 2025-12-11 00:30: Vẫn nhận data 2025-12-10 23:30 ✅
- 2025-12-11 01:01: Không nhận data 2025-12-10 23:59 ❌
  → Window 2025-12-10 ĐÓNG → ghi file
```

**Tóm lại:** Watermark = "deadline" để window đóng lại và ghi kết quả.

---

### **STEP 5: DAILY AGGREGATION (Dòng 107-139)**

```python
dailyDF = watermarkedDF \
    .groupBy(
        window(col("event_timestamp"), "1 day"),
        col("symbol")
    ) \
    .agg(...)
```

**Window Aggregation:**

```python
window(col("event_timestamp"), "1 day")
```

Chia data thành **cửa sổ 1 ngày**:

```
Window 1: 2025-12-10 00:00:00 → 2025-12-11 00:00:00
Window 2: 2025-12-11 00:00:00 → 2025-12-12 00:00:00
Window 3: 2025-12-12 00:00:00 → 2025-12-13 00:00:00
```

**Group by window + symbol:**

```
Window: 2025-12-10
├─ BTCUSDT → Group 1
└─ ETHUSDT → Group 2

Window: 2025-12-11
├─ BTCUSDT → Group 3
└─ ETHUSDT → Group 4
```

**Aggregations:**

```python
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
)
```

**Giải thích từng aggregation:**

| Function               | Ý nghĩa                          | Ví dụ               |
| ---------------------- | -------------------------------- | ------------------- |
| `first("open")`        | Giá **open đầu tiên** trong ngày | 42000 (00:00:01)    |
| `max("high")`          | Giá **cao nhất** trong ngày      | 43500 (14:23:45)    |
| `min("low")`           | Giá **thấp nhất** trong ngày     | 41800 (09:15:30)    |
| `last("price")`        | Giá **cuối cùng** (close)        | 42156.78 (23:59:59) |
| `sum("volume")`        | Tổng volume 24h                  | 1234567.89 BTC      |
| `sum("quote_volume")`  | Tổng quote volume                | 52000000000 USDT    |
| `sum("number_trades")` | Tổng số trades                   | 567890 trades       |
| `count("*")`           | Số lần nhận data (ticks)         | 86400 (nếu 1s/tick) |
| `avg("price")`         | Giá trung bình                   | 42250.33            |

**OHLC Pattern (Open-High-Low-Close):**

```
Day: 2025-12-10
Timeline:
00:00:01 - Open:  42000  ← first("open")
09:15:30 - Low:   41800  ← min("low")
14:23:45 - High:  43500  ← max("high")
23:59:59 - Close: 42156  ← last("price")

         43500 (High)
           ↑
    42000  |──┐
(Open) ────┘  │
               │
    41800  ────┘
    (Low)
               ↓
         42156 (Close)
```

**Output columns:**

```python
.select(
    col("window.start").alias("date"),
    col("symbol"),
    col("daily_open"),
    col("daily_high"),
    ...
)
```

`window.start`: Thời điểm bắt đầu window (2025-12-10 00:00:00)

**Kết quả mẫu:**

```
date                | symbol   | daily_open | daily_high | daily_low | daily_close | daily_volume
2025-12-10 00:00:00 | BTCUSDT  | 42000.0    | 43500.0    | 41800.0   | 42156.78    | 1234567.89
2025-12-10 00:00:00 | ETHUSDT  | 2200.0     | 2250.0     | 2180.0    | 2235.67     | 45678.90
```

---

### **STEP 6: WRITE STREAMS (Dòng 140-220)**

Có **3 streaming queries** chạy song song:

```
┌──────────────────────────────────────┐
│ Query 1: Daily → Parquet             │
│ Query 2: Raw → Console (monitoring)  │
│ Query 3: Stats → Memory (SQL)        │
└──────────────────────────────────────┘
```

---

#### **Query 1: Daily Aggregates → Parquet**

```python
daily_query = dailyDF.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"{OUTPUT_PATH}/daily") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/daily") \
    .partitionBy("symbol") \
    .trigger(processingTime="10 seconds") \
    .start()
```

**Từng option:**

**1. `.outputMode("append")`**

3 modes:

- **`append`**: Chỉ ghi **data mới** (window đã đóng)
- `complete`: Ghi **toàn bộ** kết quả mỗi batch
- `update`: Chỉ ghi rows **thay đổi**

```
append mode:
Batch 1: Window 2025-12-09 đóng → ghi
Batch 2: Window 2025-12-10 đóng → ghi
Batch 3: (chưa có window nào đóng) → không ghi gì
```

**2. `.format("parquet")`**

Lưu dạng **Parquet**:

- Columnar storage (query nhanh)
- Compression (tiết kiệm disk)
- Schema evolution support

**3. `.option("path", "streaming_output_spark/daily")`**

Thư mục output:

```
streaming_output_spark/
└─ daily/
   ├─ symbol=BTCUSDT/
   │  ├─ part-00000.parquet
   │  └─ part-00001.parquet
   └─ symbol=ETHUSDT/
      ├─ part-00000.parquet
      └─ part-00001.parquet
```

**4. `.option("checkpointLocation", "checkpoint_spark/daily")`**

Checkpoint lưu:

- Offset đã xử lý
- State của aggregations
- Metadata

```
checkpoint_spark/daily/
├─ commits/
├─ metadata
├─ offsets/
└─ state/
```

**5. `.partitionBy("symbol")`**

Partition theo symbol:

```
symbol=BTCUSDT/ ← BTC data
symbol=ETHUSDT/ ← ETH data
```

Lợi ích:

- Query 1 symbol → chỉ scan 1 partition
- Parallel processing

**6. `.trigger(processingTime="10 seconds")`**

**Micro-batch processing:**

```
Timeline:
00:00 - Batch 1: Thu thập data từ 23:50 → 00:00 → xử lý
00:10 - Batch 2: Thu thập data từ 00:00 → 00:10 → xử lý
00:20 - Batch 3: Thu thập data từ 00:10 → 00:20 → xử lý
00:30 - Batch 4: Thu thập data từ 00:20 → 00:30 → xử lý
```

**7. `.start()`**

Bắt đầu streaming query (chạy background).

---

#### **Query 2: Raw Stream → Console (Monitoring)**

```python
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

**Mục đích:** MONITORING real-time

**1. `.select(...)` chỉ vài columns quan trọng**

Không cần hiển thị tất cả 15 columns → chỉ chọn:

- `symbol`, `price`, `volume`, `price_change_percent`, `event_timestamp`

**2. `.format("console")`**

Hiển thị lên **terminal/console**.

**3. `.option("truncate", "false")`**

```
truncate=true:  BTCUSDT → BTCUS...
truncate=false: BTCUSDT → BTCUSDT (full text)
```

**4. `.option("numRows", "10")`**

Mỗi batch chỉ hiển thị **10 rows đầu**.

**5. `.trigger(processingTime="30 seconds")`**

Hiển thị mỗi **30 giây** (không cần quá thường xuyên).

**Output mẫu:**

```
-------------------------------------------
Batch: 5
-------------------------------------------
+----------+---------+---------+--------------------+-------------------+
|symbol    |price    |volume   |price_change_percent|event_timestamp    |
+----------+---------+---------+--------------------+-------------------+
|BTCUSDT   |42156.78 |123.45   |1.23                |2025-12-10 12:00:05|
|ETHUSDT   |2235.67  |456.78   |-0.45               |2025-12-10 12:00:05|
|BTCUSDT   |42160.50 |234.56   |1.24                |2025-12-10 12:00:06|
|ETHUSDT   |2234.00  |567.89   |-0.52               |2025-12-10 12:00:06|
...
+----------+---------+---------+--------------------+-------------------+
```

---

#### **Query 3: Daily Stats → Memory (SQL Queries)**

```python
stats_query = dailyDF.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("crypto_daily_stats") \
    .trigger(processingTime="10 seconds") \
    .start()
```

**Khác biệt:**

**1. `.outputMode("complete")`**

```
complete mode:
Batch 1: Ghi toàn bộ kết quả (1 row)
Batch 2: Ghi toàn bộ kết quả (2 rows)
Batch 3: Ghi toàn bộ kết quả (3 rows)

→ Mỗi batch OVERWRITE toàn bộ table
```

**2. `.format("memory")`**

Lưu vào **RAM** (in-memory table), không ghi disk.

**3. `.queryName("crypto_daily_stats")`**

Tên table: `crypto_daily_stats`

**Cách dùng:**

Trong terminal khác (hoặc notebook):

```python
# Query bằng SQL
spark.sql("SELECT * FROM crypto_daily_stats").show()

# Filter by symbol
spark.sql("""
    SELECT symbol, daily_high, daily_low, daily_volume
    FROM crypto_daily_stats
    WHERE symbol='BTCUSDT'
""").show()

# Sort by volume
spark.sql("""
    SELECT * FROM crypto_daily_stats
    ORDER BY daily_volume DESC
""").show()
```

**Use cases:**

- Real-time dashboard
- API queries
- Monitoring alerts

---

### **PHẦN 8: MONITORING (Dòng 262-282)**

```python
print("\n" + "=" * 80)
print("STREAMING QUERIES ACTIVE")
print("=" * 80)
print("\nActive queries:")
for query in spark.streams.active:
    print(f"  - {query.name if query.name else query.id}")
```

**`spark.streams.active`:** List tất cả streaming queries đang chạy.

**Output:**

```
================================================================================
STREAMING QUERIES ACTIVE
================================================================================

Active queries:
  - crypto_daily_stats
  - <query_id_1>
  - <query_id_2>
  - <query_id_3>
```

```python
print("\n📊 Monitor status:")
print("  - Console output will show every 30 seconds")
print("  - Parquet files updated every 10 seconds")
print("  - Check checkpoint/ for progress")
print("  - Check streaming_output_spark/ for results")
```

Hướng dẫn user cách monitor.

```python
print("\n💡 To query in-memory stats, open another terminal:")
print("  spark.sql('SELECT * FROM crypto_daily_stats').show()")
```

Hướng dẫn query in-memory table.

---

### **PHẦN 9: GRACEFUL SHUTDOWN (Dòng 283-308)**

```python
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n\n⏹ Stopping all streaming queries...")
```

**`awaitAnyTermination()`:**

- Chờ cho đến khi **bất kỳ query nào** terminate
- Block main thread (giữ program chạy)

**`KeyboardInterrupt`:**

- User nhấn **Ctrl+C**
- Exception được catch

```python
    for query in spark.streams.active:
        print(f"  Stopping: {query.name if query.name else query.id}")
        query.stop()
```

**Graceful shutdown:**

- Loop qua **tất cả queries**
- Stop từng query một
- Đảm bảo checkpoint được lưu

```
Timeline:
1. User nhấn Ctrl+C
2. Catch KeyboardInterrupt
3. Stop query 1 → checkpoint saved
4. Stop query 2 → checkpoint saved
5. Stop query 3 → checkpoint saved
6. Stop query 4 → checkpoint saved
7. Tất cả queries đã stop an toàn ✅
```

**Tại sao cần graceful shutdown?**

```
Sai:
User nhấn Ctrl+C → kill -9 process → checkpoint KHÔNG LƯU
Next run: bắt đầu lại từ đầu hoặc bị lỗi

Đúng:
User nhấn Ctrl+C → catch exception → stop queries → save checkpoint
Next run: tiếp tục từ chỗ cũ ✅
```

```python
    try:
        daily_stats = spark.sql("SELECT * FROM crypto_daily_stats")
        print("\nDaily aggregates:")
        daily_stats.show(10, truncate=False)
    except:
        print("No daily stats available yet")
```

Hiển thị **final stats** trước khi tắt:

- Nếu có data → show 10 rows
- Nếu chưa có data (vừa start) → skip

```python
    spark.stop()
    print("\n✓ Spark session closed")
```

**`spark.stop()`:**

- Đóng Spark session
- Giải phóng resources (RAM, CPU, network)
- Cleanup temporary files

---

## 🔄 LUỒNG XỬ LÝ HOÀN CHỈNH

```
┌─────────────────────────────────────────────────────────────┐
│ 1. PRODUCER (websocket_producer.py)                        │
│    - Fetch Binance API mỗi 1 giây                          │
│    - Gửi JSON vào Kafka topic "crypto-prices"              │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. KAFKA TOPIC: "crypto-prices"                            │
│    - Lưu messages (buffer)                                 │
│    - Partitions, replication                               │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. SPARK CONSUMER (spark_streaming_consumer.py)            │
│                                                             │
│    Step 1: readStream từ Kafka                             │
│    Step 2: Parse JSON → DataFrame                          │
│    Step 3: Transform timestamps                            │
│    Step 4: Watermark (late data tolerance)                 │
│    Step 5: Daily window aggregation                        │
│    Step 6: Write 3 streams:                                │
│            - Daily → Parquet                               │
│            - Raw → Console                                 │
│            - Stats → Memory                                │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. OUTPUTS                                                  │
│                                                             │
│    streaming_output_spark/                                 │
│    ├─ daily/symbol=BTCUSDT/*.parquet                       │
│    └─ daily/symbol=ETHUSDT/*.parquet                       │
│                                                             │
│    checkpoint_spark/                                        │
│    └─ daily/ (progress metadata)                           │
│                                                             │
│    In-Memory Table: crypto_daily_stats                     │
│    Console: Real-time monitoring                           │
└─────────────────────────────────────────────────────────────┘
```

---

## ⚡ KEY CONCEPTS

### **1. Structured Streaming vs DStream**

| Aspect          | Structured Streaming | DStream (Old)   |
| --------------- | -------------------- | --------------- |
| API             | DataFrame/SQL        | RDD             |
| Abstraction     | High-level           | Low-level       |
| Optimization    | Catalyst optimizer   | Manual          |
| Late data       | Watermarking         | Manual handling |
| Fault tolerance | Checkpoint auto      | Manual          |

**→ Structured Streaming được khuyến nghị!**

---

### **2. Micro-batch Processing**

```
Timeline:
├─ [Batch 1] ─ 10s ─ [Batch 2] ─ 10s ─ [Batch 3] ─ 10s ─→
   ↓                  ↓                  ↓
   Process           Process           Process
   0-10s data        10-20s data       20-30s data
```

**Không phải:**

- Batch processing (xử lý 1 lần/ngày)
- True streaming (xử lý từng message)

**Là:** Micro-batch (xử lý mỗi 10 giây)

---

### **3. Output Modes**

| Mode       | Behavior                       | Use case             |
| ---------- | ------------------------------ | -------------------- |
| `append`   | Chỉ ghi rows mới (window đóng) | Parquet, logs        |
| `complete` | Ghi toàn bộ kết quả mỗi batch  | In-memory, dashboard |
| `update`   | Chỉ ghi rows thay đổi          | Database updates     |

---

### **4. Watermark Visualization**

```
Current time: 13:00
Watermark: 1 hour
Event time watermark: 13:00 - 1h = 12:00

Timeline:
11:00   11:30   12:00   12:30   13:00   13:30
  ❌      ❌      │       ✅      ✅      ✅
  DROP    DROP   │     ACCEPT  ACCEPT  ACCEPT
                 └─ Watermark boundary
```

---

### **5. Checkpoint Recovery**

```
Scenario 1: Normal run
├─ Process batch 1 → save checkpoint
├─ Process batch 2 → save checkpoint
└─ Process batch 3 → save checkpoint

Scenario 2: Crash at batch 2
├─ Process batch 1 → save checkpoint ✅
├─ Process batch 2 → CRASH ❌
└─ Restart → read checkpoint → resume from batch 2 ✅
```

---

## 🐛 TROUBLESHOOTING

### **Lỗi 1: Kafka connector not found**

```
Error: Failed to find data source: kafka
```

**Giải pháp:**

```python
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
```

Hoặc tải thủ công:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
```

---

### **Lỗi 2: Connection refused to Kafka**

```
Error: Connection to localhost:9092 refused
```

**Kiểm tra:**

```bash
# Kafka có chạy không?
docker ps | grep kafka

# Start Kafka
cd week6_streaming
docker-compose up -d
```

---

### **Lỗi 3: No data in output**

**Nguyên nhân:**

- Producer chưa chạy
- Window chưa đóng (chờ watermark)

**Kiểm tra:**

```bash
# Producer có chạy không?
python websocket_producer.py

# Console query có hiện data không?
# Nếu có → chờ window đóng
# Nếu không → producer lỗi
```

---

### **Lỗi 4: Out of memory**

```
Error: java.lang.OutOfMemoryError
```

**Giải pháp:**

```python
.config("spark.driver.memory", "8g")  # Tăng từ 4g lên 8g
.config("spark.executor.memory", "8g")
```

---

## 📊 MONITORING

### **1. Spark UI**

```
URL: http://localhost:4040
```

Tabs:

- **Jobs:** Xem batch processing
- **Stages:** Task execution details
- **Streaming:** Query statistics, processing times
- **SQL:** Query plans

---

### **2. Query Progress**

```python
# In another terminal
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Get query by name
query = spark.streams.active[0]

# Last progress
query.lastProgress

# Status
query.status
```

---

### **3. Output Files**

```bash
# Check Parquet files
ls -lh streaming_output_spark/daily/symbol=BTCUSDT/

# Read with Spark
df = spark.read.parquet("streaming_output_spark/daily")
df.show()

# Count rows
df.count()
```

---

### **4. Checkpoint**

```bash
# Checkpoint structure
tree checkpoint_spark/daily/

checkpoint_spark/daily/
├─ commits/
│  ├─ 0
│  ├─ 1
│  └─ 2
├─ metadata
├─ offsets/
│  ├─ 0
│  └─ 1
└─ state/
```

---

## 🎯 TÓM TẮT

**File:** `spark_streaming_consumer.py` - Speed Layer Consumer

**Chức năng:**

1. Đọc real-time data từ Kafka topic `crypto-prices`
2. Parse JSON → DataFrame
3. Transform timestamps, extract date/hour
4. Watermarking (late data tolerance: 1 hour)
5. Window aggregation (Daily)
6. Ghi 3 streams: Parquet (daily), Console, Memory

**Key concepts:**

- Structured Streaming (high-level API)
- Micro-batch processing (10s trigger)
- Watermark (handle late data)
- Checkpoint (fault tolerance)
- Output modes (append, complete)

**Integration:**

```
Producer → Kafka → Consumer (file này) → Parquet/Memory
                                        ↓
                            Serving Layer (week6_merge.py)
```

**Next:** Tìm hiểu `week6_merge.py` - Serving Layer (merge Batch + Speed)
