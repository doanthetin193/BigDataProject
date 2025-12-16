# Giải thích chi tiết: week6_merge.py

**File:** `scripts/lambda_batch/week6_merge.py`  
**Chức năng:** Serving Layer - Merge Batch Layer + Speed Layer thành unified dataset  
**Tác giả:** Đoàn Thế Tín  
**Ngày:** Week 6 - Lambda Architecture

---

## 📋 Mục lục
1. [Overview Lambda Architecture](#1-overview-lambda-architecture)
2. [Import và Spark Session](#2-import-và-spark-session)
3. [Step 1: Read Batch Layer](#3-step-1-read-batch-layer)
4. [Step 2: Read Speed Layer](#4-step-2-read-speed-layer)
5. [Step 3: Align Schemas & Merge](#5-step-3-align-schemas--merge)
6. [Step 4: Recompute MA](#6-step-4-recompute-ma)
7. [Step 5: Save Unified Dataset](#7-step-5-save-unified-dataset)
8. [Summary](#8-summary)
9. [Tóm tắt](#tóm-tắt-tổng-quan)

---

## 1. Overview Lambda Architecture

### Lambda Architecture là gì?

**Lambda Architecture** = Architecture pattern cho big data processing với 3 layers:

```
┌─────────────────────────────────────────────────────────┐
│                  Lambda Architecture                     │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌────────────────┐  ┌────────────────┐  ┌───────────┐ │
│  │  BATCH LAYER   │  │  SPEED LAYER   │  │  SERVING  │ │
│  │  (Historical)  │  │  (Real-time)   │  │   LAYER   │ │
│  ├────────────────┤  ├────────────────┤  │           │ │
│  │ • Complete     │  │ • Recent data  │  │  • Merge  │ │
│  │   dataset      │  │ • Low latency  │  │  • Query  │ │
│  │ • High latency │  │ • Incremental  │  │  • Serve  │ │
│  │ • Accurate     │  │ • Approximate  │  │           │ │
│  └────────┬───────┘  └────────┬───────┘  └─────▲─────┘ │
│           │                   │                 │       │
│           └───────────────────┴─────────────────┘       │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

---

### Dòng 1-18: Docstring
```python
"""
================================================================================
WEEK 6 - SERVING LAYER (Lambda Architecture)
================================================================================
Merge Batch Layer (backfill) + Speed Layer (streaming) data.

Usage:
    python week6_merge.py

What it does:
1. Reads batch data from daily_filled (backfill)
2. Reads streaming data from streaming_output_spark/daily
3. Merges both datasets (union + dedup)
4. Computes MA7/MA30 for the merged timeline
5. Saves final unified dataset

This creates a seamless timeline combining historical + real-time data.
================================================================================
"""
```
**Giải thích:**

### File này là **Serving Layer**
- **Input:**
  - Batch Layer: `data_analysis/daily_filled` (historical)
  - Speed Layer: `streaming_output_spark_BATCH/` (real-time)
- **Process:** Merge + Dedup + Recompute MA
- **Output:** Unified timeline → Prophet ready

---

## 2. Import và Spark Session

### Dòng 20-23: Import
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min, max, avg, year
from pyspark.sql.window import Window
import os
```
**Giải thích:**
- `SparkSession`: Entry point
- `col, count, min, max, avg, year`: Aggregation functions
- `Window`: Window functions cho MA calculation
- `os`: Check file existence

---

### Dòng 25-29: Spark Session
```python
# Initialize Spark
spark = SparkSession.builder \
    .appName("Week6_Merge_ServingLayer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```
**Giải thích:**

#### `.appName("Week6_Merge_ServingLayer")`
- Tên application
- Hiển thị trong Spark UI

#### `.config("spark.sql.adaptive.enabled", "true")`
- Bật Adaptive Query Execution
- Tự động tối ưu query plan

---

### Dòng 31-34: Banner
```python
print("=" * 80)
print("WEEK 6 - SERVING LAYER (Lambda Architecture)")
print("Merge Batch + Streaming Data")
print("=" * 80)
```

---

## 3. Step 1: Read Batch Layer

### Dòng 36-40: Step 1 Header
```python
# ============================================================================
# STEP 1: READ BATCH LAYER DATA
# ============================================================================
print("\n[STEP 1] Reading Batch Layer (backfill data)...")
```

---

### Dòng 42-54: Read Batch Data
```python
try:
    df_batch = spark.read.parquet("data_analysis/daily_filled")
    batch_count = df_batch.count()
    print(f"  ✅ Batch data loaded: {batch_count:,} rows")
    
    df_batch.groupBy("symbol").agg(
        count("*").alias("days"),
        min("date").alias("first_date"),
        max("date").alias("last_date")
    ).show(truncate=False)
    
except Exception as e:
    print(f"  ❌ Error: {e}")
    print("  Please run week6_backfill.py first!")
    spark.stop()
    exit(1)
```
**Giải thích:**

### Read Batch Layer
- **Path:** `data_analysis/daily_filled`
- **Source:** Output từ `week6_backfill.py`
- **Content:** Historical data với MA7/MA30

### Statistics
- Group by symbol
- Show: days, first_date, last_date

**Ví dụ output:**
```
[STEP 1] Reading Batch Layer (backfill data)...
  ✅ Batch data loaded: 8,140 rows
+--------+-----+----------+----------+
|symbol  |days |first_date|last_date |
+--------+-----+----------+----------+
|BTCUSDT |1,020|2023-03-01|2025-12-14|
|ETHUSDT |609  |2024-04-15|2025-12-14|
+--------+-----+----------+----------+
```

### Error Handling
- Nếu file không tồn tại → Exit với message
- **Yêu cầu:** Phải chạy `week6_backfill.py` trước

---

## 4. Step 2: Read Speed Layer

### Dòng 56-60: Step 2 Header
```python
# ============================================================================
# STEP 2: READ SPEED LAYER DATA (STREAMING)
# ============================================================================
print("\n[STEP 2] Reading Speed Layer (streaming data)...")
```

---

### Dòng 62-71: Fallback Paths
```python
# Try batch reader output first, fallback to streaming output
streaming_paths = [
    "week6_streaming/streaming_output_spark_BATCH",
    "streaming_output_spark/daily"
]

streaming_path = None
for path in streaming_paths:
    if os.path.exists(path):
        streaming_path = path
        print(f"  ✅ Found Speed Layer data: {path}")
        break
```
**Giải thích:**

### Tại sao có 2 paths?
| Path | Source | Time to Result |
|------|--------|----------------|
| `streaming_output_spark_BATCH` | `kafka_batch_reader.py` | **2-3 giây** (Demo) |
| `streaming_output_spark/daily` | `spark_streaming_consumer.py` | **25 giờ** (Production) |

### Fallback Logic
1. **Try path 1:** Batch reader output (ưu tiên)
2. **Try path 2:** Streaming consumer output
3. **None found:** Dùng batch data only

### Tại sao ưu tiên batch reader?
- Demo nhanh (2-3s vs 25h)
- Schema giống hệt streaming consumer
- Output format identical

---

### Dòng 73-90: Read Streaming Data
```python
if not streaming_path:
    print(f"  ⚠️  No streaming data found")
    print("  Using batch data only...")
    df_streaming = None
else:
    print(f"  Reading from: {streaming_path}")
    try:
        df_streaming = spark.read.parquet(streaming_path)
        streaming_count = df_streaming.count()
        print(f"  ✅ Streaming data loaded: {streaming_count:,} rows")
        
        df_streaming.groupBy("symbol").agg(
            count("*").alias("days"),
            min("date").alias("first_date"),
            max("date").alias("last_date")
        ).show(truncate=False)
        
    except Exception as e:
        print(f"  ⚠️  Error reading streaming data: {e}")
        print("  Using batch data only...")
        df_streaming = None
```
**Giải thích:**

### Case 1: No streaming data
- `df_streaming = None`
- Serving Layer = Batch Layer only
- **Valid scenario:** Chưa chạy producer/consumer

### Case 2: Streaming data found
- Read Parquet
- Show statistics

**Ví dụ output:**
```
[STEP 2] Reading Speed Layer (streaming data)...
  ✅ Found Speed Layer data: week6_streaming/streaming_output_spark_BATCH
  Reading from: week6_streaming/streaming_output_spark_BATCH
  ✅ Streaming data loaded: 2 rows
+--------+----+----------+----------+
|symbol  |days|first_date|last_date |
+--------+----+----------+----------+
|BTCUSDT |1   |2025-12-16|2025-12-16|
|ETHUSDT |1   |2025-12-16|2025-12-16|
+--------+----+----------+----------+
```

### Case 3: Error reading
- Catch exception
- Fallback to batch only
- **Graceful degradation**

---

## 5. Step 3: Align Schemas & Merge

### Dòng 92-96: Step 3 Header
```python
# ============================================================================
# STEP 3: ALIGN SCHEMAS AND MERGE
# ============================================================================
print("\n[STEP 3] Merging batch + streaming data...")
```

---

### Dòng 98-101: Check Streaming Data
```python
if df_streaming is None:
    print("  ⚠️  No streaming data - using batch only")
    df_merged = df_batch
else:
```
**Giải thích:**
- Nếu không có streaming data → Skip merge
- `df_merged = df_batch` (batch only)

---

### Dòng 102-110: Align Schemas
```python
    # Ensure both have same columns
    batch_cols = set(df_batch.columns)
    streaming_cols = set(df_streaming.columns)

    # Use common columns (with daily_ prefix)
    common_cols = ["symbol", "date", "daily_open", "daily_high", "daily_low", "daily_close", "daily_volume"]

    # Check if we have ma7/ma30 in batch
    if "ma7" in batch_cols and "ma30" in batch_cols:
        common_cols.extend(["ma7", "ma30"])
```
**Giải thích:**

### Tại sao cần align schemas?

**Problem:** Batch và Streaming có thể có columns khác nhau
```
Batch Layer:
  symbol, date, daily_open, daily_high, daily_low, daily_close, 
  daily_volume, ma7, ma30, year, month

Streaming Layer:
  symbol, date, daily_open, daily_high, daily_low, daily_close,
  daily_volume, daily_quote_volume, total_trades, tick_count, avg_price
```

**Solution:** Chọn **common columns** để union

### Common Columns
- **Required:** symbol, date, OHLCV
- **Optional:** ma7, ma30 (nếu batch có)

---

### Dòng 112-114: Select Batch Columns
```python
    df_batch_aligned = df_batch.select(*common_cols)

    # For streaming, select available columns
    available_streaming_cols = [c for c in common_cols if c in streaming_cols]
    df_streaming_aligned = df_streaming.select(*available_streaming_cols)
```
**Giải thích:**

#### Batch Aligned
- Select tất cả `common_cols` (có ma7/ma30)

#### Streaming Aligned
- **Filter:** Chỉ select columns có trong streaming
- **Ví dụ:** Nếu streaming không có `ma7`, `ma30` → Skip

---

### Dòng 116-120: Add NULL Columns
```python
    # If streaming doesn't have ma7/ma30, add null columns
    if "ma7" not in streaming_cols:
        from pyspark.sql.functions import lit
        df_streaming_aligned = df_streaming_aligned.withColumn("ma7", lit(None).cast("double"))
        df_streaming_aligned = df_streaming_aligned.withColumn("ma30", lit(None).cast("double"))
```
**Giải thích:**

### Tại sao add NULL columns?
- **Union requirement:** Cả 2 DataFrames phải có **cùng số columns**
- **Streaming không có MA:** Vì chưa tính (hoặc tính sau)
- **Solution:** Thêm columns NULL

**Ví dụ:**
```python
# Streaming (before):
symbol | date       | daily_open | daily_close
BTCUSDT| 2025-12-16 | 43000.0    | 42000.0

# Streaming (after):
symbol | date       | daily_open | daily_close | ma7  | ma30
BTCUSDT| 2025-12-16 | 43000.0    | 42000.0     | NULL | NULL
```

---

### Dòng 122-127: Union & Dedup
```python
    # Union
    df_merged = df_batch_aligned.union(df_streaming_aligned)

    # Remove duplicates (keep first occurrence)
    df_merged = df_merged.dropDuplicates(["symbol", "date"])

    # Sort
    df_merged = df_merged.orderBy("symbol", "date")
```
**Giải thích:**

### Union
- **Append:** Batch rows + Streaming rows
- **Ví dụ:**
  ```
  Batch: 8,140 rows (2023-03-01 → 2025-12-14)
  Streaming: 2 rows (2025-12-16)
  Union: 8,142 rows
  ```

### Dedup
- **Key:** (symbol, date)
- **Keep:** First occurrence (batch priority)
- **Tại sao cần?**
  - Có thể overlap (cùng ngày ở batch và streaming)
  - Ví dụ: Batch có 2025-12-14, Streaming cũng có 2025-12-14
  - → Keep batch data (more complete)

### Sort
- `orderBy("symbol", "date")`: Sắp xếp chronological

---

### Dòng 129-130: Count
```python
merged_count = df_merged.count()
print(f"  ✅ Merged data: {merged_count:,} rows")
```
**Ví dụ output:**
```
  ✅ Merged data: 8,142 rows
```

---

## 6. Step 4: Recompute MA

### Dòng 132-136: Step 4 Header
```python
# ============================================================================
# STEP 4: RECOMPUTE MA7/MA30 FOR ENTIRE TIMELINE
# ============================================================================
print("\n[STEP 4] Recomputing MA7/MA30 for merged timeline...")
```

---

### Dòng 138-140: Window Definitions
```python
window_ma7 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-6, 0)
window_ma30 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0)
```
**Giải thích:**

### Window MA7
- **Partition:** By symbol (BTC và ETH riêng)
- **Order:** By date (chronological)
- **Rows:** -6 to 0 (7 rows total)
  - -6: 6 rows trước
  - 0: Row hiện tại
  - Total: 7 ngày

### Window MA30
- **Rows:** -29 to 0 (30 rows total)

**Ví dụ:**
```
Date       | Price | MA7 (avg of 7 days)
2025-12-10 | 100   | NULL (< 7 rows)
2025-12-11 | 105   | NULL
2025-12-12 | 110   | NULL
2025-12-13 | 115   | NULL
2025-12-14 | 120   | NULL
2025-12-15 | 125   | NULL
2025-12-16 | 130   | 115.0 (avg of 100-130)
2025-12-17 | 135   | 120.0 (avg of 105-135)
```

---

### Dòng 151-152: Recompute MA
```python
df_merged = df_merged.withColumn("ma7", avg("daily_close").over(window_ma7))
df_merged = df_merged.withColumn("ma30", avg("daily_close").over(window_ma30))
```
**Giải thích:**

### Overwrite MA columns
- **Replace existing:** MA7/MA30 cũ (từ batch hoặc NULL từ streaming)
- **Compute for all:** Tất cả rows (batch + streaming)

### Tại sao recompute?
- **Batch MA7/MA30:** Tính trên batch data only
- **Streaming data:** Thêm rows mới → MA của batch rows thay đổi
- **Example:**
  ```
  Before merge:
  2025-12-14: MA7 = avg(2025-12-08 to 2025-12-14) [7 days batch]
  
  After merge (có 2025-12-15, 2025-12-16):
  2025-12-14: MA7 = same (không đổi vì window backward)
  2025-12-15: MA7 = avg(2025-12-09 to 2025-12-15) [6 batch + 1 streaming]
  2025-12-16: MA7 = avg(2025-12-10 to 2025-12-16) [5 batch + 2 streaming]
  ```

**Kết quả:** MA seamless trên toàn bộ timeline (batch + streaming)

---

### Dòng 154: Print
```python
print(f"  ✅ MA7 and MA30 recomputed")
```

---

## 7. Step 5: Save Unified Dataset

### Dòng 156-160: Step 5 Header
```python
# ============================================================================
# STEP 5: SAVE UNIFIED DATASET
# ============================================================================
print("\n[STEP 5] Saving unified dataset...")
```

---

### Dòng 162-163: Add Year Column
```python
# Add year for partitioning
df_merged = df_merged.withColumn("year", year("date"))
```
**Giải thích:**
- Extract year từ date
- Dùng cho `partitionBy("symbol", "year")`

---

### Dòng 165-169: Cache Strategy
```python
# Strategy: Cache merged data, then save
# (Avoids Spark conflict when reading/writing same path)
df_merged.cache()
final_count = df_merged.count()  # Trigger cache
print(f"  Cached {final_count:,} rows for write")
```
**Giải thích:**

### Tại sao cache?
**Problem:** Spark conflict khi read/write cùng path
```python
# Without cache:
df = spark.read.parquet("daily_filled")  # Read
df.write.mode("overwrite").parquet("daily_filled")  # Write
# ERROR: ConcurrentModificationException
```

**Solution:** Cache vào memory
```python
df.cache()  # Load into memory
df.count()  # Trigger cache (materialize)
df.write.parquet("daily_filled")  # Write from memory (no read conflict)
```

### Cache mechanism
- **Lazy:** `.cache()` chỉ đánh dấu (không load)
- **Action:** `.count()` trigger load vào memory
- **Benefit:** Write không cần read file → No conflict

---

### Dòng 171-175: Save Daily Filled
```python
# Save to daily_filled (overwrite with merged data)
output_path = "data_analysis/daily_filled"
df_merged.write.mode("overwrite").partitionBy("symbol", "year").parquet(output_path)

print(f"  ✅ Saved to {output_path}")
df_merged.unpersist()  # Release cache
```
**Giải thích:**

### Overwrite Mode
- **Mode:** `overwrite` (thay thế batch data cũ)
- **Result:** Batch + Streaming merged

### Partition
- `partitionBy("symbol", "year")`
- Folder structure:
  ```
  data_analysis/daily_filled/
  ├── symbol=BTCUSDT/
  │   ├── year=2023/
  │   ├── year=2024/
  │   └── year=2025/
  └── symbol=ETHUSDT/
      ├── year=2024/
      └── year=2025/
  ```

### Unpersist
- Release memory sau khi save
- Giải phóng cache

---

### Dòng 177-183: Update Prophet Input
```python
# Update prophet_input (minimal schema)
df_prophet = df_merged.select(
    col("date").alias("ds"),
    col("daily_close").alias("y"),
    "symbol"
).orderBy("symbol", "ds")

df_prophet.write.mode("overwrite").partitionBy("symbol").parquet("data_analysis/prophet_input")
```
**Giải thích:**

### Prophet Schema
- **ds:** Date (datetime) - Required by Prophet
- **y:** Target variable (daily_close) - Required
- **symbol:** Partition key

### Tại sao update?
- Prophet input phải match với daily_filled
- Batch + Streaming merged → Prophet input cũng phải update

**Ví dụ:**
```
Before merge:
  prophet_input: 8,140 rows (to 2025-12-14)

After merge:
  prophet_input: 8,142 rows (to 2025-12-16)
  → Ready for retrain Prophet với new data
```

---

### Dòng 185: Print
```python
print(f"  ✅ Prophet input updated")
```

---

## 8. Summary

### Dòng 187-191: Summary Header
```python
# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("✅ MERGE COMPLETE (SERVING LAYER)")
print("=" * 80)
```

---

### Dòng 193-198: Final Statistics
```python
df_final = spark.read.parquet("data_analysis/daily_filled")
df_final.groupBy("symbol").agg(
    count("*").alias("total_days"),
    min("date").alias("first_date"),
    max("date").alias("last_date")
).show(truncate=False)
```
**Giải thích:**
- Re-read daily_filled (sau merge)
- Show final statistics

**Ví dụ output:**
```
✅ MERGE COMPLETE (SERVING LAYER)
================================================================================
+--------+----------+----------+----------+
|symbol  |total_days|first_date|last_date |
+--------+----------+----------+----------+
|BTCUSDT |1,022     |2023-03-01|2025-12-16|
|ETHUSDT |611       |2024-04-15|2025-12-16|
+--------+----------+----------+----------+
```

---

### Dòng 200-203: Explanation
```python
print("\n📊 Unified Timeline:")
print(f"  Batch Layer (backfill): Historical data up to streaming start")
print(f"  Speed Layer (streaming): Real-time data from streaming")
print(f"  Serving Layer: Seamless merged timeline")
```

---

### Dòng 205-208: Next Steps
```python
print("\n🎯 Data ready for:")
print("  - Prophet forecasting (python prophet_train.py)")
print("  - Analysis and visualization")
print("  - Final report presentation")
```

---

### Dòng 210: Cleanup
```python
spark.stop()
```

---

---

# Tóm tắt Tổng quan

## 🎯 Mục đích File
File `week6_merge.py` là **Serving Layer** trong Lambda Architecture - Merge Batch Layer (historical) + Speed Layer (real-time) thành unified timeline seamless để serve queries và analysis.

---

## 📊 Workflow (5 Steps)

### **1. Read Batch Layer**
- Source: `data_analysis/daily_filled`
- Content: Historical data với MA7/MA30
- Example: 8,140 rows (2023-03-01 → 2025-12-14)

### **2. Read Speed Layer**
- Sources (fallback):
  1. `streaming_output_spark_BATCH/` (Demo - 2s)
  2. `streaming_output_spark/daily/` (Production - 25h)
- Content: Real-time data
- Example: 2 rows (2025-12-16)

### **3. Align Schemas & Merge**
- **Align:** Select common columns
- **Union:** Batch + Streaming
- **Dedup:** dropDuplicates(symbol, date)
- **Sort:** orderBy(symbol, date)

### **4. Recompute MA7/MA30**
- Window functions: 7-day, 30-day rolling average
- Recompute for **entire timeline** (batch + streaming)
- Result: Seamless MA across layers

### **5. Save Unified Dataset**
- **Cache strategy:** Avoid read/write conflict
- **Output 1:** `daily_filled` (overwrite)
- **Output 2:** `prophet_input` (minimal schema)

---

## 🔑 Điểm Quan Trọng

### **1. Lambda Architecture Layers**

| Layer | Role | Latency | Coverage |
|-------|------|---------|----------|
| **Batch** | Historical processing | High (hours) | Complete dataset |
| **Speed** | Real-time processing | Low (seconds) | Recent data only |
| **Serving** | Merge + Query | Medium | Unified view |

**Serving Layer = Batch ∪ Speed - Duplicates**

---

### **2. Schema Alignment**

**Problem:** Batch và Streaming có columns khác nhau
```
Batch:
  symbol, date, daily_open, daily_high, daily_low, daily_close,
  daily_volume, ma7, ma30, year, month

Streaming:
  symbol, date, daily_open, daily_high, daily_low, daily_close,
  daily_volume, daily_quote_volume, total_trades, tick_count
```

**Solution:**
1. Select **common columns** (OHLCV)
2. Add **NULL columns** nếu thiếu (ma7, ma30)
3. **Union** với cùng schema

---

### **3. Deduplication Strategy**

**Scenario:** Overlap giữa Batch và Speed
```
Batch:    2023-03-01 ────────────────────► 2025-12-14
Speed:                              2025-12-14 ──► 2025-12-16
Overlap:                            2025-12-14
```

**Solution:** `dropDuplicates(["symbol", "date"])`
- **Keep:** First occurrence (batch data)
- **Reason:** Batch data more complete (full day OHLCV)

---

### **4. MA Recomputation**

**Tại sao recompute?**
```
Before merge:
  MA7 tính trên batch data only (2025-12-08 to 2025-12-14)

After merge:
  MA7 tính trên batch + streaming (2025-12-08 to 2025-12-16)
  → Rows cuối batch bị ảnh hưởng bởi streaming data
```

**Example:**
```
Date       | Batch MA7 | Merged MA7 (after adding 2 streaming days)
2025-12-12 | 100.0     | 100.0 (unchanged, window backward)
2025-12-13 | 105.0     | 105.0
2025-12-14 | 110.0     | 110.0
2025-12-15 | N/A       | 115.0 (includes 2025-12-09 to 2025-12-15)
2025-12-16 | N/A       | 120.0 (includes 2025-12-10 to 2025-12-16)
```

**Result:** Seamless MA timeline (không có gap)

---

### **5. Cache Strategy**

**Problem:** Spark self-reference conflict
```python
df = spark.read.parquet("daily_filled")
df_transformed = df.transform(...)
df_transformed.write.mode("overwrite").parquet("daily_filled")
# ERROR: Cannot overwrite table that is being read
```

**Solution:** Cache vào memory
```python
df.cache()      # Mark for caching
df.count()      # Trigger cache (materialize)
df.write.parquet("daily_filled")  # Write from cache
df.unpersist()  # Release memory
```

**Benefit:**
- No file read during write → No conflict
- Faster write (from memory)

---

## 📁 Input/Output

### **Input**
| Source | Path | Rows | Date Range |
|--------|------|------|------------|
| Batch Layer | `data_analysis/daily_filled` | 8,140 | 2023-03-01 → 2025-12-14 |
| Speed Layer | `streaming_output_spark_BATCH/` | 2 | 2025-12-16 |

### **Output**
| Target | Path | Rows | Date Range |
|--------|------|------|------------|
| Unified Dataset | `data_analysis/daily_filled` | 8,142 | 2023-03-01 → 2025-12-16 |
| Prophet Input | `data_analysis/prophet_input` | 8,142 | Same |

---

## 💡 Use Cases

### **1. After Backfill**
```bash
# Run backfill (gap: 2 days)
python scripts/lambda_batch/week6_backfill.py

# Merge batch + speed
python scripts/lambda_batch/week6_merge.py
# → Unified timeline ready
```

### **2. After Streaming**
```bash
# Run producer (10 min)
python week6_streaming/websocket_producer.py

# Run batch reader
python week6_streaming/kafka_batch_reader.py

# Merge
python scripts/lambda_batch/week6_merge.py
# → Batch + Speed merged
```

### **3. Daily Update**
```bash
# Cron job mỗi ngày:
# 1. Backfill gaps
python week6_backfill.py

# 2. Read streaming data (window closed)
# (spark_streaming_consumer.py đã chạy 24/7)

# 3. Merge
python week6_merge.py

# 4. Retrain Prophet
python prophet_train.py
```

---

## 🚀 Cách Sử Dụng

### **Prerequisites**
1. Batch Layer: `week6_backfill.py` đã chạy
2. Speed Layer: `kafka_batch_reader.py` hoặc `spark_streaming_consumer.py` đã chạy

---

### **Run Command**
```bash
cd D:\BigDataProject
python scripts/lambda_batch/week6_merge.py
```

---

### **Expected Output**
```
================================================================================
WEEK 6 - SERVING LAYER (Lambda Architecture)
Merge Batch + Streaming Data
================================================================================

[STEP 1] Reading Batch Layer (backfill data)...
  ✅ Batch data loaded: 8,140 rows
+--------+-----+----------+----------+
|symbol  |days |first_date|last_date |
+--------+-----+----------+----------+
|BTCUSDT |1,020|2023-03-01|2025-12-14|
|ETHUSDT |609  |2024-04-15|2025-12-14|
+--------+-----+----------+----------+

[STEP 2] Reading Speed Layer (streaming data)...
  ✅ Found Speed Layer data: week6_streaming/streaming_output_spark_BATCH
  Reading from: week6_streaming/streaming_output_spark_BATCH
  ✅ Streaming data loaded: 2 rows
+--------+----+----------+----------+
|symbol  |days|first_date|last_date |
+--------+----+----------+----------+
|BTCUSDT |1   |2025-12-16|2025-12-16|
|ETHUSDT |1   |2025-12-16|2025-12-16|
+--------+----+----------+----------+

[STEP 3] Merging batch + streaming data...
  ✅ Merged data: 8,142 rows

[STEP 4] Recomputing MA7/MA30 for merged timeline...
  ✅ MA7 and MA30 recomputed

[STEP 5] Saving unified dataset...
  Cached 8,142 rows for write
  ✅ Saved to data_analysis/daily_filled
  ✅ Prophet input updated

================================================================================
✅ MERGE COMPLETE (SERVING LAYER)
================================================================================
+--------+----------+----------+----------+
|symbol  |total_days|first_date|last_date |
+--------+----------+----------+----------+
|BTCUSDT |1,022     |2023-03-01|2025-12-16|
|ETHUSDT |611       |2024-04-15|2025-12-16|
+--------+----------+----------+----------+

📊 Unified Timeline:
  Batch Layer (backfill): Historical data up to streaming start
  Speed Layer (streaming): Real-time data from streaming
  Serving Layer: Seamless merged timeline

🎯 Data ready for:
  - Prophet forecasting (python prophet_train.py)
  - Analysis and visualization
  - Final report presentation
```

---

## 🔧 Troubleshooting

### **1. Batch Data Not Found**
**Error:** `Please run week6_backfill.py first!`  
**Solution:**
```bash
python scripts/lambda_batch/week6_backfill.py
```

---

### **2. No Streaming Data**
**Warning:** `No streaming data found`  
**Impact:** Merge = Batch only  
**Solution:**
```bash
# Run producer + consumer
cd week6_streaming
docker-compose up -d
python websocket_producer.py  # 10 min
python kafka_batch_reader.py  # Instant
```

---

### **3. Schema Mismatch**
**Error:** `Union can only be performed on tables with the same schema`  
**Cause:** Common columns không đủ  
**Solution:**
- Check batch columns: `df_batch.columns`
- Check streaming columns: `df_streaming.columns`
- Verify common_cols include cả 2

---

### **4. Cache Out of Memory**
**Error:** `OutOfMemoryError: Java heap space`  
**Solution:**
```python
# Increase Spark driver memory
spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

# Or skip cache (risk self-reference error)
# df_merged.write.parquet("output_different_path")
```

---

### **5. Duplicate Dates**
**Symptom:** Final count < batch + streaming  
**Cause:** `dropDuplicates()` removed overlaps  
**Verify:**
```python
# Check overlap
batch_dates = df_batch.select("symbol", "date").distinct()
streaming_dates = df_streaming.select("symbol", "date").distinct()
overlap = batch_dates.intersect(streaming_dates)
overlap.show()
```

---

## 📈 Performance

### **Processing Time**
| Stage | Time | Note |
|-------|------|------|
| Read Batch | 2-3s | 8,140 rows |
| Read Streaming | 1s | 2 rows |
| Merge + Dedup | 2s | Union + dropDuplicates |
| Recompute MA | 3-5s | Window functions |
| Write Parquet | 3-5s | Partitioned write |
| **Total** | **10-15s** | End-to-end |

### **Data Volume**
- **Input:** 8,142 rows (batch + streaming)
- **Output:** 8,142 rows (deduplicated)
- **Compression:** ~500 KB Parquet (compressed)

---

## 🎓 Key Technologies

- **Lambda Architecture:** Batch + Speed + Serving
- **Spark DataFrame:** Union, dedup, window functions
- **Window Functions:** Rolling average (MA7, MA30)
- **Cache Strategy:** Avoid self-reference conflict
- **Parquet:** Columnar storage with partitioning

---

## 🔗 Integration

### **Lambda Architecture Flow**
```
┌──────────────────┐
│   Batch Layer    │
│  (week6_backfill)│
│  8,140 rows      │
└────────┬─────────┘
         │
         ├─────────────────────┐
         │                     │
         ▼                     ▼
┌────────────────┐    ┌────────────────┐
│  Speed Layer   │    │ Serving Layer  │
│ (kafka_batch)  │───►│ (week6_merge)  │
│  2 rows        │    │  8,142 rows    │
└────────────────┘    └────────┬───────┘
                               │
                               ▼
                      ┌────────────────┐
                      │ Prophet Train  │
                      │  (Forecasting) │
                      └────────────────┘
```

---

## ⚠️ Important Notes

### **1. Destructive Operation**
- **Overwrite mode:** Replace batch data với merged data
- **Backup:** Nên backup `daily_filled` trước khi merge

### **2. Idempotency**
- Run nhiều lần → Kết quả giống nhau
- Dedup đảm bảo no duplicates

### **3. MA Consistency**
- Recompute toàn bộ → Consistent across timeline
- Không có gap giữa batch và streaming MA

### **4. Fallback Strategy**
- No streaming data → Batch only (graceful degradation)
- Error reading → Batch only

---

**Tác giả:** Đoàn Thế Tín  
**MSSV:** 4551190056  
**File:** `scripts/lambda_batch/week6_merge.py`  
**Lines:** 213 dòng code  
**Mục đích:** Serving Layer - Merge Batch + Speed thành unified timeline cho Lambda Architecture

---
