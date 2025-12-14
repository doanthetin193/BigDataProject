"""
Merge temp batch data into daily_filled and prophet_input
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("="*80)
print("MERGING TEMP BATCHES INTO DAILY_FILLED")
print("="*80)

spark = SparkSession.builder.appName("MergeTempBatch").getOrCreate()

# [STEP 1] Load temp batches
print("\n[STEP 1] Loading temp batch data...")
df_batch1 = spark.read.parquet("data_analysis/temp_batch1.parquet")
df_batch2 = spark.read.parquet("data_analysis/temp_batch2.parquet")

df_new = df_batch1.union(df_batch2)
print(f"  [OK] Total new data: {df_new.count():,} rows")

# [STEP 2] Aggregate to daily
print("\n[STEP 2] Aggregating 1-minute data to daily...")
df_daily = df_new.groupBy("symbol", "date").agg(
    F.first("open").alias("open"),
    F.max("high").alias("high"),
    F.min("low").alias("low"),
    F.last("close").alias("close"),
    F.sum("volume").alias("volume"),
    F.count("*").alias("cnt")
)

# Rename to daily_* format
df_daily = df_daily.select(
    "symbol",
    "date",
    F.col("open").alias("daily_open"),
    F.col("high").alias("daily_high"),
    F.col("low").alias("daily_low"),
    F.col("close").alias("daily_close"),
    F.col("volume").alias("daily_volume"),
    "cnt"
)

print(f"  [OK] Aggregated to {df_daily.count():,} daily rows")
df_daily.groupBy("symbol").agg(
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date"),
    F.count("*").alias("days")
).show(truncate=False)

# [STEP 3] Load existing daily_filled
print("\n[STEP 3] Loading existing daily_filled...")
df_old = spark.read.parquet("data_analysis/daily_filled")
old_count = df_old.count()
print(f"  [OK] Existing rows: {old_count:,}")

# CRITICAL: Persist df_old to avoid file corruption during overwrite
df_old = df_old.select("symbol", "date", "daily_open", "daily_high", "daily_low", 
                       "daily_close", "daily_volume", "cnt").cache()
df_old.count()  # Trigger cache

# [STEP 4] Merge: old + new, drop duplicates
print("\n[STEP 4] Merging old + new data...")
df_merged = df_old.union(df_daily).dropDuplicates(["symbol", "date"])

print(f"  [OK] After merge: {df_merged.count():,} rows")

# [STEP 5] Recalculate MA7 and MA30 for ALL data
print("\n[STEP 5] Recalculating MA7 and MA30...")
window_7 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-6, 0)
window_30 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0)

df_merged = df_merged.withColumn("ma7", F.avg("daily_close").over(window_7))
df_merged = df_merged.withColumn("ma30", F.avg("daily_close").over(window_30))

# Add year/month partitions
df_merged = df_merged.withColumn("year", F.year("date"))
df_merged = df_merged.withColumn("month", F.month("date"))

print(f"  [OK] MA calculated")

# [STEP 6] Save back to daily_filled
print("\n[STEP 6] Saving to daily_filled...")
df_merged.write.mode("overwrite") \
    .partitionBy("symbol", "year", "month") \
    .parquet("data_analysis/daily_filled")

print(f"  [OK] Saved {df_merged.count():,} rows")

# [STEP 7] Extract prophet_input (minimal schema)
print("\n[STEP 7] Extracting prophet_input...")
df_prophet = df_merged.select(
    F.col("date").alias("ds"),
    F.col("daily_close").alias("y"),
    "symbol"
)

df_prophet.write.mode("overwrite") \
    .partitionBy("symbol") \
    .parquet("data_analysis/prophet_input")

print(f"  [OK] Saved {df_prophet.count():,} rows to prophet_input")

# [STEP 8] Verify
print("\n" + "="*80)
print("VERIFICATION")
print("="*80)

df_check = spark.read.parquet("data_analysis/daily_filled")
df_check.groupBy("symbol").agg(
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date"),
    F.count("*").alias("total_days")
).show(truncate=False)

print("\n" + "="*80)
print("MERGE COMPLETE!")
print("="*80)
print("Next: Run prophet_train.py with full dataset")

spark.stop()
