from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
import os

# Config Spark
spark = SparkSession.builder \
    .appName("CryptoPreprocessStep1") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.python.worker.reuse", "false") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.network.timeout", "120s") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Base dir
base_dir = r"D:\BigDataProject"

# Input paths (đã clean từ clean_parquet.py)
btc_path = os.path.join(base_dir, "data_parquet", "btc_clean")
eth_path = os.path.join(base_dir, "data_parquet", "eth_clean")

# Output paths
analysis_dir = os.path.join(base_dir, "data_analysis")
per_day_counts_path = os.path.join(analysis_dir, "per_day_counts")
large_gaps_path = os.path.join(analysis_dir, "large_gaps_top100")
daily_raw_parquet = os.path.join(analysis_dir, "daily_raw")
daily_raw_csv = os.path.join(analysis_dir, "daily_raw_csv")

os.makedirs(analysis_dir, exist_ok=True)

# Read BTC + ETH parquet (đã clean)
btc_df = spark.read.parquet(btc_path).withColumn("symbol", F.lit("BTCUSDT"))
eth_df = spark.read.parquet(eth_path).withColumn("symbol", F.lit("ETHUSDT"))

df = btc_df.unionByName(eth_df)
print(f"=== Loaded BTC+ETH data: {df.count():,} rows ===")

# Fill missing values (forward fill)
w_ff = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)
df = df.withColumn("open", F.last("open", True).over(w_ff)) \
       .withColumn("high", F.last("high", True).over(w_ff)) \
       .withColumn("low", F.last("low", True).over(w_ff)) \
       .withColumn("close", F.last("close", True).over(w_ff)) \
       .withColumn("volume", F.coalesce(F.last("volume", True).over(w_ff), F.lit(0.0)))

# Drop null rows
df = df.dropna(subset=["open", "high", "low", "close", "volume"])

# Drop duplicates
before = df.count()
df = df.dropDuplicates(["symbol", "timestamp"])
after = df.count()
print(f"Removed duplicates: {before - after:,} rows")

df.cache()

# Per-day counts
per_day = df.withColumn("date", F.to_date("datetime")) \
    .groupBy("symbol", "date").count().orderBy("date")
per_day.coalesce(4).write.mode("overwrite").csv(per_day_counts_path, header=True)
print(f"Saved per_day_counts -> {per_day_counts_path}")

# Detect large gaps (>60s)
w = Window.partitionBy("symbol").orderBy("timestamp")
df_gap = df.withColumn("prev_ts", F.lag("timestamp").over(w)) \
    .withColumn("gap", F.col("timestamp") - F.col("prev_ts")) \
    .filter(F.col("gap") > 60) \
    .orderBy(F.desc("gap"))
df_gap.limit(100).coalesce(4).write.mode("overwrite").csv(large_gaps_path, header=True)
print(f"Saved large_gaps_top100 -> {large_gaps_path}")

# Compute daily OHLC
df_with_date = df.withColumn("date", F.to_date("datetime"))
minmax = df_with_date.groupBy("symbol", "date").agg(
    F.min("timestamp").alias("min_ts"),
    F.max("timestamp").alias("max_ts")
)
opens = df_with_date.join(minmax, on=["symbol", "date"]).filter(F.col("timestamp") == F.col("min_ts")).select("symbol", "date", F.col("open").alias("daily_open"))
closes = df_with_date.join(minmax, on=["symbol", "date"]).filter(F.col("timestamp") == F.col("max_ts")).select("symbol", "date", F.col("close").alias("daily_close"))
basic = df_with_date.groupBy("symbol", "date").agg(
    F.max("high").alias("daily_high"),
    F.min("low").alias("daily_low"),
    F.sum("volume").alias("daily_volume"),
    F.count("*").alias("cnt")
)
daily = basic.join(opens, ["symbol", "date"], "left").join(closes, ["symbol", "date"], "left").orderBy("symbol", "date")

# Add year, month for partitioning
daily = daily.withColumn("year", F.year("date")).withColumn("month", F.month("date"))

# Save daily OHLC
daily.write.mode("overwrite").partitionBy("symbol", "year", "month").parquet(daily_raw_parquet)
daily.coalesce(4).write.mode("overwrite").csv(daily_raw_csv, header=True)

print(f"Saved daily_raw parquet -> {daily_raw_parquet}")
print(f"Saved daily_raw csv -> {daily_raw_csv}")

df.unpersist()
spark.stop()