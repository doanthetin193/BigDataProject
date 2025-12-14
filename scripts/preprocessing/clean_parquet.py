from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_unixtime
import os

# Config Spark
spark = SparkSession.builder \
    .appName("CleanParquet") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Base directory
base_dir = r"D:\BigDataProject"

# Paths
btc_path = os.path.join(base_dir, "data_parquet", "btc")
eth_path = os.path.join(base_dir, "data_parquet", "eth")
btc_clean_path = os.path.join(base_dir, "data_parquet", "btc_clean")
eth_clean_path = os.path.join(base_dir, "data_parquet", "eth_clean")

# Normalize columns
def normalize_cols(df):
    df = df.toDF(*[c.lower() for c in df.columns])
    cols = df.columns
    rename_map = {
        'timestamp': 'timestamp', 'unix': 'timestamp', 'time': 'timestamp', 'ts': 'timestamp',
        'open': 'open', 'open_': 'open', 'high': 'high', 'low': 'low',
        'close': 'close', 'volume': 'volume', 'volume_btc': 'volume', 'volume_eth': 'volume',
        'symbol': 'symbol', 'date': 'date', 'datetime': 'datetime'
    }
    for src, dst in rename_map.items():
        if src in cols and src != dst:
            df = df.withColumnRenamed(src, dst)
    if 'timestamp' in df.columns:
        df = df.withColumn("timestamp", col("timestamp").cast("long"))
        if "datetime" not in df.columns:
            df = df.withColumn("datetime", to_timestamp(from_unixtime(col("timestamp"))))
    return df.select("timestamp", "datetime", *[c for c in df.columns if c not in ("timestamp", "datetime")])

# Clean duplicates
def clean_parquet(symbol: str, path: str, out_path: str):
    print(f"\n=== Checking duplicates for {symbol} ===")
    df = spark.read.parquet(path)
    df = normalize_cols(df)
    total_rows = df.count()
    print(f"Total rows in {symbol}: {total_rows:,}")
    duplicates = df.groupBy("timestamp").count().filter(col("count") > 1)
    dup_count = duplicates.count()
    if dup_count > 0:
        print(f"⚠ Found {dup_count:,} duplicate timestamps in {symbol}. Cleaning...")
        df_clean = df.dropDuplicates(["timestamp"])
        df_clean.write.mode("overwrite").partitionBy("year", "month").parquet(out_path)
        print(f"✅ Cleaned data written to {out_path}")
    else:
        print(f"✅ No duplicates found in {symbol}. Keeping original data.")
        df.write.mode("overwrite").partitionBy("year", "month").parquet(out_path)
        print(f"➡ Copied original data to {out_path}")

# Run cleaning
clean_parquet("BTCUSDT", btc_path, btc_clean_path)
clean_parquet("ETHUSDT", eth_path, eth_clean_path)

spark.stop()
