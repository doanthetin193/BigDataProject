"""
kafka_batch_reader.py - Doc du lieu tu Kafka bang batch mode

Muc dich: Chung minh Speed Layer hoat dong (doc tu Kafka -> aggregation -> Parquet)
Su dung: python kafka_batch_reader.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

print("="*80)
print("KAFKA BATCH READER - Speed Layer Test")
print("="*80)

# Spark Session
spark = SparkSession.builder \
    .appName("KafkaBatchReader") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("\n✓ Spark initialized\n")

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

# Doc BATCH tu Kafka (khong phai streaming)
print("Reading ALL messages from Kafka (batch mode)...")
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-prices") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

print(f"✓ Read from Kafka topic: crypto-prices")
print(f"✓ Total messages: {df.count()}\n")

# Parse JSON
parsed_df = df.select(
    from_json(col("value").cast("string"), message_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

# Transform
transformed_df = parsed_df \
    .withColumn("event_timestamp", (col("event_time") / 1000).cast("timestamp")) \
    .withColumn("date", to_date(col("event_timestamp"))) \
    .withColumn("hour", hour(col("event_timestamp")))

print("Sample data:")
transformed_df.select("symbol", "price", "volume", "event_timestamp").show(10, False)

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

print("\nDaily aggregation:")
daily_df.show(20, False)

# Luu Parquet
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(BASE_DIR, "streaming_output_spark_BATCH")

daily_df.write \
    .mode("overwrite") \
    .partitionBy("symbol") \
    .parquet(OUTPUT_PATH)

print(f"\n✓ Saved to: {OUTPUT_PATH}")
print(f"✓ Total rows: {daily_df.count()}")
print(f"\n{'='*80}")
print("SUCCESS! Speed Layer data processed from Kafka")
print(f"{'='*80}\n")

spark.stop()
