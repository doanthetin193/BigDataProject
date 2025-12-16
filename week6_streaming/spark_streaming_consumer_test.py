"""
spark_streaming_consumer_test.py - TEST VERSION (Watermark 1 phut)

DAY LA PHIEN BAN TEST:
- Watermark: 1 PHUT (thay vi 1 gio) - thay ket qua nhanh
- Output path: streaming_output_spark_TEST
- Checkpoint: checkpoint_spark_TEST
- Chi dung de test, KHONG dung production!

Goc: spark_streaming_consumer.py (watermark 1 gio - production)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# ============================================================================
# CONFIGURATION
# ============================================================================
import os
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto-prices"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # week6_streaming/
OUTPUT_PATH = os.path.join(BASE_DIR, "streaming_output_spark_TEST")
CHECKPOINT_PATH = os.path.join(BASE_DIR, "checkpoint_spark_TEST")

# ============================================================================
# SPARK SESSION
# ============================================================================
print("=" * 80)
print("SPARK STRUCTURED STREAMING - Crypto Price Analysis")
print("=" * 80)

spark = SparkSession.builder \
    .appName("CryptoPriceStructuredStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"\n✓ Spark {spark.version} initialized")
print(f"✓ Checkpoint location: {CHECKPOINT_PATH}")
print(f"✓ Output location: {OUTPUT_PATH}\n")

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

# ============================================================================
# STEP 1: READ STREAM FROM KAFKA
# ============================================================================
print("STEP 1: Reading stream from Kafka...")

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

print(f"✓ Connected to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"✓ Subscribed to topic: {KAFKA_TOPIC}\n")

# ============================================================================
# STEP 2: PARSE JSON DATA
# ============================================================================
print("STEP 2: Parsing JSON messages...")

parsedDF = kafkaDF.select(
    from_json(col("value").cast("string"), message_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

print("✓ JSON parsed successfully\n")

# ============================================================================
# STEP 3: DATA TRANSFORMATION
# ============================================================================
print("STEP 3: Transforming data...")

# Convert timestamp từ milliseconds
streamDF = parsedDF \
    .withColumn("event_timestamp", (col("event_time") / 1000).cast("timestamp")) \
    .withColumn("date", to_date(col("event_timestamp"))) \
    .withColumn("hour", hour(col("event_timestamp")))

print("✓ Timestamps converted")
print("✓ Date and hour extracted\n")

# ============================================================================
# STEP 4: WATERMARKING (Handle Late Data)
# ============================================================================
print("STEP 4: Applying watermark...")

watermarkedDF = streamDF.withWatermark("event_timestamp", "1 minute")  # TEST: 1 phut thay vi 1 gio

print("✓ Watermark: 1 MINUTE (TEST MODE - nhanh thay ket qua!)\n")

# ============================================================================
# STEP 5: WINDOW AGGREGATION - DAILY (TEST: 5 phut thay vi 1 ngay)
# ============================================================================
print("STEP 5: TEST aggregation (5-minute windows)...")

dailyDF = watermarkedDF \
    .groupBy(
        window(col("event_timestamp"), "5 minutes"),  # TEST: 5 phut
        col("symbol")
    ) \
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

print("✓ Window: 1 day")
print("✓ Aggregations: OHLC, Volume, Trades\n")

# ============================================================================
# STEP 6: WRITE STREAMS
# ============================================================================
print("STEP 6: Starting streaming queries...\n")

# Query 1: Daily data to Parquet
daily_query = dailyDF.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"{OUTPUT_PATH}/daily") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/daily") \
    .partitionBy("symbol") \
    .trigger(processingTime="10 seconds") \
    .start()

print("✓ Query 1: Daily aggregates → Parquet")
print(f"  Output: {OUTPUT_PATH}/daily")
print(f"  Trigger: 10 seconds")

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

print("✓ Query 3: Raw data → Console (monitoring)")
print(f"  Trigger: 30 seconds")

# Query 4: Real-time stats to Memory (for queries)
stats_query = dailyDF.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("crypto_daily_stats") \
    .trigger(processingTime="10 seconds") \
    .start()

print("✓ Query 4: Daily stats → Memory table")
print(f"  Table name: crypto_daily_stats")

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

print("\n💡 To query in-memory stats, open another terminal:")
print("  spark.sql('SELECT * FROM crypto_daily_stats').show()")

print("\nPress Ctrl+C to stop all queries\n")
print("=" * 80)

# ============================================================================
# WAIT FOR TERMINATION
# ============================================================================
try:
    # Wait for all queries
    spark.streams.awaitAnyTermination()
    
except KeyboardInterrupt:
    print("\n\n⏹ Stopping all streaming queries...")
    
    # Stop all queries gracefully
    for query in spark.streams.active:
        print(f"  Stopping: {query.name if query.name else query.id}")
        query.stop()
    
    print("\n✓ All queries stopped")
    print("✓ Checkpoints saved")
    
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
