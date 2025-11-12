from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.functions import col, from_unixtime, to_timestamp, lit, year, month

# 1. Tạo SparkSession
spark = SparkSession.builder \
    .appName("CryptoCSVtoParquetPartitioned") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Đường dẫn dữ liệu
btc_path = r"D:\BigDataProject\data\btc\BTCUSDT_1min_2012-2025.csv"
eth_path = r"D:\BigDataProject\data\eth\ETHUSDT_1min_2017-2025.csv"

# 3. Đọc dữ liệu CSV
btc_df = spark.read.option("header", True).csv(btc_path, inferSchema=True)
eth_df = spark.read.option("header", True).csv(eth_path, inferSchema=True)

# 4. Chuẩn hóa BTC
btc_df = btc_df \
    .withColumnRenamed("Timestamp", "timestamp") \
    .withColumnRenamed("Open", "open") \
    .withColumnRenamed("High", "high") \
    .withColumnRenamed("Low", "low") \
    .withColumnRenamed("Close", "close") \
    .withColumnRenamed("Volume", "volume") \
    .withColumn("timestamp", col("timestamp").cast(LongType())) \
    .withColumn("datetime", to_timestamp(from_unixtime(col("timestamp")))) \
    .withColumn("symbol", lit("BTCUSDT")) \
    .withColumn("year", year("datetime")) \
    .withColumn("month", month("datetime"))

# 5. Chuẩn hóa ETH
eth_df = eth_df \
    .withColumnRenamed("Open", "open") \
    .withColumnRenamed("High", "high") \
    .withColumnRenamed("Low", "low") \
    .withColumnRenamed("Close", "close") \
    .withColumnRenamed("Volume", "volume") \
    .withColumn("timestamp", col("timestamp").cast(LongType())) \
    .withColumn("datetime", to_timestamp(from_unixtime(col("timestamp")))) \
    .withColumn("symbol", lit("ETHUSDT")) \
    .withColumn("year", year("datetime")) \
    .withColumn("month", month("datetime"))


# 6. Xuất thông tin
print("=== BTC Schema ===")
btc_df.printSchema()
btc_df.show(5, truncate=False)
print(f"BTC total rows: {btc_df.count()}")

print("=== ETH Schema ===")
eth_df.printSchema()
eth_df.show(5, truncate=False)
print(f"ETH total rows: {eth_df.count()}")

# 7. Lưu Parquet partition theo year, month
btc_df.write.mode("overwrite").partitionBy("year", "month").parquet(r"D:\BigDataProject\data_parquet\btc")
eth_df.write.mode("overwrite").partitionBy("year", "month").parquet(r"D:\BigDataProject\data_parquet\eth")

print("✅ Chuyển đổi CSV -> Parquet (partition by year/month) hoàn tất!")

# 8. Kiểm tra đọc lại Parquet
print("=== BTC Parquet Sample ===")
spark.read.parquet(r"D:\BigDataProject\data_parquet\btc").show(5, truncate=False)

print("=== ETH Parquet Sample ===")
spark.read.parquet(r"D:\BigDataProject\data_parquet\eth").show(5, truncate=False)

# 9. Stop Spark
spark.stop()
