"""
Preprocess & analysis (optimized for week 3, fixed for Windows)
- Read parquet (BTC/ETH), normalize schema, union safely
- Check/fix missing values and duplicates
- Detect missing days, incomplete days, large gaps (>60s)
- Compute daily OHLC (open at min_ts, close at max_ts), daily_volume
- Forward-fill missing days with OHLC preservation
- Compute MA7/MA30
- Save outputs (Parquet + CSV) partitioned, and PNG plots
- Prepare Prophet input
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, from_unixtime, lit,
    min as spark_min, max as spark_max, sum as spark_sum, count as spark_count,
    avg as spark_avg, lag, last, coalesce, sequence, explode, desc, year, month
)
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import pandas as pd

# ---------- CONFIG ----------
PARQUET_BTC = r"D:\BigDataProject\data_parquet\btc"  # Cập nhật nếu dùng thư mục sạch
PARQUET_ETH = r"D:\BigDataProject\data_parquet\eth"  # Cập nhật nếu dùng thư mục sạch
OUT_DIR = r"D:\BigDataProject\data_analysis"
PLOTS_DIR = os.path.join(OUT_DIR, "plots")
EXPECTED_PER_DAY = 24 * 60  # 1440 minutes
PLOT_LAST_N_DAYS = 90  # Giảm để tiết kiệm bộ nhớ
SPARK_DRIVER_MEM = "6g"  # Giảm để tránh memory issues
SPARK_SHUFFLE_PARTITIONS = "200"  # Giảm để giảm shuffle
# ----------------------------

# Tạo thư mục output
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(PLOTS_DIR, exist_ok=True)

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Preprocess_Optimized") \
    .config("spark.driver.memory", SPARK_DRIVER_MEM) \
    .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS) \
    .config("spark.task.maxFailures", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def normalize_cols(df):
    """Chuẩn hóa tên cột và đảm bảo timestamp/datetime"""
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
    elif 'datetime' in df.columns:
        df = df.withColumn("datetime", col("datetime").cast("timestamp"))
        if "timestamp" not in df.columns:
            df = df.withColumn("timestamp", col("datetime").cast("long"))
    else:
        raise RuntimeError("No timestamp/datetime column found.")
    return df.select("timestamp", "datetime", "symbol", *[c for c in df.columns if c not in ("timestamp", "datetime", "symbol")])

def process_single_coin(parquet_path, symbol_hint):
    """Đọc Parquet, chuẩn hóa, thêm symbol nếu cần"""
    df = spark.read.parquet(parquet_path)
    df = normalize_cols(df)
    if "symbol" not in df.columns:
        df = df.withColumn("symbol", lit(symbol_hint))
    return df

# Đọc và chuẩn hóa dữ liệu
btc_df = process_single_coin(PARQUET_BTC, "BTCUSDT")
eth_df = process_single_coin(PARQUET_ETH, "ETHUSDT")
union_df = btc_df.unionByName(eth_df, allowMissingColumns=True)

# In schema để kiểm tra
print("Union DataFrame schema:")
union_df.printSchema()

# Kiểm tra missing values
missing_counts = union_df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in ["open", "high", "low", "close", "volume"]])
print("Missing values check:")
missing_counts.show()

# Xử lý missing values (forward-fill)
w_sym = Window.partitionBy("symbol").orderBy("timestamp")
for col_name in ["open", "high", "low", "close", "volume"]:
    union_df = union_df.withColumn(col_name, last(col_name, True).over(w_sym))
union_df = union_df.dropna(subset=["timestamp", "open", "high", "low", "close", "volume"])

# Kiểm tra duplicates
duplicates = union_df.groupBy("symbol", "timestamp").agg(spark_count("*").alias("cnt")).filter(col("cnt") > 1)
duplicates_count = duplicates.count()
if duplicates_count > 0:
    print(f"Found {duplicates_count} duplicate timestamps. Dropping duplicates.")
    union_df = union_df.dropDuplicates(["symbol", "timestamp"])

# Cache và đếm dòng
union_df = union_df.cache()
print("Total rows (union, after cleaning):", union_df.count())

# Thêm cột date
union_df = union_df.withColumn("date", to_date(col("datetime")))

# Kiểm tra số dòng mỗi ngày
per_day = union_df.groupBy("symbol", "date").agg(spark_count("*").alias("cnt")).orderBy("symbol", "date")
per_day_out = os.path.join(OUT_DIR, "per_day_counts")
per_day.coalesce(4).write.mode("overwrite").csv(per_day_out, header=True)

# Phát hiện missing days
symbols = [row['symbol'] for row in union_df.select("symbol").distinct().collect()]
for sym in symbols:
    df_sym = union_df.filter(col("symbol") == sym)
    bounds = df_sym.select(spark_min("date").alias("start"), spark_max("date").alias("end")).collect()[0]
    start_date, end_date = bounds['start'], bounds['end']
    print(f"{sym} date range: {start_date} -> {end_date}")
    full_dates = spark.createDataFrame([(start_date, end_date)], ["start", "end"]) \
        .select(explode(sequence(col("start"), col("end"))).alias("date"))
    per_day_sym = per_day.filter(col("symbol") == sym).select("date", "cnt")
    missing_days = full_dates.join(per_day_sym, "date", "left_anti").orderBy("date")
    missing_days_path = os.path.join(OUT_DIR, f"missing_days_{sym}")
    missing_days.coalesce(1).write.mode("overwrite").csv(missing_days_path, header=True)  # Giảm coalesce để tránh memory issues
    print(f"Saved missing days for {sym} -> {missing_days_path}")

# Phát hiện large gaps (>60s)
gaps = union_df.select("symbol", "timestamp", "datetime") \
    .withColumn("prev_ts", lag("timestamp").over(w_sym)) \
    .withColumn("gap_sec", col("timestamp") - col("prev_ts")) \
    .filter(col("prev_ts").isNotNull() & (col("gap_sec") > 60)) \
    .orderBy(desc("gap_sec"))
gaps_out = os.path.join(OUT_DIR, "large_gaps_top100")
gaps.limit(100).coalesce(1).write.mode("overwrite").csv(gaps_out, header=True)  # Giảm coalesce
print("Saved top gaps ->", gaps_out)

# Tính daily OHLC (open tại min_ts, close tại max_ts)
minmax = union_df.groupBy("symbol", "date").agg(
    spark_min("timestamp").alias("min_ts"),
    spark_max("timestamp").alias("max_ts")
)
opens = union_df.join(minmax, on=["symbol", "date"]).filter(col("timestamp") == col("min_ts")).select("symbol", "date", col("open").alias("daily_open"))
closes = union_df.join(minmax, on=["symbol", "date"]).filter(col("timestamp") == col("max_ts")).select("symbol", "date", col("close").alias("daily_close"))
basic = union_df.groupBy("symbol", "date").agg(
    spark_sum("volume").alias("daily_volume"),
    spark_max("high").alias("daily_high"),
    spark_min("low").alias("daily_low"),
    spark_count("*").alias("cnt")
)
daily_raw = basic.join(opens, ["symbol", "date"], "left").join(closes, ["symbol", "date"], "left").orderBy("symbol", "date")
daily_raw_parquet = os.path.join(OUT_DIR, "daily_raw_parquet")
daily_raw.coalesce(4).write.mode("overwrite").parquet(daily_raw_parquet)
daily_raw.coalesce(4).write.mode("overwrite").csv(os.path.join(OUT_DIR, "daily_raw_csv"), header=True)
print("Saved daily_raw outputs.")

# Điền missing days và forward-fill OHLC
daily_fulls = []
for sym in symbols:
    df_sym = daily_raw.filter(col("symbol") == sym).select("date", "daily_open", "daily_high", "daily_low", "daily_close", "daily_volume", "cnt")
    bounds = df_sym.select(spark_min("date").alias("start"), spark_max("date").alias("end")).collect()[0]
    start_date, end_date = bounds['start'], bounds['end']
    full_dates = spark.createDataFrame([(start_date, end_date)], ["start", "end"]) \
        .select(explode(sequence(col("start"), col("end"))).alias("date"))
    daily_full = full_dates.join(df_sym, "date", "left").orderBy("date")
    w_ff = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    daily_filled = daily_full.withColumn("daily_open", last("daily_open", True).over(w_ff)) \
                            .withColumn("daily_high", last("daily_high", True).over(w_ff)) \
                            .withColumn("daily_low", last("daily_low", True).over(w_ff)) \
                            .withColumn("daily_close", last("daily_close", True).over(w_ff)) \
                            .withColumn("daily_volume", coalesce(col("daily_volume"), lit(0))) \
                            .withColumn("cnt", coalesce(col("cnt"), lit(0))) \
                            .withColumn("symbol", lit(sym))
    win_ma7 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-6, 0)
    win_ma30 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0)
    daily_filled = daily_filled.withColumn("ma7", spark_avg("daily_close").over(win_ma7)) \
                              .withColumn("ma30", spark_avg("daily_close").over(win_ma30))
    daily_filled = daily_filled.select("symbol", "date", "daily_open", "daily_high", "daily_low", "daily_close", "daily_volume", "cnt", "ma7", "ma30")
    daily_fulls.append(daily_filled)

# Union daily_filled
daily_all = daily_fulls[0]
for df_sym in daily_fulls[1:]:
    daily_all = daily_all.unionByName(df_sym, allowMissingColumns=True)
daily_all = daily_all.withColumn("year", year(col("date"))).withColumn("month", month(col("date")))
daily_out = os.path.join(OUT_DIR, "daily_filled_parquet")
daily_all.write.mode("overwrite").partitionBy("symbol", "year", "month").parquet(daily_out)
daily_all.coalesce(4).write.mode("overwrite").csv(os.path.join(OUT_DIR, "daily_filled_csv"), header=True)
print("Saved daily_filled (partitioned) ->", daily_out)

# Chuẩn bị input cho Prophet
prophet_df = daily_all.select(col("date").alias("ds"), col("daily_close").alias("y"), "symbol")
prophet_df.coalesce(4).write.mode("overwrite").parquet(os.path.join(OUT_DIR, "prophet_input"))
print("Saved Prophet input ->", os.path.join(OUT_DIR, "prophet_input"))

# SparkSQL queries
union_df.createOrReplaceTempView("crypto_raw")
daily_all.createOrReplaceTempView("crypto_daily")
avg_close_per_day = spark.sql("""
    SELECT symbol, date, ROUND(AVG(daily_close), 6) as avg_close
    FROM crypto_daily
    GROUP BY symbol, date
    ORDER BY symbol, date DESC
    LIMIT 10
""")
avg_close_per_day.show(truncate=False)
avg_close_per_day.coalesce(4).write.mode("overwrite").csv(os.path.join(OUT_DIR, "avg_close_per_day"), header=True)
top_volume_days = spark.sql("""
    SELECT symbol, date, daily_volume
    FROM crypto_daily
    ORDER BY daily_volume DESC
    LIMIT 10
""")
top_volume_days.show(truncate=False)
top_volume_days.coalesce(4).write.mode("overwrite").csv(os.path.join(OUT_DIR, "top_volume_days"), header=True)

# Trực quan hóa
pdf = daily_all.orderBy("date").select("symbol", "date", "daily_close", "ma7", "ma30", "daily_volume").toPandas()
for sym in symbols:
    pdf_sym = pdf[pdf['symbol'] == sym].sort_values("date").tail(PLOT_LAST_N_DAYS)
    if pdf_sym.shape[0] == 0:
        print("No data to plot for", sym)
        continue
    # Plot close + MA
    plt.figure(figsize=(12, 6))
    plt.plot(pdf_sym['date'], pdf_sym['daily_close'], label='Close')
    plt.plot(pdf_sym['date'], pdf_sym['ma7'], label='MA7')
    plt.plot(pdf_sym['date'], pdf_sym['ma30'], label='MA30')
    plt.legend()
    plt.title(f"{sym} Close & MA (last {PLOT_LAST_N_DAYS} days)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, f"{sym}_close_ma.png"))
    plt.close()
    # Plot volume
    plt.figure(figsize=(12, 4))
    plt.bar(pdf_sym['date'], pdf_sym['daily_volume'])
    plt.title(f"{sym} Daily Volume (last {PLOT_LAST_N_DAYS} days)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, f"{sym}_daily_volume.png"))
    plt.close()
    print("Saved plots for", sym)

# So sánh BTC vs ETH
plt.figure(figsize=(12, 6))
for sym in symbols:
    pdf_sym = pdf[pdf['symbol'] == sym].sort_values("date").tail(PLOT_LAST_N_DAYS)
    plt.plot(pdf_sym['date'], pdf_sym['daily_close'], label=f"{sym} Close")
plt.legend()
plt.title(f"BTC vs ETH Close (last {PLOT_LAST_N_DAYS} days)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, "btc_vs_eth_close.png"))
plt.close()

# Giải phóng bộ nhớ
union_df.unpersist()
print("All done. Outputs in:", OUT_DIR)
spark.stop()