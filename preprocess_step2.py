from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import os

# Config Spark
spark = SparkSession.builder \
    .appName("CryptoPreprocessStep2Full") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.task.maxFailures", "4") \
    .config("spark.python.worker.reuse", "false") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.network.timeout", "120s") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Đường dẫn
base_dir = r"D:\BigDataProject"
daily_path = os.path.join(base_dir, "data_analysis", "daily_raw")
results_path = os.path.join(base_dir, "data_analysis", "results")
missing_days_path = os.path.join(base_dir, "data_analysis", "missing_days")
vis_path = os.path.join(base_dir, "data_analysis", "visualizations")
prophet_path = os.path.join(base_dir, "data_analysis", "prophet_input")
daily_filled_path = os.path.join(base_dir, "data_analysis", "daily_filled")

os.makedirs(results_path, exist_ok=True)
os.makedirs(missing_days_path, exist_ok=True)
os.makedirs(vis_path, exist_ok=True)
os.makedirs(prophet_path, exist_ok=True)
os.makedirs(daily_filled_path, exist_ok=True)

# Load dữ liệu daily_raw
df = spark.read.parquet(daily_path).cache()
print("=== Loaded daily_raw ===")
df.printSchema()
print("Rows:", df.count())

# Detect + Fill missing days
symbols = [r.symbol for r in df.select("symbol").distinct().collect()]
daily_fulls = []

for sym in symbols:
    sub = df.filter(F.col("symbol") == sym)
    min_date, max_date = sub.agg(F.min("date"), F.max("date")).first()
    all_days = spark.sql(f"""
        SELECT sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day) as all_dates
    """).withColumn("date", F.explode("all_dates")).drop("all_dates")

    df_full = all_days.join(sub, on="date", how="left").orderBy("date")

    # Forward fill theo từng symbol
    w_ff = Window.partitionBy("symbol").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    df_filled = df_full.withColumn("daily_open", F.last("daily_open", True).over(w_ff)) \
                       .withColumn("daily_high", F.last("daily_high", True).over(w_ff)) \
                       .withColumn("daily_low", F.last("daily_low", True).over(w_ff)) \
                       .withColumn("daily_close", F.last("daily_close", True).over(w_ff)) \
                       .withColumn("daily_volume", F.coalesce("daily_volume", F.lit(0))) \
                       .withColumn("cnt", F.coalesce("cnt", F.lit(0))) \
                       .withColumn("symbol", F.lit(sym))

    # Moving averages theo từng symbol
    w7 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-6, 0)
    w30 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0)
    df_filled = df_filled.withColumn("ma7", F.avg("daily_close").over(w7)) \
                         .withColumn("ma30", F.avg("daily_close").over(w30))

    # Missing days
    missing = df_full.filter(F.col("daily_open").isNull()).select("date").withColumn("symbol", F.lit(sym))
    if missing.count() > 0:
        out_missing = os.path.join(missing_days_path, f"missing_days_{sym}")
        missing.coalesce(4).write.mode("overwrite").csv(out_missing, header=True)
        print(f"Saved missing_days for {sym} -> {out_missing}")
    else:
        print(f"No missing days for {sym}")

    daily_fulls.append(df_filled)

# Union tất cả symbols
daily_all = daily_fulls[0]
for d in daily_fulls[1:]:
    daily_all = daily_all.unionByName(d, allowMissingColumns=True)
daily_all = daily_all.withColumn("year", F.year("date")).withColumn("month", F.month("date"))

# Save parquet only (no CSV to avoid Parquet read conflict)
daily_all.write.mode("overwrite").partitionBy("symbol", "year", "month").parquet(daily_filled_path)
print(f"✅ Saved daily_filled -> {daily_filled_path}")

# Prophet input - Extract minimal schema
prophet_df = daily_all.select(F.col("date").alias("ds"), F.col("daily_close").alias("y"), "symbol")
prophet_df.write.mode("overwrite").partitionBy("symbol").parquet(prophet_path)
print(f"✅ Saved Prophet input -> {prophet_path}")

# Queries
daily_all.createOrReplaceTempView("crypto_daily")
queries = {
    "top_price_change_days": spark.sql("""
        SELECT symbol, date, ROUND(ABS(daily_close - daily_open) / daily_open * 100, 6) as price_change_percent
        FROM crypto_daily
        ORDER BY price_change_percent DESC
        LIMIT 10
    """),
    "top_volume_days": spark.sql("""
        SELECT symbol, date, daily_volume
        FROM crypto_daily
        ORDER BY daily_volume DESC
        LIMIT 10
    """),
    "avg_volume_per_month": spark.sql("""
        SELECT symbol, date_format(date, 'yyyy-MM') as month, AVG(daily_volume) as avg_volume
        FROM crypto_daily
        GROUP BY symbol, date_format(date, 'yyyy-MM')
        ORDER BY month, symbol
    """),
    "avg_return_per_year": spark.sql("""
        SELECT symbol, year(date) as year, AVG((daily_close - daily_open) / daily_open * 100) as avg_return
        FROM crypto_daily
        GROUP BY symbol, year(date)
        ORDER BY year, symbol
    """)
}
for name, q in queries.items():
    q.show(truncate=False)
    q.coalesce(4).write.mode("overwrite").csv(os.path.join(results_path, name), header=True)
print(f"Queries saved to {results_path}")

# Visualization
pdf = daily_all.orderBy("date").toPandas()
for sym in symbols:
    pdf_sym = pdf[pdf["symbol"] == sym].sort_values("date").tail(90)
    if pdf_sym.empty:
        continue
    # Close + MA
    plt.figure(figsize=(12, 6))
    plt.plot(pdf_sym["date"], pdf_sym["daily_close"], label="Close")
    plt.plot(pdf_sym["date"], pdf_sym["ma7"], label="MA7")
    plt.plot(pdf_sym["date"], pdf_sym["ma30"], label="MA30")
    plt.title(f"{sym} Close + MA (last 90 days)")
    plt.legend(); plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(vis_path, f"{sym}_close_ma.png"))
    plt.close()
    # Volume
    plt.figure(figsize=(12, 4))
    plt.bar(pdf_sym["date"], pdf_sym["daily_volume"])
    plt.title(f"{sym} Daily Volume (last 90 days)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(vis_path, f"{sym}_volume.png"))
    plt.close()

# So sánh BTC vs ETH
plt.figure(figsize=(12, 6))
for sym in symbols:
    pdf_sym = pdf[pdf["symbol"] == sym].sort_values("date").tail(90)
    plt.plot(pdf_sym["date"], pdf_sym["daily_close"], label=f"{sym} Close")
plt.title("BTC vs ETH Close (last 90 days)")
plt.legend(); plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(vis_path, "btc_vs_eth_close.png"))
plt.close()
print(f"✅ Saved plots -> {vis_path}")

# Cleanup
df.unpersist()
spark.stop()
