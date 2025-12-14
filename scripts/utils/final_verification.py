"""
Final verification of all components
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from datetime import datetime

spark = SparkSession.builder.appName("FinalCheck").getOrCreate()

print("="*80)
print("FINAL VERIFICATION - Lambda Architecture Week 6")
print("="*80)

# 1. Check daily_filled
print("\n[1] DAILY_FILLED (Batch Layer - Source)")
df_daily = spark.read.parquet("data_analysis/daily_filled")
print(f"   Total rows: {df_daily.count():,}")
print(f"   Columns: {df_daily.columns}")

df_daily.groupBy("symbol").agg(
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date"),
    F.count("*").alias("total_days")
).show(truncate=False)

# Check today's date
today = datetime.now().date()
max_dates = df_daily.groupBy("symbol").agg(F.max("date").alias("max_date")).collect()
for row in max_dates:
    gap = (today - row['max_date']).days
    status = "✅ UP TO DATE" if gap <= 1 else f"❌ GAP: {gap} days"
    print(f"   {row['symbol']}: {status}")

# 2. Check prophet_input
print("\n[2] PROPHET_INPUT (Derived for Training)")
df_prophet = spark.read.parquet("data_analysis/prophet_input")
print(f"   Total rows: {df_prophet.count():,}")
print(f"   Columns: {df_prophet.columns}")
print(f"   Schema correct: {'✅' if set(df_prophet.columns) == {'ds', 'y', 'symbol'} else '❌'}")

# 3. Check Prophet metrics
print("\n[3] PROPHET MODEL METRICS")
metrics = pd.read_csv("data_analysis/week4_metrics/metrics.csv")
print(metrics.to_string(index=False))
avg_mape = metrics['mape'].mean()
avg_cv = metrics['cv_mape'].mean()
print(f"\n   Average MAPE: {avg_mape:.2f}%")
print(f"   Average CV MAPE: {avg_cv:.2f}%")
print(f"   Model quality: {'✅ GOOD' if avg_cv < 5 else '⚠️ NEEDS TUNING'}")

# 4. Check Lambda Architecture files
print("\n[4] LAMBDA ARCHITECTURE FILES")
import os
files = {
    "Batch Layer": "week6_backfill.py",
    "Batch Helper": "week6_backfill_batch.py", 
    "Serving Layer": "week6_merge.py",
    "Speed Producer": "week6_streaming/websocket_producer.py",
    "Speed Consumer": "week6_streaming/spark_streaming_consumer.py"
}

for name, path in files.items():
    exists = os.path.exists(path)
    print(f"   {name}: {'✅' if exists else '❌'} {path}")

# 5. Schema consistency check
print("\n[5] SCHEMA CONSISTENCY")
print("   daily_filled columns:")
for col in df_daily.columns:
    print(f"      - {col}")

expected_cols = ['date', 'daily_open', 'daily_high', 'daily_low', 'daily_close', 
                 'daily_volume', 'ma7', 'ma30', 'symbol']
has_all = all(col in df_daily.columns for col in expected_cols)
print(f"   Required columns present: {'✅' if has_all else '❌'}")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)

checks = [
    ("Data up to date", gap <= 1 if 'gap' in locals() else False),
    ("Prophet input schema", set(df_prophet.columns) == {'ds', 'y', 'symbol'}),
    ("Model quality (MAPE < 5%)", avg_cv < 5),
    ("Lambda files exist", all(os.path.exists(f) for f in files.values())),
    ("Schema consistency", has_all)
]

all_pass = all(status for _, status in checks)
for check, status in checks:
    print(f"   {'✅' if status else '❌'} {check}")

print("\n" + "="*80)
if all_pass:
    print("✅ ALL SYSTEMS READY - Lambda Architecture Week 6 COMPLETE!")
else:
    print("⚠️ SOME ISSUES FOUND - Review above")
print("="*80)

spark.stop()
