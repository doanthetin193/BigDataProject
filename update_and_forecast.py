"""
Real-time Update Script - Chạy để cập nhật dữ liệu và forecast mới
Mỗi lần chạy sẽ:
1. Thu thập data mới (chỉ lấy từ lần chạy trước đến hiện tại)
2. Xử lý và merge với data cũ
3. Tạo forecast mới với timestamp
4. Tạo visualizations mới
"""
import subprocess
import sys
from datetime import datetime
import os

RUN_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print("=" * 80)
print("REAL-TIME UPDATE & FORECAST")
print(f"Run at: {RUN_TIMESTAMP}")
print("=" * 80)

# ==========================================================
# STEP 1: Collect new data (incremental)
# ==========================================================
print("\n" + "=" * 80)
print("STEP 1: Collecting New Data (Incremental)")
print("=" * 80)
print("This will only fetch data from last collection to now")
print("-" * 80)

try:
    result = subprocess.run(
        [sys.executable, "streaming_collector.py"],
        capture_output=True,
        text=True,
        timeout=600
    )
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    if result.returncode != 0:
        print(f"Step 1 failed with exit code {result.returncode}")
        sys.exit(1)
    else:
        print("Step 1 completed!")
        
except Exception as e:
    print(f"Step 1 error: {str(e)}")
    sys.exit(1)

# ==========================================================
# STEP 2: Process with Pandas
# ==========================================================
print("\n" + "=" * 80)
print("STEP 2: Processing and Merging Data")
print("=" * 80)
print("-" * 80)

try:
    result = subprocess.run(
        [sys.executable, "pandas_processor.py"],
        capture_output=True,
        text=True,
        timeout=300
    )
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    if result.returncode != 0:
        print(f"Step 2 failed with exit code {result.returncode}")
        sys.exit(1)
    else:
        print("Step 2 completed!")
        
except Exception as e:
    print(f"Step 2 error: {str(e)}")
    sys.exit(1)

# ==========================================================
# STEP 3: Forecast with Prophet
# ==========================================================
print("\n" + "=" * 80)
print("STEP 3: Forecasting with Prophet")
print("=" * 80)
print("Creating new forecast with updated data...")
print("-" * 80)

try:
    result = subprocess.run(
        [sys.executable, "streaming_forecast.py"],
        capture_output=True,
        text=True,
        timeout=1800  # 30 minutes
    )
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    if result.returncode != 0:
        print(f"Step 3 failed with exit code {result.returncode}")
        sys.exit(1)
    else:
        print("Step 3 completed!")
        
except Exception as e:
    print(f"Step 3 error: {str(e)}")
    sys.exit(1)

# ==========================================================
# SUMMARY
# ==========================================================
print("\n" + "=" * 80)
print("REAL-TIME UPDATE COMPLETED!")
print("=" * 80)
print(f"Started:  {RUN_TIMESTAMP}")
print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Check output folders
base_dir = r"D:\BigDataProject"
forecast_latest = os.path.join(base_dir, "data_analysis", "streaming_forecasts", "latest")
vis_latest = os.path.join(base_dir, "data_analysis", "streaming_visualizations", "latest")

print("\nLatest Results:")
print(f"   Forecasts: {forecast_latest}")
print(f"   Charts:    {vis_latest}")

print("\nWhat's New:")
print("   - New data collected from last run to now")
print("   - Data merged with historical records")
print("   - Prophet model retrained with latest data")
print("   - New 30-day forecast generated")
print("   - Fresh visualizations created")

print("\nTips:")
print("   - Run this script daily to keep forecasts up-to-date")
print("   - Each run creates timestamped folder for tracking")
print("   - Check 'latest' folder for most recent results")
print("   - Old forecasts are preserved in timestamped folders")

print("\n" + "=" * 80)
print("You can now run this script anytime to get fresh forecasts!")
print("=" * 80)
