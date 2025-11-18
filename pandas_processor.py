"""
Pandas Processor - Đơn giản nhất, không dùng Spark
Merge streaming data với historical data
"""
import pandas as pd
import os
from datetime import datetime

BASE_DIR = r"D:\BigDataProject"
STREAMING_DIR = os.path.join(BASE_DIR, "streaming_data")
OUTPUT_DIR = os.path.join(BASE_DIR, "streaming_output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=" * 70)
print("PANDAS PROCESSOR - MERGE STREAMING + HISTORICAL")
print("=" * 70)

for symbol in ["BTCUSDT", "ETHUSDT"]:
    print(f"\n{symbol}:")
    print("-" * 70)
    
    # 1. Đọc streaming CSV
    streaming_csv = os.path.join(STREAMING_DIR, f"{symbol}_streaming.csv")
    if not os.path.exists(streaming_csv):
        print(f"  {symbol} not found, skipping...")
        continue
    
    df_streaming = pd.read_csv(streaming_csv)
    print(f"  Loaded {len(df_streaming):,} streaming rows")
    
    # 2. Convert timestamp -> datetime
    df_streaming['datetime'] = pd.to_datetime(df_streaming['timestamp'], unit='s')
    df_streaming['date'] = df_streaming['datetime'].dt.date
    
    # 3. Aggregate theo ngày -> OHLC
    daily = df_streaming.groupby('date').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).reset_index()
    
    daily.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    daily = daily.sort_values('date')
    
    print(f"  Created {len(daily)} daily rows")
    print(f"  Date range: {daily['date'].min()} to {daily['date'].max()}")
    
    # 4. Đọc historical data
    hist_path = os.path.join(BASE_DIR, "data_analysis", "daily_filled", f"symbol={symbol}")
    
    if os.path.exists(hist_path):
        # Đọc tất cả parquet files
        import glob
        parquet_files = glob.glob(os.path.join(hist_path, "**", "*.parquet"), recursive=True)
        
        if parquet_files:
            df_hist = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
            
            # Rename ds -> date nếu cần
            if 'ds' in df_hist.columns:
                df_hist = df_hist.rename(columns={'ds': 'date'})
            
            # Rename daily_* columns
            df_hist = df_hist.rename(columns={
                'daily_open': 'open',
                'daily_high': 'high',
                'daily_low': 'low',
                'daily_close': 'close',
                'daily_volume': 'volume'
            })
            
            # Convert date to same type
            df_hist['date'] = pd.to_datetime(df_hist['date']).dt.date
            
            print(f"  Loaded {len(df_hist):,} historical rows")
            
            # 5. Merge
            df_merged = pd.concat([df_hist[['date', 'open', 'high', 'low', 'close', 'volume']], 
                                   daily], ignore_index=True)
            df_merged = df_merged.drop_duplicates(subset=['date']).sort_values('date')
            
            print(f"  Merged total: {len(df_merged):,} rows")
        else:
            print(f"  No parquet files found in {hist_path}")
            df_merged = daily
    else:
        print(f"  No historical data, using only streaming")
        df_merged = daily
    
    # 6. Lưu kết quả
    output_csv = os.path.join(OUTPUT_DIR, f"{symbol}_daily_updated.csv")
    output_parquet = os.path.join(OUTPUT_DIR, f"{symbol}_daily_updated.parquet")
    
    df_merged.to_csv(output_csv, index=False)
    df_merged.to_parquet(output_parquet, index=False)
    
    print(f"  Saved CSV: {output_csv}")
    print(f"  Saved Parquet: {output_parquet}")
    print(f"  Final range: {df_merged['date'].min()} to {df_merged['date'].max()}")

print("\n" + "=" * 70)
print("PROCESSING COMPLETE!")
print(f"Output: {OUTPUT_DIR}")
print("=" * 70)
