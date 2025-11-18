"""
streaming_collector.py - Thu thập dữ liệu crypto real-time
Dùng Binance API (không cần API key) để lấy dữ liệu OHLCV theo phút
"""
import requests
import time
import json
import os
from datetime import datetime, timedelta, timedelta
import pandas as pd

# Config
BASE_URL = "https://api.binance.com/api/v3"
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
INTERVAL = "1m"  # 1 phút
OUTPUT_DIR = r"D:\BigDataProject\streaming_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_klines(symbol, interval="1m", limit=1000, start_time=None, end_time=None):
    """
    Lấy dữ liệu OHLCV từ Binance API
    
    Parameters:
    - symbol: BTCUSDT, ETHUSDT
    - interval: 1m, 5m, 1h, 1d
    - limit: max 1000
    - start_time: Unix timestamp (ms)
    - end_time: Unix timestamp (ms)
    
    Returns: DataFrame với cột [timestamp, open, high, low, close, volume]
    """
    endpoint = f"{BASE_URL}/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    
    if start_time:
        params["startTime"] = int(start_time)
    if end_time:
        params["endTime"] = int(end_time)
    
    try:
        response = requests.get(endpoint, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Parse response
        # [Open time, Open, High, Low, Close, Volume, Close time, ...]
        df = pd.DataFrame(data, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ])
        
        # Chọn cột cần thiết và chuẩn hóa
        df = df[['open_time', 'open', 'high', 'low', 'close', 'volume']]
        df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        
        # Convert types
        df['timestamp'] = df['timestamp'].astype(int) // 1000  # ms -> seconds
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        
        # Add datetime
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
        
        return df
        
    except Exception as e:
        print(f"Error fetching {symbol}: {str(e)}")
        return None

def get_last_collected_timestamp(symbol):
    """
    Lấy timestamp cuối cùng đã thu thập từ file streaming
    Nếu chưa có file, trả về None
    """
    filepath = os.path.join(OUTPUT_DIR, f"{symbol}_streaming.csv")
    
    if os.path.exists(filepath):
        try:
            df = pd.read_csv(filepath)
            if not df.empty and 'timestamp' in df.columns:
                last_ts = df['timestamp'].max()
                last_date = datetime.fromtimestamp(last_ts)
                print(f"  Last collected data: {last_date}")
                return last_ts
        except Exception as e:
            print(f"  Warning: Error reading existing file: {e}")
    
    return None

def collect_historical_gap(symbol, start_date, end_date):
    """
    Thu thập dữ liệu lịch sử để lấp khoảng trống từ start_date đến end_date
    """
    print(f"\nCollecting {symbol} from {start_date} to {end_date}")
    
    start_ts = int(start_date.timestamp() * 1000)  # ms
    end_ts = int(end_date.timestamp() * 1000)
    
    all_data = []
    current_start = start_ts
    
    while current_start < end_ts:
        # Binance limit: 1000 candles per request
        # 1000 minutes = ~16.67 hours
        df = get_klines(symbol, interval=INTERVAL, limit=1000, start_time=current_start)
        
        if df is None or df.empty:
            print(f"  Warning: No data returned, stopping...")
            break
        
        all_data.append(df)
        
        # Update start time for next batch
        last_ts = df['timestamp'].max()
        current_start = int((last_ts + 60) * 1000)  # next minute in ms
        
        print(f"  Fetched {len(df)} rows, last: {df['datetime'].max()}")
        
        # Rate limiting (Binance: ~1200 requests/min)
        time.sleep(0.1)
        
        # Stop if we've reached end_date
        if last_ts >= (end_ts // 1000):
            break
    
    if not all_data:
        return None
    
    # Combine all batches
    result = pd.concat(all_data, ignore_index=True)
    result = result.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
    
    print(f"  Total collected: {len(result):,} rows")
    print(f"  Range: {result['datetime'].min()} -> {result['datetime'].max()}")
    
    return result

def save_streaming_data(df, symbol, mode='append'):
    """
    Lưu dữ liệu streaming vào CSV
    mode='append' để thêm vào file cũ (incremental)
    mode='overwrite' để ghi đè
    """
    filepath = os.path.join(OUTPUT_DIR, f"{symbol}_streaming.csv")
    
    if mode == 'append' and os.path.exists(filepath):
        # Append mode - thêm vào cuối file
        df.to_csv(filepath, mode='a', header=False, index=False)
        print(f"  Appended {len(df)} rows to {filepath}")
    else:
        # Overwrite mode - tạo mới hoặc ghi đè
        df.to_csv(filepath, index=False)
        print(f"  Created {filepath} with {len(df)} rows")

def collect_realtime_batch(symbol, minutes_back=60):
    """Thu thập dữ liệu real-time (60 phút gần nhất)"""
    print(f"\n[REALTIME] Collecting {symbol} (last {minutes_back} minutes)")
    
    df = get_klines(symbol, interval=INTERVAL, limit=minutes_back)
    
    if df is not None and not df.empty:
        print(f"  Fetched {len(df)} rows")
        print(f"  Range: {df['datetime'].min()} -> {df['datetime'].max()}")
        return df
    else:
        print(f"  Failed to fetch realtime data")
        return None

# ==========================================================
# MAIN EXECUTION
# ==========================================================
if __name__ == "__main__":
    print("=" * 70)
    print("CRYPTO STREAMING DATA COLLECTOR")
    print("=" * 70)
    
    # Thời điểm hiện tại
    current_time = datetime.now()
    print(f"\nCurrent time: {current_time}")
    
    # Step 1: Thu thập dữ liệu mới (incremental update)
    print("\nSTEP 1: Collecting new data (incremental update)")
    print("-" * 70)
    
    for symbol in SYMBOLS:
        print(f"\nChecking {symbol}...")
        
        # Kiểm tra timestamp cuối cùng đã thu thập
        last_ts = get_last_collected_timestamp(symbol)
        
        if last_ts is None:
            # Lần đầu tiên chạy - lấy từ 25/9/2025
            print(f"  First time collection - starting from 2025-09-25")
            start_date = datetime(2025, 9, 25, 0, 0, 0)
        else:
            # Đã có data - chỉ lấy từ timestamp cuối + 1 phút
            start_date = datetime.fromtimestamp(last_ts) + timedelta(minutes=1)
            print(f"  Will collect from: {start_date}")
        
        end_date = current_time
        
        # Kiểm tra xem có cần thu thập không
        if start_date >= end_date:
            print(f"  {symbol} is already up-to-date! No new data to collect.")
            continue
        
        # Tính số ngày cần thu thập
        days_to_collect = (end_date - start_date).days
        print(f"  Need to collect: {days_to_collect} days + partial day")
        
        # Thu thập dữ liệu
        df = collect_historical_gap(symbol, start_date, end_date)
        
        if df is not None:
            # Lưu với mode='append' để thêm vào file cũ
            save_streaming_data(df, symbol, mode='append')
    
    # Step 2: Thu thập dữ liệu realtime (demo - 60 phút gần nhất)
    print("\n" + "=" * 70)
    print("STEP 2: Collecting real-time data (last 60 minutes)")
    print("-" * 70)
    
    for symbol in SYMBOLS:
        df = collect_realtime_batch(symbol, minutes_back=60)
        if df is not None:
            # Lưu vào thư mục riêng cho realtime
            realtime_dir = os.path.join(OUTPUT_DIR, "realtime")
            os.makedirs(realtime_dir, exist_ok=True)
            filepath = os.path.join(realtime_dir, f"{symbol}_latest.csv")
            df.to_csv(filepath, index=False)
            print(f"  Saved to {filepath}")
    
    print("\n" + "=" * 70)
    print("Data collection complete!")
    print(f"Output directory: {OUTPUT_DIR}")
    print("=" * 70)
