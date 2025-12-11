# GIẢI THÍCH CHI TIẾT FILE `week6_backfill.py`

## 📋 TỔNG QUAN

File `week6_backfill.py` là thành phần **BATCH LAYER** trong Lambda Architecture. Nhiệm vụ chính:

- Phát hiện **khoảng trống (gap)** trong dữ liệu lịch sử
- Lấy dữ liệu từ **Binance API** để lấp đầy gap
- Xử lý và tổng hợp thành **daily OHLC** với MA7/MA30
- Lưu vào `daily_filled/` và `prophet_input/`

---

## 🔄 LUỒNG HOẠT ĐỘNG

```
┌─────────────────────────────────────────────────────────────────┐
│                  WEEK6_BACKFILL.PY WORKFLOW                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ① Detect Last Date                                            │
│     └─► Tìm ngày cuối cùng có dữ liệu                          │
│                                                                 │
│  ② Calculate Gap                                               │
│     └─► Tính số ngày thiếu (gap)                               │
│                                                                 │
│  ③ Fetch from Binance API                                      │
│     └─► Lấy dữ liệu 1-minute candles                           │
│                                                                 │
│  ④ Clean Data                                                  │
│     └─► Xóa duplicates                                         │
│                                                                 │
│  ⑤ Aggregate to Daily OHLC                                     │
│     └─► Tổng hợp 1440 phút → 1 ngày                            │
│                                                                 │
│  ⑥ Forward Fill Missing Dates                                  │
│     └─► Lấp ngày thiếu bằng giá trị trước đó                   │
│                                                                 │
│  ⑦ Compute MA7 & MA30                                          │
│     └─► Tính moving averages                                   │
│                                                                 │
│  ⑧ Save Results                                                │
│     └─► Lưu vào daily_filled/ và prophet_input/                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📝 DIỄN GIẢI CHI TIẾT TỪNG BƯỚC

### **BƯỚC 1: Phát hiện ngày cuối cùng có dữ liệu**

#### **Mục đích:**

Tìm xem dữ liệu hiện có đã đến ngày nào để biết cần fetch từ đâu.

#### **Cách hoạt động:**

```python
try:
    # Thử đọc từ daily_filled trước (nơi chứa data đã clean và aggregate)
    df_existing = spark.read.parquet("data_analysis/daily_filled")
    last_date_existing = df_existing.agg(max("date")).collect()[0][0]
    data_source = "daily_filled"
except:
    try:
        # Nếu không có daily_filled, thử đọc từ prophet_input
        df_existing = spark.read.parquet("data_analysis/prophet_input")
        last_date_existing = df_existing.agg(max("ds")).collect()[0][0]
        data_source = "prophet_input"
    except:
        # Nếu cả 2 đều không có → chưa chạy Week 1-5
        print("⚠️ No existing data found!")
        print("Please run Week 1-5 pipeline first")
        spark.stop()
        exit(1)
```

#### **Giải thích:**

- **Ưu tiên 1:** Đọc từ `daily_filled` - nơi chứa dữ liệu đã được xử lý đầy đủ
- **Ưu tiên 2:** Nếu không có, đọc từ `prophet_input` - output của Week 4
- **Nếu không có gì:** Báo lỗi và yêu cầu chạy lại từ Week 1-5

#### **Kết quả:**

```
✅ Last date found in daily_filled: 2025-09-25
```

---

### **BƯỚC 2: Tính toán khoảng trống (gap)**

#### **Mục đích:**

Xác định có bao nhiêu ngày thiếu data từ ngày cuối cùng đến hôm nay.

#### **Cách hoạt động:**

```python
today = datetime.now().date()                    # Lấy ngày hiện tại
gap_days = (today - last_date_existing).days    # Tính số ngày thiếu

if gap_days <= 0:
    # Không có gap → data đã up-to-date
    print("✅ Data is already up to date!")
    print("You can start streaming for real-time data")
    spark.stop()
    exit(0)

# Có gap → cần fetch từ ngày sau ngày cuối cùng
fetch_start_date = last_date_existing + timedelta(days=1)
```

#### **Ví dụ:**

```
Last date: 2025-09-25
Today:     2025-12-09
Gap:       75 ngày

→ Cần fetch từ 2025-09-26 đến 2025-12-09
```

#### **Logic:**

- **Gap = 0 hoặc âm:** Data đã đủ → chuyển sang streaming
- **Gap > 0:** Thiếu data → cần backfill

---

### **BƯỚC 3: Fetch dữ liệu từ Binance API**

#### **Mục đích:**

Lấy dữ liệu lịch sử từ Binance API để lấp khoảng trống.

#### **3.1. Hàm fetch_binance_klines**

```python
def fetch_binance_klines(symbol, interval, start_time, end_time):
    """
    Lấy historical klines từ Binance API

    Parameters:
    - symbol: Tên coin (VD: "BTCUSDT")
    - interval: Khung thời gian (VD: "1m" = 1 phút)
    - start_time: Thời gian bắt đầu (milliseconds)
    - end_time: Thời gian kết thúc (milliseconds)
    """
    url = "https://api.binance.com/api/v3/klines"
    all_klines = []
    current_start = start_time

    while current_start < end_time:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current_start,
            "endTime": end_time,
            "limit": 1000    # Binance giới hạn tối đa 1000 candles/request
        }

        response = requests.get(url, params=params, timeout=30)
        klines = response.json()

        if not klines:
            break

        all_klines.extend(klines)

        # Dịch con trỏ đến candle tiếp theo
        current_start = klines[-1][6] + 1  # close_time + 1ms

        time.sleep(0.1)  # Đợi 0.1s để tránh bị Binance block

    return all_klines
```

#### **Giải thích chi tiết:**

**Tại sao limit = 1000?**

- Binance API giới hạn mỗi request chỉ trả về tối đa **1000 candles**
- Nếu cần nhiều hơn → phải gọi nhiều lần

**Dịch con trỏ: `klines[-1][6] + 1`**

- `klines[-1]` = candle cuối cùng trong response
- `[6]` = index của `close_time` trong mảng
- `+ 1` = cộng thêm 1 millisecond để fetch tiếp từ candle sau

**Ví dụ:**

```
Request 1: Fetch từ 00:00:00 → 00:16:39 (1000 candles)
           Candle cuối: close_time = 00:16:39.999

Request 2: Fetch từ 00:16:40.000 (close_time + 1ms)
```

**Rate limiting: `time.sleep(0.1)`**

- Đợi 0.1 giây giữa các requests
- Tránh spam quá nhanh → bị Binance block IP

#### **3.2. Chuyển đổi thời gian sang milliseconds**

```python
start_ms = int(datetime.combine(fetch_start_date, datetime.min.time()).timestamp() * 1000)
end_ms = int(datetime.combine(today, datetime.max.time()).timestamp() * 1000)
```

**Giải thích:**

- `datetime.min.time()` = 00:00:00 (đầu ngày)
- `datetime.max.time()` = 23:59:59.999999 (cuối ngày)
- `.timestamp()` = chuyển sang seconds từ 1/1/1970
- `* 1000` = chuyển sang milliseconds (Binance yêu cầu)

#### **3.3. Loop qua BTC và ETH**

```python
new_data_frames = []

for symbol in ["BTCUSDT", "ETHUSDT"]:
    print(f"\nFetching {symbol}...")

    # Gọi hàm fetch
    klines = fetch_binance_klines(symbol, "1m", start_ms, end_ms)

    # Chuyển sang pandas DataFrame
    df_klines = pd.DataFrame(klines, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_volume', 'trades', 'taker_buy_base',
        'taker_buy_quote', 'ignore'
    ])

    # Chuyển open_time từ milliseconds → datetime
    df_klines['open_time'] = pd.to_datetime(df_klines['open_time'], unit='ms')

    # Chuyển các cột giá từ string → float
    for col_name in ['open', 'high', 'low', 'close', 'volume']:
        df_klines[col_name] = df_klines[col_name].astype(float)

    # Chuyển từ pandas → Spark DataFrame (chỉ lấy cột cần thiết)
    df_spark = spark.createDataFrame(
        df_klines[['open_time', 'open', 'high', 'low', 'close', 'volume']]
    )

    # Thêm cột symbol và date
    df_spark = df_spark.withColumn("symbol", lit(symbol))
    df_spark = df_spark.withColumn("date", to_date(col("open_time")))

    new_data_frames.append(df_spark)
    print(f"✅ Fetched {df_spark.count():,} rows")
```

**Tại sao chuyển pandas → Spark?**

- **Pandas:** Dễ xử lý data từ API, nhưng chỉ chạy trên 1 máy
- **Spark:** Có thể phân tán xử lý trên nhiều máy, phù hợp với Big Data

**Tại sao chỉ lấy 6 cột?**

- Binance trả về 12 cột, nhưng chỉ cần 6 cột quan trọng:
  - `open_time`, `open`, `high`, `low`, `close`, `volume`

**Kết quả:**

```
Fetching BTCUSDT...
✅ Fetched 108,000 rows

Fetching ETHUSDT...
✅ Fetched 108,000 rows
```

#### **3.4. Gộp BTC + ETH**

```python
df_new_raw = new_data_frames[0]
if len(new_data_frames) > 1:
    df_new_raw = df_new_raw.union(new_data_frames[1])
```

**Giải thích:**

- Gộp 2 DataFrames thành 1 bằng `union` (ghép dọc - thêm rows)
- Kết quả: 216,000 rows (108,000 BTC + 108,000 ETH)

---

### **BƯỚC 4: Làm sạch dữ liệu**

#### **Mục đích:**

Xóa các dòng trùng lặp (nếu có).

```python
df_new_clean = df_new_raw.dropDuplicates(["symbol", "open_time"])
```

**Giải thích:**

- Xóa rows có cùng `symbol` VÀ cùng `open_time`
- Ví dụ: Nếu có 2 dòng `BTCUSDT` vào lúc `2025-09-26 00:00:00` → giữ lại 1

---

### **BƯỚC 5: Tổng hợp thành Daily OHLC**

#### **Mục đích:**

Chuyển dữ liệu 1-phút (1440 rows/ngày) thành dữ liệu ngày (1 row/ngày).

```python
df_daily = df_new_clean.groupBy("symbol", "date").agg(
    first("open").alias("open"),      # Giá mở cửa = giá đầu ngày
    max("high").alias("high"),        # Giá cao nhất trong ngày
    min("low").alias("low"),          # Giá thấp nhất trong ngày
    last("close").alias("close"),     # Giá đóng cửa = giá cuối ngày
    sum("volume").alias("volume")     # Tổng volume cả ngày
)
```

#### **Hình dung:**

**Trước aggregate (1440 rows/ngày):**

```
| symbol  | date       | open_time | open    | high    | low     | close   | volume |
|---------|------------|-----------|---------|---------|---------|---------|--------|
| BTCUSDT | 2025-09-26 | 00:00:00  | 96500.0 | 96800.0 | 96400.0 | 96700.0 | 10.5   |
| BTCUSDT | 2025-09-26 | 00:01:00  | 96700.0 | 96900.0 | 96650.0 | 96850.0 | 15.2   |
| BTCUSDT | 2025-09-26 | 00:02:00  | 96850.0 | 97000.0 | 96800.0 | 96950.0 | 20.3   |
| ...     | ...        | ...       | ...     | ...     | ...     | ...     | ...    |
| BTCUSDT | 2025-09-26 | 23:59:00  | 98400.0 | 98500.0 | 98350.0 | 98480.0 | 25.1   |
```

**Sau aggregate (1 row/ngày):**

```
| symbol  | date       | open    | high    | low     | close   | volume    |
|---------|------------|---------|---------|---------|---------|-----------|
| BTCUSDT | 2025-09-26 | 96500.0 | 98500.0 | 96400.0 | 98480.0 | 15,234.56 |
```

#### **Thống kê:**

```python
df_daily.groupBy("symbol").agg(
    count("*").alias("days"),
    min("date").alias("first_date"),
    max("date").alias("last_date")
).show()
```

**Output:**

```
+--------+-----+------------+------------+
|symbol  |days |first_date  |last_date   |
+--------+-----+------------+------------+
|BTCUSDT |75   |2025-09-26  |2025-12-09  |
|ETHUSDT |75   |2025-09-26  |2025-12-09  |
+--------+-----+------------+------------+
```

---

### **BƯỚC 6: Forward Fill - Lấp ngày thiếu**

#### **Mục đích:**

Một số ngày có thể không có giao dịch (sàn lỗi, nghỉ lễ) → cần lấp bằng giá ngày trước.

#### **6.1. Tạo chuỗi ngày đầy đủ**

```python
date_range_df = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{fetch_start_date}'),
        to_date('{today}'),
        interval 1 day
    )) as date
""")
```

**Giải thích:**

- `sequence(...)` tạo array chứa tất cả ngày từ start → end
- `explode(...)` chuyển array → nhiều rows (1 row/ngày)

**Kết quả:**

```
| date       |
|------------|
| 2025-09-26 |
| 2025-09-27 |
| 2025-09-28 |
| ...        |
| 2025-12-09 |
```

#### **6.2. Loop qua từng coin và Forward Fill**

```python
df_filled_list = []

for symbol in ["BTCUSDT", "ETHUSDT"]:
    # Lọc data của 1 coin
    df_symbol = df_daily.filter(col("symbol") == symbol)

    # Tạo bảng symbol + date đầy đủ (crossJoin)
    df_complete = date_range_df.crossJoin(
        df_symbol.select("symbol").distinct()
    )

    # Left join với data thực tế
    # → Ngày nào không có data sẽ có giá trị NULL
    df_with_gaps = df_complete.join(df_symbol, ["symbol", "date"], "left")

    # Tạo window để forward fill
    window_spec = Window.partitionBy("symbol") \
                        .orderBy("date") \
                        .rowsBetween(Window.unboundedPreceding, 0)

    # Áp dụng forward fill cho từng cột
    for col_name in ["open", "high", "low", "close", "volume"]:
        df_with_gaps = df_with_gaps.withColumn(
            col_name,
            F.last(col(col_name), ignorenulls=True).over(window_spec)
        )

    df_filled_list.append(df_with_gaps)

# Union BTC + ETH
df_filled = df_filled_list[0].union(df_filled_list[1])
```

#### **Hình dung Forward Fill:**

**Trước forward fill (có gaps):**

```
| date       | symbol  | close   |
|------------|---------|---------|
| 2025-09-26 | BTCUSDT | 96700.0 |
| 2025-09-27 | BTCUSDT | NULL    | ← Gap (không có data)
| 2025-09-28 | BTCUSDT | NULL    | ← Gap
| 2025-09-29 | BTCUSDT | 97000.0 |
```

**Window function hoạt động:**

```
Row 1 (2025-09-26):
  Window: [96700.0]
  F.last(..., ignorenulls=True) → 96700.0

Row 2 (2025-09-27):
  Window: [96700.0, NULL]
  F.last(..., ignorenulls=True) → 96700.0 (bỏ qua NULL)

Row 3 (2025-09-28):
  Window: [96700.0, NULL, NULL]
  F.last(..., ignorenulls=True) → 96700.0

Row 4 (2025-09-29):
  Window: [96700.0, NULL, NULL, 97000.0]
  F.last(..., ignorenulls=True) → 97000.0
```

**Sau forward fill:**

```
| date       | symbol  | close   |
|------------|---------|---------|
| 2025-09-26 | BTCUSDT | 96700.0 |
| 2025-09-27 | BTCUSDT | 96700.0 | ← Đã fill
| 2025-09-28 | BTCUSDT | 96700.0 | ← Đã fill
| 2025-09-29 | BTCUSDT | 97000.0 |
```

---

### **BƯỚC 7: Tính MA7 và MA30**

#### **Mục đích:**

Tính Moving Average 7 ngày và 30 ngày để làm features cho Prophet.

```python
window_ma7 = Window.partitionBy("symbol") \
                   .orderBy("date") \
                   .rowsBetween(-6, 0)    # 7 rows: 6 trước + 1 hiện tại

window_ma30 = Window.partitionBy("symbol") \
                    .orderBy("date") \
                    .rowsBetween(-29, 0)  # 30 rows: 29 trước + 1 hiện tại

df_filled = df_filled.withColumn("MA7", avg("close").over(window_ma7))
df_filled = df_filled.withColumn("MA30", avg("close").over(window_ma30))
```

#### **Ví dụ tính MA7:**

```
Current row: 2025-12-07
               ▼
| date       | close   | Window (7 rows)                | MA7 (avg)  |
|------------|---------|--------------------------------|------------|
| 2025-12-01 | 97000.0 | ◄─┐                            |            |
| 2025-12-02 | 97200.0 |   │                            |            |
| 2025-12-03 | 97500.0 |   │                            |            |
| 2025-12-04 | 97300.0 |   ├─ Window: 7 ngày           |            |
| 2025-12-05 | 97800.0 |   │                            |            |
| 2025-12-06 | 98000.0 |   │                            |            |
| 2025-12-07 | 98200.0 | ◄─┘                            | 97571.4    |
| 2025-12-08 | 98500.0 |     ← Không xét (ngoài window) |            |
```

**Công thức:**

```
MA7 của 2025-12-07 = (97000 + 97200 + 97500 + 97300 + 97800 + 98000 + 98200) / 7
                   = 97571.4
```

---

### **BƯỚC 8: Lưu kết quả**

#### **8.1. Lưu vào daily_filled**

```python
# Thêm cột year để partition
df_filled = df_filled.withColumn("year", year("date"))

# Lưu dữ liệu
output_path = "data_analysis/daily_filled"
df_filled.write.mode("overwrite") \
               .partitionBy("symbol", "year") \
               .parquet(output_path)
```

**Cấu trúc folder sau khi lưu:**

```
data_analysis/daily_filled/
├── symbol=BTCUSDT/
│   ├── year=2012/
│   │   └── part-00000.parquet
│   ├── year=2013/
│   │   └── part-00000.parquet
│   └── year=2025/
│       └── part-00000.parquet  ← 75 ngày mới (backfill)
└── symbol=ETHUSDT/
    ├── year=2017/
    │   └── part-00000.parquet
    └── year=2025/
        └── part-00000.parquet
```

**Tại sao partition theo symbol và year?**

1. **Query hiệu quả:** Chỉ đọc partition cần thiết (VD: chỉ BTC 2025)
2. **Tổ chức tốt:** Dữ liệu ngăn nắp, dễ quản lý
3. **Performance:** Spark scan ít file hơn

#### **8.2. Cập nhật prophet_input**

```python
df_prophet = df_filled.select(
    col("date").alias("ds"),      # Prophet yêu cầu cột tên "ds"
    col("close").alias("y"),      # Prophet yêu cầu cột tên "y"
    "symbol",
    "MA7",
    "MA30"
).orderBy("symbol", "ds")

df_prophet.write.mode("overwrite") \
                .partitionBy("symbol") \
                .parquet("data_analysis/prophet_input")
```

**Tại sao đổi tên date → ds, close → y?**

- **Prophet convention:**
  - `ds` = date stamp (cột chứa ngày)
  - `y` = giá trị cần dự đoán

---

## 📊 OUTPUT CUỐI CÙNG

### **Thống kê:**

```
+--------+----------+------------+------------+
|symbol  |total_days|first_date  |last_date   |
+--------+----------+------------+------------+
|BTCUSDT |4875      |2012-01-01  |2025-12-09  |
|ETHUSDT |3075      |2017-08-17  |2025-12-09  |
+--------+----------+------------+------------+
```

### **Files được tạo:**

1. `data_analysis/daily_filled/` - Dữ liệu backfill với MA7/MA30
2. `data_analysis/prophet_input/` - Dữ liệu sẵn sàng cho Prophet

---

## 🎯 NEXT STEPS

Sau khi chạy xong `week6_backfill.py`:

1. **Start Streaming (Speed Layer):**

   ```bash
   cd week6_streaming
   docker-compose up -d
   python websocket_producer.py      # Terminal 1
   python spark_streaming_consumer.py # Terminal 2
   ```

2. **Để streaming chạy** để thu thập real-time data

3. **Merge batch + streaming:**
   ```bash
   python week6_merge.py
   ```

---

## 💡 NHỮNG ĐIỂM QUAN TRỌNG CẦN NHỚ

### **1. Tại sao cần Backfill?**

- Máy tính không chạy 24/7 → có gaps trong dữ liệu
- Backfill = lấp lỗ hổng bằng dữ liệu lịch sử từ API

### **2. Tại sao dùng Binance API?**

- Dữ liệu lịch sử đầy đủ, chính xác
- Free API (không cần authentication cho historical data)
- Rate limit: 1000 candles/request

### **3. Tại sao Forward Fill?**

- Một số ngày có thể không có giao dịch
- Forward fill = giữ giá ổn định thay vì để NULL
- Phù hợp với time series forecasting

### **4. Tại sao tính MA7/MA30?**

- Moving averages = features cho Prophet model
- Giúp model hiểu xu hướng (trend) của giá
- MA7 = trend ngắn hạn, MA30 = trend dài hạn

### **5. Tại sao partition theo year?**

- Dữ liệu lớn (4000+ ngày × 2 coins)
- Partition giúp query nhanh hơn
- Chỉ đọc year cần thiết thay vì scan toàn bộ

---

## 🔧 TROUBLESHOOTING

### **Lỗi: "No existing data found"**

**Nguyên nhân:** Chưa chạy Week 1-5 pipeline

**Giải pháp:**

```bash
python convert_to_parquet.py
python clean_parquet.py
python preprocess_step1.py
python preprocess_step2.py
python prophet_train.py
```

### **Lỗi: "Binance API timeout"**

**Nguyên nhân:** Mạng chậm hoặc Binance API quá tải

**Giải pháp:**

- Tăng timeout: `requests.get(..., timeout=60)`
- Tăng sleep: `time.sleep(0.5)`

### **Lỗi: "Memory error"**

**Nguyên nhân:** Fetch quá nhiều data (gap quá lớn)

**Giải pháp:**

- Chia nhỏ gap: Fetch từng tháng thay vì cả năm
- Tăng Spark memory: `.config("spark.driver.memory", "8g")`

---

## 📚 TÓM TẮT

`week6_backfill.py` là công cụ tự động:

1. ✅ Phát hiện gaps trong dữ liệu
2. ✅ Fetch từ Binance API để lấp gaps
3. ✅ Clean, aggregate, forward fill
4. ✅ Tính MA7/MA30
5. ✅ Lưu kết quả cho Prophet và analysis

**File này đảm bảo dữ liệu luôn liền mạch từ 2012 đến hiện tại!**
