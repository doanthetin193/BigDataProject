# Phân tích và Dự đoán Giá Tiền Mã Hóa sử dụng Apache Spark

**Đề tài:** Phân tích và dự đoán xu hướng giá tiền mã hóa (BTC và ETH) với Lambda Architecture  
**Sinh viên:** Đoàn Thế Tín  
**MSSV:** 4551190056  
**Lớp:** KTPM45

---

## 📋 Tổng quan

Dự án xây dựng hệ thống phân tích dữ liệu lớn và dự đoán giá tiền mã hóa (Bitcoin, Ethereum) sử dụng **Lambda Architecture** với Apache Spark, Kafka, và Facebook Prophet.

### Đặc điểm nổi bật:
- ✅ **Quy mô dữ liệu:** 50+ triệu dòng (tick-level 1 phút, 2012-2025)
- ✅ **Lambda Architecture:** Batch Layer + Speed Layer + Serving Layer
- ✅ **Real-time Streaming:** Kafka + Spark Structured Streaming + WebSocket
- ✅ **Machine Learning:** Facebook Prophet với MAPE < 4%
- ✅ **Dữ liệu sạch:** 8,140 ngày sau xử lý, backfill gaps tự động

---

## 🏗️ Kiến trúc hệ thống (Lambda Architecture)

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│  - Historical CSV (50M+ rows)                                    │
│  - Binance WebSocket API (Real-time)                            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
        ┌─────────────────────┴─────────────────────┐
        ↓                                           ↓
┌───────────────────┐                   ┌───────────────────────┐
│   BATCH LAYER     │                   │    SPEED LAYER        │
│                   │                   │                       │
│ • Preprocessing   │                   │ • WebSocket Producer  │
│ • Backfill Gaps   │                   │ • Kafka (1 partition) │
│ • Daily Aggregate │                   │ • Spark Streaming     │
│ • Output: 8,140   │                   │ • Daily Aggregate     │
│   rows Parquet    │                   │ • Output: Parquet     │
└───────────────────┘                   └───────────────────────┘
        │                                           │
        └─────────────────────┬─────────────────────┘
                              ↓
                  ┌───────────────────────┐
                  │   SERVING LAYER       │
                  │                       │
                  │ • Merge Batch + Speed │
                  │ • Deduplication       │
                  │ • Prophet Input       │
                  └───────────────────────┘
                              ↓
                  ┌───────────────────────┐
                  │   MACHINE LEARNING    │
                  │                       │
                  │ • Facebook Prophet    │
                  │ • MAPE: BTC 3.36%     │
                  │        ETH 3.90%      │
                  └───────────────────────┘
```

---

## 📁 Cấu trúc thư mục

```
BigDataProject/
│
├── data/                          # Dữ liệu thô
│   ├── btc/BTCUSDT_1min_2012-2025.csv  (28M rows)
│   └── eth/ETHUSDT_1min_2017-2025.csv  (24M rows)
│
├── data_parquet/                  # Dữ liệu Parquet (partitioned by year)
│   ├── btc_clean/                 # BTC đã làm sạch
│   └── eth_clean/                 # ETH đã làm sạch
│
├── data_analysis/                 # Output phân tích
│   ├── daily_filled/              # Batch Layer output (8,140 rows)
│   ├── prophet_input/             # Input cho Prophet (merged)
│   ├── prophet_forecasts/         # Kết quả dự đoán (Parquet)
│   ├── prophet_results/           # Actual vs Predicted (CSV)
│   ├── prophet_metrics/           # MAPE, MAE, RMSE
│   └── prophet_visualizations/    # Biểu đồ HTML tương tác
│
├── week6_streaming/               # Speed Layer
│   ├── docker-compose.yml         # Kafka + Zookeeper
│   ├── websocket_producer.py      # Binance WebSocket → Kafka
│   ├── spark_streaming_consumer.py # Kafka → Spark Streaming (production)
│   ├── kafka_batch_reader.py      # Kafka → Batch read (demo nhanh)
│   ├── checkpoint_spark/          # Checkpoint metadata
│   └── streaming_output_spark_BATCH/ # Speed Layer output (2 rows)
│
├── scripts/                       # Source code
│   ├── preprocessing/             # 4 scripts tiền xử lý
│   │   ├── convert_to_parquet.py
│   │   ├── clean_parquet.py
│   │   ├── preprocess_step1.py
│   │   └── preprocess_step2.py
│   │
│   ├── lambda_batch/              # Batch Layer
│   │   ├── week6_backfill.py      # Backfill gaps < 30 ngày
│   │   ├── week6_backfill_batch.py # Backfill gaps lớn
│   │   └── week6_merge.py         # Merge Batch + Speed
│   │
│   └── ml_models/                 # Machine Learning
│       └── prophet_train.py       # Train & forecast Prophet
│
├── docs/                          # Tài liệu (sẽ tạo lại)
└── README.md                      # File này
```

---

## 🚀 Hướng dẫn chạy

### **1. Chuẩn bị môi trường**

```bash
# Cài đặt dependencies
pip install pyspark pandas numpy matplotlib prophet requests websocket-client

# Cài Docker Desktop (cho Kafka)
# Download: https://www.docker.com/products/docker-desktop
```

### **2. Preprocessing (Batch Layer - Bước 1)**

```bash
# Bước 1: Chuyển CSV → Parquet
cd D:\BigDataProject
python scripts/preprocessing/convert_to_parquet.py

# Bước 2: Làm sạch dữ liệu
python scripts/preprocessing/clean_parquet.py

# Bước 3: Tạo daily aggregates
python scripts/preprocessing/preprocess_step1.py

# Bước 4: Điền gaps và tính MA
python scripts/preprocessing/preprocess_step2.py
```

**Output:** `data_analysis/daily_filled/` (~7,980 rows)

### **3. Backfill Gaps (Batch Layer - Bước 2)**

```bash
# Backfill gaps nhỏ (<30 ngày)
python scripts/lambda_batch/week6_backfill.py

# Backfill gap lớn (79 ngày, Nov-Dec 2024)
python scripts/lambda_batch/week6_backfill_batch.py
```

**Output:** `data_analysis/daily_filled/` (8,140 rows - HOÀN CHỈNH)

### **4. Speed Layer (Real-time Streaming)**

```bash
# Bước 1: Khởi động Kafka
cd week6_streaming
docker-compose up -d

# Đợi 15s để Kafka khởi động
# Verify: docker ps (2 containers running)

# Bước 2: Tạo topic (1 partition)
docker exec kafka kafka-topics --create --topic crypto-prices \
  --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

# Bước 3: Chạy Producer (Terminal 1)
python websocket_producer.py
# Gửi real-time data từ Binance → Kafka

# Bước 4: Chạy Consumer (Terminal 2)
python spark_streaming_consumer.py
# Đọc từ Kafka → Aggregate → Parquet

# Hoặc chạy batch reader (demo nhanh):
python kafka_batch_reader.py
```

**Output:** `streaming_output_spark_BATCH/` (2 rows: BTCUSDT, ETHUSDT ngày 2025-12-14)

### **5. Serving Layer (Merge Batch + Speed)**

```bash
# Merge dữ liệu từ Batch + Speed Layer
python scripts/lambda_batch/week6_merge.py
```

**Output:** 
- `data_analysis/daily_filled/` (8,140 rows - overwrite with cache)
- `data_analysis/prophet_input/` (8,140 rows - ready for ML)

### **6. Machine Learning (Prophet)**

```bash
# Train và forecast
python scripts/ml_models/prophet_train.py
```

**Output:**
- `prophet_forecasts/` - Dự đoán 30 ngày (Parquet)
- `prophet_metrics/metrics.csv` - MAPE, MAE, RMSE
- `prophet_results/` - Actual vs Predicted CSV
- `prophet_visualizations/` - Biểu đồ HTML tương tác

**Backup tự động:**
- Mỗi lần chạy lại → backup sang `*_backup/`
- Đảm bảo không mất kết quả cũ

---

## 📊 Kết quả đạt được

### **1. Batch Layer**
- ✅ **Dữ liệu gốc:** 50M+ rows (CSV 1-phút)
- ✅ **Sau preprocessing:** 7,980 rows daily
- ✅ **Sau backfill:** 8,140 rows (100% complete từ 2017-2025)
- ✅ **Features:** OHLCV + MA7 + MA30

### **2. Speed Layer**
- ✅ **Kafka:** 1 partition, 1,008 messages
- ✅ **WebSocket Producer:** Binance real-time API
- ✅ **Spark Streaming Consumer:** Micro-batch 10s
- ✅ **Output:** 2 rows daily aggregate (BTCUSDT, ETHUSDT)

### **3. Serving Layer**
- ✅ **Merge:** Batch (8,140) + Speed (2) = 8,140 rows
- ✅ **Deduplication:** Batch priority, bỏ Speed trùng ngày
- ✅ **Cache fix:** Tránh conflict khi ghi Parquet
- ✅ **Prophet input:** OHLCV + MA7 + MA30

### **4. Machine Learning**
| Coin | MAPE | MAE | RMSE |
|------|------|-----|------|
| **BTCUSDT** | **3.36%** | $2,994 | $3,682 |
| **ETHUSDT** | **3.90%** | $120.43 | $145.82 |

**Kết luận:** MAPE < 4% → Mô hình dự đoán rất tốt!

---

## 🛠️ Công nghệ sử dụng

### **Big Data**
- **Apache Spark 3.5.3:** PySpark, SparkSQL, Structured Streaming
- **Apache Kafka:** Message broker (Confluent 7.5.0)
- **Parquet:** Columnar storage format
- **Docker:** Container cho Kafka + Zookeeper

### **Machine Learning**
- **Facebook Prophet:** Time series forecasting
- **Pandas:** Data manipulation
- **NumPy:** Numerical computation

### **Visualization**
- **Matplotlib:** Static plots
- **Plotly:** Interactive HTML charts

### **Real-time Data**
- **WebSocket:** Binance Ticker API
- **Requests:** HTTP API calls

---

## 📈 Demo Workflow

### **Scenario: Dự đoán giá BTC hôm nay**

```bash
# 1. Lấy data real-time (Speed Layer)
python week6_streaming/websocket_producer.py  # Chạy 5 phút
python week6_streaming/kafka_batch_reader.py   # Aggregate

# 2. Merge với Batch Layer
python scripts/lambda_batch/week6_merge.py

# 3. Forecast với Prophet
python scripts/ml_models/prophet_train.py

# 4. Xem kết quả
# Mở: data_analysis/prophet_visualizations/BTCUSDT_forecast_interactive.html
```

**Kết quả:** Biểu đồ dự đoán giá BTC 30 ngày tới với confidence interval!

---

## 🎯 So sánh với Đề cương

| Yêu cầu | Đề cương gốc | Thực hiện |
|---------|--------------|-----------|
| **Quy mô dữ liệu** | 50-100M rows | ✅ 50M+ rows |
| **Streaming** | Poll API đơn giản | ✅ **Kafka + WebSocket** 🌟 |
| **Architecture** | Không rõ | ✅ **Lambda Architecture** 🌟 |
| **ML Model** | Prophet | ✅ Prophet MAPE < 4% |
| **Visualization** | Matplotlib | ✅ Matplotlib + Plotly HTML |

**Điểm cộng lớn:**
- 🌟 **Kafka + Docker:** Production-ready streaming
- 🌟 **Lambda Architecture:** Batch + Speed + Serving
- 🌟 **MAPE < 4%:** Dự đoán rất chính xác

---

## 📝 Tài liệu tham khảo

1. Apache Spark Documentation: https://spark.apache.org/docs/latest/
2. Facebook Prophet: https://facebook.github.io/prophet/
3. Confluent Kafka: https://docs.confluent.io/
4. Binance WebSocket API: https://binance-docs.github.io/apidocs/spot/en/
5. Lambda Architecture: http://lambda-architecture.net/

---

## 👨‍💻 Tác giả

**Đoàn Thế Tín**  
MSSV: 4551190056  
Lớp: KTPM45  
Email: [Thêm email nếu cần]

---

## 📅 Timeline thực hiện

- **Tuần 1-2:** Thu thập và preprocessing dữ liệu (50M rows)
- **Tuần 3:** Batch Layer (daily aggregates, backfill)
- **Tuần 4:** Machine Learning (Prophet training, MAPE < 4%)
- **Tuần 5:** Tổ chức code, documentation
- **Tuần 6:** Speed Layer (Kafka + Spark Streaming) ⭐
- **Tuần 7-8:** Hoàn thiện báo cáo và demo

---

## 🎓 Ghi chú

Dự án này là đồ án cá nhân môn Big Data Analytics, minh họa quy trình phân tích dữ liệu lớn từ thu thập, xử lý, đến dự đoán với công nghệ production-ready (Kafka, Spark Streaming, Lambda Architecture).

**License:** Educational use only.
