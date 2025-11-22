# 🎓 Big Data Project - Cryptocurrency Analysis & Forecasting

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange.svg)](https://spark.apache.org/)
[![Prophet](https://img.shields.io/badge/Prophet-Time%20Series-green.svg)](https://facebook.github.io/prophet/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.4-red.svg)](https://kafka.apache.org/)

## 📋 Mô tả dự án

Dự án phân tích và dự báo giá cryptocurrency (Bitcoin & Ethereum) sử dụng **Apache Spark** để xử lý big data, **Facebook Prophet** cho forecasting, và **Kafka + Spark Structured Streaming** cho real-time processing.

### 🎯 Mục tiêu
- ✅ Xử lý **15+ triệu rows** dữ liệu crypto (2012-2025)
- ✅ Phân tích xu hướng, volume, biến động giá
- ✅ Dự báo giá với **MAPE < 4%** (BTC: 2.82%, ETH: 3.61%)
- ✅ **TRUE Structured Streaming** (Kafka + Spark)
- ✅ Real-time processing với **1-10s latency**

## 📊 Dataset

- **BTC (Bitcoin)**: 1-minute OHLCV data from 2012-2025
- **ETH (Ethereum)**: 1-minute OHLCV data from 2017-2025
- **Format**: CSV → Parquet (partitioned by year/month)

> **Lưu ý**: Dữ liệu không được đẩy lên GitHub do dung lượng lớn. Bạn cần tải dataset riêng và đặt vào thư mục `data/`

## 🛠️ Tech Stack

- **Apache Spark (PySpark)** - Big data processing
- **Facebook Prophet** - Time series forecasting
- **Pandas** - Data manipulation
- **Matplotlib & Plotly** - Visualization
- **Scikit-learn** - Model evaluation

## 📁 Cấu trúc dự án

```
BigDataProject/
├── data/                          # Raw CSV data (15M+ rows)
│   ├── btc/ & eth/
├── data_parquet/                  # Parquet format (70% reduction)
│   ├── btc_clean/ & eth_clean/
├── data_analysis/                 # Analysis outputs
│   ├── daily_filled/              # Daily OHLC
│   ├── week4_results/             # Prophet forecasts
│   └── visualizations/
│
├── week6_streaming/               # ✅ Kafka + Spark Structured Streaming
│   ├── docker-compose.yml        # Kafka infrastructure
│   ├── websocket_producer.py     # Producer (133 dòng)
│   ├── spark_streaming_consumer.py # Consumer (268 dòng)
│   └── README.md                 # ← Chi tiết ở đây!
│
├── prophet_train.py               # Week 4: Forecasting
├── preprocess_step1.py            # Week 2: CSV → Parquet
├── preprocess_step2.py            # Week 3: Clean & aggregate
│
├── STRUCTURED_STREAMING_SUCCESS.md # ⭐ Validation report
└── README.md                      # ← Bạn đang đọc file này
    ├── prophet_input/
    ├── week4_forecasts/
    ├── week4_metrics/
    └── week4_visualizations/
```

## 🚀 Cách chạy toàn bộ project

### 1️⃣ Setup môi trường
```bash
# Cài đặt Python packages
pip install pyspark pandas prophet plotly kafka-python binance-connector

# Cài Docker Desktop (cho Kafka Week 6)
# Download từ: https://www.docker.com/products/docker-desktop
```

### 2️⃣ Xử lý historical data (Weeks 1-5)
```bash
# Convert CSV → Parquet
python preprocess_step1.py

# Clean & aggregate → daily OHLC
python preprocess_step2.py

# Train Prophet & forecast
python prophet_train.py
```

### 3️⃣ Chạy Structured Streaming (Week 6)
```bash
cd week6_streaming

# Start Kafka infrastructure
docker-compose up -d

# Terminal 1: Producer (Binance → Kafka)
python websocket_producer.py

# Terminal 2: Consumer (Kafka → Spark → Parquet)
python spark_streaming_consumer.py
```

📖 **Chi tiết Week 6:** Xem [week6_streaming/README.md](week6_streaming/README.md)

## 📊 Kết quả đạt được

### Data Processing
- ✅ Processed **15+ million rows** (1-minute OHLCV data)
- ✅ Converted to Parquet format (~70% storage reduction)
- ✅ Cleaned & aggregated to **8,078 daily records**
- ✅ Forward-filled missing days (5,066 BTC + 3,012 ETH days)

### Forecasting Accuracy
- ✅ **BTC MAPE: 2.82%** (Excellent!)
- ✅ **ETH MAPE: 3.61%** (Very Good!)
- ✅ 7-day forecast horizon
- ✅ Interactive visualization (Plotly)

### Streaming Performance
- ✅ **TRUE Structured Streaming** (Kafka + Spark)
- ✅ **1-10s latency** (near real-time)
- ✅ **1000 msg/min throughput**
- ✅ **Watermarking & windowing** (handle late data)
- ✅ **Fault tolerance** (checkpoint recovery)
- ✅ **Production-ready** architecture

### Visualization
- ✅ Close price + Moving Averages
- ✅ Daily volume charts
- ✅ BTC vs ETH comparison
- ✅ Forecast plots (static & interactive)
- ✅ Prophet components decomposition

## 📊 Outputs

### Analysis Results
- `daily_filled/` - Daily OHLC với missing days đã fill
- `prophet_input/` - Input cho Prophet (ds, y, symbol)
- `results/` - SparkSQL query results

### Forecast Results
- `week4_forecasts/` - Prophet forecast parquet
- `week4_metrics/metrics.csv` - Model performance
- `week4_results/` - Actual vs Predicted CSV
- `week4_visualizations/` - Charts (PNG + HTML interactive)

## 🔧 Configuration

### Spark Configuration
```python
spark = SparkSession.builder \
    .appName("CryptoAnalysis") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()
```

### Prophet Hyperparameters
```python
seasonality_mode: ["additive", "multiplicative"]
changepoint_prior_scale: [0.01, 0.05, 0.1]
daily_seasonality: True
```

## 📄 License

MIT License

## 👤 Author

**Doan The Tin**
- GitHub: [@doanthetin193](https://github.com/doanthetin193)

## 🙏 Acknowledgments

- Apache Spark Documentation
- Facebook Prophet Documentation
- Cryptocurrency data providers

---

⭐ **Star this repo if you find it helpful!**
