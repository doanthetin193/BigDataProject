#  Big Data Project - Cryptocurrency Analysis & Forecasting

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange.svg)](https://spark.apache.org/)
[![Prophet](https://img.shields.io/badge/Prophet-Time%20Series-green.svg)](https://facebook.github.io/prophet/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-red.svg)](https://kafka.apache.org/)

##  M� t? d? �n

D? �n ph�n t�ch v� d? b�o gi� cryptocurrency (Bitcoin & Ethereum) s? d?ng **Apache Spark** d? x? l� big data, **Facebook Prophet** cho forecasting, v� **Kafka + Spark Structured Streaming** cho real-time processing.

###  M?c ti�u
-  X? l� **15+ tri?u rows** d? li?u crypto (2012-2025)
-  Ph�n t�ch xu hu?ng, volume, bi?n d?ng gi�
-  D? b�o gi� v?i **MAPE < 4%** (BTC: 2.82%, ETH: 3.61%)
-  **TRUE Structured Streaming** (Kafka + Spark)
-  Real-time processing v?i **1-10s latency**

---

##  Dataset

- **BTC (Bitcoin)**: 1-minute OHLCV data from 2012-2025
- **ETH (Ethereum)**: 1-minute OHLCV data from 2017-2025
- **Format**: CSV  Parquet (partitioned by year)

> **Luu �**: D? li?u kh�ng du?c d?y l�n GitHub do dung lu?ng l?n (15M+ rows). �?t dataset v�o thu m?c `data/btc/` v� `data/eth/`.

---

##  C?u tr�c d? �n

```
BigDataProject/
 data/                          # Raw CSV data (15M+ rows)
    btc/BTCUSDT_1min_2012-2025.csv
    eth/ETHUSDT_1min_2017-2025.csv

 data_parquet/                  # Parquet format (70% storage reduction)
    btc_clean/
    eth_clean/

 data_analysis/                 # Analysis outputs
    daily_filled/              # Daily OHLC (missing days filled)
    week4_results/             # Prophet forecasts (actual vs predicted)
    week4_visualizations/      # Interactive charts (HTML)

 week6_streaming/               #  Kafka + Spark Structured Streaming
    docker-compose.yml         # Kafka infrastructure
    websocket_producer.py      # Producer: Binance  Kafka
    spark_streaming_consumer.py # Consumer: Kafka  Spark  Parquet
    streaming_output_spark/    # Output: daily aggregates
    checkpoint_spark/          # Checkpoints for fault tolerance
    README.md                  #  Chi ti?t streaming setup!

 preprocess_step1.py            # Week 2: CSV  Parquet conversion
 preprocess_step2.py            # Week 3: Clean & daily aggregation
 prophet_train.py               # Week 5: Train Prophet & forecast

 STRUCTURED_STREAMING_SUCCESS.md #  Validation report
 README.md                      #  B?n dang d?c file n�y
```

---

##  C�ch ch?y to�n b? project

### 1 Setup m�i tru?ng
```bash
# C�i d?t Python packages
pip install pyspark pandas prophet plotly kafka-python binance-connector

# C�i Docker Desktop (cho Kafka)
# Download: https://www.docker.com/products/docker-desktop
```

### 2 X? l� historical data (Weeks 1-5)
```bash
# Convert CSV  Parquet
python preprocess_step1.py

# Clean & aggregate  daily OHLC
python preprocess_step2.py

# Train Prophet & forecast
python prophet_train.py
```

### 3 Ch?y Structured Streaming (Week 6)
```bash
cd week6_streaming

# Start Kafka infrastructure
docker-compose up -d

# Terminal 1: Producer (Binance  Kafka)
python websocket_producer.py

# Terminal 2: Consumer (Kafka  Spark  Parquet)
python spark_streaming_consumer.py
```

 **Chi ti?t Week 6:** Xem [week6_streaming/README.md](week6_streaming/README.md)

---

##  K?t qu? d?t du?c

### Data Processing
-  Processed **15+ million rows** (1-minute OHLCV data)
-  Converted to Parquet format (~70% storage reduction)
-  Cleaned & aggregated to **8,078 daily records**
-  Forward-filled missing days (5,066 BTC + 3,012 ETH days)

### Forecasting Accuracy
-  **BTC MAPE: 2.82%** (Excellent!)
-  **ETH MAPE: 3.61%** (Very Good!)
-  7-day forecast horizon
-  Interactive visualization (Plotly HTML)

### Streaming Performance
-  **TRUE Structured Streaming** (Kafka + Spark)
-  **1-10s latency** (near real-time)
-  **1000 msg/min throughput**
-  **Watermarking & windowing** (1-hour late data tolerance)
-  **Fault tolerance** (checkpoint recovery)
-  **Production-ready** architecture

---

##  Tech Stack

### Big Data & Streaming
- **Apache Spark 3.5.3** - Distributed processing & Structured Streaming
- **Apache Kafka 7.5.0** - Message broker (Confluent)
- **Zookeeper 7.5.0** - Kafka coordination
- **PySpark** - Python API for Spark
- **Parquet** - Columnar storage format

### Machine Learning & Visualization
- **Prophet** - Time series forecasting (Facebook)
- **Pandas** - Data manipulation
- **Plotly** - Interactive visualization

### Infrastructure
- **Docker & Docker Compose** - Containerization
- **Windows PowerShell** - Terminal environment

---

##  Outputs

### Historical Analysis
- `data_analysis/daily_filled/` - Daily OHLC v?i missing days d� fill
- `data_analysis/week4_results/` - Actual vs Predicted CSV
- `data_analysis/week4_visualizations/` - Interactive charts (HTML)

### Streaming Outputs
- `week6_streaming/streaming_output_spark/daily/` - Daily aggregates (partitioned by symbol)
- `week6_streaming/checkpoint_spark/` - Checkpoints for recovery

---

##  Configuration

### Spark Settings
```python
spark = SparkSession.builder \
    .appName("CryptoAnalysis") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()
```

### Kafka Settings
- **Topic:** `crypto-prices` (2 partitions)
- **Retention:** 7 days
- **Compression:** gzip
- **Acks:** all (strongest durability)

---

##  License

MIT License

##  Author

**�o�n Th? T�n**  
GitHub: [@doanthetin193](https://github.com/doanthetin193)

##  Acknowledgments

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Facebook Prophet](https://facebook.github.io/prophet/)
- [Binance API](https://binance-docs.github.io/apidocs/)

---

**C?p nh?t l?n cu?i:** 22/11/2025  
**Status:**  Ho�n th�nh 6 tu?n v?i TRUE Structured Streaming

---

 **Star repo n�y n?u th?y h?u �ch!**
