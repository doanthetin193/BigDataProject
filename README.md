# 🚀 Big Data Project - Cryptocurrency Analysis & Forecasting

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange.svg)](https://spark.apache.org/)
[![Prophet](https://img.shields.io/badge/Prophet-Time%20Series-green.svg)](https://facebook.github.io/prophet/)

## 📋 Mô tả dự án

Dự án phân tích và dự báo giá cryptocurrency (Bitcoin & Ethereum) sử dụng **Apache Spark** để xử lý big data và **Facebook Prophet** cho time series forecasting.

### 🎯 Mục tiêu
- Xử lý dữ liệu giá crypto theo từng phút (2012-2025)
- Phân tích xu hướng, volume, và biến động giá
- Dự báo giá 30 ngày sử dụng Prophet
- Tối ưu hóa hyperparameters với grid search

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
├── convert_to_parquet.py       # CSV → Parquet conversion
├── clean_parquet.py            # Data cleaning (duplicates, nulls)
├── preprocess_step1.py         # Daily OHLC aggregation
├── preprocess_step2.py         # Missing days fill + MA calculation
├── preprocess.py               # Unified preprocessing pipeline
├── prophet_train.py            # Prophet forecasting with grid search
├── data/                       # Raw CSV data (not in git)
│   ├── btc/
│   └── eth/
├── data_parquet/               # Parquet files (not in git)
│   ├── btc_clean/
│   └── eth_clean/
└── data_analysis/              # Analysis outputs (not in git)
    ├── daily_filled/
    ├── prophet_input/
    ├── week4_forecasts/
    ├── week4_metrics/
    └── week4_visualizations/
```

## 🚀 Installation

### 1. Clone repository
```bash
git clone https://github.com/doanthetin193/BigDataProject.git
cd BigDataProject
```

### 2. Cài đặt dependencies
```bash
pip install pyspark pandas numpy matplotlib plotly prophet scikit-learn pyarrow
```

### 3. Tải dataset
- Tải dữ liệu BTCUSDT và ETHUSDT (1-minute OHLCV)
- Đặt vào thư mục `data/btc/` và `data/eth/`

### 4. Cài đặt Hadoop (Windows)
- Tải `winutils.exe` cho Spark trên Windows
- Đặt vào `hadoop/bin/`
- Set biến môi trường: `HADOOP_HOME=D:\BigDataProject\hadoop`

## 📝 Usage

### Pipeline xử lý dữ liệu

```bash
# 1. Convert CSV to Parquet
python convert_to_parquet.py

# 2. Clean data (remove duplicates)
python clean_parquet.py

# 3. Preprocess step 1 (daily OHLC)
python preprocess_step1.py

# 4. Preprocess step 2 (fill missing + MA)
python preprocess_step2.py

# Hoặc chạy unified pipeline:
python preprocess.py
```

### Training Prophet model

```bash
python prophet_train.py
```

## 📈 Features

### Data Processing
- ✅ Duplicate removal
- ✅ Forward fill missing values
- ✅ Missing days detection & filling
- ✅ Large gap detection (>60s)
- ✅ Daily OHLC aggregation
- ✅ Moving Averages (MA7, MA30)

### Forecasting
- ✅ Grid search hyperparameters
- ✅ Cross-validation (30-day horizon)
- ✅ Multiple metrics (MSE, MAPE, CV-MAPE)
- ✅ Holiday effects (BTC halving events)
- ✅ Regressors (MA7, MA30)

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
