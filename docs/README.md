# 📂 BigDataProject - Cấu Trúc Dự Án

## 🗂️ Cấu Trúc Thư Mục

```
D:\BigDataProject/
│
├── 📜 scripts/                    # All Python scripts
│   ├── preprocessing/             # Data preparation
│   │   ├── convert_to_parquet.py  # CSV → Parquet conversion
│   │   ├── clean_parquet.py       # Remove duplicates
│   │   ├── preprocess_step1.py    # 1-min → daily aggregation
│   │   └── preprocess_step2.py    # Forward fill + MA calculation
│   │
│   ├── lambda_batch/              # Batch Layer (Lambda Architecture)
│   │   ├── week6_backfill.py      # Regular backfill (gap < 30 days)
│   │   ├── week6_backfill_batch.py # Large gap backfill (> 30 days)
│   │   ├── week6_merge.py         # Serving Layer merge
│   │   └── week6_merge_temp.py    # One-time merge utility
│   │
│   ├── ml_models/                 # Machine Learning
│   │   └── prophet_train.py       # Prophet forecasting
│   │
│   └── utils/                     # Utilities & debugging
│       ├── check_data.py          # Data verification
│       ├── check_forecast.py      # Forecast results check
│       ├── final_verification.py  # System verification
│       └── cleanup_analysis.py    # Cleanup recommendations
│
├── 📚 docs/                       # Documentation
│   ├── README.md                  # This file
│   ├── PROJECT_SUMMARY.md         # Complete project summary
│   ├── proposed_structure.md      # Directory structure proposal
│   ├── WEEK6_01_TONG_QUAN.md      # Lambda Architecture overview
│   ├── WEEK6_02_BATCH_LAYER.md    # Batch layer docs
│   ├── WEEK6_03_SPEED_LAYER.md    # Speed layer docs
│   ├── WEEK6_04_SERVING_LAYER.md  # Serving layer docs
│   ├── WEEK6_HUONG_DAN_CHAY.md    # Running instructions
│   ├── WEEK6_LAMBDA_ARCHITECTURE.md # Architecture details
│   └── WEEK6_SPARK_STREAMING_CONSUMER_GIAI_THICH.md
│
├── 💾 data/                       # Raw CSV data
│   ├── btc/
│   │   └── BTCUSDT_1min_2012-2025.csv (7.2M rows)
│   └── eth/
│       └── ETHUSDT_1min_2017-2025.csv (4M+ rows)
│
├── 📊 data_parquet/               # Parquet format (partitioned)
│   ├── btc_clean/                 # year=YYYY/month=MM/
│   └── eth_clean/                 # year=YYYY/month=MM/
│
├── 📈 data_analysis/              # Processed data & results
│   ├── daily_filled/              # 8,140 daily rows (OHLCV + MA)
│   ├── prophet_input/             # 8,140 rows (ds, y, symbol)
│   ├── week4_forecasts/           # Prophet forecast parquet
│   ├── week4_metrics/             # Model performance metrics
│   ├── week4_results/             # Actual vs predicted CSV
│   └── week4_visualizations/      # Interactive HTML charts
│
├── 🌊 week6_streaming/            # Speed Layer (Lambda Architecture)
│   ├── docker-compose.yml         # Kafka setup
│   ├── websocket_producer.py      # Binance WebSocket → Kafka
│   ├── spark_streaming_consumer.py # Kafka → Spark Streaming
│   ├── checkpoint_spark/          # Spark checkpoints
│   └── streaming_output_spark/    # Real-time aggregations
│
├── 🔧 hadoop/                     # Hadoop binaries (for Spark on Windows)
│   └── bin/
│
└── 📝 logs/                       # Application logs
```

---

## 🚀 Quick Start

### 1️⃣ **Preprocessing (First Time Setup)**
```bash
cd scripts/preprocessing
python convert_to_parquet.py  # CSV → Parquet
python clean_parquet.py        # Remove duplicates
python preprocess_step1.py     # Aggregate to daily
python preprocess_step2.py     # Forward fill + MA
```

### 2️⃣ **Train Prophet Model**
```bash
cd scripts/ml_models
python prophet_train.py
```

### 3️⃣ **Batch Layer - Backfill Missing Data**

**Regular update (< 30 days gap):**
```bash
cd scripts/lambda_batch
python week6_backfill.py
```

**Large update (> 30 days gap):**
```bash
cd scripts/lambda_batch
python week6_backfill_batch.py
python week6_merge_temp.py
```

### 4️⃣ **Speed Layer - Real-time Streaming**
```bash
cd week6_streaming
docker-compose up -d           # Start Kafka
python websocket_producer.py   # Producer (background)
python spark_streaming_consumer.py  # Consumer
```

### 5️⃣ **Serving Layer - Query Unified View**
```bash
cd scripts/lambda_batch
python week6_merge.py
```

---

## 📊 Data Flow

```
CSV (1-min raw)
    ↓
Parquet (partitioned)
    ↓
daily_raw (OHLCV)
    ↓
daily_filled (OHLCV + MA7/MA30) ← SOURCE
    ↓
prophet_input (ds, y, symbol) ← DERIVED
    ↓
Prophet Model → Forecasts
```

---

## 🏗️ Lambda Architecture

```
┌─────────────────────────────────────┐
│       SERVING LAYER                 │
│    (week6_merge.py)                 │
│  Batch View + Real-time View        │
└────────┬──────────────┬─────────────┘
         │              │
    ┌────┴────┐    ┌───┴──────┐
    │ BATCH   │    │  SPEED   │
    │ LAYER   │    │  LAYER   │
    └─────────┘    └──────────┘
```

**Batch Layer:**
- Historical data processing
- Immutable master dataset
- Backfill automation

**Speed Layer:**
- Real-time data ingestion
- WebSocket → Kafka → Spark Streaming
- Low-latency updates

**Serving Layer:**
- Merge Batch + Speed views
- Unified query interface
- Consistent data model

---

## 📈 Results

**Dataset:**
- 8,140 daily rows (BTC: 5,097 + ETH: 3,043)
- Date range: 2012-01-01 → 2025-12-14
- Complete with MA7/MA30 indicators

**Model Performance:**
- BTCUSDT: MAPE 3.36% (CV)
- ETHUSDT: MAPE 3.90% (CV)
- Quality: ✅ GOOD (< 5%)

**Visualizations:**
- Interactive Plotly charts in `data_analysis/week4_visualizations/`
- Actual vs predicted CSVs in `data_analysis/week4_results/`

---

## 🛠️ Utilities

**Verification:**
```bash
cd scripts/utils
python final_verification.py  # Check entire system
python check_data.py          # Verify data completeness
python check_forecast.py      # Inspect forecast results
```

**Cleanup:**
```bash
cd scripts/utils
python cleanup_analysis.py    # Analyze disk usage
```

---

## 📚 Documentation

Xem thêm chi tiết trong `docs/`:

- **PROJECT_SUMMARY.md**: Tổng quan toàn bộ dự án
- **WEEK6_01_TONG_QUAN.md**: Lambda Architecture overview
- **WEEK6_HUONG_DAN_CHAY.md**: Hướng dẫn chạy từng bước
- **WEEK6_02_BATCH_LAYER.md**: Chi tiết Batch Layer
- **WEEK6_03_SPEED_LAYER.md**: Chi tiết Speed Layer
- **WEEK6_04_SERVING_LAYER.md**: Chi tiết Serving Layer

---

## ✅ System Status

Chạy để kiểm tra:
```bash
cd scripts/utils
python final_verification.py
```

Kết quả mong đợi:
```
✅ Data up to date
✅ Prophet input schema
✅ Model quality (MAPE < 5%)
✅ Lambda files exist
✅ Schema consistency

✅ ALL SYSTEMS READY!
```

---

## 🔗 Links

- Binance API: https://binance-docs.github.io/apidocs/
- Prophet: https://facebook.github.io/prophet/
- PySpark: https://spark.apache.org/docs/latest/api/python/

---

*Last updated: 2025-12-14*
