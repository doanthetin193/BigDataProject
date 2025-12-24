# 📊 MÃ MERMAID CHO BÁO CÁO

File này chứa các mã Mermaid để vẽ sơ đồ cho báo cáo. Bạn có thể copy và paste vào [Mermaid Live Editor](https://mermaid.live/) để vẽ, sau đó export ra PNG/SVG.

---

## 1. SƠ ĐỒ KIẾN TRÚC LAMBDA ARCHITECTURE (Phần Mở đầu / Phần 1.1)

**Vị trí chèn:** Sau dòng "Kiến trúc Lambda Architecture:" trong phần Mở đầu

```mermaid
flowchart TB
    subgraph DS["📥 DATA SOURCES"]
        KAGGLE["📁 Kaggle CSV<br/>11.5M rows<br/>2012-2025"]
        BINANCE["🌐 Binance API<br/>Real-time<br/>Backfill"]
    end

    subgraph BL["⚙️ BATCH LAYER"]
        CSV2PQ["CSV → Parquet"]
        AGG["Aggregate<br/>1-min → Daily"]
        MA["Compute<br/>MA7/MA30"]
        BACKFILL["Backfill<br/>Binance API"]
        PROPHET["🔮 Prophet<br/>Training"]
    end

    subgraph SL["⚡ SPEED LAYER"]
        PRODUCER["Producer<br/>Poll 1s"]
        KAFKA["Apache Kafka<br/>crypto-prices"]
        CONSUMER["Spark Consumer<br/>Batch Reader"]
    end

    subgraph SVL["📊 SERVING LAYER"]
        MERGE["Merge<br/>Batch + Speed"]
        DAILY["daily_filled<br/>Unified Data"]
        DASHBOARD["📈 Streamlit<br/>Dashboard"]
    end

    KAGGLE --> CSV2PQ
    BINANCE --> BACKFILL
    BINANCE --> PRODUCER

    CSV2PQ --> AGG --> MA --> BACKFILL --> PROPHET
    PRODUCER --> KAFKA --> CONSUMER

    PROPHET --> MERGE
    CONSUMER --> MERGE
    MERGE --> DAILY --> DASHBOARD

    style DS fill:#e1f5fe
    style BL fill:#fff3e0
    style SL fill:#f3e5f5
    style SVL fill:#e8f5e9
```

---

## 2. SƠ ĐỒ DATA PIPELINE (Phần 2.4 hoặc Phần 3.1)

**Vị trí chèn:** Sau phần "Khối lượng dữ liệu" hoặc đầu Phần 3

```mermaid
flowchart LR
    subgraph INPUT["📥 Input"]
        CSV["CSV Files<br/>557 MB<br/>11.5M rows"]
    end

    subgraph PREPROCESSING["⚙️ Preprocessing"]
        P1["convert_to_parquet.py<br/>→ Parquet 335MB"]
        P2["preprocess_step1.py<br/>→ daily_raw 7,980 rows"]
        P3["preprocess_step2.py<br/>→ daily_filled + MA"]
    end

    subgraph BATCH["📦 Batch Layer"]
        BF["week6_backfill.py<br/>+80 days from API"]
        MG["week6_merge.py<br/>Merge batch+streaming"]
    end

    subgraph ML["🔮 Machine Learning"]
        PT["prophet_train.py<br/>Grid Search + CV"]
        FC["Forecasts<br/>MAPE 2.38%"]
    end

    subgraph OUTPUT["📊 Output"]
        DASH["Streamlit<br/>Dashboard"]
    end

    CSV --> P1 --> P2 --> P3 --> BF --> MG --> PT --> FC --> DASH

    style INPUT fill:#ffebee
    style PREPROCESSING fill:#e3f2fd
    style BATCH fill:#fff8e1
    style ML fill:#f3e5f5
    style OUTPUT fill:#e8f5e9
```

---

## 3. SƠ ĐỒ SPEED LAYER / STREAMING (Phần 3.2)

**Vị trí chèn:** Đầu phần 3.2 "Thu thập và xử lý dữ liệu thời gian thực"

```mermaid
flowchart LR
    subgraph API["🌐 Binance API"]
        TICKER["GET /ticker/24hr<br/>BTCUSDT, ETHUSDT"]
    end

    subgraph PRODUCER["📤 Producer"]
        WS["websocket_producer.py<br/>Poll every 1 second"]
    end

    subgraph KAFKA["📨 Apache Kafka"]
        TOPIC["Topic: crypto-prices<br/>Port 9092"]
    end

    subgraph CONSUMER["📥 Consumer"]
        STREAM["spark_streaming_consumer.py<br/>⏱️ Window 1 day + 1h watermark<br/>⚠️ Cần 25h cho output"]
        BATCH["kafka_batch_reader.py<br/>✅ Batch mode<br/>⚡ 2-3 giây có kết quả"]
    end

    subgraph OUTPUT["📁 Output"]
        PARQUET["streaming_output_spark_BATCH/<br/>Daily OHLC Parquet"]
    end

    API --> TICKER --> WS
    WS -->|"JSON messages<br/>86,400/day"| TOPIC
    TOPIC --> STREAM
    TOPIC --> BATCH
    BATCH -->|"Demo Mode"| PARQUET

    style API fill:#e3f2fd
    style PRODUCER fill:#fff3e0
    style KAFKA fill:#fce4ec
    style CONSUMER fill:#f3e5f5
    style OUTPUT fill:#e8f5e9
```

---

## 4. SƠ ĐỒ PROPHET TRAINING PROCESS (Phần 3.1.3)

**Vị trí chèn:** Đầu phần 3.1.3 "Xây dựng mô hình Prophet"

```mermaid
flowchart TB
    subgraph DATA["📥 Data Preparation"]
        INPUT["prophet_input<br/>+ daily_filled"]
        SPLIT["Train/Test Split<br/>80% / 20%"]
        HOLIDAY["BTC Halving<br/>Holidays"]
    end

    subgraph TUNING["🔧 Hyperparameter Tuning"]
        GRID["Grid Search<br/>6 combinations"]
        MODE["seasonality_mode<br/>additive / multiplicative"]
        PRIOR["changepoint_prior<br/>0.01 / 0.05 / 0.1"]
    end

    subgraph TRAINING["🔮 Training"]
        REGRESSOR["Add Regressors<br/>MA7, MA30"]
        FIT["model.fit()"]
        PREDICT["model.predict()"]
    end

    subgraph EVAL["📊 Evaluation"]
        MAPE["MAPE Calculation"]
        CV["Cross-Validation<br/>30 days horizon"]
        BEST["Select Best Model"]
    end

    subgraph OUTPUT["📁 Output"]
        FORECAST["Forecasts<br/>Parquet"]
        METRICS["Metrics<br/>CSV"]
        VIS["Visualizations<br/>PNG, HTML"]
    end

    INPUT --> SPLIT --> HOLIDAY
    HOLIDAY --> GRID
    GRID --> MODE & PRIOR
    MODE & PRIOR --> REGRESSOR --> FIT --> PREDICT
    PREDICT --> MAPE --> CV --> BEST
    BEST --> FORECAST & METRICS & VIS

    style DATA fill:#e3f2fd
    style TUNING fill:#fff8e1
    style TRAINING fill:#f3e5f5
    style EVAL fill:#ffebee
    style OUTPUT fill:#e8f5e9
```

---

## 5. SƠ ĐỒ SERVING LAYER / MERGE (Phần 3.2.4)

**Vị trí chèn:** Đầu phần 3.2.4 "Merge Batch Layer và Speed Layer"

```mermaid
flowchart TB
    subgraph BATCH["⚙️ Batch Layer"]
        DAILY_FILLED["daily_filled/<br/>5,097 rows BTC<br/>3,043 rows ETH"]
    end

    subgraph SPEED["⚡ Speed Layer"]
        STREAMING["streaming_output_spark_BATCH/<br/>2 rows (mới)"]
    end

    subgraph MERGE["🔄 Merge Process"]
        UNION["UNION<br/>batch + streaming"]
        DEDUP["dropDuplicates<br/>(symbol, date)"]
        RECOMPUTE["Recompute<br/>MA7 / MA30"]
        CACHE["Cache DataFrame"]
    end

    subgraph OUTPUT["📊 Output"]
        NEW_DAILY["daily_filled/<br/>Updated"]
        PROPHET_INPUT["prophet_input/<br/>Updated"]
        DASHBOARD["Streamlit<br/>Dashboard"]
    end

    DAILY_FILLED --> UNION
    STREAMING --> UNION
    UNION --> DEDUP --> RECOMPUTE --> CACHE
    CACHE --> NEW_DAILY & PROPHET_INPUT
    NEW_DAILY --> DASHBOARD

    style BATCH fill:#fff3e0
    style SPEED fill:#f3e5f5
    style MERGE fill:#e3f2fd
    style OUTPUT fill:#e8f5e9
```

---

## 6. SƠ ĐỒ DEMO WORKFLOW (Phần 4.3.2)

**Vị trí chèn:** Thay thế hoặc bổ sung cho phần "Quy trình chạy Demo"

```mermaid
flowchart TB
    subgraph STEP1["1️⃣ Preprocessing"]
        S1A["convert_to_parquet.py"]
        S1B["preprocess_step1.py"]
        S1C["preprocess_step2.py"]
    end

    subgraph STEP2["2️⃣ Backfill"]
        S2["week6_backfill.py<br/>+80 days API data"]
    end

    subgraph STEP3["3️⃣ Streaming Demo"]
        S3A["docker-compose up -d"]
        S3B["websocket_producer.py<br/>⏱️ 10 phút"]
        S3C["kafka_batch_reader.py<br/>⏱️ 2-3 giây"]
    end

    subgraph STEP4["4️⃣ Merge"]
        S4["week6_merge.py"]
    end

    subgraph STEP5["5️⃣ Train"]
        S5["prophet_train.py<br/>⏱️ ~5 phút"]
    end

    subgraph STEP6["6️⃣ Dashboard"]
        S6["streamlit run app.py<br/>🌐 localhost:8501"]
    end

    S1A --> S1B --> S1C --> S2 --> S3A --> S3B --> S3C --> S4 --> S5 --> S6

    style STEP1 fill:#e3f2fd
    style STEP2 fill:#fff8e1
    style STEP3 fill:#f3e5f5
    style STEP4 fill:#ffebee
    style STEP5 fill:#e8f5e9
    style STEP6 fill:#e1f5fe
```

---

## 7. SƠ ĐỒ TECHNOLOGY STACK (Phần 4.1)

**Vị trí chèn:** Sau bảng "Lựa chọn công cụ"

```mermaid
flowchart TB
    subgraph PROCESSING["⚙️ Processing"]
        SPARK["Apache Spark 3.5.3<br/>PySpark"]
    end

    subgraph STORAGE["💾 Storage"]
        PARQUET["Parquet<br/>Columnar Format"]
    end

    subgraph STREAMING["📨 Streaming"]
        KAFKA["Apache Kafka 7.5.0"]
        ZK["Zookeeper"]
    end

    subgraph ML["🔮 Machine Learning"]
        PROPHET["Facebook Prophet 1.2.1"]
        SKLEARN["scikit-learn"]
    end

    subgraph VIZ["📊 Visualization"]
        STREAMLIT["Streamlit 1.28+"]
        PLOTLY["Plotly"]
        MPL["Matplotlib"]
    end

    subgraph LANG["🐍 Language"]
        PYTHON["Python 3.10.11"]
    end

    PYTHON --> SPARK & KAFKA & PROPHET & STREAMLIT
    SPARK --> PARQUET
    KAFKA --> ZK
    PROPHET --> SKLEARN
    STREAMLIT --> PLOTLY & MPL

    style PROCESSING fill:#ff9800,color:#fff
    style STORAGE fill:#4caf50,color:#fff
    style STREAMING fill:#9c27b0,color:#fff
    style ML fill:#2196f3,color:#fff
    style VIZ fill:#f44336,color:#fff
    style LANG fill:#795548,color:#fff
```

---

## 8. SƠ ĐỒ KQUA - ACTUAL VS PREDICTED (Phần 4.4)

**Vị trí chèn:** Phần kết quả minh họa (nếu cần)

```mermaid
xychart-beta
    title "BTCUSDT: Actual vs Predicted (Dec 2025)"
    x-axis ["Dec 10", "Dec 11", "Dec 12", "Dec 13", "Dec 14"]
    y-axis "Price ($)" 42000 --> 45000
    line "Actual" [43250, 42800, 44100, 43500, 42900]
    line "Predicted" [43150, 43000, 43950, 43780, 43200]
```

---

## 📝 HƯỚNG DẪN SỬ DỤNG

1. Copy mã Mermaid (phần trong ```mermaid ... ```)
2. Truy cập [Mermaid Live Editor](https://mermaid.live/)
3. Paste code vào editor
4. Điều chỉnh màu sắc/layout nếu cần
5. Export ra PNG hoặc SVG
6. Chèn hình vào báo cáo Word

**Lưu ý:** Một số diagram phức tạp có thể cần chỉnh sửa thêm trong editor để đẹp hơn.
