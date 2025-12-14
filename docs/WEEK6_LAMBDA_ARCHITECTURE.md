# WEEK 6 - LAMBDA ARCHITECTURE

## 🏗️ KIẾN TRÚC TỔNG QUAN

```
┌─────────────────────────────────────────────────────────────────────┐
│                    LAMBDA ARCHITECTURE                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  📦 BATCH LAYER (Historical Processing)                            │
│  ├─ Input: Binance API historical data                            │
│  ├─ Processing: Clean → Daily OHLC → Forward Fill → MA7/MA30      │
│  ├─ Output: data_analysis/daily_filled/                           │
│  └─ Run: python week6_backfill.py (when needed)                   │
│                                                                     │
│  ⚡ SPEED LAYER (Real-time Processing)                             │
│  ├─ Input: Binance API live stream                                │
│  ├─ Processing: Kafka → Spark Streaming → Windowing               │
│  ├─ Output: streaming_output_spark/daily/                         │
│  └─ Run: Streaming (continuous when machine is on)                │
│                                                                     │
│  🎯 SERVING LAYER (Unified View)                                   │
│  ├─ Input: Batch + Speed Layer data                               │
│  ├─ Processing: Merge → Dedup → Recompute MA7/MA30                │
│  ├─ Output: Unified daily_filled + prophet_input                  │
│  └─ Run: python week6_merge.py (after collecting streaming data)  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📋 QUY TRÌNH SỬ DỤNG

### **BƯỚC 1: Backfill Historical Gap (Chạy 1 lần đầu)**

```bash
python week6_backfill.py
```

**Chức năng:**
- Phát hiện ngày cuối cùng trong data (từ Week 1-5)
- Fetch dữ liệu từ ngày đó → hôm nay (Binance API)
- Xử lý: Clean, daily OHLC, forward fill, MA7/MA30
- Lưu vào `data_analysis/daily_filled/`

**Kết quả:**
```
Last date: 2025-09-25
Today: 2025-11-24
Gap: 60 days

Fetching BTCUSDT... ✅ 84,961 rows
Fetching ETHUSDT... ✅ 84,961 rows

Daily aggregation: 120 rows
Forward fill: 120 rows
MA7/MA30 computed

✅ Saved to data_analysis/daily_filled/
```

---

### **BƯỚC 2: Start Streaming (Chạy liên tục khi có thể)**

**Terminal 1 - Start Kafka + Zookeeper:**
```bash
cd week6_streaming
docker-compose up -d
```

**Terminal 2 - Start Producer:**
```bash
python week6_streaming/websocket_producer.py
```
→ Poll Binance API mỗi 1 giây, gửi vào Kafka

**Terminal 3 - Start Consumer:**
```bash
python week6_streaming/spark_streaming_consumer.py
```
→ Spark Structured Streaming xử lý real-time

**Kết quả:**
- Streaming chạy liên tục
- Thu thập dữ liệu real-time vào `streaming_output_spark/daily/`
- Watermarking, windowing, checkpointing tự động

---

### **BƯỚC 3: Merge Batch + Streaming (Khi cần timeline hoàn chỉnh)**

```bash
python week6_merge.py
```

**Chức năng:**
- Đọc batch data (backfill)
- Đọc streaming data
- Merge, dedup, recompute MA7/MA30
- Tạo timeline liền mạch

**Kết quả:**
```
Batch data: 120 rows (26/9 - 24/11)
Streaming data: 5 rows (24/11 - 29/11)
Merged: 125 rows

✅ Unified timeline: 2012 → 29/11/2025
```

---

## 🎯 SCENARIO THỰC TẾ SINH VIÊN

### **Scenario 1: Lần đầu setup (Hôm nay 24/11)**

```bash
# Bước 1: Backfill gap
python week6_backfill.py
# → Lấy 26/9 → 24/11 (60 ngày)

# Bước 2: Start streaming
cd week6_streaming
docker-compose up -d
python websocket_producer.py &
python spark_streaming_consumer.py

# Để streaming chạy 2-3 giờ, thu thập data
# Sau đó tắt (Ctrl+C) và tắt máy
```

---

### **Scenario 2: Vào lại sau vài ngày (28/11)**

```bash
# Bước 1: Backfill gap mới
python week6_backfill.py
# → Tự động phát hiện thiếu 25/11 - 27/11
# → Fetch 3 ngày từ Binance API

# Bước 2: Start streaming lại
cd week6_streaming
docker-compose up -d
python websocket_producer.py &
python spark_streaming_consumer.py

# Chạy thêm vài giờ, thu thập 28/11
```

---

### **Scenario 3: Trình bày final (15/12)**

```bash
# Bước 1: Backfill lần cuối
python week6_backfill.py
# → Lấy 25/11 → 15/12

# Bước 2: Merge tất cả data
python week6_merge.py
# → Tạo timeline hoàn chỉnh 2012 → 15/12

# Bước 3: Train Prophet với data mới
python prophet_train.py

# Bước 4: Demo streaming live cho giảng viên
cd week6_streaming
docker-compose up -d
python websocket_producer.py &
python spark_streaming_consumer.py
# → Cho giảng viên thấy real-time processing
```

---

## 📊 CẤU TRÚC OUTPUT

```
data_analysis/
├── daily_filled/              ← Batch Layer output (backfill)
│   ├── symbol=BTCUSDT/
│   └── symbol=ETHUSDT/
│
├── prophet_input/             ← Ready for forecasting
│   ├── symbol=BTCUSDT/
│   └── symbol=ETHUSDT/
│
└── (after merge complete, daily_filled contains unified data)

streaming_output_spark/
└── daily/                     ← Speed Layer output (streaming)
    ├── symbol=BTCUSDT/
    └── symbol=ETHUSDT/
```

---

## 🎓 TRÌNH BÀY CHO GIẢNG VIÊN

### **1. Giới thiệu Lambda Architecture:**

"Em áp dụng Lambda Architecture cho Week 6 vì:
- ✅ Đúng yêu cầu Streaming (Speed Layer)
- ✅ Giải quyết constraint sinh viên (không có server 24/7)
- ✅ Không bỏ sót data (Batch Layer backfill)
- ✅ Timeline liền mạch (Serving Layer merge)"

---

### **2. Demo Batch Layer:**

```bash
python week6_backfill.py
```

"Script tự động:
- Phát hiện gap trong data
- Fetch từ Binance API
- Xử lý giống pipeline Week 1-5 (clean, forward fill, MA7/MA30)
- Lưu vào daily_filled"

---

### **3. Demo Speed Layer (Streaming):**

```bash
# Terminal 1
cd week6_streaming
docker-compose up -d

# Terminal 2
python websocket_producer.py

# Terminal 3
python spark_streaming_consumer.py
```

"Streaming architecture:
- Kafka: Message broker (buffering, fault tolerance)
- Producer: Poll Binance API mỗi 1 giây
- Consumer: Spark Structured Streaming
- Features: Watermarking (1 hour), Windowing (1 day), Checkpointing"

**Cho giảng viên thấy console output:**
```
Batch 0: BTCUSDT $84,569, ETHUSDT $2,757
Batch 1: BTCUSDT $84,601, ETHUSDT $2,763
...
```

---

### **4. Demo Serving Layer:**

```bash
python week6_merge.py
```

"Merge batch + streaming:
- Union 2 data sources
- Remove duplicates
- Recompute MA7/MA30 cho toàn bộ timeline
- Kết quả: Timeline liền mạch 2012 → hôm nay"

---

### **5. Kết luận:**

"Lambda Architecture cho phép em:
- **Batch Layer:** Xử lý lịch sử khi tắt máy (backfill)
- **Speed Layer:** Xử lý real-time khi máy bật (streaming)
- **Serving Layer:** Merge tạo view thống nhất

Đây là practice chuẩn trong Big Data production khi có constraints về infrastructure."

---

## ✅ ƯU ĐIỂM

| Aspect | Lambda Architecture |
|--------|---------------------|
| **Đúng đề cương** | ✅ Có Streaming (Speed Layer) |
| **Sinh viên** | ✅ Không cần server 24/7 |
| **Data loss** | ✅ Backfill khi tắt máy |
| **Timeline** | ✅ Liền mạch (batch + streaming) |
| **Complexity** | ⚠️ Vừa phải (3 scripts) |
| **Cơ sở lý thuyết** | ✅ Nathan Marz - Big Data standard |

---

## 📚 TÀI LIỆU THAM KHẢO

- **Lambda Architecture:** Nathan Marz (2011)
- **Spark Structured Streaming:** Apache Spark Documentation
- **Kafka:** Apache Kafka Documentation
- **Binance API:** Binance Official API Docs

---

## 🎯 CHECKLIST TRƯỚC KHI TRÌNH BÀY

- [ ] Run week6_backfill.py thành công
- [ ] Docker Desktop đang chạy
- [ ] Kafka + Zookeeper up (docker-compose up -d)
- [ ] Streaming chạy được ít nhất 1-2 giờ (có data)
- [ ] Run week6_merge.py thành công
- [ ] daily_filled có data liền mạch
- [ ] Hiểu rõ Lambda Architecture concept
- [ ] Chuẩn bị giải thích tại sao dùng approach này
