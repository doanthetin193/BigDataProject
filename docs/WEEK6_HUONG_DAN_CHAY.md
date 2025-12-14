# 📘 HƯỚNG DẪN CHẠY WEEK 6 - LAMBDA ARCHITECTURE

## 🎯 Tổng quan

Week 6 sử dụng **Lambda Architecture** gồm 3 layer:
- **Batch Layer**: Backfill dữ liệu gap từ Binance API
- **Speed Layer**: Streaming real-time qua Kafka + Spark
- **Serving Layer**: Merge batch + streaming → Forecast

---

## 📋 Yêu cầu trước khi chạy

### 1. Phần mềm cần cài đặt:
- ✅ Python 3.10+
- ✅ Docker Desktop (đã cài và chạy)
- ✅ Các thư viện Python: `pyspark`, `kafka-python`, `requests`, `prophet`

### 2. Kiểm tra Docker:
```powershell
# Mở Docker Desktop trước
# Sau đó kiểm tra:
docker --version
docker ps
```

---

## 🚀 CÁC BƯỚC CHẠY

### BƯỚC 1: Backfill dữ liệu gap (Batch Layer)

**Mục đích**: Tự động phát hiện và fetch dữ liệu từ ngày cuối cùng trong database đến hôm nay.

```powershell
cd D:\BigDataProject
python week6_backfill.py
```

**Kết quả mong đợi:**
```
[STEP 1] Detecting last date in existing data...
  ✅ Last date found in daily_filled: 2025-XX-XX
  📅 Today: 2025-XX-XX
  📊 Gap: X days

  🎯 Will backfill: 2025-XX-XX → 2025-XX-XX (X days)

[STEP 2] Fetching data from Binance API...
  ✅ Fetched XXXX rows

...

✅ BACKFILL COMPLETE (BATCH LAYER)
```

**Lưu ý:**
- Cần có kết nối Internet để fetch từ Binance API
- Nếu không có gap (chạy cùng ngày), script sẽ báo "No gap detected"

---

### BƯỚC 2: Khởi động Kafka (Speed Layer - Phần 1)

**Mục đích**: Khởi động Kafka + Zookeeper để nhận dữ liệu streaming.

```powershell
cd D:\BigDataProject\week6_streaming
docker-compose up -d
```

**Kết quả mong đợi:**
```
[+] Running 3/3
 ✔ Network week6_streaming_crypto-network  Created
 ✔ Container zookeeper                     Started
 ✔ Container kafka                         Started
```

**Kiểm tra containers đã chạy:**
```powershell
docker ps
```

**Kết quả mong đợi:**
```
CONTAINER ID   IMAGE                             STATUS          PORTS                    NAMES
xxxx           confluentinc/cp-kafka:7.5.0       Up X seconds    0.0.0.0:9092->9092/tcp   kafka
xxxx           confluentinc/cp-zookeeper:7.5.0   Up X seconds    0.0.0.0:2181->2181/tcp   zookeeper
```

**⚠️ Nếu Kafka không chạy (chỉ thấy zookeeper):**
```powershell
# Xem log lỗi
docker logs kafka

# Nếu lỗi "InconsistentClusterIdException", chạy:
docker-compose down -v
docker-compose up -d
```

**Chờ 10-15 giây** để Kafka khởi động hoàn toàn trước khi chạy bước tiếp theo.

---

### BƯỚC 3: Chạy Producer (Speed Layer - Phần 2)

**Mục đích**: Fetch dữ liệu real-time từ Binance API và gửi vào Kafka.

**Mở Terminal mới (Terminal 2):**
```powershell
cd D:\BigDataProject
python week6_streaming/websocket_producer.py
```

**Kết quả mong đợi:**
```
======================================================================
BINANCE API → KAFKA PRODUCER
Continuous Streaming (1 second interval)
======================================================================
✓ Producer connected
✓ Sending real-time data to Kafka topic: crypto-prices

Press Ctrl+C to stop

📊 BTCUSDT: $XX,XXX.XX | Vol: XX,XXX | Change: +X.XX% | Messages: XX
📊 ETHUSDT: $X,XXX.XX | Vol: XXX,XXX | Change: +X.XX% | Messages: XX
```

**Để chạy 1-2 phút** để collect đủ dữ liệu demo, sau đó **nhấn Ctrl+C** để dừng.

**⚠️ Nếu lỗi "NoBrokersAvailable":**
- Kafka chưa sẵn sàng, chờ thêm 10-15 giây rồi chạy lại
- Hoặc kiểm tra `docker ps` xem Kafka có đang chạy không

---

### BƯỚC 4: Kiểm tra dữ liệu trong Kafka (Tùy chọn)

**Mục đích**: Xác nhận dữ liệu đã được gửi vào Kafka.

```powershell
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto-prices --from-beginning --max-messages 3
```

**Kết quả mong đợi:**
```json
{"symbol": "BTCUSDT", "price": 92817.92, "volume": 29318.80, ...}
{"symbol": "ETHUSDT", "price": 3031.0, "volume": 531945.72, ...}
{"symbol": "BTCUSDT", "price": 92820.00, ...}
Processed a total of 3 messages
```

---

### BƯỚC 5: Merge dữ liệu (Serving Layer)

**Mục đích**: Kết hợp dữ liệu Batch + Streaming thành một timeline thống nhất.

```powershell
cd D:\BigDataProject
python week6_merge.py
```

**Kết quả mong đợi:**
```
================================================================================
WEEK 6 - SERVING LAYER (Lambda Architecture)
Merge Batch + Streaming Data
================================================================================

[STEP 1] Reading Batch Layer (backfill data)...
  ✅ Batch data loaded: XX rows

[STEP 2] Reading Speed Layer (streaming data)...
  ⚠️  No streaming data found (hoặc ✅ Streaming data loaded: XX rows)

✅ Using batch data only (hoặc ✅ Merged: XX rows)
```

**Lưu ý:** Nếu streaming chưa chạy đủ lâu (< 1 ngày), sẽ không có output file. Script vẫn hoạt động bình thường với batch data.

---

### BƯỚC 6: Chạy Prophet Forecast

**Mục đích**: Dự đoán giá crypto dựa trên dữ liệu đã merge.

```powershell
python prophet_train.py
```

**Kết quả mong đợi:**
```
✅ Backed up week4_forecasts → week4_forecasts_old
✅ Backed up week4_visualizations → week4_visualizations_old
...

=== Processing BTCUSDT ===
BTCUSDT - Train: XX rows, Test: XX rows
...
BTCUSDT - MAPE: X.XX%

=== Processing ETHUSDT ===
...

=== Summary ===
  symbol      mse       mape    mode
  BTCUSDT     XXXXX     X.XX    multiplicative
  ETHUSDT     XXXXX     X.XX    additive

✅ Forecast & evaluation complete!
```

---

### BƯỚC 7: Dừng Kafka (Cleanup)

**Sau khi demo xong:**
```powershell
cd D:\BigDataProject\week6_streaming
docker-compose down
```

**Kết quả mong đợi:**
```
[+] Running 3/3
 ✔ Container kafka      Removed
 ✔ Container zookeeper  Removed
 ✔ Network week6_streaming_crypto-network  Removed
```

---

## 📁 CẤU TRÚC OUTPUT

Sau khi chạy xong, các output sẽ nằm ở:

```
data_analysis/
├── daily_filled/              ← Dữ liệu đã backfill (Batch Layer)
├── prophet_input/             ← Input cho Prophet
├── week4_forecasts/           ← Forecast MỚI
├── week4_forecasts_old/       ← Forecast CŨ (backup)
├── week4_visualizations/      ← Biểu đồ MỚI
├── week4_visualizations_old/  ← Biểu đồ CŨ (backup)
├── week4_results/             ← Actual vs Predicted
└── week4_metrics/             ← MAPE metrics
```

---

## 🔄 QUICK START (Chạy nhanh)

Nếu đã quen, có thể chạy tuần tự:

```powershell
# Terminal 1:
cd D:\BigDataProject

# Bước 1: Backfill
python week6_backfill.py

# Bước 2: Start Kafka
cd week6_streaming
docker-compose up -d
Start-Sleep -Seconds 15

# Bước 3: Producer (chạy 1-2 phút rồi Ctrl+C)
cd ..
python week6_streaming/websocket_producer.py

# Bước 4: Merge
python week6_merge.py

# Bước 5: Forecast
python prophet_train.py

# Bước 6: Cleanup
cd week6_streaming
docker-compose down
```

---

## ❓ XỬ LÝ LỖI THƯỜNG GẶP

### 1. Lỗi "NoBrokersAvailable"
```
kafka.errors.NoBrokersAvailable: NoBrokersAvailable
```
**Nguyên nhân:** Kafka chưa khởi động xong
**Giải pháp:** Chờ 15-20 giây rồi chạy lại

### 2. Lỗi "InconsistentClusterIdException"
```
The Cluster ID xxx doesn't match stored clusterId
```
**Nguyên nhân:** Volume cũ conflict với cluster mới
**Giải pháp:**
```powershell
cd week6_streaming
docker-compose down -v    # -v để xóa volume
docker-compose up -d
```

### 3. Lỗi "Connection to api.binance.com timed out"
**Nguyên nhân:** Mạng chậm hoặc bị chặn
**Giải pháp:** 
- Kiểm tra kết nối Internet
- Tắt VPN nếu có
- Thử lại sau vài giây

### 4. Kafka container không chạy (chỉ thấy zookeeper)
**Giải pháp:**
```powershell
docker logs kafka          # Xem lỗi
docker-compose down -v     # Reset
docker-compose up -d       # Khởi động lại
```

### 5. Prophet warning "Less data than horizon"
**Nguyên nhân:** Dữ liệu quá ít (< 2 tuần)
**Giải pháp:** Bình thường, cross-validation sẽ skip nhưng forecast vẫn chạy

---

## 📊 GIẢI THÍCH KIẾN TRÚC

```
┌─────────────────────────────────────────────────────────────────┐
│                    LAMBDA ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐       │
│  │ BATCH LAYER │     │ SPEED LAYER │     │SERVING LAYER│       │
│  │             │     │             │     │             │       │
│  │ week6_      │     │ Kafka +     │     │ week6_      │       │
│  │ backfill.py │     │ Spark       │     │ merge.py    │       │
│  │             │     │ Streaming   │     │             │       │
│  │ Binance API │     │             │     │ Merge →     │       │
│  │ → Gap data  │     │ Real-time   │     │ Prophet     │       │
│  └──────┬──────┘     └──────┬──────┘     └──────┬──────┘       │
│         │                   │                   │               │
│         └───────────────────┼───────────────────┘               │
│                             ▼                                   │
│                    ┌─────────────┐                              │
│                    │  FORECAST   │                              │
│                    │  prophet_   │                              │
│                    │  train.py   │                              │
│                    └─────────────┘                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## ✅ CHECKLIST TRƯỚC KHI THI

- [ ] Docker Desktop đã cài và chạy
- [ ] Có kết nối Internet (để fetch Binance API)
- [ ] Đã test thử ít nhất 1 lần trước khi thi
- [ ] Biết cách xử lý các lỗi thường gặp
- [ ] Hiểu workflow: Backfill → Kafka → Producer → Merge → Forecast

---

## 📝 GHI CHÚ

- **Thời gian chạy:** Khoảng 5-10 phút cho toàn bộ workflow
- **Backup tự động:** Mỗi lần chạy `prophet_train.py`, biểu đồ cũ sẽ được backup sang `*_old/`
- **So sánh kết quả:** Có thể so sánh `week4_visualizations/` (mới) với `week4_visualizations_old/` (cũ)

---

*Tạo bởi: Big Data Project - Week 6 Lambda Architecture*
*Cập nhật: 03/12/2025*
