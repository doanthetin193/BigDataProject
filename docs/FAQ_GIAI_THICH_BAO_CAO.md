# 📚 FAQ - GIẢI THÍCH CÁC THUẬT NGỮ TRONG BÁO CÁO

**Tác giả:** Đoàn Thế Tín  
**Ngày tạo:** 24/12/2025  
**Mục đích:** Ghi chú các thuật ngữ và khái niệm đã được giải thích, để ôn lại khi cần

---

## MỤC LỤC

1. [Hyperparameter Tuning](#1-hyperparameter-tuning)
2. [Cross-Validation](#2-cross-validation)
3. [Checkpoint và Fault Tolerance](#3-checkpoint-và-fault-tolerance)
4. [Tính Module (Modularity)](#4-tính-module-modularity)
5. [Apache Kafka và Message Broker](#5-apache-kafka-và-message-broker)
6. [Moving Average (MA7, MA30)](#6-moving-average-ma7-ma30)
7. [Outliers](#7-outliers)
8. [Forward Fill và Window Function](#8-forward-fill-và-window-function)
9. [OHLC Aggregation](#9-ohlc-aggregation)
10. [Prophet Training - Grid Search](#10-prophet-training---grid-search)
11. [Tại sao dùng Batch Reader thay Streaming Consumer](#11-tại-sao-dùng-batch-reader-thay-streaming-consumer)

---

## 1. Hyperparameter Tuning

### Hyperparameter là gì?

**Hyperparameter** (siêu tham số) là những **thông số cấu hình** của mô hình mà bạn **phải đặt TRƯỚC khi huấn luyện**, không phải do model tự học được.

**Ví dụ dễ hiểu:**
- Khi nấu phở, bạn cần quyết định **bao nhiêu nước, bao nhiêu muối, ninh bao lâu** → Đây giống như hyperparameter
- Còn **vị của nước dùng cuối cùng** → Đây là kết quả sau khi nấu (giống như model đã train xong)

### Trong Prophet, các hyperparameter chính là:

| Hyperparameter | Ý nghĩa | Giá trị thử trong project |
|----------------|---------|---------------------------|
| `seasonality_mode` | Cách tính mùa vụ (cộng hay nhân) | `additive`, `multiplicative` |
| `changepoint_prior_scale` | Model nhạy với thay đổi xu hướng như nào | `0.01`, `0.05`, `0.1` |

### Hyperparameter Tuning là gì?

**Tuning** = **Điều chỉnh, tinh chỉnh**

**Hyperparameter Tuning** = **Thử nhiều tổ hợp hyperparameter khác nhau để tìm ra tổ hợp tốt nhất**

**Trong project:**
```python
# Thử 2 x 3 = 6 tổ hợp
seasonality_modes = ["additive", "multiplicative"]  # 2 lựa chọn
changepoint_priors = [0.01, 0.05, 0.1]              # 3 lựa chọn

# Kết quả tốt nhất:
# - seasonality_mode = "additive" 
# - changepoint_prior_scale = 0.01
# → Cho MAPE thấp nhất (2.38% cho BTC)
```

---

## 2. Cross-Validation

### Vấn đề: Tại sao cần Cross-Validation?

Khi chia dữ liệu thành **Train (80%) / Test (20%)**, kết quả đánh giá **chỉ dựa trên 1 phần test duy nhất**. 

**Vấn đề:** Nếu phần test đó "may mắn" dễ dự đoán → MAPE thấp giả tạo!

### Cross-Validation là gì?

**Cross-Validation** = **Đánh giá chéo** = **Thử nghiệm trên nhiều phần dữ liệu khác nhau**

```
Dữ liệu: |-------- TRAIN --------|-- TEST --|

Cross-Validation (3 folds):

Fold 1: |--- TRAIN ---|-- TEST 1 --|---------| 
Fold 2: |------ TRAIN ------|-- TEST 2 --|---| 
Fold 3: |--------- TRAIN ---------|-- TEST 3 --| 

→ Tính trung bình MAPE của 3 folds = CV MAPE
```

### Trong project:

```python
cv_df = cross_validation(
    model,
    horizon="30 days",    # Dự đoán 30 ngày mỗi fold
    period="15 days",     # Mỗi fold cách nhau 15 ngày
    initial="4000 days"   # Training tối thiểu 4000 ngày
)
```

### Kết quả:

| Metric | BTCUSDT | ETHUSDT |
|--------|---------|---------|
| **Test MAPE** (1 lần) | 2.38% | 3.54% |
| **CV MAPE** (nhiều lần) | 3.36% | 3.90% |

**Ý nghĩa:** CV MAPE ≈ Test MAPE → **Model không overfitting, đáng tin cậy!**

---

## 3. Checkpoint và Fault Tolerance

### Checkpoint là gì?

**Checkpoint** = **Điểm lưu tiến trình** = Giống như **"Save Game"** trong game

- Khi chơi game, bạn save game → Nếu thua, bạn load lại từ điểm save
- Checkpoint trong streaming cũng vậy → Nếu hệ thống crash, nó sẽ tiếp tục từ checkpoint

**Trong project:**

```python
# spark_streaming_consumer.py
daily.writeStream \
    .option("checkpointLocation", "checkpoint_spark")  # ← CHECKPOINT
    .start()
```

**Checkpoint lưu gì?**
- Đã đọc đến message nào trong Kafka (offset)
- Đang xử lý window nào
- Trạng thái aggregation hiện tại

### Fault Tolerance là gì?

**Fault Tolerance** = **Khả năng chịu lỗi** = Hệ thống không sập khi có lỗi xảy ra

**Trong project:**

| Thành phần | Fault Tolerance như thế nào? |
|------------|------------------------------|
| **Kafka** | Messages được lưu trên disk, không mất khi restart |
| **Spark Streaming** | Checkpoint giúp recover từ điểm lưu |
| **Producer** | Có `retries=3`, tự động gửi lại nếu fail |

---

## 4. Tính Module (Modularity)

### Tính Module là gì?

**Module** = **Khối/Thành phần riêng biệt**

**Tính Module** = Hệ thống được chia thành **các phần độc lập**, mỗi phần làm **một việc cụ thể**, có thể **thay đổi/nâng cấp riêng** mà không ảnh hưởng phần khác.

### Trong project:

```
BigDataProject/
├── scripts/preprocessing/     ← MODULE 1: Tiền xử lý
├── scripts/lambda_batch/      ← MODULE 2: Batch Layer
├── scripts/ml_models/         ← MODULE 3: Machine Learning
├── week6_streaming/           ← MODULE 4: Speed Layer
└── pages/                     ← MODULE 5: Dashboard
```

### Ưu điểm:

| Tình huống | Vì có tính module nên... |
|------------|--------------------------|
| Muốn thêm coin mới (SOL) | Chỉ cần thêm data, không sửa code |
| Muốn đổi ML model | Chỉ sửa 1 file, không ảnh hưởng phần khác |
| Kafka lỗi | Batch layer vẫn hoạt động |

---

## 5. Apache Kafka và Message Broker

### Message Broker là gì?

**Broker** = **Người môi giới**

**Message Broker** = **Hệ thống trung gian** chuyển tin nhắn giữa các ứng dụng

```
┌──────────┐     ┌─────────┐     ┌──────────┐
│ Producer │ ──► │  KAFKA  │ ──► │ Consumer │
│ (gửi)    │     │ (Broker)│     │ (nhận)   │
└──────────┘     └─────────┘     └──────────┘
```

### Các thuật ngữ Kafka:

| Khái niệm | Ý nghĩa | Ví dụ |
|-----------|---------|-------|
| **Broker** | Server chạy Kafka | Bưu điện |
| **Zookeeper** | Quản lý cluster Kafka | Quản lý bưu điện |
| **Topic** | Kênh để gửi message | Hộp thư theo chủ đề |
| **Partition** | Chia nhỏ topic | Ngăn trong hộp thư |
| **Offset** | Số thứ tự message | Số thứ tự bưu phẩm |
| **Producer** | Ứng dụng gửi message | Người gửi thư |
| **Consumer** | Ứng dụng nhận message | Người nhận thư |

### Trong project đã dùng:

| Thành phần | File | Chi tiết |
|------------|------|----------|
| Zookeeper | `docker-compose.yml` | Port 2181 |
| Kafka Broker | `docker-compose.yml` | Port 9092 |
| Topic | Tự động tạo | `crypto-prices` |
| Producer | `websocket_producer.py` | Gửi giá từ Binance |
| Consumer | `kafka_batch_reader.py` | Đọc và xử lý |

---

## 6. Moving Average (MA7, MA30)

### Moving Average là gì?

**MA7** = **Trung bình 7 ngày gần nhất** (tính cho TỪNG NGÀY)

**Mỗi ngày có MA riêng, không phải 1 giá trị cho cả bảng!**

### Ví dụ:

```
Ngày        | Giá đóng cửa | MA7 (trung bình 7 ngày gần nhất)
------------|--------------|----------------------------------
2025-12-01  | 40,000       | NULL (chưa đủ 7 ngày)
2025-12-02  | 41,000       | NULL
...
2025-12-07  | 46,000       | (40+41+42+43+44+45+46)/7 = 43,000 ✓
2025-12-08  | 47,000       | (41+42+43+44+45+46+47)/7 = 44,000 ✓
```

### Code:

```python
window_ma7 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-6, 0)
#                                                                     ↑   ↑
#                                                               6 ngày trước  ngày hiện tại

df = df.withColumn("ma7", avg("daily_close").over(window_ma7))
```

### Xử lý NULL đầu tiên:

- 6 ngày đầu MA7 = NULL, 29 ngày đầu MA30 = NULL
- Code sử dụng `ffill().fillna(0)` để xử lý
- Ảnh hưởng rất nhỏ (< 1% dữ liệu)

---

## 7. Outliers

### Outliers là gì?

**Outliers** = **Giá trị ngoại lai** = Những giá trị **khác biệt rất lớn** so với phần còn lại

**Ví dụ:**
```
Điểm thi lớp: 7, 8, 7.5, 8, 7, 8.5, 2, 8, 7.5
                                    ↑
                              OUTLIER (2 điểm)
```

### Tại sao KHÔNG loại bỏ outliers trong project?

| Lý do | Giải thích |
|-------|------------|
| Crypto biến động cao là bình thường | ±10-20%/ngày có thể xảy ra |
| Flash crash là sự kiện thực | 12/03/2020 BTC giảm -40% trong ngày |
| Model cần học market behavior | Nếu loại bỏ, model không biết thị trường có thể crash |
| Aggregate daily làm mượt | OHLC tự động "làm dịu" biến động cực đoan |

---

## 8. Forward Fill và Window Function

### Forward Fill là gì?

**Forward Fill** = **Điền tiến** = Lấy giá trị TRƯỚC để điền vào NULL

```
TRƯỚC:                    SAU (Forward Fill):
│ 00:01 │ 43000 │         │ 00:01 │ 43000 │
│ 00:02 │ 43100 │         │ 00:02 │ 43100 │
│ 00:03 │ NULL  │   →     │ 00:03 │ 43100 │  ← Lấy từ 00:02
│ 00:04 │ NULL  │   →     │ 00:04 │ 43100 │  ← Lấy từ 00:02
│ 00:05 │ 43200 │         │ 00:05 │ 43200 │
```

### Code:

```python
window_ffill = Window.partitionBy("symbol").orderBy("timestamp") \
    .rowsBetween(Window.unboundedPreceding, 0)
#               ↑                           ↑
#         Từ đầu dữ liệu            đến dòng hiện tại

df = df.withColumn("close", F.last("close", ignorenulls=True).over(window_ffill))
#                           ↑              ↑
#                   Lấy giá trị cuối   Bỏ qua NULL
```

---

## 9. OHLC Aggregation

### OHLC là gì?

**O**pen, **H**igh, **L**ow, **C**lose = Giá mở, cao nhất, thấp nhất, đóng cửa

### Cách aggregate từ 1-phút sang ngày:

| Cột | Cách tính |
|-----|-----------|
| `daily_open` | Giá `open` của **phút đầu tiên** trong ngày |
| `daily_high` | `MAX(high)` của tất cả phút trong ngày |
| `daily_low` | `MIN(low)` của tất cả phút trong ngày |
| `daily_close` | Giá `close` của **phút cuối cùng** trong ngày |
| `daily_volume` | `SUM(volume)` của tất cả phút trong ngày |

### Luồng xử lý:

```
11.5 triệu dòng (1-phút)
        ↓ Forward Fill
        ↓ dropDuplicates
        ↓ Tìm min/max timestamp mỗi ngày
        ↓ Lấy Open (phút đầu), Close (phút cuối)
        ↓ Aggregate High, Low, Volume
        ↓
8,140 dòng (1-ngày)
```

---

## 10. Prophet Training - Grid Search

### Grid Search là gì?

**Grid Search** = **Thử TẤT CẢ tổ hợp tham số** để tìm tốt nhất

```python
season_modes = ["additive", "multiplicative"]  # 2 lựa chọn
changepoint_priors = [0.01, 0.05, 0.1]         # 3 lựa chọn
# → 2 x 3 = 6 tổ hợp được thử
```

### Các tham số Prophet:

| Tham số | Ý nghĩa | Kết quả tốt nhất |
|---------|---------|------------------|
| `seasonality_mode` | Cách tính mùa vụ | `additive` |
| `changepoint_prior_scale` | Độ nhạy xu hướng | `0.01` (ổn định) |
| `regressors` | Biến phụ trợ | MA7, MA30 |
| `holidays` | Sự kiện đặc biệt | BTC Halving |

### BTC Halving:

| Halving | Ngày | Ý nghĩa |
|---------|------|---------|
| #2 | 09/07/2016 | Phần thưởng đào giảm 50% |
| #3 | 11/05/2020 | Giá thường tăng mạnh sau đó |
| #4 | 20/04/2024 | Sự kiện quan trọng cho dự đoán |

---

## 11. Tại sao dùng Batch Reader thay Streaming Consumer

### Vấn đề của Streaming Consumer:

```python
# Aggregate theo window 1 ngày
daily = watermarked.groupBy(
    window("event_time", "1 day"),  # ← Window 1 ngày = 24 giờ
    "symbol"
)
```

- Window **1 ngày** = Thu thập data từ 00:00 → 23:59
- Watermark **1 giờ** = Chờ thêm cho late data
- **Tổng: 25 TIẾNG** mới có output file!

### Vấn đề khi demo:

```
Demo 2-3 tiếng:
10:00 - Bắt đầu chạy
13:00 - Demo xong, tắt Spark
    ↓
KHÔNG CÓ OUTPUT FILE!
(Window 1 ngày chưa đóng)
```

### Giải pháp - Batch Reader:

```python
df = spark.read \      # READ (batch) thay vì readStream
    .format("kafka") \
    .load()

# Aggregate ngay lập tức (không cần chờ window)
# OUTPUT trong 2-3 giây!
```

### So sánh:

| Tiêu chí | Streaming Consumer | Batch Reader |
|----------|-------------------|--------------|
| Thời gian có output | 25 tiếng | 2-3 giây |
| Phù hợp demo | ❌ | ✅ |
| Phù hợp production | ✅ (24/7) | ⚠️ (cần schedule) |
| Logic xử lý | Giống nhau | Giống nhau |

### Giải thích cho giảng viên:

> "Em đã implement Spark Structured Streaming với window 1 ngày phù hợp cho production 24/7. Tuy nhiên, vì cần 25 tiếng để có output, nên demo em sử dụng Kafka Batch Reader với **cùng logic aggregate**. Hai file này chứng minh Kafka + Spark hoạt động đúng."

---

## 📝 BẢNG THUẬT NGỮ TIẾNG ANH - TIẾNG VIỆT

| English | Tiếng Việt |
|---------|------------|
| Hyperparameter | Siêu tham số |
| Hyperparameter Tuning | Tinh chỉnh siêu tham số |
| Grid Search | Tìm kiếm lưới |
| Cross-Validation | Xác thực chéo / Đánh giá chéo |
| Fold | Lượt / Phần |
| Checkpoint | Điểm kiểm tra / Điểm lưu |
| Fault Tolerance | Khả năng chịu lỗi |
| Modularity | Tính mô-đun |
| Message Broker | Trung gian tin nhắn |
| Producer | Bên gửi |
| Consumer | Bên nhận |
| Topic | Chủ đề / Kênh |
| Partition | Phân vùng |
| Offset | Vị trí / Số thứ tự |
| Moving Average | Trung bình trượt |
| Outlier | Giá trị ngoại lai |
| Forward Fill | Điền tiến |
| Window Function | Hàm cửa sổ |
| OHLC | Mở-Cao-Thấp-Đóng |
| Regressor | Biến hồi quy |
| Seasonality | Tính mùa vụ |
| Changepoint | Điểm thay đổi xu hướng |
| Watermark | Mốc thời gian chờ |

---

**Ghi chú cuối:** File này tổng hợp các câu hỏi và giải thích từ phiên làm việc ngày 24/12/2025. Khi quên có thể đọc lại để ôn! 📖
