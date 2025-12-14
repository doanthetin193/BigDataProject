# GIẢI THÍCH CHI TIẾT FILE `websocket_producer.py`

## 📋 TỔNG QUAN

File `websocket_producer.py` là thành phần **PRODUCER** trong Speed Layer của Lambda Architecture. Đây là file **ĐỠN GIẢN NHẤT** trong Week 6.

### **Nhiệm vụ chính:**

- Lấy giá **real-time** từ Binance API
- Gửi vào **Kafka** topic
- Chạy **liên tục** cho đến khi dừng (Ctrl+C)

### **Vai trò trong hệ thống:**

```
┌─────────────────────────────────────────────────────────────┐
│                    SPEED LAYER                              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Binance API  →  websocket_producer.py  →  Kafka  →  Spark │
│  (Nguồn data)      (File này - Producer)   (Buffer)         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔄 LUỒNG HOẠT ĐỘNG

```
┌──────────────────────────────────────────────────────────────┐
│            WEBSOCKET_PRODUCER.PY WORKFLOW                    │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ① Setup & Connect Kafka                                    │
│     └─► Tạo KafkaProducer với config                        │
│                                                              │
│  ② Infinite Loop (while True)                               │
│     │                                                        │
│     ├─► For BTC:                                            │
│     │    ├─ Fetch từ Binance API                            │
│     │    ├─ Serialize dict → JSON → bytes                   │
│     │    ├─ Send to Kafka topic                             │
│     │    └─ Update statistics                               │
│     │                                                        │
│     ├─► For ETH:                                            │
│     │    └─ (Tương tự BTC)                                  │
│     │                                                        │
│     └─► Sleep 1 giây                                        │
│                                                              │
│  ③ Ctrl+C → Graceful Shutdown                               │
│     ├─ Flush pending messages                               │
│     ├─ Close Kafka connection                               │
│     └─ Print statistics                                     │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## 📝 DIỄN GIẢI CHI TIẾT TỪNG PHẦN

### **PHẦN 1: Import và Configuration**

#### **Import thư viện:**

```python
import json           # Chuyển dict → JSON string
import logging        # Ghi log (thay vì print)
import time           # Sleep 1 giây giữa các lần fetch
import requests       # Gọi HTTP API
from datetime import datetime        # Lấy timestamp
from kafka import KafkaProducer      # Gửi tin vào Kafka
```

**Giải thích:**

- **json:** Kafka yêu cầu data dạng bytes, cần chuyển dict → JSON → bytes
- **logging:** Ghi log chuyên nghiệp (có timestamp, level INFO/ERROR)
- **time:** Dùng `time.sleep(1)` để đợi 1 giây giữa các lần fetch
- **requests:** Thư viện HTTP phổ biến để gọi Binance API
- **KafkaProducer:** Class chính để gửi messages vào Kafka

---

#### **Setup Logging:**

```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
```

**Giải thích:**

- `level=logging.INFO`: Ghi log từ INFO trở lên (INFO, WARNING, ERROR)
- `format`: Định dạng log: `[Thời gian] - [Level] - [Message]`

**Output mẫu:**

```
2025-12-10 10:30:15 - INFO - ✓ Kafka Producer connected
2025-12-10 10:30:16 - ERROR - Error fetching BTCUSDT: timeout
```

---

#### **Kafka Configuration:**

```python
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'crypto-prices'
```

**Giải thích:**

| Config                    | Giá trị              | Ý nghĩa                                 |
| ------------------------- | -------------------- | --------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | `['localhost:9092']` | Địa chỉ Kafka server (local, port 9092) |
| `KAFKA_TOPIC`             | `'crypto-prices'`    | Tên "kênh" để gửi messages              |

**Hình dung Topic:**

```
Kafka Server (localhost:9092)
│
├── Topic: crypto-prices   ← Producer gửi vào đây
│   ├── Message 1 (BTC data)
│   ├── Message 2 (ETH data)
│   ├── Message 3 (BTC data)
│   └── ...
│
└── Topic: other-data
```

---

#### **Statistics Variables:**

```python
message_count = {'BTCUSDT': 0, 'ETHUSDT': 0}
start_time = time.time()
```

**Giải thích:**

- `message_count`: Đếm số messages đã gửi cho mỗi coin
- `start_time`: Lưu thời gian bắt đầu để tính tốc độ gửi

---

### **PHẦN 2: Hàm tạo Kafka Producer**

```python
def create_kafka_producer():
    """Tạo Kafka Producer với retry logic"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            acks='all',
            compression_type='gzip',
            max_in_flight_requests_per_connection=1
        )
        logger.info(f"✓ Kafka Producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"✗ Failed to create Kafka Producer: {e}")
        raise
```

#### **Giải thích từng parameter:**

**1. `bootstrap_servers`:**

- Địa chỉ Kafka server cần kết nối
- Value: `['localhost:9092']`

---

**2. `value_serializer`:**

```python
lambda v: json.dumps(v).encode('utf-8')
```

**Quá trình serialize:**

```
Bước 1: Python dict
{'symbol': 'BTCUSDT', 'price': 97234.50}

Bước 2: json.dumps(v) → JSON string
'{"symbol":"BTCUSDT","price":97234.50}'

Bước 3: .encode('utf-8') → bytes
b'{"symbol":"BTCUSDT","price":97234.50}'

→ Gửi vào Kafka
```

**Tại sao cần?**

- Kafka chỉ nhận data dạng **bytes**
- Python dict không thể gửi trực tiếp
- Cần: dict → JSON string → bytes

---

**3. `retries=3`:**

- Nếu gửi message thất bại → thử lại tối đa **3 lần**
- Tránh mất data do lỗi mạng tạm thời

**Ví dụ:**

```
Lần 1: Send → Network error → Retry
Lần 2: Send → Timeout → Retry
Lần 3: Send → Success ✓
```

---

**4. `acks='all'`:**

- `acks` = acknowledgment (xác nhận)
- `'all'` = đợi **TẤT CẢ** Kafka replicas xác nhận đã lưu message

**So sánh các mode:**

| Mode         | Tốc độ        | Độ an toàn                | Dùng khi nào                 |
| ------------ | ------------- | ------------------------- | ---------------------------- |
| `acks=0`     | ⚡ Nhanh nhất | ⚠️ Thấp (có thể mất data) | Không quan trọng (logs)      |
| `acks=1`     | 🔄 Trung bình | 🔒 Trung bình             | Cân bằng                     |
| `acks='all'` | 🐢 Chậm nhất  | 🔐 Cao nhất               | **Crypto data (quan trọng)** |

---

**5. `compression_type='gzip'`:**

- Nén message trước khi gửi
- **Giảm bandwidth** và tăng tốc độ

**Ví dụ:**

```
Message gốc: 1000 bytes
Sau nén gzip: 300 bytes (giảm 70%)
→ Gửi nhanh hơn, tiết kiệm network
```

---

**6. `max_in_flight_requests_per_connection=1`:**

- Giới hạn **1 request đang gửi** cùng lúc
- **Đảm bảo thứ tự messages**

**So sánh:**

```
max_in_flight = 1:
  Message 1 → Send → Wait ACK → Message 2 → Send
  ✅ Thứ tự: 1 → 2 (đúng)

max_in_flight = 3:
  Message 1 → Send ┐
  Message 2 → Send ├─ Gửi song song
  Message 3 → Send ┘
  ❌ Thứ tự: 3 → 1 → 2 (có thể sai)
```

**Vì crypto data cần thứ tự đúng → dùng 1**

---

### **PHẦN 3: Hàm fetch dữ liệu từ Binance**

```python
def fetch_ticker_data(symbol):
    """Lấy dữ liệu ticker real-time từ Binance API"""
    try:
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol.upper()}"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()

        return {
            'symbol': data['symbol'],
            'event_time': int(data['closeTime']),
            'price': float(data['lastPrice']),
            'open': float(data['openPrice']),
            'high': float(data['highPrice']),
            'low': float(data['lowPrice']),
            'volume': float(data['volume']),
            'quote_volume': float(data['quoteVolume']),
            'number_trades': int(data['count']),
            'price_change': float(data['priceChange']),
            'price_change_percent': float(data['priceChangePercent']),
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error fetching {symbol}: {e}")
        return None
```

#### **Giải thích từng bước:**

**Bước 1: Tạo URL**

```python
url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol.upper()}"
```

**URL đầy đủ:**

```
https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT
```

**Endpoint này trả về:**

- Giá hiện tại (`lastPrice`)
- Thống kê 24h: open, high, low, volume
- % thay đổi giá

---

**Bước 2: Gửi HTTP GET request**

```python
response = requests.get(url, timeout=5)
```

- `timeout=5`: Đợi tối đa 5 giây
- Quá 5s → raise `Timeout` exception

---

**Bước 3: Kiểm tra status code**

```python
response.raise_for_status()
```

- Nếu status = 200 (OK) → tiếp tục
- Nếu status = 4xx/5xx (Error) → raise exception

---

**Bước 4: Parse JSON response**

```python
data = response.json()
```

**Response mẫu từ Binance:**

```json
{
  "symbol": "BTCUSDT",
  "lastPrice": "97234.50",
  "openPrice": "96500.00",
  "highPrice": "98000.00",
  "lowPrice": "95000.00",
  "volume": "12345.67",
  "quoteVolume": "1193456789.12",
  "count": 123456,
  "priceChange": "734.50",
  "priceChangePercent": "0.76",
  "closeTime": 1733650800000
}
```

---

**Bước 5: Chuẩn hóa dữ liệu**

```python
return {
    'symbol': data['symbol'],              # String: "BTCUSDT"
    'event_time': int(data['closeTime']),  # Int: 1733650800000
    'price': float(data['lastPrice']),     # Float: 97234.50
    ...
}
```

**Tại sao chuyển đổi kiểu?**

- Binance trả về **string**: `"97234.50"`
- Cần chuyển sang **float**: `97234.50` để Spark xử lý số học

---

**Bước 6: Exception handling**

```python
except Exception as e:
    logger.error(f"Error fetching {symbol}: {e}")
    return None  # Không crash program
```

**Các lỗi có thể xảy ra:**

- Network timeout
- Binance API down
- Rate limit exceeded
- Invalid symbol

**Quan trọng:** Return `None` thay vì crash → program tiếp tục chạy

---

### **PHẦN 4: Main Loop - Trái tim của Producer**

```python
def main():
    # Setup
    global producer
    producer = create_kafka_producer()
    symbols = ['BTCUSDT', 'ETHUSDT']

    try:
        while True:  # ← Vòng lặp VÔ HẠN
            for symbol in symbols:
                # Fetch data
                data = fetch_ticker_data(symbol)

                if data:
                    # Send to Kafka
                    future = producer.send(KAFKA_TOPIC, value=data)
                    future.get(timeout=10)

                    # Update statistics
                    message_count[symbol] += 1

                    # Log mỗi 10 messages
                    if message_count[symbol] % 10 == 0:
                        # ... log statistics

            # Wait 1 second
            time.sleep(1)
```

#### **Phân tích chi tiết:**

**1. `while True:`**

```
Vòng lặp VÔ HẠN:
  - Chạy mãi mãi cho đến khi Ctrl+C
  - Không có điều kiện dừng
  - Không tự thoát
```

---

**2. `for symbol in symbols:`**

```
Mỗi vòng lặp xử lý 2 coins:
  Lần 1: symbol = "BTCUSDT"
  Lần 2: symbol = "ETHUSDT"
```

---

**3. Fetch data**

```python
data = fetch_ticker_data(symbol)
```

**Kết quả:**

- Thành công → dict chứa giá, volume, v.v.
- Thất bại → `None`

---

**4. Gửi vào Kafka**

```python
if data:  # Chỉ gửi nếu fetch thành công
    future = producer.send(KAFKA_TOPIC, value=data)
    future.get(timeout=10)
```

**`producer.send(...)` - Asynchronous:**

- Gửi message nhưng **không đợi** kết quả ngay
- Return: `FutureRecordMetadata` object

**`future.get(timeout=10)` - Đợi xác nhận:**

- Block cho đến khi Kafka xác nhận đã nhận message
- Timeout 10s: Nếu quá 10s → raise exception

**Tại sao cần `.get()`?**

```
Không có .get():
  producer.send(...)  # Gửi
  # Không biết có thành công không
  # Có thể mất data

Có .get():
  producer.send(...)  # Gửi
  future.get()        # Đợi xác nhận
  # Chắc chắn message đã vào Kafka
```

---

**5. Update statistics**

```python
message_count[symbol] = message_count.get(symbol, 0) + 1
```

**Ví dụ:**

```
Ban đầu: {'BTCUSDT': 0, 'ETHUSDT': 0}

Sau gửi 1 BTC: {'BTCUSDT': 1, 'ETHUSDT': 0}
Sau gửi 1 ETH: {'BTCUSDT': 1, 'ETHUSDT': 1}
Sau gửi 1 BTC: {'BTCUSDT': 2, 'ETHUSDT': 1}
```

---

**6. Log mỗi 10 messages**

```python
if message_count[symbol] % 10 == 0:
    elapsed = time.time() - start_time
    rate = sum(message_count.values()) / elapsed
    logger.info(f"📊 {symbol}: ${data['price']:,.2f} | ...")
```

**Tại sao % 10?**

- Nếu log mỗi message → quá nhiều log (spam)
- Log mỗi 10 messages → vừa đủ để monitor

**Output mẫu:**

```
2025-12-10 10:30:15 - INFO - 📊 BTCUSDT: $97,234.50 | Vol: 12,345 | Change: +0.76% | Messages: 10 | Rate: 2.0 msg/s
```

---

**7. Sleep 1 giây**

```python
time.sleep(1)
```

**Timeline:**

```
00:00:00 - Fetch BTC + ETH → Send (mất ~0.3s)
00:00:01 - sleep(1)
00:00:01 - Lặp lại: Fetch BTC + ETH → Send
00:00:02 - sleep(1)
00:00:02 - Lặp lại...
```

**Tại sao 1 giây?**

- Đủ nhanh để real-time (giá thay đổi mỗi giây)
- Không spam Binance API (tránh bị block)

---

### **PHẦN 5: Graceful Shutdown (Ctrl+C)**

```python
except KeyboardInterrupt:
    logger.info("\n⏹ Stopping producer...")
    producer.flush()
    producer.close()

    # Print final statistics
    total = sum(message_count.values())
    elapsed = time.time() - start_time
    print(f"Total messages sent: {total:,}")
    print(f"Duration: {elapsed:.1f}s")
    print(f"Average rate: {total/elapsed:.1f} messages/second")
```

#### **Giải thích từng bước:**

**1. `except KeyboardInterrupt:`**

- Bắt exception khi user nhấn **Ctrl+C**
- Không crash, mà chạy cleanup code

---

**2. `producer.flush()` - Quan trọng!**

**Kafka Producer có cơ chế BATCHING:**

- Messages không gửi ngay lập tức
- Đưa vào **buffer** (RAM), đợi đủ lớn mới gửi
- **Hiệu quả hơn** (1 request gửi nhiều messages)

**Timeline thực tế:**

```
00:00:00 - Fetch BTC từ API → data1 ✅ (đã có trong RAM)
         - producer.send(data1) → Vào BUFFER (pending ⏳)

00:00:01 - Fetch ETH từ API → data2 ✅ (đã có trong RAM)
         - producer.send(data2) → Vào BUFFER (pending ⏳)

00:00:02 - Buffer đủ lớn → GỬI HÀNG LOẠT data1, data2 ✅

00:00:03 - Fetch BTC từ API → data3 ✅ (đã có trong RAM)
         - producer.send(data3) → Vào BUFFER (pending ⏳)

[USER NHẤN Ctrl+C]

         - data3 đã fetch từ API, đã vào buffer
         - NHƯNG chưa kịp gửi đến Kafka!
         - producer.flush() ← "Gửi NGAY data3!"
         → Gửi ngay không đợi batch
         → Đợi Kafka xác nhận
         → ✅ Không mất data
```

**Buffer visualization:**

```
┌────────────────────────────────────┐
│ Message 1 (sent ✅)                │
│ Message 2 (sent ✅)                │
│ Message 3 (pending ⏳)              │ ← Đã fetch từ API
│ Message 4 (pending ⏳)              │ ← Chưa gửi Kafka
└────────────────────────────────────┘

producer.flush():
  → Gửi message 3, 4 NGAY LẬP TỨC
  → Đợi đến khi TẤT CẢ đã gửi xong
  → Đảm bảo không mất message cuối cùng
```

**Tóm lại:** Messages pending đã được fetch từ API, chỉ đang chờ trong buffer. Flush() = "cơ hội cuối" để gửi chúng trước khi đóng cửa!

---

**3. `producer.close()`**

- Đóng connection với Kafka
- Giải phóng resources (memory, network)

---

**4. In thống kê cuối cùng**

```python
total = sum(message_count.values())  # Tổng BTC + ETH
elapsed = time.time() - start_time    # Thời gian đã chạy
rate = total / elapsed                # Messages/second
```

**Output mẫu:**

```
======================================================================
STATISTICS
======================================================================
Total messages sent: 1,200
Duration: 600.0s (10 phút)
Average rate: 2.0 messages/second
  BTCUSDT: 600 messages
  ETHUSDT: 600 messages
======================================================================
```

---

## ⏱️ TIMELINE THỰC TẾ

### **Scenario 1: Test ngắn (5 phút)**

```bash
# Start producer
python websocket_producer.py

00:00 - Start
00:01 - Fetched BTC, ETH → Sent
00:02 - Fetched BTC, ETH → Sent
...
05:00 - [Ctrl+C]
      - Flush messages
      - Close producer
      - Statistics:
        Total: 600 messages
        Duration: 300s
        Rate: 2.0 msg/s
```

---

### **Scenario 2: Thu thập data cả ngày (12 giờ)**

```bash
python websocket_producer.py

08:00 - Start (buổi sáng)
...
20:00 - [Ctrl+C] (buổi tối)

Statistics:
  Total: 86,400 messages
  Duration: 43,200s (12 giờ)
  Rate: 2.0 msg/s
```

---

## 🎯 CÁC CÁCH DỪNG PRODUCER

### **✅ Cách ĐÚNG: Nhấn Ctrl+C**

```python
try:
    while True:
        ...
except KeyboardInterrupt:  # ← Bắt Ctrl+C
    producer.flush()        # Gửi hết messages pending
    producer.close()        # Đóng connection
    # Print statistics
```

**Kết quả:**

- ✅ Không mất data
- ✅ Cleanup resources
- ✅ Statistics đầy đủ

---

### **❌ Cách SAI: Kill process**

```bash
# Tìm PID
ps aux | grep websocket_producer

# Kill
kill -9 <PID>
```

**Kết quả:**

- ❌ Có thể mất messages cuối cùng
- ❌ Không cleanup
- ❌ Không có statistics

---

### **❌ Cách SAI: Tắt terminal**

**Kết quả:** Giống kill process

---

## 💡 NHỮNG ĐIỂM QUAN TRỌNG CẦN NHỚ

### **1. Vòng lặp vô hạn**

```python
while True:  # Chạy MÃI MÃI cho đến khi Ctrl+C
    ...
```

**Không tự dừng!** Phải dừng thủ công.

---

### **2. Tại sao sleep 1 giây?**

```python
time.sleep(1)  # Đợi 1 giây giữa các lần fetch
```

**Lý do:**

- ✅ Real-time: Giá thay đổi mỗi giây
- ✅ Không spam Binance API
- ✅ Giảm tải CPU/Network

---

### **3. Tại sao cần serializer?**

```python
value_serializer=lambda v: json.dumps(v).encode('utf-8')
```

**Kafka chỉ nhận bytes:**

```
dict → JSON string → bytes → Kafka
```

---

### **4. Asynchronous + Synchronous?**

```python
future = producer.send(...)  # Asynchronous (không đợi)
future.get(timeout=10)       # Synchronous (đợi xác nhận)
```

**Tại sao kết hợp?**

- Gửi nhanh (async)
- Nhưng đảm bảo không mất data (sync)

---

### **5. Exception handling**

```python
try:
    data = fetch_ticker_data(symbol)
except:
    return None  # Không crash, tiếp tục chạy
```

**Quan trọng:** Một lỗi nhỏ không làm dừng toàn bộ producer.

---

## 🔧 TROUBLESHOOTING

### **Lỗi: "Connection refused [localhost:9092]"**

**Nguyên nhân:** Kafka chưa chạy

**Giải pháp:**

```bash
cd week6_streaming
docker-compose up -d
```

---

### **Lỗi: "Timeout fetching BTCUSDT"**

**Nguyên nhân:** Mạng chậm hoặc Binance API quá tải

**Giải pháp:**

- Kiểm tra mạng
- Tăng timeout: `requests.get(url, timeout=10)`

---

### **Lỗi: "Message too large"**

**Nguyên nhân:** Message > Kafka max size

**Giải pháp:**

- Đã có `compression_type='gzip'` → nén message
- Hoặc tăng Kafka config: `message.max.bytes`

---

## 📊 MONITORING

### **Cách theo dõi Producer:**

**1. Xem logs:**

```bash
# Logs hiện trên terminal
2025-12-10 10:30:15 - INFO - 📊 BTCUSDT: $97,234.50 | Messages: 10 | Rate: 2.0 msg/s
```

**2. Kiểm tra Kafka:**

```bash
# Vào container Kafka
docker exec -it kafka bash

# Xem messages trong topic
kafka-console-consumer --bootstrap-server localhost:9092 \
                       --topic crypto-prices \
                       --from-beginning \
                       --max-messages 5
```

**3. Monitor statistics:**

```python
# Trong code
if message_count[symbol] % 10 == 0:
    logger.info(f"Rate: {rate:.1f} msg/s")
```

---

## 📝 TÓM TẮT

### **Producer làm gì?**

1. Fetch giá từ Binance API (mỗi 1 giây)
2. Serialize dict → JSON → bytes
3. Gửi vào Kafka topic `crypto-prices`
4. Chạy liên tục cho đến khi Ctrl+C

### **Key Concepts:**

- **Producer** = Người gửi messages
- **Kafka** = Message broker (bưu điện)
- **Topic** = Kênh chứa messages
- **Serializer** = Chuyển dict → bytes
- **Asynchronous send** = Gửi nhanh, đợi xác nhận sau
- **Flush** = Đẩy tất cả messages pending

### **Điểm khác với week6_backfill.py:**

- Backfill: Xử lý **historical data** (batch, 1 lần xong)
- Producer: Xử lý **real-time data** (stream, chạy liên tục)

### **Khi nào dùng?**

✅ Muốn thu thập real-time data
✅ Sau khi chạy backfill (đã có historical)
✅ Kafka đã chạy

**File này là "vòi nước" - bật thì chảy mãi, phải tắt thủ công!**
