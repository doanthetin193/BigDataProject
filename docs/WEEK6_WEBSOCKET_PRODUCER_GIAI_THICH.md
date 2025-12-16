# Giải thích chi tiết: websocket_producer.py

**File:** `week6_streaming/websocket_producer.py`  
**Chức năng:** Speed Layer Producer - Thu thập real-time data từ Binance API và push vào Kafka  
**Tác giả:** Đoàn Thế Tín  
**Ngày:** Week 6 - Lambda Architecture

---

## 📋 Mục lục
1. [Import và Configuration](#1-import-và-configuration)
2. [Kafka Producer Setup](#2-kafka-producer-setup)
3. [Fetch Ticker Data](#3-fetch-ticker-data)
4. [Main Streaming Loop](#4-main-streaming-loop)
5. [Statistics & Cleanup](#5-statistics--cleanup)
6. [Tóm tắt](#tóm-tắt-tổng-quan)

---

## 1. Import và Configuration

### Dòng 1-8: Docstring
```python
"""
websocket_producer.py - Thu thập dữ liệu real-time từ Binance và push vào Kafka

STRUCTURED STREAMING approach:
- Continuous polling (mỗi 1 giây)
- Push vào Kafka message broker
- Spark sẽ consume từ Kafka
"""
```
**Giải thích:**
- File này là **Producer** trong Kafka architecture
- **Không dùng WebSocket** (mặc dù tên file là websocket_producer.py)
- Thực tế dùng **REST API polling** (mỗi 1 giây)
- **Tại sao polling thay vì WebSocket?**
  - WebSocket cần maintain connection liên tục
  - REST API đơn giản hơn, ổn định hơn
  - 1 giây/lần đủ cho demo (không cần millisecond latency)

---

### Dòng 9-13: Import Libraries
```python
import json
import logging
import time
import requests
from datetime import datetime
from kafka import KafkaProducer
```
**Giải thích:**
- `json`: Serialize data thành JSON khi gửi Kafka
- `logging`: Ghi log (thay vì print)
- `time`: Sleep 1 giây giữa các lần polling
- `requests`: Call Binance REST API
- `datetime`: Timestamp cho mỗi message
- `KafkaProducer`: Kafka client library (kafka-python)

---

### Dòng 15-20: Logging Setup
```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
```
**Giải thích:**
- `basicConfig()`: Cấu hình logging
  - `level=INFO`: Log INFO, WARNING, ERROR (không log DEBUG)
  - `format`: Format log message
    - `%(asctime)s`: Timestamp (2025-12-16 10:30:45)
    - `%(levelname)s`: INFO/WARNING/ERROR
    - `%(message)s`: Message content
- `getLogger(__name__)`: Tạo logger instance
  - `__name__` = 'websocket_producer' (tên module)

**Ví dụ output:**
```
2025-12-16 10:30:45,123 - INFO - ✓ Kafka Producer connected
```

---

### Dòng 22-24: Kafka Configuration
```python
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'crypto-prices'
```
**Giải thích:**
- `BOOTSTRAP_SERVERS`: Danh sách Kafka brokers
  - `localhost:9092`: Kafka chạy trên Docker (port 9092)
  - List vì có thể có nhiều brokers (cluster)
- `KAFKA_TOPIC`: Tên topic
  - `crypto-prices`: Topic chứa BTC và ETH data
  - Consumer sẽ subscribe topic này

---

### Dòng 26-28: Statistics Variables
```python
message_count = {'BTCUSDT': 0, 'ETHUSDT': 0}
start_time = time.time()
```
**Giải thích:**
- `message_count`: Dictionary đếm số messages gửi
  - Key: Symbol (BTCUSDT, ETHUSDT)
  - Value: Số messages
- `start_time`: Timestamp lúc start (Unix timestamp)
  - Dùng để tính duration và rate (msg/s)

---

## 2. Kafka Producer Setup

### Dòng 30-32: Function Definition
```python
def create_kafka_producer():
    """Tạo Kafka Producer với retry logic"""
    try:
```
**Giải thích:** Hàm tạo và configure Kafka Producer.

---

### Dòng 33-40: Producer Configuration
```python
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            acks='all',  # Wait for all replicas
            compression_type='gzip',
            max_in_flight_requests_per_connection=1
        )
```
**Giải thích từng parameter:**

#### `bootstrap_servers`
- Danh sách Kafka brokers để connect
- `['localhost:9092']`

#### `value_serializer`
- Hàm chuyển đổi Python object → bytes
- `lambda v: json.dumps(v).encode('utf-8')`
  - `json.dumps(v)`: Dict → JSON string
  - `.encode('utf-8')`: String → bytes
- **Ví dụ:**
  ```python
  {'symbol': 'BTCUSDT', 'price': 42000.0}
  → '{"symbol":"BTCUSDT","price":42000.0}'
  → b'{"symbol":"BTCUSDT","price":42000.0}'
  ```

#### `retries=3`
- Nếu send fail → Retry tối đa 3 lần
- Tránh mất message do network glitch

#### `acks='all'`
- **Acknowledgment level:**
  - `acks=0`: Không đợi ack (fastest, mất data)
  - `acks=1`: Đợi leader ack (medium)
  - `acks='all'`: Đợi tất cả replicas ack (slowest, safest)
- Chọn `'all'` để **đảm bảo không mất data**

#### `compression_type='gzip'`
- Nén message trước khi gửi
- Giảm network bandwidth
- Tăng CPU nhẹ (trade-off)

#### `max_in_flight_requests_per_connection=1`
- Số requests đồng thời tối đa trên 1 connection
- `1` = Gửi tuần tự (không parallel)
- **Tại sao?** Đảm bảo **ordering** (message gửi trước đến trước)

---

### Dòng 41-42: Success Log
```python
        logger.info(f"✓ Kafka Producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
```
**Giải thích:**
- Log success message
- Return producer instance

---

### Dòng 43-45: Exception Handling
```python
    except Exception as e:
        logger.error(f"✗ Failed to create Kafka Producer: {e}")
        raise
```
**Giải thích:**
- Nếu không connect được Kafka → Log error và raise exception
- `raise`: Re-raise exception để program dừng lại
- **Common errors:**
  - Kafka chưa start: `NoBrokersAvailable`
  - Port sai: `ConnectionRefusedError`

---

## 3. Fetch Ticker Data

### Dòng 47-49: Function Definition
```python
def fetch_ticker_data(symbol):
    """Lấy dữ liệu ticker real-time từ Binance API"""
    try:
```
**Giải thích:** Hàm call Binance API để lấy ticker data (OHLCV + statistics).

---

### Dòng 50-53: API Call
```python
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol.upper()}"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
```
**Giải thích:**

#### API Endpoint
- `GET /api/v3/ticker/24hr`
- **Chức năng:** 24-hour ticker price change statistics
- **Parameter:** `symbol=BTCUSDT` (hoặc ETHUSDT)
- `.upper()`: Đảm bảo uppercase (BTCUSDT, không phải btcusdt)

#### Request
- `requests.get(url, timeout=5)`
  - `timeout=5`: Timeout 5 giây
  - Nếu > 5s → Raise `Timeout` exception

#### Error Handling
- `raise_for_status()`: Raise exception nếu HTTP error
  - 4xx (Client Error): `HTTPError`
  - 5xx (Server Error): `HTTPError`

#### Parse JSON
- `.json()`: Parse response body → Python dict

**Ví dụ response:**
```json
{
  "symbol": "BTCUSDT",
  "priceChange": "-1000.00",
  "priceChangePercent": "-2.38",
  "lastPrice": "42000.00",
  "openPrice": "43000.00",
  "highPrice": "43500.00",
  "lowPrice": "41800.00",
  "volume": "12345.67",
  "quoteVolume": "520000000.00",
  "closeTime": 1734134400000,
  "count": 123456
}
```

---

### Dòng 55-67: Extract Data
```python
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
```
**Giải thích từng field:**

| Field | Source | Type | Ý nghĩa |
|-------|--------|------|---------|
| `symbol` | `data['symbol']` | str | BTCUSDT/ETHUSDT |
| `event_time` | `data['closeTime']` | int | Timestamp của price (ms) |
| `price` | `data['lastPrice']` | float | Giá gần nhất (current price) |
| `open` | `data['openPrice']` | float | Giá mở cửa 24h trước |
| `high` | `data['highPrice']` | float | Giá cao nhất 24h |
| `low` | `data['lowPrice']` | float | Giá thấp nhất 24h |
| `volume` | `data['volume']` | float | Volume 24h (BTC/ETH) |
| `quote_volume` | `data['quoteVolume']` | float | Volume 24h (USDT) |
| `number_trades` | `data['count']` | int | Số trades 24h |
| `price_change` | `data['priceChange']` | float | Thay đổi giá 24h (absolute) |
| `price_change_percent` | `data['priceChangePercent']` | float | Thay đổi giá 24h (%) |
| `timestamp` | `datetime.now().isoformat()` | str | Timestamp lúc fetch (ISO 8601) |

**Chú ý:**
- `event_time`: Timestamp từ Binance (data time)
- `timestamp`: Timestamp lúc producer fetch (system time)
- **Tại sao cần 2 timestamps?**
  - `event_time`: Event time (dữ liệu thật)
  - `timestamp`: Processing time (audit/debug)

**Ví dụ output:**
```python
{
  'symbol': 'BTCUSDT',
  'event_time': 1734134400000,
  'price': 42000.0,
  'open': 43000.0,
  'high': 43500.0,
  'low': 41800.0,
  'volume': 12345.67,
  'quote_volume': 520000000.0,
  'number_trades': 123456,
  'price_change': -1000.0,
  'price_change_percent': -2.38,
  'timestamp': '2025-12-16T10:30:45.123456'
}
```

---

### Dòng 68-70: Exception Handling
```python
    except Exception as e:
        logger.error(f"Error fetching {symbol}: {e}")
        return None
```
**Giải thích:**
- Nếu API call fail → Log error, return `None`
- **Không raise exception** (vì muốn producer tiếp tục chạy)
- **Common errors:**
  - Timeout: `requests.exceptions.Timeout`
  - Network: `requests.exceptions.ConnectionError`
  - API rate limit: HTTP 429

---

## 4. Main Streaming Loop

### Dòng 72-78: Function Header
```python
def main():
    """Khởi động continuous streaming vào Kafka"""
    
    print("=" * 70)
    print("BINANCE API → KAFKA PRODUCER")
    print("Continuous Streaming (1 second interval)")
    print("=" * 70)
```
**Giải thích:** In banner cho console output.

---

### Dòng 80-82: Create Producer
```python
    # Tạo Kafka Producer
    global producer
    producer = create_kafka_producer()
```
**Giải thích:**
- `global producer`: Declare global variable (để cleanup trong except)
- Call `create_kafka_producer()` để tạo producer
- **Nếu fail:** Exception raised → Program thoát

---

### Dòng 84-90: Setup
```python
    symbols = ['BTCUSDT', 'ETHUSDT']
    logger.info(f"📡 Streaming symbols: {', '.join(symbols)}")
    
    print("\n✓ Producer connected")
    print("✓ Sending real-time data to Kafka topic: crypto-prices")
    print("\nPress Ctrl+C to stop\n")
```
**Giải thích:**
- `symbols`: List symbols cần stream
- Log và print thông tin khởi động
- Hướng dẫn user dùng Ctrl+C để stop

---

### Dòng 92-94: Main Loop
```python
    try:
        while True:
            for symbol in symbols:
```
**Giải thích:**
- `try`: Bắt KeyboardInterrupt (Ctrl+C)
- `while True`: Infinite loop (continuous streaming)
- `for symbol in symbols`: Lặp qua BTC và ETH

---

### Dòng 95-97: Fetch Data
```python
                # Fetch data
                data = fetch_ticker_data(symbol)
                
                if data:
```
**Giải thích:**
- Call `fetch_ticker_data()` để lấy data
- `if data`: Chỉ xử lý nếu fetch thành công (data ≠ None)

---

### Dòng 98-100: Send to Kafka
```python
                    # Send to Kafka
                    future = producer.send(KAFKA_TOPIC, value=data)
                    future.get(timeout=10)
```
**Giải thích:**

#### `producer.send()`
- **Asynchronous operation:** Không block
- Return `FutureRecordMetadata` (promise)
- **Parameters:**
  - `KAFKA_TOPIC`: 'crypto-prices'
  - `value=data`: Message value (dict)
    - Tự động serialize bằng `value_serializer`

#### `future.get(timeout=10)`
- **Synchronous wait:** Đợi Kafka ack
- `timeout=10`: Timeout 10 giây
- **Tại sao cần .get()?**
  - Đảm bảo message đã gửi thành công
  - Catch lỗi ngay (không delay)
  - **Alternative:** Dùng `producer.flush()` cuối loop (batch)

---

### Dòng 102-103: Update Statistics
```python
                    # Update statistics
                    message_count[symbol] = message_count.get(symbol, 0) + 1
```
**Giải thích:**
- Increment counter cho symbol
- `.get(symbol, 0)`: Lấy count hiện tại (default 0 nếu chưa có)
- `+ 1`: Tăng thêm 1

---

### Dòng 105-115: Logging
```python
                    # Log mỗi 10 messages
                    if message_count[symbol] % 10 == 0:
                        elapsed = time.time() - start_time
                        rate = sum(message_count.values()) / elapsed
                        logger.info(
                            f"📊 {symbol}: ${data['price']:,.2f} | "
                            f"Vol: {data['volume']:,.0f} | "
                            f"Change: {data['price_change_percent']:+.2f}% | "
                            f"Messages: {message_count[symbol]} | "
                            f"Rate: {rate:.1f} msg/s"
                        )
```
**Giải thích:**

#### Điều kiện log
- `if message_count[symbol] % 10 == 0`: Log mỗi 10 messages
  - `%` (modulo): 10, 20, 30, ... → True
- **Tại sao không log mỗi message?**
  - Tránh spam console
  - Giảm overhead

#### Statistics
- `elapsed`: Thời gian chạy (giây)
- `rate`: Tốc độ gửi (messages/second)
  - `sum(message_count.values())`: Tổng messages (BTC + ETH)
  - `/ elapsed`: Chia cho thời gian

#### Log Format
- `f"📊 {symbol}: ${data['price']:,.2f}"`: BTCUSDT: $42,000.00
  - `:,.2f`: Format số với dấu phẩy, 2 chữ số thập phân
- `f"Vol: {data['volume']:,.0f}"`: Vol: 12,345
  - `:,.0f`: Format số với dấu phẩy, không thập phân
- `f"Change: {data['price_change_percent']:+.2f}%"`: Change: -2.38%
  - `:+.2f`: Dấu +/- trước số
- `f"Rate: {rate:.1f} msg/s"`: Rate: 2.0 msg/s
  - `:.1f`: 1 chữ số thập phân

**Ví dụ output:**
```
2025-12-16 10:30:45 - INFO - 📊 BTCUSDT: $42,000.00 | Vol: 12,345 | Change: -2.38% | Messages: 10 | Rate: 2.0 msg/s
```

---

### Dòng 117-118: Sleep
```python
            # Wait 1 second (continuous streaming)
            time.sleep(1)
```
**Giải thích:**
- Đợi 1 giây trước khi loop tiếp
- **Tại sao 1 giây?**
  - 2 symbols × 1s = ~2 msg/s (đủ cho demo)
  - Tránh spam Binance API
  - Tránh rate limit (Binance: 1200 requests/minute = 20 req/s)

---

## 5. Statistics & Cleanup

### Dòng 120-123: Keyboard Interrupt
```python
    except KeyboardInterrupt:
        logger.info("\n⏹ Stopping producer...")
        producer.flush()
        producer.close()
```
**Giải thích:**
- `except KeyboardInterrupt`: Bắt Ctrl+C
- `logger.info()`: Log thông báo dừng
- `producer.flush()`: Đợi tất cả messages được gửi
  - Nếu có pending messages → Đợi ack
- `producer.close()`: Đóng connection
  - Giải phóng resources

---

### Dòng 125-136: Final Statistics
```python
        # Print final statistics
        total = sum(message_count.values())
        elapsed = time.time() - start_time
        print("\n" + "=" * 70)
        print("STATISTICS")
        print("=" * 70)
        print(f"Total messages sent: {total:,}")
        print(f"Duration: {elapsed:.1f}s")
        print(f"Average rate: {total/elapsed:.1f} messages/second")
        for symbol, count in message_count.items():
            print(f"  {symbol}: {count:,} messages")
        print("=" * 70)
```
**Giải thích:**

#### Tính toán
- `total`: Tổng messages (BTC + ETH)
- `elapsed`: Tổng thời gian chạy (giây)

#### Print Statistics
- Total messages sent
- Duration (thời gian chạy)
- Average rate (messages/second)
- Breakdown theo từng symbol

**Ví dụ output:**
```
======================================================================
STATISTICS
======================================================================
Total messages sent: 1,008
Duration: 504.2s
Average rate: 2.0 messages/second
  BTCUSDT: 504 messages
  ETHUSDT: 504 messages
======================================================================
```

---

### Dòng 138-139: Main Guard
```python
if __name__ == "__main__":
    main()
```
**Giải thích:**
- `if __name__ == "__main__"`: Chỉ chạy khi execute file trực tiếp
  - Không chạy khi import module
- Call `main()` để start producer

---

---

# Tóm tắt Tổng quan

## 🎯 Mục đích File
File `websocket_producer.py` là **Speed Layer Producer** trong Lambda Architecture - Thu thập real-time cryptocurrency data từ Binance và push vào Kafka message broker cho Spark Streaming consume.

---

## 📊 Workflow (4 Steps)

### **1. Initialize Kafka Producer**
- Connect to Kafka broker (`localhost:9092`)
- Configure serializer (JSON → bytes)
- Setup retry logic và compression
- Configure acknowledgment level (`acks='all'`)

### **2. Fetch Real-time Data**
- Call Binance API endpoint: `/api/v3/ticker/24hr`
- Lấy 24-hour statistics (OHLCV + price change)
- Timeout: 5 giây
- Error handling: Return `None` nếu fail

### **3. Stream to Kafka**
- Send message to topic `crypto-prices`
- Wait for acknowledgment (`.get(timeout=10)`)
- Update statistics counter
- Log mỗi 10 messages

### **4. Loop Continuously**
- Sleep 1 giây giữa mỗi lần poll
- Handle Ctrl+C gracefully
- Flush và close producer
- Print final statistics

---

## 🔑 Điểm Quan Trọng

### **1. REST API Polling vs WebSocket**
- File tên `websocket_producer.py` nhưng dùng **REST API polling**
- **Tại sao?**
  - Đơn giản hơn, ổn định hơn
  - 1 giây/lần đủ cho demo (không cần millisecond latency)
  - Tránh maintain WebSocket connection liên tục

### **2. Kafka Configuration**
| Config | Value | Lý do |
|--------|-------|-------|
| `acks` | `'all'` | Đảm bảo không mất data |
| `retries` | `3` | Retry nếu network glitch |
| `compression_type` | `'gzip'` | Giảm bandwidth |
| `max_in_flight_requests` | `1` | Đảm bảo ordering |

### **3. Asynchronous Send**
- `producer.send()`: Async (không block)
- `future.get(timeout=10)`: Sync wait (đợi ack)
- **Alternative:** `producer.flush()` cuối loop (batch mode)

### **4. Error Handling**
- API timeout: Return `None`, continue loop
- Kafka send fail: Retry 3 lần
- KeyboardInterrupt: Flush + Close gracefully

### **5. Statistics Tracking**
- Counter cho mỗi symbol
- Rate calculation (msg/s)
- Log mỗi 10 messages (tránh spam)

---

## 📁 Data Schema

### **Message Structure**
```json
{
  "symbol": "BTCUSDT",
  "event_time": 1734134400000,
  "price": 42000.0,
  "open": 43000.0,
  "high": 43500.0,
  "low": 41800.0,
  "volume": 12345.67,
  "quote_volume": 520000000.0,
  "number_trades": 123456,
  "price_change": -1000.0,
  "price_change_percent": -2.38,
  "timestamp": "2025-12-16T10:30:45.123456"
}
```

### **Fields Explanation**
| Field | Type | Source | Ý nghĩa |
|-------|------|--------|---------|
| `symbol` | str | API | BTCUSDT/ETHUSDT |
| `event_time` | int | API | Timestamp của data (ms) |
| `price` | float | API | Giá hiện tại |
| `open/high/low` | float | API | OHLC 24h |
| `volume` | float | API | Volume BTC/ETH 24h |
| `quote_volume` | float | API | Volume USDT 24h |
| `number_trades` | int | API | Số trades 24h |
| `price_change` | float | API | Thay đổi giá 24h ($) |
| `price_change_percent` | float | API | Thay đổi giá 24h (%) |
| `timestamp` | str | Producer | Timestamp lúc fetch (ISO 8601) |

---

## 💡 Use Cases

### **Khi nào chạy file này?**
1. ✅ Sau khi start Kafka: `docker-compose up -d`
2. ✅ Muốn collect real-time data cho Speed Layer
3. ✅ Demo Lambda Architecture (Batch + Speed)
4. ✅ Test Kafka connectivity

### **Khi nào KHÔNG chạy?**
- ❌ Kafka chưa start (sẽ fail ngay)
- ❌ Binance API đang down (sẽ log error liên tục)
- ❌ Network không stable (nhiều timeouts)

---

## 🚀 Cách Sử Dụng

### **1. Start Kafka First**
```bash
cd week6_streaming
docker-compose up -d
```

### **2. Run Producer**
```bash
python websocket_producer.py
```

### **3. Expected Output**
```
======================================================================
BINANCE API → KAFKA PRODUCER
Continuous Streaming (1 second interval)
======================================================================

✓ Producer connected
✓ Sending real-time data to Kafka topic: crypto-prices

Press Ctrl+C to stop

2025-12-16 10:30:45 - INFO - 📊 BTCUSDT: $42,000.00 | Vol: 12,345 | Change: -2.38% | Messages: 10 | Rate: 2.0 msg/s
2025-12-16 10:30:55 - INFO - 📊 ETHUSDT: $3,200.00 | Vol: 45,678 | Change: +1.25% | Messages: 10 | Rate: 2.0 msg/s
```

### **4. Stop Producer**
- Press `Ctrl+C`
- Producer sẽ flush pending messages
- Print final statistics

---

## 🔧 Troubleshooting

### **1. Kafka Connection Failed**
**Error:** `NoBrokersAvailable`  
**Giải pháp:**
```bash
# Check Kafka status
docker ps | grep kafka

# Start if not running
docker-compose up -d

# Check logs
docker logs week6_streaming_kafka_1
```

### **2. Binance API Timeout**
**Error:** `Timeout: 5s`  
**Giải pháp:**
- Check network connection
- Tăng timeout: `requests.get(url, timeout=10)`
- Thử VPN nếu bị chặn

### **3. Rate Limit**
**Error:** HTTP 429 (Too Many Requests)  
**Giải pháp:**
- Tăng sleep time: `time.sleep(2)` thay vì 1 giây
- Giảm số symbols (chỉ test 1 symbol)

### **4. Messages Not Sent**
**Triệu chứng:** Counter không tăng  
**Giải pháp:**
- Check `fetch_ticker_data()` return `None`
- Check log: `Error fetching BTCUSDT: ...`
- Check Kafka topic tồn tại: `docker exec -it kafka_container kafka-topics --list --bootstrap-server localhost:9092`

---

## 📈 Performance

### **Throughput**
- **2 symbols × 1 message/symbol/second = 2 msg/s**
- **1 phút:** 120 messages
- **10 phút:** 1,200 messages
- **1 giờ:** 7,200 messages

### **Message Size**
- **JSON:** ~400 bytes/message
- **Compressed (gzip):** ~200 bytes/message
- **1 giờ:** 1.44 MB (compressed)

### **Binance API Limit**
- **Weight:** 1 per request
- **Limit:** 1,200 requests/minute = 20 req/s
- **Usage:** 2 req/s → **10% limit** (safe)

---

## 🎓 Key Technologies

- **Kafka:** Message broker for streaming data
- **kafka-python:** Python client library
- **Binance API:** REST API endpoint `/api/v3/ticker/24hr`
- **JSON Serialization:** Dict → JSON → bytes
- **Logging:** Python logging module
- **Lambda Architecture:** Speed Layer Producer component

---

## 🔗 Integration

### **Producer → Kafka → Consumer**
```
websocket_producer.py
  ↓ (Kafka topic: crypto-prices)
spark_streaming_consumer.py  (Production: 1-day window)
kafka_batch_reader.py         (Demo: Batch mode)
```

### **Next Steps After Running Producer**
1. **Production Mode:**
   ```bash
   python spark_streaming_consumer.py
   # Wait 24 hours for window to close
   ```

2. **Demo Mode (Quick):**
   ```bash
   # Ctrl+C sau 10 phút (1,200 messages)
   python kafka_batch_reader.py
   # Instant output
   ```

3. **Merge với Batch Layer:**
   ```bash
   python ../scripts/lambda_batch/week6_merge.py
   ```

---

**Tác giả:** Đoàn Thế Tín  
**MSSV:** 4551190056  
**File:** `week6_streaming/websocket_producer.py`  
**Lines:** 143 dòng code  
**Mục đích:** Speed Layer Producer - Real-time data collection cho Lambda Architecture

---
