# 📘 WEEK 6 - PHẦN 3: SPEED LAYER (Kafka + Spark Streaming)

## 📑 Mục lục
1. [Mục đích của Speed Layer](#1-mục-đích-của-speed-layer)
2. [Kiến trúc Speed Layer](#2-kiến-trúc-speed-layer)
3. [Apache Kafka - Giải thích chi tiết](#3-apache-kafka---giải-thích-chi-tiết)
4. [Docker và docker-compose.yml](#4-docker-và-docker-composeyml)
5. [Producer - websocket_producer.py](#5-producer---websocket_producerpy)
6. [Consumer - spark_streaming_consumer.py](#6-consumer---spark_streaming_consumerpy)
7. [Watermark và Window](#7-watermark-và-window)
8. [Output và Checkpoint](#8-output-và-checkpoint)
9. [Câu hỏi thường gặp](#9-câu-hỏi-thường-gặp)

---

## 1. Mục đích của Speed Layer

### 1.1. Vai trò trong Lambda Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    LAMBDA ARCHITECTURE                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   BATCH LAYER              SPEED LAYER              SERVING LAYER  │
│   ┌─────────┐              ┌─────────┐              ┌─────────┐    │
│   │ Xử lý   │              │ Xử lý   │              │ Merge   │    │
│   │ LỊCH SỬ │              │REAL-TIME│              │ kết quả │    │
│   │         │              │         │              │         │    │
│   │ Chính   │              │ Nhanh   │              │ Phục vụ │    │
│   │ xác     │              │         │              │ query   │    │
│   └─────────┘              └────┬────┘              └─────────┘    │
│                                 │                                   │
│                                 │                                   │
│                          ┌──────┴──────┐                           │
│                          │   ĐÂY LÀ    │                           │
│                          │ SPEED LAYER │                           │
│                          │ (Kafka +    │                           │
│                          │  Spark)     │                           │
│                          └─────────────┘                           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.2. Đặc điểm của Speed Layer

```
Speed Layer chịu trách nhiệm:

1. THU THẬP dữ liệu real-time
   └── Poll Binance API mỗi giây

2. TRUYỀN TẢI qua message broker
   └── Kafka: Đảm bảo không mất data

3. XỬ LÝ với streaming engine
   └── Spark Structured Streaming

4. AGGREGATION theo thời gian
   └── Daily windows

5. LƯU TRỮ kết quả
   └── Parquet files
```

### 1.3. Tại sao cần Speed Layer?

```
Vấn đề: Batch Layer xử lý chậm, không real-time

Ví dụ:
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│  09:00 - Batch Layer chạy xong, data đến 08:59                    │
│  09:01 - BTC tăng $1000 ← Batch Layer không biết!                 │
│  09:02 - BTC tăng thêm $500 ← Batch Layer vẫn không biết!         │
│  ...                                                               │
│  10:00 - Batch Layer chạy lại, mới thấy BTC đã tăng $2000         │
│                                                                    │
│  → Chậm 1 tiếng! Trong trading, 1 tiếng = rất nguy hiểm          │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘

Giải pháp: Speed Layer bổ sung dữ liệu real-time

┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│  09:00 - Speed Layer: BTC = $92,000                               │
│  09:01 - Speed Layer: BTC = $93,000 (+$1,000) ✓                   │
│  09:02 - Speed Layer: BTC = $93,500 (+$500)   ✓                   │
│  ...                                                               │
│  09:59 - Speed Layer: BTC = $94,000                               │
│                                                                    │
│  → Real-time! Luôn có dữ liệu mới nhất                            │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

---

## 2. Kiến trúc Speed Layer

### 2.1. Các thành phần

```
┌─────────────────────────────────────────────────────────────────────┐
│                       SPEED LAYER ARCHITECTURE                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│                      ┌──────────────────┐                          │
│                      │   BINANCE API    │                          │
│                      │  api.binance.com │                          │
│                      └────────┬─────────┘                          │
│                               │                                     │
│                               │ HTTP GET (1 req/sec)                │
│                               ▼                                     │
│                      ┌──────────────────┐                          │
│                      │    PRODUCER      │                          │
│                      │ websocket_       │                          │
│                      │ producer.py      │                          │
│                      │                  │                          │
│                      │ Python +         │                          │
│                      │ kafka-python     │                          │
│                      └────────┬─────────┘                          │
│                               │                                     │
│                               │ Kafka Protocol                      │
│                               ▼                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                      APACHE KAFKA                             │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │  ZOOKEEPER (Cluster coordination)                       │ │  │
│  │  │  Port: 2181                                             │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  │                                                               │  │
│  │  ┌─────────────────────────────────────────────────────────┐ │  │
│  │  │  KAFKA BROKER                                           │ │  │
│  │  │  Port: 9092                                             │ │  │
│  │  │                                                         │ │  │
│  │  │  Topic: crypto-prices                                   │ │  │
│  │  │  ┌─────────────────┐  ┌─────────────────┐              │ │  │
│  │  │  │  Partition 0    │  │  Partition 1    │              │ │  │
│  │  │  │  [msg][msg]...  │  │  [msg][msg]...  │              │ │  │
│  │  │  └─────────────────┘  └─────────────────┘              │ │  │
│  │  │                                                         │ │  │
│  │  └─────────────────────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                               │                                     │
│                               │ Kafka Protocol                      │
│                               ▼                                     │
│                      ┌──────────────────┐                          │
│                      │    CONSUMER      │                          │
│                      │ spark_streaming_ │                          │
│                      │ consumer.py      │                          │
│                      │                  │                          │
│                      │ PySpark +        │                          │
│                      │ Structured       │                          │
│                      │ Streaming        │                          │
│                      └────────┬─────────┘                          │
│                               │                                     │
│                               │ Write to disk                       │
│                               ▼                                     │
│                      ┌──────────────────┐                          │
│                      │     OUTPUT       │                          │
│                      │ streaming_       │                          │
│                      │ output_spark/    │                          │
│                      │   daily/         │                          │
│                      └──────────────────┘                          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2. Files trong Speed Layer

```
week6_streaming/
├── docker-compose.yml           # Config Kafka + Zookeeper
├── websocket_producer.py        # Producer: Binance → Kafka
├── spark_streaming_consumer.py  # Consumer: Kafka → Parquet
└── README.md                    # Hướng dẫn
```

---

## 3. Apache Kafka - Giải thích chi tiết

### 3.1. Kafka là gì?

```
Apache Kafka là một DISTRIBUTED STREAMING PLATFORM:

1. PUBLISH-SUBSCRIBE messaging system
   └── Producer gửi message
   └── Consumer nhận message

2. DISTRIBUTED
   └── Chạy trên nhiều server (cluster)
   └── Fault tolerant (không mất data)

3. HIGH-THROUGHPUT
   └── Xử lý hàng triệu messages/giây
   └── Low latency (< 10ms)

4. PERSISTENT
   └── Lưu messages trên disk
   └── Có thể replay messages
```

### 3.2. Các khái niệm cơ bản

```
┌─────────────────────────────────────────────────────────────────────┐
│                        KAFKA CONCEPTS                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. BROKER                                                          │
│     └── Một Kafka server                                           │
│     └── Lưu trữ và phân phối messages                             │
│     └── Trong project: 1 broker (localhost:9092)                  │
│                                                                     │
│  2. TOPIC                                                           │
│     └── "Folder" chứa messages                                     │
│     └── Producers gửi vào topic                                    │
│     └── Consumers đọc từ topic                                     │
│     └── Trong project: topic "crypto-prices"                       │
│                                                                     │
│  3. PARTITION                                                       │
│     └── Chia nhỏ topic                                             │
│     └── Cho phép parallel processing                               │
│     └── Trong project: 2 partitions                                │
│                                                                     │
│  4. PRODUCER                                                        │
│     └── Gửi messages vào topic                                     │
│     └── Trong project: websocket_producer.py                       │
│                                                                     │
│  5. CONSUMER                                                        │
│     └── Đọc messages từ topic                                      │
│     └── Trong project: spark_streaming_consumer.py                 │
│                                                                     │
│  6. ZOOKEEPER                                                       │
│     └── Quản lý Kafka cluster                                      │
│     └── Lưu metadata                                               │
│     └── Coordinator cho brokers                                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.3. Message flow trong Kafka

```
┌─────────────────────────────────────────────────────────────────────┐
│                      MESSAGE FLOW                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Step 1: Producer gửi message                                       │
│                                                                     │
│  websocket_producer.py:                                            │
│  ┌─────────────────────────────────────────┐                       │
│  │ {                                        │                       │
│  │   "symbol": "BTCUSDT",                   │                       │
│  │   "price": 92817.92,                     │                       │
│  │   "volume": 29318.80,                    │                       │
│  │   "timestamp": "2025-12-03T10:00:01"     │                       │
│  │ }                                        │                       │
│  └─────────────────┬───────────────────────┘                       │
│                    │                                                │
│                    │ producer.send("crypto-prices", message)        │
│                    ▼                                                │
│                                                                     │
│  Step 2: Kafka lưu message vào partition                           │
│                                                                     │
│  Topic: crypto-prices                                              │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │                                                             │    │
│  │  Partition 0: [msg1] [msg3] [msg5] [msg7] [msg9] ...       │    │
│  │               offset: 0     1      2      3      4          │    │
│  │                                                             │    │
│  │  Partition 1: [msg2] [msg4] [msg6] [msg8] [msg10] ...      │    │
│  │               offset: 0     1      2      3      4          │    │
│  │                                                             │    │
│  └────────────────────────────────────────────────────────────┘    │
│                    │                                                │
│                    │ Kafka chọn partition (round-robin hoặc key)   │
│                    │                                                │
│                                                                     │
│  Step 3: Consumer đọc messages                                      │
│                                                                     │
│  spark_streaming_consumer.py:                                      │
│  ┌─────────────────────────────────────────┐                       │
│  │ spark.readStream                        │                       │
│  │   .format("kafka")                      │                       │
│  │   .option("subscribe", "crypto-prices") │                       │
│  │   .load()                               │                       │
│  └─────────────────────────────────────────┘                       │
│                                                                     │
│  → Consumer đọc từ cả 2 partitions song song                       │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.4. Tại sao cần Kafka?

```
Không có Kafka (Direct connection):
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Producer ──────────────────────────────► Consumer                  │
│                                                                     │
│  Vấn đề:                                                           │
│  ❌ Nếu Consumer chậm → Producer phải đợi                          │
│  ❌ Nếu Consumer crash → Mất messages                              │
│  ❌ Không thể có nhiều Consumers                                   │
│  ❌ Không thể replay messages                                      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

Có Kafka (Message broker):
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Producer ─────► KAFKA ─────► Consumer 1                           │
│                    │                                                │
│                    └────────► Consumer 2                           │
│                    │                                                │
│                    └────────► Consumer 3                           │
│                                                                     │
│  Lợi ích:                                                          │
│  ✅ DECOUPLING: Producer/Consumer độc lập                          │
│  ✅ BUFFERING: Kafka giữ messages nếu Consumer chậm                │
│  ✅ DURABILITY: Messages lưu trên disk, không mất                  │
│  ✅ SCALABILITY: Nhiều Consumers đọc cùng topic                    │
│  ✅ REPLAY: Có thể đọc lại messages cũ                             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Docker và docker-compose.yml

### 4.1. Tại sao dùng Docker?

```
Không có Docker:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  1. Tải Kafka binary từ Apache                                     │
│  2. Cài đặt Java JDK                                               │
│  3. Cấu hình JAVA_HOME                                             │
│  4. Cấu hình Kafka config files                                    │
│  5. Tải Zookeeper binary                                           │
│  6. Cấu hình Zookeeper                                             │
│  7. Start Zookeeper                                                │
│  8. Start Kafka                                                    │
│  9. Debug nếu có lỗi...                                            │
│                                                                     │
│  → Mất 30 phút - 1 tiếng, có thể gặp nhiều lỗi                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

Có Docker:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  docker-compose up -d                                              │
│                                                                     │
│  → 10 giây, không cần cài đặt gì thêm!                            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.2. Giải thích docker-compose.yml

```yaml
# docker-compose.yml - GIẢI THÍCH CHI TIẾT

version: '3.8'  # Phiên bản docker-compose syntax

services:
  # ================================================================
  # SERVICE 1: ZOOKEEPER
  # ================================================================
  zookeeper:
    # Image: Confluent Zookeeper (production-grade)
    image: confluentinc/cp-zookeeper:7.5.0
    
    # Tên container (để dễ identify)
    container_name: zookeeper
    
    # Biến môi trường
    environment:
      # Port mà clients connect
      ZOOKEEPER_CLIENT_PORT: 2181
      
      # Tick time (milliseconds) - heartbeat interval
      ZOOKEEPER_TICK_TIME: 2000
    
    # Port mapping: host:container
    # Cho phép access từ localhost:2181
    ports:
      - "2181:2181"
    
    # Network để các containers giao tiếp
    networks:
      - crypto-network

  # ================================================================
  # SERVICE 2: KAFKA
  # ================================================================
  kafka:
    # Image: Confluent Kafka
    image: confluentinc/cp-kafka:7.5.0
    
    container_name: kafka
    
    # Kafka phải đợi Zookeeper start trước
    depends_on:
      - zookeeper
    
    # Port mapping
    ports:
      - "9092:9092"   # External clients (producer, consumer)
      - "9093:9093"   # Internal (giữa các containers)
    
    environment:
      # ID của broker này
      KAFKA_BROKER_ID: 1
      
      # Địa chỉ Zookeeper
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      
      # Listeners: Kafka lắng nghe ở đâu
      # PLAINTEXT: Không encryption
      KAFKA_ADVERTISED_LISTENERS: >
        PLAINTEXT://localhost:9092,
        PLAINTEXT_INTERNAL://kafka:9093
      
      # Security protocol (không dùng SSL/TLS)
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: >
        PLAINTEXT:PLAINTEXT,
        PLAINTEXT_INTERNAL:PLAINTEXT
      
      # Replication factor = 1 (chỉ có 1 broker)
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      
      # Tự động tạo topic khi producer gửi message
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    
    networks:
      - crypto-network
    
    # Persist data
    volumes:
      - kafka-data:/var/lib/kafka/data

# ================================================================
# NETWORK
# ================================================================
networks:
  crypto-network:
    driver: bridge   # Default network driver

# ================================================================
# VOLUME
# ================================================================
volumes:
  kafka-data:        # Persistent storage cho Kafka data
```

### 4.3. Các lệnh Docker thường dùng

```bash
# Khởi động Kafka + Zookeeper (background)
docker-compose up -d

# Xem containers đang chạy
docker ps

# Xem logs của Kafka
docker logs kafka

# Xem logs của Zookeeper
docker logs zookeeper

# Dừng tất cả containers
docker-compose down

# Dừng và xóa volumes (reset data)
docker-compose down -v

# List topics trong Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Tạo topic mới
docker exec kafka kafka-topics --create \
  --topic crypto-prices \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1

# Xem messages trong topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto-prices \
  --from-beginning \
  --max-messages 5
```

---

## 5. Producer - websocket_producer.py

### 5.1. Tổng quan

```python
"""
websocket_producer.py

Nhiệm vụ:
1. Poll Binance API mỗi giây
2. Gửi data vào Kafka topic "crypto-prices"

Flow:
Binance API → Python → Kafka Producer → Kafka Topic
"""
```

### 5.2. Kafka Producer Configuration

```python
def create_kafka_producer():
    """Tạo Kafka Producer với retry logic"""
    try:
        producer = KafkaProducer(
            # ============================================
            # BOOTSTRAP SERVERS
            # ============================================
            # Địa chỉ Kafka broker
            # Có thể có nhiều brokers: ['host1:9092', 'host2:9092']
            bootstrap_servers=['localhost:9092'],
            
            # ============================================
            # SERIALIZER
            # ============================================
            # Convert Python dict → JSON bytes
            # Kafka chỉ nhận bytes, không nhận dict
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            
            # ============================================
            # RELIABILITY SETTINGS
            # ============================================
            # Số lần retry nếu gửi thất bại
            retries=3,
            
            # acks='all': Đợi tất cả replicas confirm
            # Chậm hơn nhưng đảm bảo không mất data
            # Các options:
            #   acks=0: Không đợi confirm (nhanh, có thể mất)
            #   acks=1: Đợi leader confirm (trung bình)
            #   acks='all': Đợi tất cả replicas (chậm, an toàn)
            acks='all',
            
            # ============================================
            # PERFORMANCE SETTINGS
            # ============================================
            # Nén data bằng gzip (giảm bandwidth)
            compression_type='gzip',
            
            # Chỉ 1 request in-flight tại 1 thời điểm
            # Đảm bảo thứ tự messages
            max_in_flight_requests_per_connection=1
        )
        logger.info(f"✓ Kafka Producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"✗ Failed to create Kafka Producer: {e}")
        raise
```

### 5.3. Fetch data từ Binance

```python
def fetch_ticker_data(symbol):
    """
    Lấy dữ liệu ticker 24hr từ Binance API
    
    API Endpoint: GET /api/v3/ticker/24hr
    
    Returns 24-hour statistics cho symbol
    """
    try:
        # Binance ticker API
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol.upper()}"
        
        # HTTP GET với timeout 5 giây
        response = requests.get(url, timeout=5)
        response.raise_for_status()  # Raise exception nếu HTTP error
        
        data = response.json()
        
        # Transform data
        return {
            'symbol': data['symbol'],           # "BTCUSDT"
            'event_time': int(data['closeTime']),  # Timestamp (ms)
            'price': float(data['lastPrice']),  # Giá hiện tại
            'open': float(data['openPrice']),   # Giá mở cửa 24h
            'high': float(data['highPrice']),   # Giá cao nhất 24h
            'low': float(data['lowPrice']),     # Giá thấp nhất 24h
            'volume': float(data['volume']),    # Volume 24h (BTC)
            'quote_volume': float(data['quoteVolume']),  # Volume 24h (USDT)
            'number_trades': int(data['count']),  # Số giao dịch 24h
            'price_change': float(data['priceChange']),  # Thay đổi giá
            'price_change_percent': float(data['priceChangePercent']),  # % thay đổi
            'timestamp': datetime.now().isoformat()  # Timestamp local
        }
    except Exception as e:
        logger.error(f"Error fetching {symbol}: {e}")
        return None
```

### 5.4. Main loop - Continuous streaming

```python
def main():
    """Khởi động continuous streaming vào Kafka"""
    
    # Tạo Kafka Producer
    producer = create_kafka_producer()
    
    symbols = ['BTCUSDT', 'ETHUSDT']
    
    try:
        # ============================================
        # INFINITE LOOP - Chạy liên tục
        # ============================================
        while True:
            for symbol in symbols:
                # Fetch data từ Binance
                data = fetch_ticker_data(symbol)
                
                if data:
                    # ============================================
                    # GỬI VÀO KAFKA
                    # ============================================
                    # producer.send() trả về Future
                    # future.get() đợi cho đến khi gửi xong
                    future = producer.send(KAFKA_TOPIC, value=data)
                    future.get(timeout=10)  # Block until sent
                    
                    # Log mỗi 10 messages
                    if message_count[symbol] % 10 == 0:
                        logger.info(f"📊 {symbol}: ${data['price']:,.2f}")
            
            # ============================================
            # SLEEP 1 SECOND
            # ============================================
            # Polling rate: 1 request/second/symbol
            # = 2 requests/second total (BTC + ETH)
            time.sleep(1)
            
    except KeyboardInterrupt:
        # User nhấn Ctrl+C
        logger.info("\n⏹ Stopping producer...")
        producer.flush()   # Gửi hết messages còn trong buffer
        producer.close()   # Đóng connection
```

### 5.5. Message format

```json
{
    "symbol": "BTCUSDT",
    "event_time": 1764730800011,
    "price": 92817.92,
    "open": 86554.46,
    "high": 93051.64,
    "low": 86214.99,
    "volume": 29318.80585,
    "quote_volume": 2635333520.175692,
    "number_trades": 6468949,
    "price_change": 6263.46,
    "price_change_percent": 7.236,
    "timestamp": "2025-12-03T10:00:01.226298"
}
```

---

## 6. Consumer - spark_streaming_consumer.py

### 6.1. Tổng quan

```python
"""
spark_streaming_consumer.py

Đây là STRUCTURED STREAMING thật sự:
- Đọc continuous stream từ Kafka
- Micro-batch processing (trigger 10s)
- Watermarking (xử lý late data)
- Window aggregation (1 day, 1 hour)
- Checkpoint (fault tolerance)
"""
```

### 6.2. Spark Session Configuration

```python
spark = SparkSession.builder \
    # Tên application (hiển thị trong Spark UI)
    .appName("CryptoPriceStructuredStreaming") \
    
    # Thêm Kafka connector package
    # Spark không có sẵn Kafka support, cần thêm dependency
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    
    # Nơi lưu checkpoints (fault tolerance)
    .config("spark.sql.streaming.checkpointLocation", "checkpoint_spark") \
    
    # Bật Adaptive Query Execution
    .config("spark.sql.adaptive.enabled", "true") \
    
    # Memory cho driver
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

### 6.3. Schema Definition

```python
# Schema cho JSON message từ Kafka
# Phải match với format mà Producer gửi
message_schema = StructType([
    StructField("symbol", StringType(), True),        # "BTCUSDT"
    StructField("event_time", LongType(), True),      # Timestamp (ms)
    StructField("price", DoubleType(), True),         # 92817.92
    StructField("open", DoubleType(), True),          # 86554.46
    StructField("high", DoubleType(), True),          # 93051.64
    StructField("low", DoubleType(), True),           # 86214.99
    StructField("volume", DoubleType(), True),        # 29318.80
    StructField("quote_volume", DoubleType(), True),  # 2635333520.17
    StructField("number_trades", IntegerType(), True),# 6468949
    StructField("price_change", DoubleType(), True),  # 6263.46
    StructField("price_change_percent", DoubleType(), True),  # 7.236
    StructField("timestamp", StringType(), True)      # ISO format
])
```

### 6.4. Read Stream từ Kafka

```python
# ============================================
# STEP 1: ĐỌC TỪ KAFKA
# ============================================
kafkaDF = spark.readStream \
    # Format: Kafka source
    .format("kafka") \
    
    # Địa chỉ Kafka broker
    .option("kafka.bootstrap.servers", "localhost:9092") \
    
    # Subscribe topic "crypto-prices"
    .option("subscribe", "crypto-prices") \
    
    # Bắt đầu từ đâu:
    # - "earliest": Đọc từ đầu
    # - "latest": Chỉ đọc messages mới
    .option("startingOffsets", "earliest") \
    
    # Không fail nếu mất data (cho demo)
    .option("failOnDataLoss", "false") \
    .load()

# Kafka DataFrame có các cột:
# key, value, topic, partition, offset, timestamp, timestampType
# - key: Có thể null
# - value: JSON message (bytes)
# - topic: "crypto-prices"
# - partition: 0 hoặc 1
# - offset: Vị trí trong partition
# - timestamp: Kafka timestamp
```

### 6.5. Parse JSON

```python
# ============================================
# STEP 2: PARSE JSON
# ============================================
parsedDF = kafkaDF.select(
    # Parse JSON từ cột "value"
    # value là bytes, cast sang string trước
    from_json(
        col("value").cast("string"),  # bytes → string
        message_schema                 # Schema đã định nghĩa
    ).alias("data"),
    
    # Giữ lại Kafka timestamp
    col("timestamp").alias("kafka_timestamp")
)

# Expand nested columns
# data.symbol, data.price, etc → symbol, price, etc
.select("data.*", "kafka_timestamp")
```

### 6.6. Data Transformation

```python
# ============================================
# STEP 3: TRANSFORM DATA
# ============================================
streamDF = parsedDF \
    # Convert event_time (milliseconds) → timestamp
    .withColumn(
        "event_timestamp",
        (col("event_time") / 1000).cast("timestamp")
    ) \
    
    # Extract date
    .withColumn("date", to_date(col("event_timestamp"))) \
    
    # Extract hour
    .withColumn("hour", hour(col("event_timestamp")))

# Ví dụ:
# event_time = 1764730800011 (ms)
# event_timestamp = 2025-12-03 10:00:00
# date = 2025-12-03
# hour = 10
```

---

## 7. Watermark và Window

### 7.1. Watermark là gì?

```
WATERMARK: Xử lý late data (dữ liệu đến muộn)

Vấn đề:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  10:00:00 - Message 1 arrive (event_time: 10:00:00) ✓              │
│  10:00:01 - Message 2 arrive (event_time: 10:00:01) ✓              │
│  10:00:02 - Message 3 arrive (event_time: 09:59:30) ← LATE!        │
│                                                                     │
│  Message 3 có event_time trong quá khứ (09:59:30)                  │
│  Điều này xảy ra do network delay, retry, v.v.                     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

Giải pháp: Watermark

Watermark = Current max event_time - Threshold

Ví dụ với threshold 1 hour:
  Current max event_time = 10:00:00
  Watermark = 10:00:00 - 1 hour = 09:00:00
  
  Message với event_time > 09:00:00 → Được xử lý ✓
  Message với event_time ≤ 09:00:00 → Bị drop ✗
```

### 7.2. Code Watermark

```python
# ============================================
# STEP 4: WATERMARKING
# ============================================
watermarkedDF = streamDF.withWatermark("event_timestamp", "1 hour")

# Giải thích:
# - "event_timestamp": Cột chứa event time
# - "1 hour": Threshold - chấp nhận late data đến 1 giờ

# Ví dụ:
# Current event_timestamp = 10:30:00
# Watermark = 09:30:00
# 
# Message với event_timestamp = 10:00:00 → OK (> 09:30:00)
# Message với event_timestamp = 09:00:00 → DROP (≤ 09:30:00)
```

### 7.3. Window Aggregation

```python
# ============================================
# STEP 5: WINDOW AGGREGATION - DAILY
# ============================================
dailyDF = watermarkedDF \
    .groupBy(
        # Window: Nhóm theo khoảng thời gian
        # window(timestamp_column, window_duration)
        window(col("event_timestamp"), "1 day"),
        
        # Thêm groupBy symbol
        col("symbol")
    ) \
    .agg(
        # OHLC (Open, High, Low, Close)
        first("open").alias("daily_open"),
        max("high").alias("daily_high"),
        min("low").alias("daily_low"),
        last("price").alias("daily_close"),
        
        # Volume
        sum("volume").alias("daily_volume"),
        sum("quote_volume").alias("daily_quote_volume"),
        
        # Trades
        sum("number_trades").alias("total_trades"),
        
        # Statistics
        count("*").alias("tick_count"),      # Số messages
        avg("price").alias("avg_price")      # Giá trung bình
    )

# Window tạo ra cột "window" có 2 fields:
# - window.start: Bắt đầu window
# - window.end: Kết thúc window

# Ví dụ:
# window.start = 2025-12-03 00:00:00
# window.end = 2025-12-04 00:00:00
```

### 7.4. Minh họa Window

```
Window 1 day với data stream:

Timeline:
──────────────────────────────────────────────────────────────────────►
  00:00        06:00        12:00        18:00        00:00
  │             │             │             │             │
  │◄────────── Window Day 1 (Dec 3) ──────────────────►│
  │             │             │             │             │
  │ msg1  msg2  │   msg3      │  msg4  msg5  │    msg6    │
  │             │             │             │             │
  └─────────────┴─────────────┴─────────────┴─────────────┘

Tất cả messages trong Day 1 được aggregate:
- daily_open = first(open) = msg1.open
- daily_high = max(high) = max(msg1.high, msg2.high, ..., msg6.high)
- daily_low = min(low)
- daily_close = last(price) = msg6.price
- daily_volume = sum(volume)
```

---

## 8. Output và Checkpoint

### 8.1. Write Streams

```python
# ============================================
# QUERY 1: Daily data → Parquet
# ============================================
daily_query = dailyDF.writeStream \
    # Output mode:
    # - "append": Chỉ ghi records mới
    # - "complete": Ghi lại toàn bộ result
    # - "update": Ghi records thay đổi
    .outputMode("append") \
    
    # Format output
    .format("parquet") \
    
    # Đường dẫn output
    .option("path", "streaming_output_spark/daily") \
    
    # Checkpoint location (fault tolerance)
    .option("checkpointLocation", "checkpoint_spark/daily") \
    
    # Partition theo symbol
    .partitionBy("symbol") \
    
    # Trigger: Chạy mỗi 10 giây
    # Các options:
    # - processingTime="10 seconds": Chạy mỗi 10s
    # - continuous="1 second": True streaming (experimental)
    # - once=True: Chạy 1 lần rồi stop
    .trigger(processingTime="10 seconds") \
    .start()

# ============================================
# QUERY 2: Raw data → Console (monitoring)
# ============================================
console_query = streamDF \
    .select("symbol", "price", "volume", "price_change_percent", "event_timestamp") \
    .writeStream \
    .outputMode("append") \
    .format("console")          # In ra console
    .option("truncate", "false")
    .option("numRows", "10")    # Max 10 rows
    .trigger(processingTime="30 seconds")
    .start()

# ============================================
# QUERY 4: Daily stats → Memory (for queries)
# ============================================
stats_query = dailyDF.writeStream \
    .outputMode("complete")     # Ghi toàn bộ
    .format("memory")           # Lưu trong memory
    .queryName("crypto_daily_stats")  # Tên table
    .trigger(processingTime="10 seconds")
    .start()

# Có thể query:
# spark.sql("SELECT * FROM crypto_daily_stats").show()
```

### 8.2. Checkpoint - Fault Tolerance

```
Checkpoint là gì?
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Checkpoint lưu trạng thái của streaming query:                    │
│                                                                     │
│  1. OFFSET: Đã đọc đến offset nào trong Kafka                      │
│  2. STATE: Trạng thái aggregation (window states)                  │
│  3. METADATA: Query information                                     │
│                                                                     │
│  Tại sao cần?                                                       │
│  - Nếu Spark crash, restart sẽ đọc checkpoint                      │
│  - Tiếp tục từ chỗ dừng, không xử lý lại từ đầu                   │
│  - Không mất data, không duplicate                                 │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

Cấu trúc checkpoint folder:
checkpoint_spark/
└── daily/
    ├── offsets/           # Kafka offsets đã đọc
    │   ├── 0
    │   ├── 1
    │   └── ...
    ├── commits/           # Batches đã commit
    ├── state/             # Aggregation state
    └── metadata           # Query metadata
```

### 8.3. Output Structure

```
streaming_output_spark/
└── daily/
    ├── symbol=BTCUSDT/
    │   ├── part-00000-xxx.snappy.parquet
    │   └── ...
    └── symbol=ETHUSDT/
        └── ...

Lưu ý:
- Append mode: Mỗi trigger tạo file mới
- Có thể có nhiều small files
- Cần compact định kỳ trong production
```

---

## 9. Câu hỏi thường gặp

### Q1: Tại sao dùng Kafka thay vì gửi thẳng vào Spark?

```
A: Kafka mang lại nhiều lợi ích:

1. DECOUPLING:
   - Producer và Consumer độc lập
   - Có thể restart một bên mà không ảnh hưởng bên kia

2. BUFFERING:
   - Nếu Spark chậm, Kafka giữ messages
   - Không mất data

3. REPLAY:
   - Có thể đọc lại messages từ đầu
   - Debug dễ dàng

4. MULTI-CONSUMER:
   - Nhiều consumers đọc cùng topic
   - Có thể thêm consumer analytics, monitoring, etc

5. DURABILITY:
   - Messages lưu trên disk
   - Không mất khi crash
```

### Q2: Tại sao dùng Structured Streaming thay vì DStreams?

```
A: Structured Streaming là API mới và tốt hơn:

DStreams (cũ):
├── RDD-based
├── Khó debug
├── Không có exactly-once semantics
└── Deprecated

Structured Streaming (mới):
├── DataFrame/SQL API
├── Dễ sử dụng hơn
├── Exactly-once semantics
├── Tích hợp với Spark SQL
├── Watermark, Window built-in
└── Production-ready
```

### Q3: Watermark 1 hour có quá dài không?

```
A: Tùy use case:

1 hour watermark nghĩa là:
- Chấp nhận late data đến 1 giờ
- State giữ trong memory 1 giờ
- Tradeoff: Memory vs Tolerance

Trong project:
- 1 hour là conservative
- Binance API có thể delay vài giây → 1 hour quá dư
- Có thể giảm xuống 5-10 phút cho production

Các giá trị phổ biến:
- IoT sensors: 1-5 phút
- Mobile app events: 5-15 phút
- Batch-like streaming: 1-24 giờ
```

### Q4: Trigger 10 seconds có ý nghĩa gì?

```
A: Trigger xác định tần suất processing:

trigger(processingTime="10 seconds"):
├── Spark đợi 10 giây
├── Thu thập messages trong 10s đó
├── Xử lý micro-batch
├── Output kết quả
└── Lặp lại

Micro-batch vs True streaming:
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Micro-batch (10s trigger):                                        │
│  ────┬────────┬────────┬────────┬────────►                         │
│      │ batch1 │ batch2 │ batch3 │                                  │
│     10s      20s      30s      40s                                 │
│                                                                     │
│  True streaming (continuous):                                       │
│  ─────msg─msg─msg─msg─msg─msg─msg─────►                            │
│  Xử lý ngay từng message                                           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

10 seconds là good default:
- Không quá chậm (có kết quả mỗi 10s)
- Không quá nhanh (overhead thấp)
- Phù hợp cho demo
```

### Q5: Tại sao cần cả Parquet output và Memory table?

```
A: Hai mục đích khác nhau:

Parquet output:
├── Persistent storage
├── Có thể đọc lại sau khi Spark stop
├── Dùng cho Serving Layer merge

Memory table:
├── Real-time monitoring
├── Query ngay lập tức
├── Mất khi Spark stop

Trong project:
- Parquet: Serving Layer đọc để merge với Batch Layer
- Memory: Demo realtime, query trực tiếp
```

---

## 📚 Tài liệu tiếp theo

Sau khi hiểu Speed Layer, tiếp tục với:

**WEEK6_04_SERVING_LAYER.md** - Giải thích merge + forecast

---

*Tạo bởi: Big Data Project - Week 6 Documentation*
*Cập nhật: 03/12/2025*
