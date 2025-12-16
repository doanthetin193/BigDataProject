# Giải thích chi tiết: docker-compose.yml

**File:** `week6_streaming/docker-compose.yml`  
**Chức năng:** Infrastructure setup - Kafka + Zookeeper cho Speed Layer  
**Tác giả:** Đoàn Thế Tín  
**Ngày:** Week 6 - Lambda Architecture

---

## 📋 Mục lục
1. [Docker Compose Overview](#1-docker-compose-overview)
2. [Version và Services](#2-version-và-services)
3. [Zookeeper Service](#3-zookeeper-service)
4. [Kafka Service](#4-kafka-service)
5. [Networks](#5-networks)
6. [Volumes](#6-volumes)
7. [Cách Sử Dụng](#7-cách-sử-dụng)
8. [Tóm tắt](#tóm-tắt-tổng-quan)

---

## 1. Docker Compose Overview

### Docker Compose là gì?
**Docker Compose** là tool để define và run **multi-container Docker applications**.

**Tại sao dùng Docker Compose?**
- **Single file:** Define tất cả services trong 1 file YAML
- **One command:** Start/stop tất cả containers: `docker-compose up -d`
- **Networking:** Auto tạo network cho containers communicate
- **Dependencies:** Manage startup order (`depends_on`)

**Alternative (không dùng Docker Compose):**
```bash
# Phải chạy 2 commands riêng:
docker run -d --name zookeeper confluentinc/cp-zookeeper:7.5.0
docker run -d --name kafka --link zookeeper confluentinc/cp-kafka:7.5.0
# Rất phức tạp với nhiều options!
```

**Với Docker Compose:**
```bash
# Chỉ 1 command:
docker-compose up -d
```

---

## 2. Version và Services

### Dòng 1: Version
```yaml
version: '3.8'
```
**Giải thích:**
- **Docker Compose file format version:** 3.8
- **Supported features:**
  - Services, Networks, Volumes
  - Healthchecks
  - Depends_on
  - Named volumes
- **Compatibility:** Docker Engine 19.03.0+

**Version history:**
| Version | Released | Key Features |
|---------|----------|--------------|
| 3.0 | 2016 | Basic services |
| 3.4 | 2017 | Long syntax configs |
| 3.8 | 2019 | **Current stable** |

---

### Dòng 3: Services
```yaml
services:
```
**Giải thích:**
- **Services:** Danh sách containers cần chạy
- File này có **2 services:**
  1. `zookeeper`: Coordination service
  2. `kafka`: Message broker

---

## 3. Zookeeper Service

### Dòng 4-6: Service Definition
```yaml
  # Zookeeper - Quản lý Kafka cluster
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
```
**Giải thích:**

#### Service name: `zookeeper`
- Tên service (dùng trong `depends_on`, DNS)
- Containers khác có thể connect bằng hostname `zookeeper`

#### `image: confluentinc/cp-zookeeper:7.5.0`
- **Docker image:** Confluent Platform Zookeeper
- **Version:** 7.5.0 (latest stable)
- **Source:** Docker Hub (auto download nếu chưa có)

---

### Zookeeper là gì?

**Zookeeper** = **Coordination service** cho distributed systems.

**Vai trò trong Kafka:**
- **Cluster management:** Quản lý Kafka brokers
- **Leader election:** Chọn leader cho mỗi partition
- **Configuration management:** Lưu configs, metadata
- **Synchronization:** Đồng bộ state giữa brokers

**Analogy:**
- Kafka = Workers (xử lý messages)
- Zookeeper = Manager (quản lý workers)

**Tại sao cần Zookeeper?**
- Kafka **không thể chạy standalone** (cần Zookeeper)
- Zookeeper track:
  - Broker nào đang online?
  - Partition nào ở broker nào?
  - Consumer group offsets (legacy)

**Kafka future:**
- **KRaft mode (Kafka 3.0+):** Không cần Zookeeper
- **Hiện tại:** Vẫn recommend dùng Zookeeper (stable)

---

### Dòng 7: Container Name
```yaml
    container_name: zookeeper
```
**Giải thích:**
- **Container name:** `zookeeper` (khi chạy `docker ps`)
- **Default behavior:** Docker Compose auto generate name
  - Format: `{project}_{service}_{index}`
  - Ví dụ: `week6_streaming_zookeeper_1`
- **Set cố định:** Dễ reference (`docker logs zookeeper`)

---

### Dòng 8-11: Environment Variables
```yaml
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
```
**Giải thích:**

#### `ZOOKEEPER_CLIENT_PORT: 2181`
- **Client port:** Port để Kafka connect vào Zookeeper
- **Default:** 2181 (standard Zookeeper port)
- **Usage:** Kafka connect via `zookeeper:2181`

#### `ZOOKEEPER_TICK_TIME: 2000`
- **Tick time:** 2000 milliseconds (2 giây)
- **Định nghĩa:** Basic time unit của Zookeeper
- **Dùng để:**
  - Session timeout: 2 × tick_time = 4s
  - Heartbeat interval: 1 × tick_time = 2s
- **Tại sao 2000ms?**
  - Cân bằng giữa responsiveness và stability
  - Quá nhỏ → False positives (tưởng node chết)
  - Quá lớn → Slow failure detection

---

### Dòng 12-13: Port Mapping
```yaml
    ports:
      - "2181:2181"
```
**Giải thích:**

#### Format: `"HOST:CONTAINER"`
- **Host port:** 2181 (máy local)
- **Container port:** 2181 (inside Docker)
- **Mapping:** localhost:2181 → container:2181

#### Tại sao expose port?
- **Producer/Consumer từ host:** Connect qua `localhost:2181`
- **Monitoring tools:** Check Zookeeper health
- **Debugging:** Manual queries với zkCli

**Ví dụ:**
```bash
# Từ host machine (outside Docker):
nc -zv localhost 2181
# Connection to localhost 2181 port [tcp/*] succeeded!
```

---

### Dòng 14-15: Network
```yaml
    networks:
      - crypto-network
```
**Giải thích:**
- Join network `crypto-network`
- Kafka cũng join network này → Communicate được

---

## 4. Kafka Service

### Dòng 17-19: Service Definition
```yaml
  # Kafka - Message broker
  kafka:
    image: confluentinc/cp-kafka:7.5.0
```
**Giải thích:**

#### Service name: `kafka`
- Hostname: `kafka` (DNS trong Docker network)

#### `image: confluentinc/cp-kafka:7.5.0`
- **Confluent Platform Kafka:** Enterprise distribution
- **Version:** 7.5.0 (match với Zookeeper)
- **Features:**
  - Apache Kafka core
  - Additional tools (Schema Registry, etc.)
  - Production-ready configs

---

### Kafka là gì?

**Kafka** = **Distributed streaming platform** / **Message broker**.

**Core concepts:**
- **Producers:** Send messages
- **Consumers:** Read messages
- **Topics:** Categories (ví dụ: `crypto-prices`)
- **Partitions:** Parallel processing
- **Brokers:** Kafka servers

**Use cases:**
- **Real-time streaming:** Stock prices, IoT sensors
- **Event sourcing:** User activity logs
- **Message queue:** Decouple microservices
- **Log aggregation:** Centralized logging

**Tại sao dùng Kafka?**
- **High throughput:** Millions messages/second
- **Scalable:** Add brokers horizontally
- **Durable:** Persist messages to disk
- **Fault-tolerant:** Replication, no data loss

---

### Dòng 20: Container Name
```yaml
    container_name: kafka
```
**Giải thích:** Container name `kafka` (thay vì auto-generated).

---

### Dòng 21-22: Dependencies
```yaml
    depends_on:
      - zookeeper
```
**Giải thích:**

#### `depends_on: [zookeeper]`
- **Startup order:** Start Zookeeper trước Kafka
- **Tại sao?** Kafka cần connect Zookeeper lúc startup

#### Limitation
- `depends_on` chỉ đảm bảo **start order**
- **KHÔNG đợi** Zookeeper ready (healthy)
- **Risk:** Kafka có thể start trước khi Zookeeper ready → Fail

#### Solution (production)
```yaml
depends_on:
  zookeeper:
    condition: service_healthy
```
- Cần define `healthcheck` cho Zookeeper

---

### Dòng 23-25: Port Mapping
```yaml
    ports:
      - "9092:9092"
      - "9093:9093"
```
**Giải thích:**

#### Port 9092
- **PLAINTEXT listener:** External connections (từ host)
- **Usage:** Producer, Consumer từ host connect qua `localhost:9092`
- **Example:**
  ```python
  producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
  ```

#### Port 9093
- **PLAINTEXT_INTERNAL listener:** Internal connections (giữa containers)
- **Usage:** Kafka consumers trong Docker network connect qua `kafka:9093`
- **Example:** Nếu có Spark container cùng network

#### Tại sao 2 ports?
- **9092:** Host → Kafka (external)
- **9093:** Container → Kafka (internal)
- **Lý do:** Docker networking isolation

---

### Dòng 26-35: Environment Variables

#### Dòng 27: Broker ID
```yaml
    environment:
      KAFKA_BROKER_ID: 1
```
**Giải thích:**
- **Broker ID:** Unique identifier cho Kafka broker
- **Value:** 1 (single broker)
- **Multi-broker cluster:** Mỗi broker cần ID khác nhau (1, 2, 3, ...)

---

#### Dòng 28: Zookeeper Connect
```yaml
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
```
**Giải thích:**
- **Zookeeper address:** `zookeeper:2181`
  - `zookeeper`: Hostname (Docker DNS)
  - `2181`: Zookeeper client port
- **Tại sao dùng hostname?**
  - Containers trong cùng network resolve DNS
  - `zookeeper` → IP của Zookeeper container

---

#### Dòng 29: Advertised Listeners
```yaml
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:9093
```
**Giải thích:**

**Advertised listeners** = Addresses mà Kafka **quảng cáo** cho clients.

**Format:** `LISTENER_NAME://HOST:PORT`

#### `PLAINTEXT://localhost:9092`
- **Listener name:** PLAINTEXT
- **Address:** localhost:9092
- **Purpose:** External clients (Producer, Consumer từ host)
- **Why localhost?**
  - Client ở host machine
  - Connect via `localhost:9092`

#### `PLAINTEXT_INTERNAL://kafka:9093`
- **Listener name:** PLAINTEXT_INTERNAL
- **Address:** kafka:9093
- **Purpose:** Internal clients (containers trong Docker network)
- **Why kafka?**
  - Client ở Docker network
  - Connect via `kafka:9093`

**Flow:**
```
Producer (host) → localhost:9092 → Kafka container
  Kafka advertises: "Connect to localhost:9092"

Consumer (Docker) → kafka:9093 → Kafka container
  Kafka advertises: "Connect to kafka:9093"
```

---

#### Dòng 30: Listener Security Protocol Map
```yaml
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
```
**Giải thích:**

**Security protocol map** = Mapping từ listener name → protocol.

#### `PLAINTEXT:PLAINTEXT`
- Listener `PLAINTEXT` dùng protocol `PLAINTEXT` (no encryption)

#### `PLAINTEXT_INTERNAL:PLAINTEXT`
- Listener `PLAINTEXT_INTERNAL` dùng protocol `PLAINTEXT`

**Protocols available:**
- `PLAINTEXT`: No encryption, no auth (demo only)
- `SSL`: TLS encryption
- `SASL_PLAINTEXT`: Authentication, no encryption
- `SASL_SSL`: Authentication + TLS

**Production:**
```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: SSL:SSL,SASL_SSL:SASL_SSL
```

---

#### Dòng 31: Offsets Topic Replication Factor
```yaml
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```
**Giải thích:**

#### `__consumer_offsets` topic
- **Internal topic:** Lưu consumer group offsets
- **Purpose:** Track consumer progress (đã đọc đến offset nào)

#### `REPLICATION_FACTOR: 1`
- **Replication:** 1 copy (no backup)
- **Tại sao 1?** Single broker (no cluster)
- **Production (3 brokers):** `REPLICATION_FACTOR: 3`
  - 3 copies → Fault tolerance
  - 1 broker chết → Vẫn còn 2 copies

---

#### Dòng 32: Transaction State Log Min ISR
```yaml
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```
**Giải thích:**

#### `__transaction_state` topic
- **Internal topic:** Lưu transaction states
- **Purpose:** Exactly-once semantics

#### `MIN_ISR: 1`
- **Min In-Sync Replicas:** Tối thiểu 1 replica in-sync
- **Single broker:** Must be 1
- **Production:** `MIN_ISR: 2` (with 3 replicas)

---

#### Dòng 33: Transaction State Log Replication Factor
```yaml
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```
**Giải thích:**
- **Replication factor:** 1 copy
- **Reason:** Single broker

---

#### Dòng 34: Auto Create Topics
```yaml
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```
**Giải thích:**

#### `AUTO_CREATE_TOPICS_ENABLE: true`
- **Behavior:** Tự động tạo topic khi producer send message
- **Example:**
  ```python
  # Topic "crypto-prices" chưa tồn tại
  producer.send("crypto-prices", value=data)
  # → Kafka auto create topic "crypto-prices"
  ```

#### Default settings (auto-created topic)
- **Partitions:** 1
- **Replication factor:** 1
- **Retention:** 7 days

#### Production recommendation
```yaml
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
```
- **Reason:** Explicit control (partitions, retention, ...)
- **Manually create:**
  ```bash
  kafka-topics --create \
    --topic crypto-prices \
    --partitions 3 \
    --replication-factor 2
  ```

#### Trong project này: `true`
- **Lý do:** Demo đơn giản, không cần manual setup

---

### Dòng 35-36: Network
```yaml
    networks:
      - crypto-network
```
**Giải thích:**
- Join `crypto-network` (cùng network với Zookeeper)

---

### Dòng 37-38: Volumes
```yaml
    volumes:
      - kafka-data:/var/lib/kafka/data
```
**Giải thích:**

#### Volume mapping
- **Named volume:** `kafka-data`
- **Mount point:** `/var/lib/kafka/data` (inside container)

#### Tại sao cần volume?
- **Persist data:** Messages không mất khi restart container
- **Without volume:** Data mất khi container removed

#### What's stored?
- **Topic data:** Messages
- **Partition logs:** Segment files
- **Indexes:** Offset indexes

#### Example
```bash
# Container removed
docker-compose down

# Data vẫn còn trong volume
docker volume ls
# kafka-data

# Restart container
docker-compose up -d
# → Data restored from volume
```

---

## 5. Networks

### Dòng 40-42: Network Definition
```yaml
networks:
  crypto-network:
    driver: bridge
```
**Giải thích:**

#### Network name: `crypto-network`
- Custom network cho project

#### `driver: bridge`
- **Bridge network:** Default Docker network type
- **Features:**
  - Containers trong cùng network communicate
  - DNS resolution (hostname → IP)
  - Isolated từ host network

**Network topology:**
```
Host Network (192.168.1.x)
  ↓
Docker Bridge Network (172.18.0.x)
  ├── zookeeper (172.18.0.2:2181)
  └── kafka (172.18.0.3:9092, 9093)
```

**Communication:**
```
Kafka → Zookeeper:
  kafka container connects to zookeeper:2181
  DNS resolves zookeeper → 172.18.0.2

Producer (host) → Kafka:
  localhost:9092 → port mapping → kafka:9092
```

---

## 6. Volumes

### Dòng 44-45: Volume Definition
```yaml
volumes:
  kafka-data:
```
**Giải thích:**

#### Named volume: `kafka-data`
- **Managed by Docker:** Location auto-determined
- **Default path (Linux):** `/var/lib/docker/volumes/kafka-data/_data`
- **Default path (Windows):** `\\wsl$\docker-desktop-data\version-pack-data\community\docker\volumes\kafka-data\_data`

#### Tại sao dùng named volume?
- **Portable:** Không hardcode path
- **Managed:** Docker handles lifecycle
- **Backup friendly:** Easy to backup/restore

**Alternative: bind mount**
```yaml
volumes:
  - ./kafka-data:/var/lib/kafka/data
```
- **Bind mount:** Specific host path
- **Use case:** Need direct access to data

---

## 7. Cách Sử Dụng

### **Start Services**
```bash
cd week6_streaming
docker-compose up -d
```
**Output:**
```
Creating network "crypto-network" with driver "bridge"
Creating volume "kafka-data" with default driver
Creating zookeeper ... done
Creating kafka     ... done
```

**Flags:**
- `-d`: Detached mode (background)
- Without `-d`: See logs in foreground

---

### **Check Status**
```bash
docker-compose ps
```
**Output:**
```
   Name                 Command            State                    Ports
---------------------------------------------------------------------------------
kafka        /etc/confluent/docker/run   Up      0.0.0.0:9092->9092/tcp,
                                                  0.0.0.0:9093->9093/tcp
zookeeper    /etc/confluent/docker/run   Up      0.0.0.0:2181->2181/tcp
```

---

### **View Logs**
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f kafka
docker-compose logs -f zookeeper
```

---

### **Stop Services**
```bash
docker-compose stop
```
- **Stop containers** (data vẫn còn trong volumes)

---

### **Remove Containers**
```bash
docker-compose down
```
- **Stop và remove** containers
- **Keep volumes** (data không mất)

---

### **Remove Everything (including volumes)**
```bash
docker-compose down -v
```
- Remove containers + networks + volumes
- **Warning:** Data sẽ mất!

---

### **Restart Services**
```bash
docker-compose restart
```

---

### **Exec into Container**
```bash
# Kafka container
docker exec -it kafka bash

# Zookeeper container
docker exec -it zookeeper bash
```

---

### **Create Topic Manually**
```bash
docker exec -it kafka kafka-topics \
  --create \
  --topic crypto-prices \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

---

### **List Topics**
```bash
docker exec -it kafka kafka-topics \
  --list \
  --bootstrap-server localhost:9092
```

---

### **Consume Messages**
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto-prices \
  --from-beginning
```

---

## 8. Tóm tắt Tổng quan

## 🎯 Mục đích File
File `docker-compose.yml` define infrastructure cho Speed Layer - Kafka message broker + Zookeeper coordination service chạy trong Docker containers.

---

## 📊 Architecture

### **Services (2)**
```
┌─────────────────────────────────────────────────┐
│              crypto-network (bridge)             │
│                                                   │
│  ┌─────────────────────┐  ┌──────────────────┐  │
│  │    Zookeeper        │  │      Kafka       │  │
│  │  Port: 2181         │←─│  Ports: 9092,    │  │
│  │  Image: 7.5.0       │  │         9093     │  │
│  │  Role: Coordinator  │  │  Image: 7.5.0    │  │
│  └─────────────────────┘  │  Role: Broker    │  │
│                            └──────────────────┘  │
│                                   │              │
│                            ┌──────▼──────────┐   │
│                            │  kafka-data     │   │
│                            │  (volume)       │   │
│                            └─────────────────┘   │
└─────────────────────────────────────────────────┘
         ▲                           ▲
         │ :2181                     │ :9092
         │                           │
    ┌────┴────────────────────────┬──┴──────┐
    │       Host Machine          │         │
    │  websocket_producer.py      │  Spark  │
    └─────────────────────────────┴─────────┘
```

---

## 🔑 Key Configurations

### **Zookeeper**
| Config | Value | Purpose |
|--------|-------|---------|
| `CLIENT_PORT` | 2181 | Kafka connection port |
| `TICK_TIME` | 2000ms | Heartbeat interval |
| Port mapping | 2181:2181 | Host access |

### **Kafka**
| Config | Value | Purpose |
|--------|-------|---------|
| `BROKER_ID` | 1 | Unique broker ID |
| `ZOOKEEPER_CONNECT` | zookeeper:2181 | Zookeeper address |
| `ADVERTISED_LISTENERS` | localhost:9092, kafka:9093 | Client addresses |
| `AUTO_CREATE_TOPICS` | true | Auto create topics |
| Port mapping | 9092:9092, 9093:9093 | External/Internal access |
| Volume | kafka-data | Data persistence |

---

## 💡 Design Decisions

### **1. Single Broker Setup**
```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```
- **Reason:** Demo/Development environment
- **Production:** 3+ brokers with replication factor 3

### **2. Auto Create Topics**
```yaml
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```
- **Reason:** Simplicity (no manual setup)
- **Production:** `false` (explicit control)

### **3. PLAINTEXT Security**
```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
```
- **Reason:** Local development
- **Production:** SSL/SASL_SSL

### **4. Named Volume**
```yaml
volumes:
  - kafka-data:/var/lib/kafka/data
```
- **Reason:** Data persistence across restarts
- **Alternative:** Bind mount for direct access

---

## 🚀 Common Commands

```bash
# Start
docker-compose up -d

# Status
docker-compose ps

# Logs
docker-compose logs -f kafka

# Stop
docker-compose stop

# Remove (keep data)
docker-compose down

# Remove (delete data)
docker-compose down -v

# Restart
docker-compose restart
```

---

## 🔧 Troubleshooting

### **1. Kafka Not Starting**
**Error:** `Connection to Zookeeper failed`  
**Solution:**
```bash
# Check Zookeeper healthy
docker logs zookeeper

# Restart both
docker-compose restart
```

---

### **2. Port Already in Use**
**Error:** `Bind for 0.0.0.0:9092 failed: port is already allocated`  
**Solution:**
```bash
# Check process using port
netstat -ano | findstr :9092

# Kill process or change port in docker-compose.yml
ports:
  - "9094:9092"  # Map to different host port
```

---

### **3. Cannot Connect from Host**
**Error:** `Connection refused: localhost:9092`  
**Solution:**
```bash
# Check advertised listeners
docker exec kafka cat /etc/kafka/server.properties | grep advertised

# Should see: advertised.listeners=PLAINTEXT://localhost:9092

# Check port mapping
docker ps | grep kafka
# Should see: 0.0.0.0:9092->9092/tcp
```

---

### **4. Data Lost After Restart**
**Problem:** Messages disappear  
**Solution:**
```bash
# Check volume exists
docker volume ls | grep kafka-data

# If missing, recreate with volume
docker-compose down
docker-compose up -d

# Verify volume mounted
docker inspect kafka | grep Mounts -A 20
```

---

### **5. Zookeeper Connection Timeout**
**Error:** `Timed out waiting for connection while in state: CONNECTING`  
**Solution:**
```bash
# Increase tick time
ZOOKEEPER_TICK_TIME: 4000

# Or restart services
docker-compose restart zookeeper
sleep 10
docker-compose restart kafka
```

---

## 📈 Performance Tuning

### **Development (Current)**
```yaml
# Single broker, minimal resources
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

### **Production**
```yaml
# Multi-broker cluster
version: '3.8'
services:
  kafka1:
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
  kafka2:
    environment:
      KAFKA_BROKER_ID: 2
  kafka3:
    environment:
      KAFKA_BROKER_ID: 3
```

---

## 🎓 Key Technologies

- **Docker Compose:** Multi-container orchestration
- **Confluent Platform:** Enterprise Kafka distribution
- **Zookeeper:** Distributed coordination
- **Kafka:** Distributed streaming platform
- **Bridge Network:** Container networking
- **Named Volumes:** Data persistence

---

## 🔗 Integration

### **Project Flow**
```
1. Start infrastructure:
   docker-compose up -d
   
2. Producer sends data:
   python websocket_producer.py
   ↓ (Kafka topic: crypto-prices)
   
3. Consumer reads data:
   python kafka_batch_reader.py
   python spark_streaming_consumer.py
   
4. Merge layers:
   python week6_merge.py
```

---

## 📊 Comparison: Dev vs Prod

| Aspect | Development (File này) | Production |
|--------|----------------------|------------|
| **Brokers** | 1 | 3+ |
| **Zookeeper** | 1 | 3+ (ensemble) |
| **Replication** | 1 | 3 |
| **Security** | PLAINTEXT | SSL/SASL |
| **Auto-create** | ✅ Enabled | ❌ Disabled |
| **Resources** | Minimal | High (tuned) |
| **Monitoring** | Logs only | Prometheus, Grafana |
| **Backup** | Manual | Automated |

---

## ⚠️ Important Notes

### **1. Data Persistence**
- Volume `kafka-data` persist data
- `docker-compose down` **giữ** data
- `docker-compose down -v` **xóa** data

### **2. Network Isolation**
- Containers communicate via `crypto-network`
- Host access via port mapping (9092, 2181)

### **3. Startup Order**
- Zookeeper starts first
- Kafka starts after (depends_on)
- Wait ~10s for full initialization

### **4. Production Readiness**
- File này cho **development/demo**
- Production cần:
  - Multi-broker setup
  - SSL/SASL security
  - Resource limits
  - Monitoring
  - Backup strategy

---

**Tác giả:** Đoàn Thế Tín  
**MSSV:** 4551190056  
**File:** `week6_streaming/docker-compose.yml`  
**Lines:** 45 dòng YAML  
**Mục đích:** Infrastructure setup cho Speed Layer - Kafka + Zookeeper trong Docker

---
