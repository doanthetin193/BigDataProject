"""
websocket_producer.py - Thu thập dữ liệu real-time từ Binance và push vào Kafka

STRUCTURED STREAMING approach:
- Continuous polling (mỗi 1 giây)
- Push vào Kafka message broker
- Spark sẽ consume từ Kafka
"""
import json
import logging
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'crypto-prices'

# Statistics
message_count = {'BTCUSDT': 0, 'ETHUSDT': 0}
start_time = time.time()

def create_kafka_producer():
    """Tạo Kafka Producer với retry logic"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            acks='all',  # Wait for all replicas
            compression_type='gzip',
            max_in_flight_requests_per_connection=1
        )
        logger.info(f"✓ Kafka Producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"✗ Failed to create Kafka Producer: {e}")
        raise

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

def main():
    """Khởi động continuous streaming vào Kafka"""
    
    print("=" * 70)
    print("BINANCE API → KAFKA PRODUCER")
    print("Continuous Streaming (1 second interval)")
    print("=" * 70)
    
    # Tạo Kafka Producer
    global producer
    producer = create_kafka_producer()
    
    symbols = ['BTCUSDT', 'ETHUSDT']
    logger.info(f"📡 Streaming symbols: {', '.join(symbols)}")
    
    print("\n✓ Producer connected")
    print("✓ Sending real-time data to Kafka topic: crypto-prices")
    print("\nPress Ctrl+C to stop\n")
    
    try:
        while True:
            for symbol in symbols:
                # Fetch data
                data = fetch_ticker_data(symbol)
                
                if data:
                    # Send to Kafka
                    future = producer.send(KAFKA_TOPIC, value=data)
                    future.get(timeout=10)
                    
                    # Update statistics
                    message_count[symbol] = message_count.get(symbol, 0) + 1
                    
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
            
            # Wait 1 second (continuous streaming)
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("\n⏹ Stopping producer...")
        producer.flush()
        producer.close()
        
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

if __name__ == "__main__":
    main()
