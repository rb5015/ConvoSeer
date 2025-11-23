# Kafka Setup and Testing Guide

This guide will help you set up and test Kafka locally for the ConvoSeer project.

## Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ with `kafka-python` package

### Installing kafka-python

```bash
# Option 1: Using pip with user flag (recommended)
pip3 install --user kafka-python

# Option 2: Using a virtual environment (best practice)
python3 -m venv venv
source venv/bin/activate
pip install kafka-python

# Option 3: If you need to override system protection (macOS)
pip3 install --break-system-packages kafka-python
```

## Quick Start

### 1. Start Kafka and Zookeeper

```bash
# Start only Kafka infrastructure (Zookeeper + Kafka)
docker compose up -d zookeeper kafka

# Check if containers are running
docker compose ps
```

You should see:
- `zookeeper` container running on port 2181
- `kafka` container running on port 9092

### 2. Wait for Kafka to be Ready

Kafka takes a few seconds to start. Check logs:

```bash
# Watch Kafka logs
docker compose logs -f kafka

# Or check if it's ready (look for "started" message)
docker compose logs kafka | grep -i "started"
```

### 3. Test Kafka Connection

Run the test script:

```bash
# Install test dependencies (if not already installed)
pip install kafka-python

# Run the test
python3 scripts/test_kafka.py

# Or test with custom broker address
python3 scripts/test_kafka.py --brokers localhost:9092 --topic calls.raw
```

Expected output:
```
============================================================
Kafka Connection Test
============================================================

🔍 Checking if topic 'calls.raw' exists...
   ✅ Topic 'calls.raw' exists

📤 Testing Producer...
   ✅ Message sent successfully!
   Topic: calls.raw
   Partition: 0
   Offset: 0

📥 Testing Consumer...
   ✅ Received message #1:
   ✅ Consumer working! Received 1 message(s)

============================================================
Test Summary
============================================================
Topic check:  ✅ PASS
Producer:     ✅ PASS
Consumer:     ✅ PASS

🎉 All tests passed! Kafka is working correctly.
```

## Manual Testing

### Using Kafka Console Tools (Inside Container)

```bash
# Produce a test message
docker compose exec kafka kafka-console-producer.sh \
  --bootstrap-server localhost:29092 \
  --topic calls.raw

# Then type a message and press Enter
# Example: {"test": "hello"}

# In another terminal, consume messages
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic calls.raw \
  --from-beginning
```

### Using Python Scripts

#### Simple Producer Test

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send('calls.raw', value={'test': 'message'})
producer.flush()
print("Message sent!")
```

#### Simple Consumer Test

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'calls.raw',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

for message in consumer:
    print(f"Received: {message.value}")
```

## Kafka UI (Web Interface)

The project includes Kafka UI for a visual interface:

```bash
# Start Kafka UI (requires Kafka to be running)
docker compose up -d kafka-ui

# Access at http://localhost:8085
```

In Kafka UI you can:
- View all topics and their messages
- Browse message content
- Monitor consumer groups
- View topic configurations

## Troubleshooting

### Kafka won't start

```bash
# Check logs
docker compose logs kafka

# Common issues:
# 1. Port 9092 already in use
#    Solution: Stop other Kafka instances or change port in docker-compose.yml

# 2. Zookeeper not ready
#    Solution: Wait a few seconds after starting Zookeeper before starting Kafka
```

### Connection refused errors

```bash
# Verify containers are running
docker compose ps

# Check if Kafka is listening
docker compose exec kafka netstat -tlnp | grep 9092

# Test connection from host
telnet localhost 9092
# Or
nc -zv localhost 9092
```

### Topic not found

Topics are auto-created by default (see `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true` in docker-compose.yml).

If you need to manually create a topic:

```bash
docker compose exec kafka kafka-topics.sh \
  --create \
  --bootstrap-server localhost:29092 \
  --topic calls.raw \
  --partitions 1 \
  --replication-factor 1
```

### List all topics

```bash
docker compose exec kafka kafka-topics.sh \
  --list \
  --bootstrap-server localhost:29092
```

### Describe a topic

```bash
docker compose exec kafka kafka-topics.sh \
  --describe \
  --bootstrap-server localhost:29092 \
  --topic calls.raw
```

### Delete a topic (if needed)

```bash
docker compose exec kafka kafka-topics.sh \
  --delete \
  --bootstrap-server localhost:29092 \
  --topic calls.raw
```

## Configuration

### Broker Addresses

- **From host machine**: `localhost:9092`
- **From Docker containers**: `kafka:29092`

This is configured in `docker-compose.yml`:
- `PLAINTEXT://localhost:9092` - External access
- `PLAINTEXT_INTERNAL://kafka:29092` - Internal Docker network

### Environment Variables

Set in `.env` file (see `config/env.template`):
```bash
KAFKA_BROKERS=kafka:29092  # For containers
# or
KAFKA_BROKERS=localhost:9092  # For host machine
```

## Next Steps

Once Kafka is working:

1. **Test the producer**: Run `python3 ingest/producer/producer.py --help`
2. **Start Spark Streaming**: See `streaming/README.md`
3. **Monitor with Kafka UI**: http://localhost:8085

## Clean Up

```bash
# Stop Kafka and Zookeeper
docker compose stop kafka zookeeper

# Remove containers and volumes (⚠️ deletes all data)
docker compose down -v
```

