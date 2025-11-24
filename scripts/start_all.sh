#!/bin/bash
# Start all required services for ConvoSeer

set -e

echo "🚀 Starting ConvoSeer Services"
echo "================================"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Start Kafka and Zookeeper
echo "📦 Starting Kafka infrastructure..."
cd "$(dirname "$0")/.."
docker compose up -d zookeeper kafka

echo ""
echo "⏳ Waiting for Kafka to be ready (15 seconds)..."
sleep 15

# Verify Kafka is running
if ! docker compose ps kafka | grep -q "Up"; then
    echo "❌ Kafka failed to start. Check logs with: docker compose logs kafka"
    exit 1
fi

echo "✅ Kafka is running!"
echo ""

# Check if topic exists, create if not
echo "📋 Checking Kafka topics..."
if ! docker compose exec -T kafka kafka-topics --list --bootstrap-server localhost:29092 2>/dev/null | grep -q "calls.raw"; then
    echo "Creating calls.raw topic..."
    docker compose exec -T kafka kafka-topics --create \
        --topic calls.raw \
        --bootstrap-server localhost:29092 \
        --partitions 1 \
        --replication-factor 1 2>/dev/null || true
fi

if ! docker compose exec -T kafka kafka-topics --list --bootstrap-server localhost:29092 2>/dev/null | grep -q "calls.enriched"; then
    echo "Creating calls.enriched topic..."
    docker compose exec -T kafka kafka-topics --create \
        --topic calls.enriched \
        --bootstrap-server localhost:29092 \
        --partitions 1 \
        --replication-factor 1 2>/dev/null || true
fi

echo "✅ Topics ready!"
echo ""

# Show status
echo "📊 Service Status:"
echo "=================="
docker compose ps zookeeper kafka | grep -E "NAME|zookeeper|kafka"
echo ""

echo "✅ All required services are running!"
echo ""
echo "📝 Next steps:"
echo "  1. Start live audio transcription:"
echo "     cd ingest/audio"
echo "     source venv/bin/activate"
echo "     python3 live_producer.py --brokers localhost:9092"
echo ""
echo "  2. Monitor Kafka messages (optional):"
echo "     docker compose exec kafka kafka-console-consumer \\"
echo "       --topic calls.raw \\"
echo "       --bootstrap-server localhost:29092 \\"
echo "       --from-beginning"
echo ""
echo "  3. View Kafka UI (optional):"
echo "     docker compose up -d kafka-ui"
echo "     Then open http://localhost:8085"
echo ""

