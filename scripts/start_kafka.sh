#!/bin/bash
# Quick script to start Kafka and verify it's working

set -e

echo "🚀 Starting Kafka and Zookeeper..."
docker compose up -d zookeeper kafka

echo ""
echo "⏳ Waiting for Kafka to be ready (this may take 10-15 seconds)..."
sleep 5

# Wait for Kafka to be ready
MAX_WAIT=30
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    if docker compose exec -T kafka kafka-broker-api-versions.sh --bootstrap-server localhost:29092 > /dev/null 2>&1; then
        echo "✅ Kafka is ready!"
        break
    fi
    echo "   Still waiting... ($WAITED/$MAX_WAIT seconds)"
    sleep 2
    WAITED=$((WAITED + 2))
done

if [ $WAITED -ge $MAX_WAIT ]; then
    echo "❌ Kafka did not start in time. Check logs with: docker compose logs kafka"
    exit 1
fi

echo ""
echo "📊 Kafka Status:"
docker compose ps zookeeper kafka

echo ""
echo "🧪 Running Kafka connection test..."
python3 scripts/test_kafka.py --brokers localhost:9092

echo ""
echo "✅ Kafka setup complete!"
echo ""
echo "📝 Useful commands:"
echo "   View logs:        docker compose logs -f kafka"
echo "   Kafka UI:         docker compose up -d kafka-ui (then visit http://localhost:8085)"
echo "   Stop Kafka:       docker compose stop kafka zookeeper"
echo "   Test again:       python3 scripts/test_kafka.py"

