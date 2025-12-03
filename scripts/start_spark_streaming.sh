#!/bin/bash
# Start Spark Streaming Job

set -e

echo "🚀 Starting Spark Streaming Job..."
echo ""

# Check if Spark services are running
if ! docker ps --format '{{.Names}}' | grep -q "^spark-master$"; then
    echo "❌ Spark master is not running. Starting Spark services..."
    docker-compose up -d spark-master spark-worker
    echo "⏳ Waiting for Spark services to be ready..."
    sleep 10
fi

# Check if worker is running
if ! docker ps --format '{{.Names}}' | grep -q "^spark-worker$"; then
    echo "❌ Spark worker is not running. Starting Spark worker..."
    docker-compose up -d spark-worker
    sleep 5
fi

# Check if streaming app exists
if [ ! -f "streaming/app.py" ]; then
    echo "❌ streaming/app.py not found!"
    exit 1
fi

echo "✅ Spark services are running"
echo "📦 Submitting Spark streaming job..."
echo ""

# Submit the Spark streaming job
docker exec -d spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints \
  /opt/spark/work-dir/app.py

echo "✅ Spark streaming job submitted!"
echo ""
echo "📊 Monitor the job:"
echo "   - Spark Master UI: http://localhost:8080"
echo "   - Spark Worker UI: http://localhost:8081"
echo ""
echo "📝 View logs:"
echo "   docker logs -f spark-master"
echo "   docker logs -f spark-worker"
echo ""
echo "🛑 To stop the job, kill the Spark application from the Master UI or:"
echo "   docker exec spark-master pkill -f 'spark-submit.*app.py'"

