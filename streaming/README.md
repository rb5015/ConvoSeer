# Spark Streaming Job

Spark Structured Streaming processes raw utterances from Kafka, enriches them with sentiment analysis, and publishes to enriched and sentiment topics.

## Automatic Startup (Recommended)

Spark streaming now auto-starts via docker-compose service:

```bash
# Start Spark streaming automatically with other services
docker compose up -d spark-streaming

# Or start all services together
docker compose up -d spark-master spark-worker spark-streaming
```

The `spark-streaming` service automatically:
- Connects to Spark master
- Submits the streaming job with required packages
- Handles restarts automatically
- Uses checkpointing for fault tolerance

## Manual Startup (Alternative)

For manual control, you can submit the job directly:

### Option 1: Using Volume Mount

```bash
# Start Spark services
docker compose up -d spark-master spark-worker

# Submit streaming job (app.py is mounted as volume)
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints \
  /opt/spark/work-dir/app.py
```

### Option 2: Using Script

```bash
# Use the provided script
./scripts/start_spark_streaming.sh
```

### Option 3: Copy file into container

```bash
# Copy app.py to container
docker compose cp streaming/app.py spark-master:/tmp/app.py

# Submit job
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints \
  /tmp/app.py
```

## Environment Variables

Set in `.env` or docker-compose.yml:
- `KAFKA_BROKERS` (e.g. `kafka:29092`)
- `KAFKA_TOPIC_RAW` (e.g. `calls.raw`)
- `KAFKA_TOPIC_ENRICHED` (e.g. `calls.enriched`)
- `KAFKA_TOPIC_SENTIMENT` (e.g. `calls.sentiment`)
- `SENTIMENT_URL` (e.g. `http://sentiment-service:8000`)
- `STREAM_WINDOW_SECONDS` (default: `10`)

## Monitoring

- Spark Master UI: http://localhost:8080
- Spark Worker UI: http://localhost:8081

## Troubleshooting

If the job fails:
1. Check logs: `docker compose logs spark-master`
2. Verify Kafka is accessible: `docker compose exec spark-master ping kafka`
3. Check topic exists: `docker compose exec kafka kafka-topics --list --bootstrap-server localhost:29092`
