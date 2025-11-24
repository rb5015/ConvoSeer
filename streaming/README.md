# Spark Streaming Job

Submit the streaming job with Spark providing the Kafka package.

## Using Apache Spark Image

The Apache Spark image requires the app to be mounted as a volume or copied into the container.

### Option 1: Using Volume Mount (Recommended)

```bash
# Start Spark services
docker compose up -d spark-master spark-worker

# Submit streaming job (app.py is mounted as volume)
docker compose exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/work-dir/app.py
```

### Option 2: Copy file into container

```bash
# Copy app.py to container
docker compose cp streaming/app.py spark-master:/tmp/app.py

# Submit job
docker compose exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /tmp/app.py
```

## Environment Variables

Set in `.env` or docker-compose.yml:
- `KAFKA_BROKERS` (e.g. `kafka:29092`)
- `KAFKA_TOPIC_RAW` (e.g. `calls.raw`)
- `KAFKA_TOPIC_ENRICHED` (e.g. `calls.enriched`)

## Monitoring

- Spark Master UI: http://localhost:8080
- Spark Worker UI: http://localhost:8081

## Troubleshooting

If the job fails:
1. Check logs: `docker compose logs spark-master`
2. Verify Kafka is accessible: `docker compose exec spark-master ping kafka`
3. Check topic exists: `docker compose exec kafka kafka-topics --list --bootstrap-server localhost:29092`
