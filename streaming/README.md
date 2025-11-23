# Spark Streaming Job

Submit the streaming job with Spark providing the Kafka package:

Example (inside spark-master container shell):
```
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /workspace/streaming/app.py
```

Environment variables expected:
- `KAFKA_BROKERS` (e.g. `kafka:29092`)
- `KAFKA_TOPIC_RAW` (e.g. `calls.raw`)
- `KAFKA_TOPIC_ENRICHED` (e.g. `calls.enriched`)


