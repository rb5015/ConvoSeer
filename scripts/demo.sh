#!/usr/bin/env bash
set -euo pipefail

# 1) Copy config/env.template to .env and fill in OPENAI_API_KEY and Mongo vars.
if [ ! -f ".env" ]; then
  echo "Create .env from config/env.template before running."
  exit 1
fi

# 2) Start infra and services
docker compose build
docker compose up -d zookeeper kafka spark-master spark-worker embedder rag ui kafka-ui

echo "Waiting 10s for services to initialize..."
sleep 10

# 3) Prepare dataset (requires chunks.jsonl created by preprocess.py)
python3 scripts/prepare_dataset.py

# 4) Create vector index (requires MongoDB Atlas URI in .env)
python3 scripts/create_vector_index.py

# 5) Start embedder worker
docker compose up -d embedder-worker

echo "Demo setup complete."
echo "- Kafka UI: http://localhost:8085"
echo "- UI: http://localhost:8501"
echo "- RAG: http://localhost:8002/health"
echo
echo "Replay utterances:"
echo "  docker compose run --rm -e KAFKA_BROKERS=kafka:29092 producer python producer.py -i datasets/prepared/utterances.jsonl -r 5"


