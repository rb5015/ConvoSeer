# ConvoSeer

Real-time Agent Assist system using RAG (Retrieval-Augmented Generation) for customer service calls. Provides live suggestions to agents based on historical conversation data and sentiment analysis.

## Overview

ConvoSeer processes customer service conversations in real-time, analyzes sentiment, and provides agents with contextually relevant response suggestions by searching through a knowledge base of past successful interactions.

### Key Features

- **Real-time transcription**: Live audio-to-text using Whisper (local)
- **Streaming processing**: Spark Structured Streaming for scalable data processing
- **Sentiment analysis**: Real-time sentiment scoring of customer utterances
- **Semantic search**: Vector embeddings for similarity-based retrieval
- **RAG-powered suggestions**: Context-aware response generation using GPT-4
- **Metadata filtering**: Filter by industry, product, sentiment
- **Live dashboard**: Streamlit UI for agent assistance

## Architecture

```
┌─────────────────┐
│  Live Audio     │ ──► Whisper (local)
│  or Dataset     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Kafka          │ ◄── calls.raw topic
│  (Message Bus)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Spark Streaming │ ──► Clean, sentiment, role assignment
│  (Processing)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Kafka          │ ◄── calls.enriched topic
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Embedder Worker │ ──► Gemini embeddings (gemini-embedding-001)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ MongoDB Atlas   │ ◄── Vector storage + search index
│ (Vector Store)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  RAG Service    │ ──► Retrieval + GPT-4 generation
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Streamlit UI   │ ◄── Agent dashboard
└─────────────────┘
```

## Tech Stack

- **Streaming**: Apache Kafka, Spark Structured Streaming
- **Storage**: MongoDB Atlas (vector search)
- **ML/AI**: OpenAI Whisper (local), OpenAI GPT-4, Gemini embeddings (gemini-embedding-001)
- **Backend**: FastAPI (Python)
- **Frontend**: Streamlit
- **Infrastructure**: Docker Compose

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- MongoDB Atlas account (free tier works)
- Gemini API key (for embeddings) - get from https://aistudio.google.com/apikey
- OpenAI API key (for generation model)

### 1. Clone and setup

```bash
git clone https://github.com/yourusername/ConvoSeer.git
cd ConvoSeer

# Copy environment template
cp config/env.template .env

# Edit .env with your credentials
# - GEMINI_API_KEY (for embeddings)
# - OPENAI_API_KEY (for generation)
# - MONGODB_URI
```

### 2. Start infrastructure

```bash
# Start Kafka, Zookeeper, Spark
docker compose up -d zookeeper kafka spark-master spark-worker
```

### 3. Setup MongoDB Atlas

Follow instructions in `config/mongodb/SETUP.md`:
1. Create free cluster
2. Create database `agent_assist` and collection `utterances`
3. Get connection string
4. Create vector search index (run `scripts/create_vector_index.py`)

### 4. Prepare dataset

```bash
# Preprocess raw transcripts
python3 preprocess.py --input-dir datasets/

# Prepare for pipeline
python3 scripts/prepare_dataset.py
```

### 5. Start services

```bash
# Build and start all services
docker compose up -d embedder embedder-worker rag ui

# Start Spark streaming job
docker compose exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /workspace/streaming/app.py
```

### 6. Ingest data

```bash
# Replay historical data
python3 ingest/producer/producer.py \
  --input datasets/prepared/utterances.jsonl \
  --rate 5.0
```

### 7. Access UI

Open http://localhost:8501 in your browser

## Live Audio Transcription

For real-time voice-to-text:

```bash
cd ingest/audio

# Setup (first time only)
./test_pipeline.sh

# Start live transcription
source venv/bin/activate
python live_producer.py --brokers localhost:9092
```

See `ingest/audio/SETUP.md` for detailed instructions.

## Project Structure

```
ConvoSeer/
├── config/                 # Configuration files
│   ├── env.template       # Environment variables template
│   └── mongodb/           # MongoDB setup instructions
├── datasets/              # Raw transcript data
│   └── prepared/          # Processed utterances
├── ingest/
│   ├── audio/            # Live audio transcription
│   │   ├── audio_capture.py
│   │   ├── whisper_transcriber.py
│   │   └── live_producer.py
│   └── producer/         # Kafka producer (replay)
├── streaming/            # Spark Structured Streaming
│   └── app.py           # Main streaming job
├── services/
│   ├── embedder/        # Embedding service
│   │   ├── main.py     # FastAPI server
│   │   └── worker.py   # Kafka consumer
│   └── rag/            # RAG service
│       ├── main.py     # FastAPI server
│       └── prompts.py  # Prompt templates
├── ui/                  # Streamlit dashboard
│   └── app.py
├── scripts/            # Utility scripts
│   ├── prepare_dataset.py
│   ├── create_vector_index.py
│   ├── test_kafka.py
│   └── demo.sh
├── preprocess.py       # Raw data preprocessing
└── docker-compose.yml  # Infrastructure definition
```

## Data Flow

### Batch Processing (Historical Data)

1. **Raw transcripts** (JSON files) → `preprocess.py`
2. **Chunks** (chunks.jsonl) → `prepare_dataset.py`
3. **Utterances** (utterances.jsonl) → Kafka producer
4. **Kafka** (calls.raw) → Spark Streaming
5. **Enriched** (calls.enriched) → Embedder worker
6. **MongoDB** (with embeddings) → RAG service
7. **UI** (agent suggestions)

### Live Streaming (Real-time)

1. **Microphone** → Whisper transcription
2. **Kafka** (calls.raw) → [same as above from step 4]

## Configuration

### Environment Variables

Required in `.env`:

```bash
# Gemini API (for embeddings)
GEMINI_API_KEY=...
# OpenAI API (for generation)
OPENAI_API_KEY=sk-...

# MongoDB Atlas
MONGODB_URI=mongodb+srv://...
MONGODB_DB=agent_assist
MONGODB_COLLECTION=utterances

# Kafka
KAFKA_BROKERS=kafka:29092
KAFKA_TOPIC_RAW=calls.raw
KAFKA_TOPIC_ENRICHED=calls.enriched

# Models
EMBEDDING_MODEL=gemini-embedding-001
GENERATION_MODEL=gpt-4o-mini

# Service URLs (for containers)
EMBEDDER_URL=http://embedder:8000
RAG_URL=http://rag:8000
```

### Ports

- 2181: Zookeeper
- 9092: Kafka (external)
- 29092: Kafka (internal)
- 7077: Spark master
- 8080: Spark master UI
- 8081: Spark worker UI
- 8001: Embedder API
- 8002: RAG API
- 8501: Streamlit UI
- 8085: Kafka UI

## Development

### Testing Kafka

```bash
# Start Kafka
docker compose up -d zookeeper kafka

# Test connection
python3 scripts/test_kafka.py --brokers localhost:9092
```

### Testing Services

```bash
# Embedder API
curl -X POST http://localhost:8001/embed \
  -H "Content-Type: application/json" \
  -d '{"texts": ["Hello world"]}'

# RAG API
curl -X POST http://localhost:8002/assist \
  -H "Content-Type: application/json" \
  -d '{"latest_utterance": "I need help with my policy", "k": 5}'
```

### Monitoring

```bash
# Kafka UI
open http://localhost:8085

# Spark UI
open http://localhost:8080

# View logs
docker compose logs -f embedder
docker compose logs -f rag
docker compose logs -f spark-master
```

## Troubleshooting

### Kafka won't start

```bash
# Check logs
docker compose logs kafka

# Restart
docker compose restart kafka
```

### Embedder fails

Check OpenAI API key in `.env` and ensure MongoDB is accessible.

### Spark job fails

Ensure Kafka package is included:
```bash
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
```

### No suggestions in UI

1. Check MongoDB has data: `scripts/create_vector_index.py --check`
2. Check RAG service logs: `docker compose logs rag`
3. Verify embedder is running: `curl http://localhost:8001/health`

## Performance

### Benchmarks (Laptop, 2020 M1 MacBook Pro)

- **Whisper base**: 0.8s to transcribe 5s audio (~6x real-time)
- **Spark processing**: ~100 msgs/sec
- **Embedding**: ~50 texts/sec (batched)
- **RAG retrieval**: ~200ms per query
- **End-to-end latency**: ~2-3s from speech to suggestion

### Scaling

- **Kafka**: Add partitions for horizontal scaling
- **Spark**: Add workers with `docker compose scale spark-worker=3`
- **MongoDB**: Use sharding for large datasets (>1M docs)
- **Embedder**: Deploy multiple instances behind load balancer

## Dataset

Using AIxBlock 92k Call Center Transcripts:
- https://huggingface.co/datasets/AIxBlock/92k-real-world-call-center-scripts-english
- 92,000 real customer service conversations
- Multiple industries: insurance, healthcare, telecom, automotive

## Future Enhancements

- [ ] Speaker diarization (auto-detect agent vs customer)
- [ ] Voice Activity Detection (skip silence)
- [ ] Multi-language support
- [ ] Custom fine-tuned models
- [ ] A/B testing framework for suggestions
- [ ] Analytics dashboard
- [ ] WebSocket support for remote audio
- [ ] Kubernetes deployment

## License

MIT License - see LICENSE file

## Contributing

Contributions welcome! Please open an issue or PR.

## Acknowledgments

- OpenAI Whisper for speech recognition
- Apache Kafka and Spark for streaming
- MongoDB Atlas for vector search
- AIxBlock for the dataset

## Contact

For questions or support, open an issue on GitHub.

