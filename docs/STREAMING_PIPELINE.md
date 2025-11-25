# Streaming Audio→Sentiment→RAG Pipeline

## Overview

The streaming pipeline enables real-time sentiment analysis and RAG suggestions with ~10 second windows. As audio is transcribed, it flows through multiple stages to provide live feedback to agents.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Live Audio Producer (ingest/audio/live_producer.py)            │
│ • Captures microphone audio in 5s chunks                       │
│ • Transcribes with Whisper                                     │
│ • Publishes to calls.raw with event_time metadata              │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
              ┌─────────────────┐
              │  Kafka Topic    │
              │   calls.raw     │
              └────────┬────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │  Spark Streaming             │
        │  (streaming/app.py)          │
        │  • Text cleaning             │
        │  • Role inference            │
        │  • Sentiment analysis        │
        │  • 10s windowing             │
        └──────┬───────────────┬───────┘
               │               │
               ▼               ▼
      ┌────────────┐   ┌──────────────┐
      │ calls.     │   │ calls.       │
      │ enriched   │   │ sentiment    │
      └─────┬──────┘   └──────┬───────┘
            │                 │
            ▼                 ▼
    ┌───────────────┐  ┌──────────────────┐
    │ Embedder      │  │ RAG Stream Worker│
    │ Worker        │  │ (services/rag/   │
    │               │  │  stream_worker.py)│
    │ → MongoDB     │  │ • Calls RAG API  │
    └───────────────┘  │ • Publishes to   │
                       │   calls.rag      │
                       └────────┬─────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  Kafka Topic    │
                       │   calls.rag     │
                       └────────┬────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  Stream API     │
                       │  (services/     │
                       │   stream/)      │
                       │  • SSE endpoint │
                       │  • Broadcasts   │
                       └────────┬────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  Live UI        │
                       │  (ui/stream_    │
                       │   app.py)       │
                       │  • Sentiment    │
                       │    graph        │
                       │  • RAG updates  │
                       └─────────────────┘
```

## Kafka Topics

### calls.raw
- **Producer**: Live audio producer, batch producer
- **Schema**: Raw utterances with event_time, chunk_id
- **Purpose**: Entry point for all transcribed audio

### calls.enriched
- **Producer**: Spark Streaming
- **Consumer**: Embedder worker
- **Schema**: Cleaned text + sentiment (per utterance)
- **Purpose**: Individual utterance processing

### calls.sentiment (NEW)
- **Producer**: Spark Streaming (windowed aggregation)
- **Consumer**: RAG stream worker
- **Schema**: Window summary with avg sentiment, utterance list
- **Window**: 10 seconds (configurable via STREAM_WINDOW_SECONDS)
- **Purpose**: Trigger RAG queries at regular intervals

### calls.rag (NEW)
- **Producer**: RAG stream worker
- **Consumer**: Stream API
- **Schema**: RAG response + sentiment context
- **Purpose**: Deliver agent suggestions to UI

## Components

### 1. Live Producer Enhancements
**File**: `ingest/audio/live_producer.py`

Added metadata:
- `event_time`: Unix timestamp in ms for windowing
- `chunk_id`: Unique identifier for each audio chunk
- `audio_energy`: Voice energy metric (for future features)

### 2. Spark Streaming Windows
**File**: `streaming/app.py`

New functionality:
- Tumbling windows of 10 seconds (configurable)
- Aggregates sentiment per call_id
- Collects utterances within each window
- Publishes window summaries to `calls.sentiment`

Configuration:
```python
WINDOW_SECONDS = int(os.getenv("STREAM_WINDOW_SECONDS", "10"))
```

### 3. RAG Stream Worker
**File**: `services/rag/stream_worker.py`

Responsibilities:
- Consumes sentiment windows from `calls.sentiment`
- Extracts latest customer utterance
- Calls RAG service `/assist` endpoint
- Publishes responses to `calls.rag`

Key features:
- Filters for customer utterances only
- Includes sentiment context in response
- Error handling with fallback messages

### 4. Stream API Service
**File**: `services/stream/main.py`

Provides SSE endpoints:
- `/stream/{call_id}` - Combined sentiment + RAG updates
- `/stream/sentiment/{call_id}` - Sentiment only
- `/stream/rag/{call_id}` - RAG only

Features:
- Server-Sent Events (SSE) for real-time push
- Per-call_id subscriptions
- Keepalive messages every 30s
- CORS enabled for frontend access

### 5. Live Streaming UI
**File**: `ui/stream_app.py`

Features:
- Real-time sentiment graph (rolling 20 windows)
- Live RAG suggestions as they arrive
- History view for both sentiment and RAG
- SSE client connection

## Configuration

### Environment Variables

Add to `.env`:
```bash
# Streaming topics
KAFKA_TOPIC_SENTIMENT=calls.sentiment
KAFKA_TOPIC_RAG=calls.rag

# Window settings
STREAM_WINDOW_SECONDS=10

# Stream API URL
STREAM_URL=http://stream:8003
```

### Docker Compose

New services:
```yaml
rag-stream-worker:
  # Consumes sentiment, produces RAG responses
  
stream:
  # SSE API for frontend
  ports:
    - "8003:8003"
```

## Usage

### Start All Services

```bash
# Start infrastructure
docker compose up -d zookeeper kafka

# Start Spark streaming (with windowing)
docker compose up -d spark-master spark-worker
docker compose exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/work-dir/app.py

# Start embedding and RAG services
docker compose up -d embedder embedder-worker rag rag-stream-worker

# Start stream API
docker compose up -d stream

# Start UI
docker compose up -d ui
```

### Run Live Audio

```bash
cd ingest/audio
source venv/bin/activate
python live_producer.py --brokers localhost:9092
```

### Access Live UI

```bash
# Open streaming dashboard
streamlit run ui/stream_app.py
# or
open http://localhost:8501
```

## Monitoring

### Check Topics

```bash
# List all topics
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:29092

# Monitor sentiment windows
docker compose exec kafka kafka-console-consumer \
  --topic calls.sentiment \
  --bootstrap-server localhost:29092 \
  --from-beginning

# Monitor RAG responses
docker compose exec kafka kafka-console-consumer \
  --topic calls.rag \
  --bootstrap-server localhost:29092 \
  --from-beginning
```

### View Logs

```bash
# Spark streaming (windowing)
docker compose logs -f spark-master

# RAG stream worker
docker compose logs -f rag-stream-worker

# Stream API
docker compose logs -f stream
```

### Kafka UI

```bash
open http://localhost:8085
```

## Performance Tuning

### Window Size
Adjust `STREAM_WINDOW_SECONDS` based on needs:
- **5s**: More frequent updates, higher load
- **10s**: Balanced (default)
- **15-30s**: Less frequent, lower load

### Batch Size
RAG worker processes one window at a time. For high-volume:
- Scale horizontally: Run multiple `rag-stream-worker` instances
- Adjust consumer group settings

### Caching
- Embedder service caches embeddings (100k entries)
- RAG service caches embeddings (50k entries)

## Troubleshooting

### No sentiment windows appearing
```bash
# Check Spark is running
docker compose logs spark-master

# Verify raw messages are flowing
docker compose exec kafka kafka-console-consumer \
  --topic calls.raw \
  --bootstrap-server localhost:29092
```

### No RAG responses
```bash
# Check RAG stream worker
docker compose logs rag-stream-worker

# Verify RAG service is accessible
curl http://localhost:8002/health
```

### SSE connection issues
```bash
# Check stream API
docker compose logs stream

# Test SSE endpoint
curl -N http://localhost:8003/stream/test-call-id
```

## Latency Expectations

Typical end-to-end latency (audio → UI update):
- Audio capture: 5s (chunk duration)
- Whisper transcription: 0.5-2s
- Spark windowing: 0-10s (depends on window position)
- RAG processing: 1-3s
- SSE delivery: <100ms

**Total**: ~7-20 seconds from speech to suggestion

## Future Enhancements

- [ ] Real-time sentiment visualization with live charts
- [ ] Multi-speaker detection and tracking
- [ ] Conversation context accumulation across windows
- [ ] WebSocket support for bidirectional communication
- [ ] Agent feedback loop for suggestion quality
- [ ] A/B testing framework for different RAG strategies

