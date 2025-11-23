# Live Audio Pipeline Documentation

## Overview

The live audio pipeline enables real-time transcription of customer service calls, feeding directly into the ConvoSeer system for immediate agent assistance.

## Architecture

```
┌──────────────┐
│  Microphone  │
└──────┬───────┘
       │ Audio stream (PCM 16kHz mono)
       ▼
┌──────────────────────────────────────────────────────┐
│  Audio Capture Module (audio_capture.py)            │
│  - Captures audio via PyAudio                        │
│  - Buffers into 5-second chunks                      │
│  - Converts to numpy arrays                          │
└──────────────┬───────────────────────────────────────┘
               │ numpy array (float32)
               ▼
┌──────────────────────────────────────────────────────┐
│  Whisper Transcriber (whisper_transcriber.py)       │
│  - Loads Whisper model (base recommended)            │
│  - Transcribes audio chunks to text                  │
│  - Returns text + metadata (language, duration)      │
└──────────────┬───────────────────────────────────────┘
               │ TranscriptionResult
               ▼
┌──────────────────────────────────────────────────────┐
│  Live Producer (live_producer.py)                    │
│  - Formats as pipeline-compatible message            │
│  - Adds call_id, utterance_id, timestamps           │
│  - Publishes to Kafka                                │
└──────────────┬───────────────────────────────────────┘
               │ JSON message
               ▼
┌──────────────────────────────────────────────────────┐
│  Kafka Topic: calls.raw                              │
└──────────────┬───────────────────────────────────────┘
               │
               ▼
         [Rest of pipeline: Spark → Embedder → MongoDB → RAG → UI]
```

## Components

### 1. Audio Capture (`audio_capture.py`)

**Purpose**: Capture audio from microphone in real-time

**Key Features**:
- PyAudio-based capture
- Configurable sample rate (16kHz for Whisper)
- Chunk-based buffering (default 5 seconds)
- Thread-safe queue for audio data
- Device selection support

**Configuration**:
```python
AudioConfig(
    sample_rate=16000,      # Whisper requirement
    channels=1,             # Mono
    chunk_duration=5.0,     # Seconds per chunk
    format=pyaudio.paInt16  # 16-bit PCM
)
```

**Usage**:
```python
capture = AudioCapture()
capture.set_chunk_callback(on_audio_chunk)
capture.start(device_index=0)
```

### 2. Whisper Transcriber (`whisper_transcriber.py`)

**Purpose**: Convert audio chunks to text using Whisper

**Key Features**:
- Multiple model sizes (tiny to large)
- GPU acceleration support
- Configurable transcription options
- Word-level timestamps (optional)
- Language detection

**Model Selection**:
| Model  | Size  | Speed  | Quality | Use Case |
|--------|-------|--------|---------|----------|
| tiny   | 39M   | ~32x   | Basic   | Testing  |
| base   | 74M   | ~16x   | Good    | **Recommended** |
| small  | 244M  | ~6x    | Better  | High quality |
| medium | 769M  | ~2x    | Great   | Near real-time |
| large  | 1550M | ~1x    | Best    | Offline only |

**Usage**:
```python
transcriber = WhisperTranscriber(model_size="base")
result = transcriber.transcribe(audio_array)
print(result.text)  # "Hello, I need help with my policy"
```

### 3. Live Producer (`live_producer.py`)

**Purpose**: Orchestrate the full pipeline and publish to Kafka

**Key Features**:
- Manages call ID and utterance indexing
- Formats messages for pipeline compatibility
- Handles interrupts gracefully
- Configurable speaker role
- Real-time metrics

**Message Format**:
```json
{
  "call_id": "live-abc12345",
  "utterance_id": "live-abc12345:0:def67890",
  "utterance_index": 0,
  "timestamp_ms": 5234,
  "speaker_role": "customer",
  "text": "Hello, I need help with my insurance policy",
  "metadata": {
    "source": "live_audio",
    "whisper_model": "base",
    "transcription_duration": 0.82,
    "language": "en"
  }
}
```

**Usage**:
```bash
python live_producer.py \
  --brokers localhost:9092 \
  --topic calls.raw \
  --model base \
  --speaker customer
```

## Performance Characteristics

### Latency Breakdown (5-second audio chunk)

| Stage | Time | Notes |
|-------|------|-------|
| Audio capture | 5.0s | Chunk duration |
| Buffer/convert | ~0.01s | Negligible |
| Whisper (base) | ~0.8s | CPU, ~6x real-time |
| Kafka publish | ~0.01s | Local network |
| **Total** | **~5.8s** | From speech start to Kafka |

### Throughput

- **Audio capture**: Continuous, no bottleneck
- **Transcription**: Limited by model speed
  - Base model: ~6 chunks/second (theoretical)
  - Actual: 1 chunk every 5.8s (sequential processing)
- **Kafka**: 1000s msgs/sec (not a bottleneck)

### Resource Usage

**CPU (base model)**:
- Idle: ~5% (audio capture only)
- Transcribing: ~100% (single core)
- Average: ~15% (with 5s chunks)

**Memory**:
- Whisper base: ~1GB
- Audio buffers: ~10MB
- Total: ~1.2GB

**GPU (if available)**:
- 3-5x faster transcription
- Recommended for production

## Setup Instructions

### Quick Setup

```bash
cd ingest/audio
./test_pipeline.sh
```

### Manual Setup

1. **Install system dependencies**:
   ```bash
   # macOS
   brew install portaudio ffmpeg
   
   # Linux
   sudo apt-get install portaudio19-dev ffmpeg
   ```

2. **Create virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install Python packages**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Test installation**:
   ```bash
   python whisper_transcriber.py --model base --benchmark
   ```

### Running

1. **Start Kafka** (if not running):
   ```bash
   docker compose up -d zookeeper kafka
   ```

2. **List audio devices**:
   ```bash
   python live_producer.py --list-devices
   ```

3. **Start transcription**:
   ```bash
   python live_producer.py --brokers localhost:9092
   ```

4. **Monitor output**:
   ```bash
   docker compose exec kafka kafka-console-consumer \
     --topic calls.raw \
     --bootstrap-server localhost:29092 \
     --from-beginning
   ```

## Integration with Main Pipeline

Once messages are in Kafka `calls.raw`, they flow through:

1. **Spark Streaming**: Cleans text, assigns roles, analyzes sentiment
2. **Kafka** (`calls.enriched`): Enriched messages with sentiment
3. **Embedder Worker**: Generates embeddings via OpenAI
4. **MongoDB**: Stores with vector embeddings
5. **RAG Service**: Retrieves similar conversations, generates suggestions
6. **UI**: Displays suggestions to agent

## Comparison: Live vs Batch

| Aspect | Live Audio | Batch (Historical) |
|--------|-----------|-------------------|
| Source | Microphone | JSON files |
| Timing | Real-time | Replay with rate limit |
| Latency | ~6s from speech | Configurable delay |
| Use case | Active calls | Knowledge base population |
| Timestamps | Real call time | Synthetic or historical |
| Processing | Continuous | One-time or periodic |

## Troubleshooting

### No audio captured

**Check**:
1. Microphone permissions (macOS: System Preferences → Security)
2. Device selection: `python live_producer.py --list-devices`
3. Input level: Speak loudly during test

**Fix**:
```bash
# Test audio capture standalone
python audio_capture.py
```

### Transcription too slow

**Symptoms**: Lag accumulates, can't keep up with real-time

**Solutions**:
1. Use smaller model: `--model tiny`
2. Enable GPU acceleration
3. Increase chunk duration: `--chunk-duration 10.0`
4. Reduce audio quality (not recommended)

**Benchmark**:
```bash
python whisper_transcriber.py --model base --benchmark
```

### Poor transcription quality

**Causes**:
- Background noise
- Low microphone quality
- Wrong language setting
- Model too small

**Solutions**:
1. Use better microphone
2. Reduce background noise
3. Use larger model: `--model small`
4. Check language: Whisper auto-detects but can force with `language="en"`

### Kafka connection issues

**Check**:
```bash
# Verify Kafka is running
docker compose ps kafka

# Test connection
python scripts/test_kafka.py --brokers localhost:9092
```

**Fix**:
```bash
# Restart Kafka
docker compose restart kafka
sleep 10  # Wait for startup
```

## Advanced Configuration

### Custom Audio Settings

Edit `audio_capture.py` → `AudioConfig`:
```python
AudioConfig(
    sample_rate=16000,      # Don't change (Whisper requirement)
    channels=1,             # Mono (don't change)
    chunk_duration=10.0,    # Longer = less frequent transcription
    format=pyaudio.paInt16  # 16-bit PCM (don't change)
)
```

### Custom Whisper Options

Edit `whisper_transcriber.py` → `options`:
```python
self.options = {
    "language": "en",       # Force language (faster)
    "task": "transcribe",   # vs "translate"
    "beam_size": 5,         # Beam search width
    "temperature": 0.0,     # Deterministic (0.0) vs creative (>0)
}
```

### GPU Configuration

```python
# Force GPU
transcriber = WhisperTranscriber(model_size="base", device="cuda")

# Force CPU
transcriber = WhisperTranscriber(model_size="base", device="cpu")

# Auto-detect (default)
transcriber = WhisperTranscriber(model_size="base")
```

## Future Enhancements

1. **Voice Activity Detection (VAD)**:
   - Skip silent chunks
   - Reduce processing overhead
   - Improve latency

2. **Speaker Diarization**:
   - Auto-detect agent vs customer
   - No manual role assignment needed
   - Use pyannote.audio

3. **Streaming Transcription**:
   - Partial results before chunk completes
   - Lower latency
   - Use Whisper streaming mode

4. **Audio Preprocessing**:
   - Noise reduction
   - Normalization
   - Echo cancellation

5. **Multi-channel Support**:
   - Separate agent/customer audio
   - Better quality
   - Easier role assignment

## References

- OpenAI Whisper: https://github.com/openai/whisper
- PyAudio: https://people.csail.mit.edu/hubert/pyaudio/
- Kafka Python: https://kafka-python.readthedocs.io/

