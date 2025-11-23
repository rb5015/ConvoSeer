# Live Audio Transcription Pipeline Setup

Complete setup guide for the voice-to-text Kafka pipeline.

## Quick Start (5 minutes)

```bash
# 1. Navigate to audio directory
cd ingest/audio

# 2. Run test script (installs dependencies and tests)
./test_pipeline.sh

# 3. Start live transcription
source venv/bin/activate
python live_producer.py --brokers localhost:9092
```

## Detailed Setup

### Step 1: System Dependencies

#### macOS

```bash
brew install portaudio ffmpeg
```

#### Linux (Ubuntu/Debian)

```bash
sudo apt-get update
sudo apt-get install -y portaudio19-dev ffmpeg python3-dev
```

#### Windows

```powershell
# Install FFmpeg via Chocolatey
choco install ffmpeg

# Download PyAudio wheel from:
# https://www.lfd.uci.edu/~gohlke/pythonlibs/#pyaudio
```

### Step 2: Python Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate
source venv/bin/activate  # macOS/Linux
# OR
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

This will install:

- `openai-whisper` - Local speech recognition
- `pyaudio` - Audio capture
- `kafka-python` - Kafka client
- `torch` - PyTorch for Whisper
- Other utilities

### Step 3: Verify Installation

```bash
# Test Whisper model loading
python whisper_transcriber.py --model base

# List audio devices
python live_producer.py --list-devices

# Benchmark transcription speed
python whisper_transcriber.py --model base --benchmark
```

Expected output:

```
Loading Whisper model 'base' on cpu...
✓ Model loaded in 2.3s

Benchmarking...
  Audio duration: 5.0s
  Transcription time: 0.82s
  Real-time factor: 0.16x
  ✓ FASTER than real-time
```

### Step 4: Start Kafka (if not running)

```bash
# From project root
cd ../..
docker compose up -d zookeeper kafka
```

### Step 5: Run Live Transcription

```bash
cd ingest/audio
source venv/bin/activate

# Basic usage
python live_producer.py --brokers localhost:9092

# With options
python live_producer.py \
  --brokers localhost:9092 \
  --topic calls.raw \
  --model base \
  --speaker customer \
  --chunk-duration 5.0 \
  --device 0
```

### Step 6: Monitor Output

In another terminal:

```bash
# Watch Kafka messages
docker compose exec kafka kafka-console-consumer \
  --topic calls.raw \
  --bootstrap-server localhost:29092 \
  --from-beginning
```

## Model Selection Guide

### Recommended: `base` model

- Good balance of speed and accuracy
- ~16x faster than real-time on CPU
- ~1GB RAM
- Suitable for live transcription

### Alternative models:

**For testing/development:**

```bash
python live_producer.py --model tiny  # Fastest, lower quality
```

**For better quality (if you have GPU):**

```bash
python live_producer.py --model small  # Better quality, slower
```

## Troubleshooting

### Issue: PyAudio won't install

**macOS:**

```bash
brew install portaudio
pip install pyaudio
```

**Linux:**

```bash
sudo apt-get install portaudio19-dev python3-dev
pip install pyaudio
```

### Issue: No audio devices found

```bash
# Check system audio settings
# macOS: System Preferences → Sound → Input
# Linux: alsamixer or pavucontrol

# List devices
python live_producer.py --list-devices
```

### Issue: Transcription too slow

1. Use smaller model: `--model tiny`
2. Check GPU: `python -c "import torch; print(torch.cuda.is_available())"`
3. Increase chunk duration: `--chunk-duration 10.0`

### Issue: Kafka connection refused

```bash
# Check if Kafka is running
docker compose ps kafka

# Start Kafka
docker compose up -d zookeeper kafka

# Wait a few seconds for startup
sleep 10
```

### Issue: Poor audio quality

1. Check microphone input level
2. Reduce background noise
3. Use better microphone
4. Test with: `python audio_capture.py`

## GPU Acceleration (Optional)

For 3-5x faster transcription:

```bash
# Install CUDA-enabled PyTorch (if you have NVIDIA GPU)
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

# Verify GPU
python -c "import torch; print(f'CUDA available: {torch.cuda.is_available()}')"

# Run with GPU
python live_producer.py --model base  # Will auto-detect GPU
```

## Integration Testing

### End-to-end test:

```bash
# Terminal 1: Start full pipeline
cd /path/to/ConvoSeer
docker compose up -d zookeeper kafka spark-master spark-worker

# Terminal 2: Start Spark streaming
docker compose exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /workspace/streaming/app.py

# Terminal 3: Start live audio
cd ingest/audio
source venv/bin/activate
python live_producer.py --brokers localhost:9092

# Terminal 4: Monitor enriched output
docker compose exec kafka kafka-console-consumer \
  --topic calls.enriched \
  --bootstrap-server localhost:29092 \
  --from-beginning

# Speak into microphone in Terminal 3
# See transcriptions in Terminal 4 with sentiment analysis
```

## Performance Benchmarks

Typical performance on modern laptop (2020+):

| Model | CPU Time | GPU Time | Quality |
| ----- | -------- | -------- | ------- |
| tiny  | 0.3s/5s  | 0.1s/5s  | Basic   |
| base  | 0.8s/5s  | 0.3s/5s  | Good    |
| small | 2.5s/5s  | 0.8s/5s  | Better  |

For real-time (5s audio in <5s processing), use `base` or `tiny` on CPU.

## Next Steps

Once live audio is working:

1. Start the embedder service
2. Start the RAG service
3. Open the UI at http://localhost:8501
4. Speak into microphone
5. See real-time suggestions in UI

Full pipeline:

```
Microphone → Whisper → Kafka → Spark → Embedder → MongoDB → RAG → UI
```
