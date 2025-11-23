# Agent Assist UI

Streamlit-based dashboard for real-time agent assistance with RAG-powered suggestions.

## Features

- **Dual Input Methods**:
  - Type customer utterances directly
  - Record audio and transcribe with Whisper (local)
  
- **Real-time Suggestions**: Get context-aware response suggestions from RAG service
- **Metadata Filtering**: Filter by industry, product, sentiment
- **Retrieved Context**: View similar past conversations that informed suggestions

## Setup

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run UI
streamlit run app.py
```

### Docker

```bash
# Build and run
docker compose up -d ui

# Access at http://localhost:8501
```

## Usage

1. **Select Input Method**:
   - **Type**: Enter customer utterance in text area
   - **Record Audio**: Click microphone button to record, click again to stop

2. **Configure Settings** (sidebar):
   - Top-k retrieved: Number of similar conversations to retrieve
   - Filters: Industry, product, sentiment
   - Whisper Model: Model size for audio transcription (tiny/base/small)

3. **Get Suggestions**: Click "Get Suggestions" button

4. **View Results**:
   - Primary suggestion
   - Alternative suggestions
   - Retrieved context with similarity scores

## Audio Recording

The UI uses `streamlit-audio-recorder` for browser-based audio recording:
- Click microphone button to start recording
- Click again to stop
- Audio is automatically transcribed using Whisper
- First run will download the Whisper model (~150MB for 'base')

## Requirements

- Python 3.8+
- Streamlit
- OpenAI Whisper (for audio transcription)
- RAG service running (default: http://localhost:8002)

## Troubleshooting

### Audio recording not working

- Check browser permissions for microphone access
- Ensure HTTPS or localhost (browsers require secure context for microphone)

### Transcription fails

- First run downloads Whisper model (may take time)
- Check disk space (~150MB for 'base' model)
- Verify ffmpeg is installed (required by Whisper)

### RAG service connection error

- Ensure RAG service is running: `docker compose up -d rag`
- Check RAG_URL environment variable
- Verify service is accessible at configured URL

