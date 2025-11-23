# UI Audio Recording Feature

## Overview

The Streamlit UI now supports **dual input methods** for customer utterances:
1. **Type**: Direct text input (original feature)
2. **Record Audio**: Browser-based audio recording with Whisper transcription

## Features Added

### Audio Recording
- **Browser-based recording**: Uses `streamlit-audio-recorder` component
- **One-click recording**: Click microphone button to start/stop
- **Automatic transcription**: Whisper transcribes audio locally
- **Model selection**: Choose Whisper model size (tiny/base/small) in sidebar
- **Audio playback**: Listen to recorded audio after transcription

### User Experience
- **Radio button toggle**: Switch between "Type" and "Record Audio" modes
- **Real-time feedback**: Shows transcription progress and results
- **Error handling**: Clear error messages and helpful tips
- **State management**: Transcribed text persists and can be edited

## How It Works

```
User clicks microphone → Browser records audio → Audio bytes sent to Streamlit
    ↓
Whisper model loads (cached after first use) → Transcribes audio → Text displayed
    ↓
User clicks "Get Suggestions" → Text sent to RAG service → Suggestions displayed
```

## Technical Details

### Components

1. **`app.py`**: Main UI with audio recorder integration
2. **`transcriber.py`**: Whisper transcription module with model caching
3. **`requirements.txt`**: Added `streamlit-audio-recorder` and Whisper dependencies

### Dependencies Added

- `streamlit-audio-recorder==0.0.8`: Browser audio recording component
- `openai-whisper==20231117`: Local speech recognition
- `torch==2.1.0`: PyTorch for Whisper
- `torchaudio==2.1.0`: Audio processing

### Model Caching

Whisper models are cached in memory after first load:
- First transcription: Downloads model (~150MB for 'base') + loads
- Subsequent transcriptions: Uses cached model (fast)

### Audio Format

- **Input**: WAV format from browser (via streamlit-audio-recorder)
- **Processing**: Whisper automatically handles resampling
- **Output**: Transcribed text string

## Usage

### For Users

1. Select "Record Audio" radio button
2. Click microphone button to start recording
3. Speak into microphone
4. Click microphone again to stop
5. Wait for transcription (first time may take longer)
6. Review transcribed text
7. Click "Get Suggestions" to get RAG-powered suggestions

### For Developers

```python
# In app.py
from audio_recorder_streamlit import audio_recorder
from transcriber import transcribe_audio

# Record audio
audio_bytes = audio_recorder(...)

# Transcribe
if audio_bytes:
    text = transcribe_audio(audio_bytes, model_size="base")
```

## Configuration

### Whisper Model Selection

Available in sidebar:
- **tiny**: Fastest, lower quality (~39MB)
- **base**: Balanced (recommended, ~150MB)
- **small**: Better quality, slower (~500MB)

### Environment Variables

- `RAG_URL`: RAG service endpoint (default: http://localhost:8002)

## Performance

### First Run
- Model download: ~1-2 minutes (depends on internet speed)
- Model loading: ~1-2 seconds
- Transcription: ~0.5-1 second per 5 seconds of audio

### Subsequent Runs
- Model loading: ~0.1 seconds (cached)
- Transcription: ~0.5-1 second per 5 seconds of audio

### Real-time Factor
- **base model**: ~5-10x faster than real-time (CPU)
- **GPU**: 3-5x faster than CPU

## Browser Requirements

- **Microphone access**: Browser will prompt for permission
- **HTTPS or localhost**: Required for microphone access (security)
- **Modern browser**: Chrome, Firefox, Safari, Edge (latest versions)

## Troubleshooting

### Microphone not working
- Check browser permissions
- Ensure HTTPS or localhost
- Try different browser

### Transcription slow
- First run downloads model (one-time)
- Use smaller model (tiny) for faster transcription
- Check CPU/GPU availability

### Model download fails
- Check internet connection
- Verify disk space (~500MB for small model)
- Check firewall/proxy settings

### Audio quality issues
- Use better microphone
- Reduce background noise
- Speak clearly and at normal volume

## Comparison: UI Audio vs Live Pipeline

| Feature | UI Audio Recording | Live Pipeline (`live_producer.py`) |
|---------|-------------------|-----------------------------------|
| Input | Browser microphone | System microphone |
| Processing | On-demand (button click) | Continuous streaming |
| Use Case | Manual testing/demos | Production live calls |
| Latency | ~1-2s per recording | ~6s end-to-end |
| Integration | Direct to UI | Via Kafka pipeline |

## Future Enhancements

- [ ] Real-time streaming transcription (partial results)
- [ ] Multiple language support
- [ ] Audio preprocessing (noise reduction)
- [ ] Speaker diarization (agent vs customer)
- [ ] Voice Activity Detection (auto-stop on silence)
- [ ] Audio file upload option

## Files Modified

- `ui/app.py`: Added audio recording UI
- `ui/transcriber.py`: New Whisper transcription module
- `ui/requirements.txt`: Added audio dependencies
- `ui/Dockerfile`: Added ffmpeg for Whisper
- `ui/README.md`: Updated documentation

