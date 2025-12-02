# Migration from Streamlit to React

This document describes the migration from the Streamlit-based `streaming_app.py` to the new React frontend.

## What Changed

### Frontend Technology
- **Before**: Streamlit (Python-based web framework)
- **After**: React with Vite (modern JavaScript framework)

### Key Improvements
1. **Better Performance**: React's virtual DOM provides faster UI updates
2. **Modern UI**: Tailwind CSS for responsive, modern design
3. **Better Audio Handling**: Native MediaRecorder API with proper interval-based chunking
4. **Improved SSE Handling**: Native EventSource API for real-time updates
5. **No Page Reloads**: Single-page application with smooth state management

## Architecture Comparison

### Streamlit Version
- Used `audio_recorder_streamlit` library
- Relied on Streamlit's session state and reruns
- SSE handled via `sseclient` Python library
- UI updates required page reruns

### React Version
- Uses native `MediaRecorder` API
- React hooks for state management
- Native `EventSource` for SSE
- Real-time UI updates without page reloads

## Backend Compatibility

**All existing backend services work without modification:**

1. **Audio Service** (`/transcribe`):
   - Accepts audio files via multipart/form-data
   - Returns transcription JSON
   - ✅ Fully compatible

2. **Stream Service** (`/stream/{call_id}`):
   - Provides SSE endpoint with events: `transcription`, `sentiment`, `rag`
   - ✅ Fully compatible

3. **RAG Service**:
   - Used indirectly via Kafka/stream service
   - ✅ Fully compatible

## Feature Parity

| Feature | Streamlit | React | Status |
|---------|-----------|-------|--------|
| Audio recording | ✅ | ✅ | ✅ Complete |
| Interval-based chunking | ✅ | ✅ | ✅ Complete |
| Real-time transcriptions | ✅ | ✅ | ✅ Complete |
| Sentiment visualization | ✅ | ✅ | ✅ Complete |
| RAG suggestions | ✅ | ✅ | ✅ Complete |
| Backend health checks | ✅ | ✅ | ✅ Complete |
| Call ID management | ✅ | ✅ | ✅ Complete |
| Duplicate prevention | ✅ | ✅ | ✅ Complete |

## Audio Format Handling

### Streamlit Version
- Used `audio_recorder_streamlit` which records in WAV format
- Direct upload to backend

### React Version
- Uses browser's `MediaRecorder` API
- Typically records in WebM format (browser-dependent)
- Backend handles format conversion via ffmpeg
- Supports WebM, MP4, and WAV formats

## Setup Differences

### Streamlit
```bash
pip install -r requirements.txt
streamlit run streaming_app.py
```

### React
```bash
npm install
npm run dev
```

## Configuration

### Streamlit
- Environment variables: `RAG_URL`, `AUDIO_SERVICE_URL`, `STREAM_URL`
- Set in shell or `.env` file

### React
- Environment variables: `VITE_AUDIO_SERVICE_URL`, `VITE_STREAM_URL`, `VITE_RAG_URL`
- Set in `.env` file (Vite requires `VITE_` prefix)
- Defaults to `http://localhost:8004`, `http://localhost:8003`, `http://localhost:8002`

## Running Both Versions

You can run both versions simultaneously if needed:
- Streamlit: `http://localhost:8501` (default Streamlit port)
- React: `http://localhost:3000` (configured in vite.config.js)

Both will work with the same backend services.

## Migration Checklist

- [x] Create React project structure
- [x] Implement audio recording with interval-based chunking
- [x] Implement SSE streaming for real-time updates
- [x] Create UI components matching Streamlit functionality
- [x] Add sentiment visualization
- [x] Add RAG suggestions display
- [x] Implement backend health checks
- [x] Add error handling and user feedback
- [x] Create documentation and setup scripts

## Next Steps

1. Test the React frontend with your backend services
2. Update any deployment scripts to use the React build
3. Optionally remove Streamlit dependencies if no longer needed
4. Update main project README to reference React frontend

## Notes

- The React frontend maintains full compatibility with existing backend services
- No changes to backend code are required
- The React version provides a better user experience with faster updates
- Audio format conversion is handled automatically by the backend

