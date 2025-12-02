# Quick Start Guide

## Prerequisites

1. **Node.js 18+** installed
2. **Backend services running**:
   - Audio Service (port 8004)
   - Stream Service (port 8003)
   - RAG Service (port 8002)

## Setup

1. **Install dependencies**:
   ```bash
   npm install
   ```

2. **Configure environment** (optional):
   ```bash
   cp .env.example .env
   # Edit .env if your backend URLs are different
   ```

3. **Start development server**:
   ```bash
   npm run dev
   ```

4. **Open browser**:
   Navigate to `http://localhost:3000`

## Usage

1. **Enter a Call ID** (or use the default generated one)
2. **Click "Start Recording"** to begin audio capture
3. **Speak into your microphone** - audio chunks are automatically sent every 2-3 seconds
4. **View real-time updates**:
   - Transcriptions appear in the left panel
   - Sentiment analysis updates in the right panel
   - Agent suggestions appear below sentiment

## Features

- **Automatic chunking**: Audio is recorded and sent in intervals (2-3 seconds)
- **Real-time streaming**: SSE connection provides live transcriptions, sentiment, and RAG suggestions
- **Duplicate prevention**: Transcriptions are deduplicated by utterance_id
- **Error handling**: Automatic retry and error messages for failed operations

## Troubleshooting

### Microphone not working
- Check browser permissions (click the lock icon in address bar)
- Ensure you're using HTTPS or localhost (browsers require secure context)
- Try a different browser (Chrome/Edge recommended)

### No transcriptions appearing
- Check that Audio Service is running: `curl http://localhost:8004/health`
- Check browser console for errors
- Verify microphone is working in other apps

### SSE connection issues
- Check that Stream Service is running: `curl http://localhost:8003/health`
- Verify the call_id is valid
- Check browser console for CORS errors

### Audio upload failures
- Check audio chunk size (should be < 5MB)
- Verify network connectivity
- Check Audio Service logs for errors

## Development

### Project Structure
```
src/
  ├── components/       # React components
  │   ├── AudioRecorder.jsx
  │   ├── TranscriptionWindow.jsx
  │   ├── SentimentPanel.jsx
  │   └── RAGPanel.jsx
  ├── hooks/           # Custom React hooks
  │   ├── useAudioRecorder.js
  │   ├── useSSEStream.js
  │   └── useAudioUpload.js
  ├── App.jsx          # Main application
  └── main.jsx         # Entry point
```

### Building for Production

```bash
npm run build
```

Output will be in the `dist` directory, ready to be served by any static file server.

### Preview Production Build

```bash
npm run preview
```

