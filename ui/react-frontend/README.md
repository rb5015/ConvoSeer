# ConvoSeer Streaming Frontend

React-based frontend for the ConvoSeer Agent Assist streaming application. This frontend replaces the Streamlit-based UI with a modern React interface that supports real-time audio streaming and SSE updates.

## Features

- 🎤 **Interval-based audio recording**: Records audio in chunks and sends them every 2-3 seconds
- 📡 **Real-time SSE streaming**: Receives live transcriptions, sentiment analysis, and RAG suggestions via Server-Sent Events
- 📊 **Live sentiment visualization**: Real-time sentiment charts and metrics
- 🤖 **Agent suggestions**: Displays RAG-powered agent suggestions
- 🎨 **Modern UI**: Built with React, Tailwind CSS, and Recharts

## Prerequisites

- Node.js 18+ and npm/yarn
- Backend services running:
  - Audio Service (port 8004)
  - Stream Service (port 8003)
  - RAG Service (port 8002)

## Installation

1. Install dependencies:
```bash
npm install
```

2. Configure environment variables (optional):
```bash
cp .env.example .env
# Edit .env with your backend URLs if different from defaults
```

## Development

Start the development server:
```bash
npm run dev
```

The app will be available at `http://localhost:3000`.

## Building for Production

Build the production bundle:
```bash
npm run build
```

The built files will be in the `dist` directory.

Preview the production build:
```bash
npm run preview
```

## Architecture

### Components

- **App.jsx**: Main application component
- **AudioRecorder**: Handles audio recording and chunk upload
- **TranscriptionWindow**: Displays live transcriptions
- **SentimentPanel**: Shows sentiment analysis and charts
- **RAGPanel**: Displays agent suggestions

### Hooks

- **useAudioRecorder**: Manages MediaRecorder API for interval-based recording
- **useSSEStream**: Connects to SSE endpoint and handles real-time updates
- **useAudioUpload**: Handles uploading audio chunks to the transcription service

### Backend Integration

The frontend communicates with three backend services:

1. **Audio Service** (`/transcribe`): Receives audio chunks and returns transcriptions
2. **Stream Service** (`/stream/{call_id}`): SSE endpoint for real-time updates
3. **RAG Service**: Used indirectly via Kafka/stream service

## Audio Format

The frontend records audio using the browser's MediaRecorder API, which typically produces WebM format. The backend service handles format conversion via ffmpeg, so WebM, MP4, and WAV formats are all supported.

## Browser Compatibility

- Chrome/Edge: Full support
- Firefox: Full support
- Safari: Full support (may require user gesture for microphone access)

## Environment Variables

- `VITE_AUDIO_SERVICE_URL`: Audio transcription service URL (default: http://localhost:8004)
- `VITE_STREAM_URL`: SSE streaming service URL (default: http://localhost:8003)
- `VITE_RAG_URL`: RAG service URL (default: http://localhost:8002)

## Troubleshooting

### Microphone not working
- Ensure browser permissions are granted for microphone access
- Check that HTTPS is used (or localhost) - browsers require secure context for microphone access

### SSE connection issues
- Verify the stream service is running and accessible
- Check browser console for CORS errors
- Ensure the call_id is valid

### Audio upload failures
- Check audio service health endpoint
- Verify audio chunk size is within limits (5MB max)
- Check network connectivity to backend services

