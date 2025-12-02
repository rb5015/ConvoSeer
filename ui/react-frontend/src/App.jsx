import { useState, useEffect } from 'react';
import { useSSEStream } from './hooks/useSSEStream';
import { AudioRecorder } from './components/AudioRecorder';
import { TranscriptionWindow } from './components/TranscriptionWindow';
import { SentimentPanel } from './components/SentimentPanel';
import { RAGPanel } from './components/RAGPanel';
import axios from 'axios';

function App() {
  const [callId, setCallId] = useState(`call-${Math.floor(Date.now() / 1000)}`);
  const [isRecording, setIsRecording] = useState(false);
  const [backendStatus, setBackendStatus] = useState({ audio: false, stream: false });
  const [localTranscriptions, setLocalTranscriptions] = useState([]);
  
  // SSE stream hook
  const {
    transcriptions: sseTranscriptions,
    sentimentHistory,
    ragSuggestions,
    isConnected: streamConnected,
    error: streamError,
    clearData,
  } = useSSEStream(callId, isRecording);

  // Combine local and SSE transcriptions, removing duplicates by utterance_id
  const allTranscriptionsMap = new Map();
  
  // Add local transcriptions first
  localTranscriptions.forEach(trans => {
    if (trans.utterance_id) {
      allTranscriptionsMap.set(trans.utterance_id, trans);
    }
  });
  
  // Add SSE transcriptions (they may override local ones if same ID)
  sseTranscriptions.forEach(trans => {
    if (trans.utterance_id) {
      allTranscriptionsMap.set(trans.utterance_id, trans);
    }
  });
  
  // Convert to array and sort by index, then slice to last 100
  const allTranscriptions = Array.from(allTranscriptionsMap.values())
    .sort((a, b) => (a.index || 0) - (b.index || 0))
    .slice(-100);

  // Check backend health
  useEffect(() => {
    const checkHealth = async () => {
      const audioServiceUrl = import.meta.env.VITE_AUDIO_SERVICE_URL || 'http://localhost:8004';
      const streamUrl = import.meta.env.VITE_STREAM_URL || 'http://localhost:8003';
      
      try {
        const [audioRes, streamRes] = await Promise.all([
          axios.get(`${audioServiceUrl}/health`, { timeout: 2000 }),
          axios.get(`${streamUrl}/health`, { timeout: 2000 }),
        ]);
        
        setBackendStatus({
          audio: audioRes.status === 200,
          stream: streamRes.status === 200,
        });
      } catch (err) {
        setBackendStatus({ audio: false, stream: false });
      }
    };

    checkHealth();
    const interval = setInterval(checkHealth, 10000); // Check every 10 seconds
    return () => clearInterval(interval);
  }, []);

  const handleTranscription = (transcription) => {
    setLocalTranscriptions(prev => [...prev, transcription].slice(-100));
  };

  const handleClear = () => {
    setLocalTranscriptions([]);
    clearData();
  };

  const getStatusIndicator = () => {
    if (isRecording) {
      return (
        <div className="bg-red-500 text-white px-4 py-2 rounded-lg font-semibold pulse-animation">
          🔴 RECORDING
        </div>
      );
    } else if (backendStatus.audio && backendStatus.stream) {
      return (
        <div className="bg-green-500 text-white px-4 py-2 rounded-lg font-semibold">
          ✅ Connected
        </div>
      );
    } else {
      return (
        <div className="bg-gray-500 text-white px-4 py-2 rounded-lg font-semibold">
          ⚪ Idle
        </div>
      );
    }
  };

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Header */}
      <div className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <h1 className="text-3xl font-bold bg-gradient-to-r from-purple-500 to-purple-700 bg-clip-text text-transparent mb-4">
            🎙️ Agent Assist - Live Streaming
          </h1>
          
          {/* Top bar */}
          <div className="grid grid-cols-4 gap-4 items-center">
            <div className="col-span-2">
              <input
                type="text"
                value={callId}
                onChange={(e) => setCallId(e.target.value)}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                placeholder="Call ID"
              />
            </div>
            <div>
              {getStatusIndicator()}
            </div>
            <div className="flex items-center gap-4">
              <div className="text-sm text-gray-600">
                Utterances: <span className="font-semibold">{allTranscriptions.length}</span>
              </div>
              <button
                onClick={handleClear}
                className="px-4 py-2 bg-gray-200 hover:bg-gray-300 rounded-lg font-semibold transition-colors"
              >
                🔄 Clear
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Main content */}
      <div className="max-w-7xl mx-auto px-6 py-6">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Left column - Transcription */}
          <div className="lg:col-span-2 space-y-4">
            <h2 className="text-xl font-semibold text-gray-800">📝 Live Transcription</h2>
            
            <AudioRecorder 
              callId={callId} 
              onTranscription={handleTranscription}
              onRecordingChange={setIsRecording}
            />
            
            <TranscriptionWindow transcriptions={allTranscriptions} />
          </div>

          {/* Right columns - Sentiment and RAG */}
          <div className="space-y-6">
            <div>
              <h2 className="text-xl font-semibold text-gray-800 mb-4">📊 Sentiment Analysis</h2>
              <SentimentPanel sentimentHistory={sentimentHistory} />
            </div>

            <div>
              <h2 className="text-xl font-semibold text-gray-800 mb-4">🤖 Agent Suggestions</h2>
              <RAGPanel ragSuggestions={ragSuggestions} />
            </div>
          </div>
        </div>
      </div>

      {/* Footer */}
      <div className="mt-8 border-t border-gray-200 bg-white">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="grid grid-cols-3 gap-4 text-sm">
            <div>
              <strong>Audio Service:</strong>{' '}
              <span className={backendStatus.audio ? 'text-green-600' : 'text-red-600'}>
                {backendStatus.audio ? '✅' : '❌'}
              </span>
            </div>
            <div>
              <strong>Stream Service:</strong>{' '}
              <span className={backendStatus.stream ? 'text-green-600' : 'text-red-600'}>
                {backendStatus.stream ? '✅' : '❌'}
              </span>
              {streamConnected && (
                <span className="ml-2 text-green-600">(Connected)</span>
              )}
              {streamError && (
                <span className="ml-2 text-yellow-600">({streamError})</span>
              )}
            </div>
            <div>
              <strong>Call ID:</strong> {callId}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;

