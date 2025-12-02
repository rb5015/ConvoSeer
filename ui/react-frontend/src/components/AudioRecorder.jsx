import { useEffect, useState } from 'react';
import { useAudioRecorder } from '../hooks/useAudioRecorder';
import { useAudioUpload } from '../hooks/useAudioUpload';

export function AudioRecorder({ callId, onTranscription, onRecordingChange }) {
  const [utteranceIndex, setUtteranceIndex] = useState(0);
  const [statusMessage, setStatusMessage] = useState('');
  const [lastSentHash, setLastSentHash] = useState(null);
  
  // Use 3 seconds interval to capture more audio per chunk
  const { isRecording, audioBlob, error: recorderError, startRecording, stopRecording } = useAudioRecorder(3000);
  const { uploadAudio, uploading, error: uploadError } = useAudioUpload();

  // Notify parent of recording state changes
  useEffect(() => {
    if (onRecordingChange) {
      onRecordingChange(isRecording);
    }
  }, [isRecording, onRecordingChange]);

  // Handle audio blob changes and upload
  useEffect(() => {
    if (!isRecording || !audioBlob || uploading) return;

    // Minimum size check - for 2.5 seconds of WebM audio, expect at least 5KB
    const MIN_CHUNK_SIZE = 5000; // 5KB minimum for meaningful audio
    if (audioBlob.size < MIN_CHUNK_SIZE) {
      console.log(`⚠️ Audio chunk too small to send: ${audioBlob.size} bytes`);
      return;
    }

    const processAudio = async () => {
      // Create a hash to detect if audio has changed
      const hash = `${audioBlob.size}`;
      
      // Only upload if audio has changed (size increased)
      if (hash === lastSentHash) {
        return; // Same chunk, skip
      }
      
      setLastSentHash(hash);
      
      const audioSizeMB = audioBlob.size / (1024 * 1024);
      setStatusMessage(`🔄 Sending ${audioBlob.size.toLocaleString()} bytes (${audioSizeMB.toFixed(2)} MB) to backend...`);
      
      try {
        const result = await uploadAudio(audioBlob, callId, utteranceIndex);
        
        if (result && result.published && result.text) {
          // Add transcription directly to UI
          const transcription = {
            text: result.text,
            utterance_id: result.utterance_id || `${callId}:${utteranceIndex}`,
            timestamp: new Date().toLocaleTimeString(),
            index: utteranceIndex,
            sentiment: 'NEU',
          };
          
          onTranscription(transcription);
          setUtteranceIndex(prev => prev + 1);
          setStatusMessage(`✓ Sent: "${result.text.substring(0, 50)}${result.text.length > 50 ? '...' : ''}"`);
          
          // Clear status message after 3 seconds
          setTimeout(() => {
            setStatusMessage('');
          }, 3000);
        } else if (result && result.text) {
          setStatusMessage('⚠️ Transcribed but not published to Kafka');
        } else if (result === null) {
          // Upload failed
          setStatusMessage('❌ Failed to upload audio. Will retry with next chunk...');
        } else if (result && !result.text) {
          // No speech detected in this chunk
          const audioSizeKB = (audioBlob.size / 1024).toFixed(1);
          setStatusMessage(`ℹ️ No speech detected in ${audioSizeKB}KB chunk. Keep speaking...`);
        } else {
          setStatusMessage('ℹ️ Processing audio...');
        }
      } catch (err) {
        console.error('Error processing audio:', err);
        // Don't set error status immediately - will retry with next chunk
        setStatusMessage('ℹ️ Processing...');
      }
    };

    processAudio();
  }, [audioBlob, isRecording, callId, utteranceIndex, lastSentHash, uploadAudio, onTranscription, uploading]);

  const handleStart = () => {
    setUtteranceIndex(0);
    setLastSentHash(null);
    setStatusMessage('');
    startRecording();
  };

  const handleStop = () => {
    stopRecording();
    setStatusMessage('');
  };

  const error = recorderError || uploadError;

  return (
    <div className="space-y-4">
      <div className="flex gap-4 items-center">
        {!isRecording ? (
          <button
            onClick={handleStart}
            className="flex-1 bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 px-6 rounded-lg transition-colors"
          >
            🎤 Start Recording
          </button>
        ) : (
          <button
            onClick={handleStop}
            className="flex-1 bg-red-600 hover:bg-red-700 text-white font-semibold py-3 px-6 rounded-lg transition-colors"
          >
            ⏹️ Stop Recording
          </button>
        )}
        {isRecording && (
          <div className="text-sm text-gray-600">
            Index: {utteranceIndex}
          </div>
        )}
      </div>

      {isRecording && (
        <>
          <p className="text-sm text-gray-600">
            🎤 Recording... Speak clearly. Audio chunks are automatically sent every 3 seconds.
          </p>
          
          {audioBlob && (
            <p className="text-sm text-gray-500">
              📊 Audio buffer: {audioBlob.size.toLocaleString()} bytes ({(audioBlob.size / (1024 * 1024)).toFixed(2)} MB)
            </p>
          )}
          
          {statusMessage && (
            <p className={`text-sm ${
              statusMessage.startsWith('✓') ? 'text-green-600' :
              statusMessage.startsWith('⚠️') ? 'text-yellow-600' :
              statusMessage.startsWith('❌') ? 'text-red-600' :
              'text-blue-600'
            }`}>
              {statusMessage}
            </p>
          )}
          
          {uploading && (
            <div className="flex items-center gap-2 text-sm text-blue-600">
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-600"></div>
              Uploading...
            </div>
          )}
        </>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
          {error}
        </div>
      )}
    </div>
  );
}

