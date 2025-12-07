import { useEffect, useRef, useState } from 'react';
import { useAudioRecorder } from '../hooks/useAudioRecorder';
import { useAudioUpload } from '../hooks/useAudioUpload';
import {
  Alert,
  Button,
  Chip,
  CircularProgress,
  Stack,
  Typography,
} from '@mui/material';

export function AudioRecorder({ callId, onTranscription, onRecordingChange }) {
  const [utteranceIndex, setUtteranceIndex] = useState(0);
  const [statusMessage, setStatusMessage] = useState('');
  const lastSentBlobRef = useRef(null);

  const { isRecording, audioBlob, error: recorderError, startRecording, stopRecording } =
    useAudioRecorder(6000);
  const { uploadAudio, uploading, error: uploadError } = useAudioUpload();

  useEffect(() => {
    if (onRecordingChange) {
      onRecordingChange(isRecording);
    }
  }, [isRecording, onRecordingChange]);

  useEffect(() => {
    if (!isRecording || !audioBlob) {
      return;
    }

    if (uploading) {
      console.log('⏳ Upload in progress, waiting...');
      return;
    }

    const MIN_CHUNK_SIZE = 10000;
    if (audioBlob.size < MIN_CHUNK_SIZE) {
      console.log('⚠️ Audio chunk too small to send');
      return;
    }

    const processAudio = async () => {
      if (audioBlob === lastSentBlobRef.current) {
        return;
      }

      lastSentBlobRef.current = audioBlob;
      setStatusMessage(`🔄 Sending ${(audioBlob.size / 1024).toFixed(1)} KB chunk...`);

      try {
        const result = await uploadAudio(audioBlob, callId, utteranceIndex);

        if (result && result.published && result.text) {
          const transcription = {
            text: result.text,
            utterance_id: result.utterance_id || `${callId}:${utteranceIndex}`,
            timestamp: new Date().toLocaleTimeString(),
            index: utteranceIndex,
            sentiment: 'NEU',
          };

          onTranscription(transcription);
          setUtteranceIndex((prev) => prev + 1);
          setStatusMessage(
            `✓ Sent: "${result.text.substring(0, 60)}${result.text.length > 60 ? '...' : ''}"`,
          );
          setTimeout(() => setStatusMessage(''), 3000);
        } else if (result && result.text) {
          setStatusMessage('⚠️ Transcribed but not published to Kafka');
        } else if (result === null) {
          setStatusMessage('❌ Failed to upload audio. Retrying with next chunk...');
        } else if (result && !result.text) {
          setStatusMessage('ℹ️ No speech detected in this chunk. Keep speaking...');
        } else {
          setStatusMessage('ℹ️ Processing audio...');
        }
      } catch (err) {
        console.error('Error processing audio:', err);
        setStatusMessage('ℹ️ Processing...');
      }
    };

    processAudio();
  }, [audioBlob, callId, isRecording, utteranceIndex, uploadAudio, uploading, onTranscription]);

  const handleStart = () => {
    setUtteranceIndex(0);
    lastSentBlobRef.current = null;
    setStatusMessage('');
    startRecording();
  };

  const handleStop = () => {
    stopRecording();
    setStatusMessage('');
  };

  const error = recorderError || uploadError;

  const determineStatusColor = () => {
    if (!statusMessage) {
      return 'text.secondary';
    }

    if (statusMessage.startsWith('✓')) return 'success.main';
    if (statusMessage.startsWith('⚠️')) return 'warning.main';
    if (statusMessage.startsWith('❌')) return 'error.main';
    return 'text.primary';
  };

  return (
    <Stack spacing={3}>
      <Stack
        direction={{ xs: 'column', sm: 'row' }}
        spacing={2}
        alignItems="center"
        justifyContent="space-between"
      >
        {!isRecording ? (
          <Button fullWidth variant="contained" color="primary" onClick={handleStart}>
            🎤 Start recording
          </Button>
        ) : (
          <Button fullWidth variant="contained" color="error" onClick={handleStop}>
            ⏹️ Stop recording
          </Button>
        )}
        {isRecording && (
          <Chip label={`Index: ${utteranceIndex}`} color="secondary" variant="outlined" />
        )}
      </Stack>

      {isRecording && (
        <Stack spacing={1} px={0.5}>
          <Typography variant="body2" color="text.secondary">
            🎤 Recording... Audio chunks ship every ~6 seconds while you speak.
          </Typography>
          {audioBlob && (
            <Typography variant="body2" color="text.secondary">
              📊 Buffer: {audioBlob.size.toLocaleString()} B (
              {(audioBlob.size / (1024 * 1024)).toFixed(2)} MB)
            </Typography>
          )}
          {statusMessage && (
            <Typography variant="body2" color={determineStatusColor()}>
              {statusMessage}
            </Typography>
          )}
          {uploading && (
            <Stack direction="row" spacing={1} alignItems="center">
              <CircularProgress size={16} />
              <Typography variant="body2" color="text.secondary">
                Uploading...
              </Typography>
            </Stack>
          )}
        </Stack>
      )}

      {error && (
        <Alert severity="error" variant="outlined">
          {error}
        </Alert>
      )}
    </Stack>
  );
}
