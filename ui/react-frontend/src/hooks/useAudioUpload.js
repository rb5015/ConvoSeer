import { useState, useCallback } from 'react';
import axios from 'axios';

/**
 * Custom hook for uploading audio chunks to the transcription service
 */
export function useAudioUpload() {
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState(null);
  const [lastResponse, setLastResponse] = useState(null);

  const uploadAudio = useCallback(async (audioBlob, callId, utteranceIndex) => {
    if (!audioBlob || audioBlob.size < 100) {
      console.warn('Audio chunk too small:', audioBlob?.size);
      return null;
    }

    // Check maximum size (5MB)
    const MAX_AUDIO_SIZE = 5 * 1024 * 1024;
    if (audioBlob.size > MAX_AUDIO_SIZE) {
      console.warn(`Audio chunk too large: ${(audioBlob.size / (1024 * 1024)).toFixed(2)} MB`);
      return null;
    }

    setUploading(true);
    setError(null);

    try {
      // Get audio service URL from environment or use default
      const audioServiceUrl = import.meta.env.VITE_AUDIO_SERVICE_URL || 'http://localhost:8004';
      
      // Convert blob to File for FormData
      // Backend accepts various formats (webm, mp4, wav) and converts via ffmpeg
      const formData = new FormData();
      
      // Determine file extension and MIME type based on blob type
      let filename = 'audio.webm';
      let mimeType = audioBlob.type || 'audio/webm';
      
      if (audioBlob.type) {
        if (audioBlob.type.includes('webm')) {
          filename = 'audio.webm';
          mimeType = 'audio/webm';
        } else if (audioBlob.type.includes('mp4') || audioBlob.type.includes('m4a')) {
          filename = 'audio.mp4';
          mimeType = audioBlob.type.includes('m4a') ? 'audio/mp4' : audioBlob.type;
        } else if (audioBlob.type.includes('wav')) {
          filename = 'audio.wav';
          mimeType = 'audio/wav';
        } else if (audioBlob.type.includes('ogg')) {
          filename = 'audio.ogg';
          mimeType = 'audio/ogg';
        }
      }
      
      // Create a File object with explicit type to ensure MIME type is preserved
      const audioFile = new File([audioBlob], filename, { type: mimeType });
      formData.append('audio_file', audioFile);
      formData.append('call_id', callId);
      formData.append('speaker_role', 'customer');
      formData.append('utterance_index', utteranceIndex.toString());

      const response = await axios.post(
        `${audioServiceUrl}/transcribe`,
        formData,
        {
          headers: {
            'Content-Type': 'multipart/form-data',
          },
          timeout: 90000, // 90 second timeout
        }
      );

      setLastResponse(response.data);
      return response.data;
    } catch (err) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to upload audio';
      setError(errorMessage);
      console.error('Error uploading audio:', err);
      return null;
    } finally {
      setUploading(false);
    }
  }, []);

  return {
    uploadAudio,
    uploading,
    error,
    lastResponse,
  };
}

