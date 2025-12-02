import { useState, useRef, useCallback, useEffect } from 'react';

/**
 * Custom hook for interval-based audio recording
 * Records audio in chunks and sends them at specified intervals
 */
export function useAudioRecorder(intervalMs = 2500) {
  const [isRecording, setIsRecording] = useState(false);
  const [audioBlob, setAudioBlob] = useState(null);
  const [error, setError] = useState(null);
  
  const mediaRecorderRef = useRef(null);
  const audioChunksRef = useRef([]);
  const streamRef = useRef(null);
  const dataRequestIntervalRef = useRef(null);
  const processIntervalRef = useRef(null);
  const lastSentHashRef = useRef(null);
  const mimeTypeRef = useRef(null);
  const recorderOptionsRef = useRef(null);

  const startRecording = useCallback(async () => {
    try {
      setError(null);
      
      // Request microphone access
      const stream = await navigator.mediaDevices.getUserMedia({ 
        audio: {
          channelCount: 1,
          sampleRate: 16000,
          echoCancellation: true,
          noiseSuppression: true,
        } 
      });
      
      streamRef.current = stream;
      
      // Create MediaRecorder with WAV format if supported, otherwise use default
      const options = {
        mimeType: 'audio/webm;codecs=opus', // Most browsers support this
      };
      
      // Try to find a supported MIME type
      if (MediaRecorder.isTypeSupported('audio/webm')) {
        options.mimeType = 'audio/webm';
      } else if (MediaRecorder.isTypeSupported('audio/webm;codecs=opus')) {
        options.mimeType = 'audio/webm;codecs=opus';
      } else if (MediaRecorder.isTypeSupported('audio/mp4')) {
        options.mimeType = 'audio/mp4';
      }
      
      const mediaRecorder = new MediaRecorder(stream, options);
      mediaRecorderRef.current = mediaRecorder;
      audioChunksRef.current = [];
      mimeTypeRef.current = options.mimeType; // Store MIME type for blob creation
      recorderOptionsRef.current = options; // Store options for restarting
      
      mediaRecorder.ondataavailable = (event) => {
        if (event.data && event.data.size > 0) {
          audioChunksRef.current.push(event.data);
        }
      };
      
      mediaRecorder.onerror = (event) => {
        console.error('MediaRecorder error:', event);
        setError('Recording error occurred');
      };
      
      // Handle when recording stops (we get complete blob)
      mediaRecorder.onstop = () => {
        // Get the complete blob
        if (audioChunksRef.current.length > 0) {
          const completeBlob = new Blob(audioChunksRef.current, { type: mimeTypeRef.current });
          
          // Minimum size check - for 2.5 seconds of WebM audio, expect at least 5KB
          // WebM compressed audio at 16kHz should be roughly 5-20KB for 2-3 seconds
          const MIN_CHUNK_SIZE = 5000; // 5KB minimum for meaningful audio
          if (completeBlob.size >= MIN_CHUNK_SIZE) {
            // Create a hash to detect if audio has changed
            const hash = `${completeBlob.size}`;
            
            // Only update if audio has changed (size increased)
            if (hash !== lastSentHashRef.current && completeBlob.size > (lastSentHashRef.current ? parseInt(lastSentHashRef.current) : 0)) {
              lastSentHashRef.current = hash;
              setAudioBlob(completeBlob);
              console.log(`📊 Audio chunk ready: ${completeBlob.size} bytes (${(completeBlob.size / 1024).toFixed(2)} KB)`);
            }
          } else {
            console.log(`⚠️ Audio chunk too small: ${completeBlob.size} bytes (${(completeBlob.size / 1024).toFixed(2)} KB), skipping`);
          }
          
          // Clear chunks for next recording
          audioChunksRef.current = [];
        }
        
        // Restart recording if we're still supposed to be recording
        const currentRecorder = mediaRecorderRef.current;
        if (currentRecorder && currentRecorder._shouldContinue !== false && streamRef.current) {
          // Small delay to ensure stop event completes
          setTimeout(() => {
            const stillRecording = mediaRecorderRef.current && mediaRecorderRef.current._shouldContinue !== false;
            if (stillRecording && streamRef.current && recorderOptionsRef.current) {
              try {
                // Create new MediaRecorder with same stream and options
                const newRecorder = new MediaRecorder(streamRef.current, recorderOptionsRef.current);
                newRecorder.ondataavailable = mediaRecorder.ondataavailable;
                newRecorder.onstop = mediaRecorder.onstop;
                newRecorder.onerror = mediaRecorder.onerror;
                newRecorder._shouldContinue = true;
                newRecorder.start();
                mediaRecorderRef.current = newRecorder;
              } catch (err) {
                console.error('Error restarting recorder:', err);
                setError(`Failed to restart recording: ${err.message}`);
                setIsRecording(false);
              }
            }
          }, 100);
        }
      };
      
      // Start recording - we'll stop and restart to get complete blobs
      // This ensures WebM files are complete and valid
      mediaRecorder.start();
      setIsRecording(true);
      
      // Set up interval to capture complete audio chunks
      processIntervalRef.current = setInterval(() => {
        if (mediaRecorderRef.current && mediaRecorderRef.current.state === 'recording') {
          // Stop recording to get complete blob (onstop will restart it)
          mediaRecorderRef.current.stop();
        }
      }, intervalMs);
      
      // Store shouldContinueRecording flag for cleanup
      mediaRecorderRef.current._shouldContinue = true;
      
    } catch (err) {
      console.error('Error starting recording:', err);
      setError(`Failed to start recording: ${err.message}`);
      setIsRecording(false);
    }
  }, [intervalMs]);

  const stopRecording = useCallback(() => {
    // Mark that we should stop continuing to record
    if (mediaRecorderRef.current) {
      mediaRecorderRef.current._shouldContinue = false;
    }
    
    if (processIntervalRef.current) {
      clearInterval(processIntervalRef.current);
      processIntervalRef.current = null;
    }
    
    if (mediaRecorderRef.current && mediaRecorderRef.current.state !== 'inactive') {
      mediaRecorderRef.current.stop();
    }
    
    if (streamRef.current) {
      streamRef.current.getTracks().forEach(track => track.stop());
      streamRef.current = null;
    }
    
    setIsRecording(false);
    setAudioBlob(null);
    lastSentHashRef.current = null;
  }, []);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      stopRecording();
    };
  }, [stopRecording]);

  return {
    isRecording,
    audioBlob,
    error,
    startRecording,
    stopRecording,
  };
}

