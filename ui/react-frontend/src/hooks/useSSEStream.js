import { useEffect, useRef, useState } from 'react';

/**
 * Custom hook for Server-Sent Events (SSE) streaming
 * Connects to SSE endpoint and handles transcription, sentiment, and RAG events
 */
export function useSSEStream(callId, enabled = true) {
  const [transcriptions, setTranscriptions] = useState([]);
  const [sentimentHistory, setSentimentHistory] = useState([]);
  const [ragSuggestions, setRagSuggestions] = useState([]);
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState(null);
  
  const eventSourceRef = useRef(null);
  const lastTranscriptionIdRef = useRef(null);

  useEffect(() => {
    if (!enabled || !callId) {
      return;
    }

    // Get stream URL from environment or use default
    const streamUrl = import.meta.env.VITE_STREAM_URL || 'http://localhost:8003';
    const url = `${streamUrl}/stream/${callId}`;
    
    console.log('Connecting to SSE stream:', url);
    
    const eventSource = new EventSource(url);
    eventSourceRef.current = eventSource;
    
    eventSource.onopen = () => {
      console.log('SSE connection opened');
      setIsConnected(true);
      setError(null);
    };
    
    eventSource.onerror = (err) => {
      console.error('SSE error:', err);
      setError('Connection error. Retrying...');
      setIsConnected(false);
    };
    
    // Handle transcription events
    eventSource.addEventListener('transcription', (event) => {
      try {
        const data = JSON.parse(event.data);
        const utteranceId = data.utterance_id;
        
        // Avoid duplicates
        if (utteranceId !== lastTranscriptionIdRef.current) {
          const transcription = {
            text: data.text || '',
            utterance_id: utteranceId,
            timestamp: new Date().toLocaleTimeString(),
            index: data.utterance_index || 0,
            sentiment: data.sentiment?.label || 'NEU',
          };
          
          setTranscriptions(prev => {
            const updated = [...prev, transcription];
            // Keep only last 100 transcriptions
            return updated.slice(-100);
          });
          
          lastTranscriptionIdRef.current = utteranceId;
        }
      } catch (err) {
        console.error('Error parsing transcription event:', err);
      }
    });
    
    // Handle sentiment events
    eventSource.addEventListener('sentiment', (event) => {
      try {
        const data = JSON.parse(event.data);
        setSentimentHistory(prev => {
          const updated = [...prev, data];
          // Keep only last 50 windows
          return updated.slice(-50);
        });
      } catch (err) {
        console.error('Error parsing sentiment event:', err);
      }
    });
    
    // Handle RAG events
    eventSource.addEventListener('rag', (event) => {
      try {
        const data = JSON.parse(event.data);
        setRagSuggestions(prev => {
          const updated = [...prev, data];
          // Keep only last 20 suggestions
          return updated.slice(-20);
        });
      } catch (err) {
        console.error('Error parsing RAG event:', err);
      }
    });
    
    // Cleanup on unmount or when dependencies change
    return () => {
      console.log('Closing SSE connection');
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
      setIsConnected(false);
    };
  }, [callId, enabled]);

  const clearData = () => {
    setTranscriptions([]);
    setSentimentHistory([]);
    setRagSuggestions([]);
    lastTranscriptionIdRef.current = null;
  };

  return {
    transcriptions,
    sentimentHistory,
    ragSuggestions,
    isConnected,
    error,
    clearData,
  };
}

