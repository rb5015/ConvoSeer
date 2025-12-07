import { useEffect, useRef } from 'react';

export function TranscriptionWindow({ transcriptions }) {
  const scrollRef = useRef(null);

  // Auto-scroll to bottom when new transcriptions arrive
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [transcriptions]);

  const getSentimentClass = (sentiment) => {
    switch (sentiment) {
      case 'POS':
        return 'border-l-green-500';
      case 'NEG':
        return 'border-l-red-500';
      default:
        return 'border-l-orange-500';
    }
  };

  return (
    <div className="bg-gray-50 border-2 border-gray-300 rounded-lg p-4 max-h-[300px] overflow-y-auto transcription-window" ref={scrollRef}>
      {transcriptions.length > 0 ? (
        transcriptions.map((trans, index) => (
          <div
            key={`${trans.utterance_id}-${index}`}
            className={`p-3 my-2 border-l-4 bg-white rounded shadow-sm hover:shadow-md transition-shadow ${getSentimentClass(trans.sentiment)}`}
          >
            <strong className="text-gray-600">[{trans.timestamp}]</strong>{' '}
            <span className="text-gray-800">{trans.text}</span>
          </div>
        ))
      ) : (
        <div className="py-8 text-center text-gray-500">
          No transcriptions yet. Start recording to begin.
          <br />
          <small>Transcriptions will appear here as audio is processed...</small>
        </div>
      )}
    </div>
  );
}

