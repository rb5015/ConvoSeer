export function RAGPanel({ ragSuggestions }) {
  const latestRAG = ragSuggestions[ragSuggestions.length - 1];
  const ragResponse = latestRAG?.rag_response || {};

  return (
    <div className="space-y-4">
      {latestRAG ? (
        <>
          <div className="bg-gradient-to-br from-purple-500 to-purple-700 text-white p-6 rounded-lg shadow-xl border-2 border-purple-600">
            <h4 className="text-xl font-bold mb-3">💡 Latest Suggestion</h4>
            <p className="text-xl leading-relaxed">{ragResponse.suggestion || 'No suggestion'}</p>
          </div>

          {ragResponse.alternatives && ragResponse.alternatives.length > 0 && (
            <div className="bg-white p-5 rounded-lg border-2 border-gray-300 shadow-md">
              <h5 className="font-bold text-gray-800 mb-3 text-lg">Alternatives:</h5>
              <ul className="list-disc list-inside space-y-2 text-gray-700 text-base">
                {ragResponse.alternatives.map((alt, index) => (
                  <li key={index}>{alt}</li>
                ))}
              </ul>
            </div>
          )}

          <details className="bg-white p-4 rounded-lg border border-gray-200 shadow-sm">
            <summary className="cursor-pointer text-sm font-semibold text-gray-700">
              🔍 Context
            </summary>
            <div className="mt-3 space-y-2 text-sm text-gray-600">
              {(latestRAG.query_text || latestRAG.latest_utterance) && (
                <p>
                  <strong>Query:</strong>{' '}
                  {(latestRAG.query_text || latestRAG.latest_utterance).length > 80
                    ? `${(latestRAG.query_text || latestRAG.latest_utterance).substring(0, 80)}...`
                    : (latestRAG.query_text || latestRAG.latest_utterance)}
                </p>
              )}
              <p>
                <strong>Retrieved:</strong> {ragResponse.retrieved_count || 0} similar conversations
              </p>
              {latestRAG.sentiment && (
                <p>
                  <strong>Sentiment:</strong> {latestRAG.sentiment.avg_score?.toFixed(2) || '0.00'}
                </p>
              )}
              {latestRAG.window_start && latestRAG.window_end && (
                <p className="text-xs text-gray-500">
                  <strong>Window:</strong> {new Date(latestRAG.window_start).toLocaleTimeString()} - {new Date(latestRAG.window_end).toLocaleTimeString()}
                </p>
              )}
            </div>
          </details>
        </>
      ) : (
        <div className="bg-blue-50 p-4 rounded-lg border border-blue-200">
          <p className="text-blue-800">Waiting for suggestions...</p>
          <p className="text-sm text-blue-600 mt-1">Suggestions appear every ~10 seconds after transcription</p>
        </div>
      )}
    </div>
  );
}

