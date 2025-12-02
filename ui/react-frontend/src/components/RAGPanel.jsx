export function RAGPanel({ ragSuggestions }) {
  const latestRAG = ragSuggestions[ragSuggestions.length - 1];
  const ragResponse = latestRAG?.rag_response || {};

  return (
    <div className="space-y-4">
      {latestRAG ? (
        <>
          <div className="bg-gradient-to-br from-purple-500 to-purple-700 text-white p-6 rounded-lg shadow-lg">
            <h4 className="text-lg font-semibold mb-2">💡 Latest Suggestion</h4>
            <p className="text-lg">{ragResponse.suggestion || 'No suggestion'}</p>
          </div>

          {ragResponse.alternatives && ragResponse.alternatives.length > 0 && (
            <div className="bg-white p-4 rounded-lg border border-gray-200 shadow-sm">
              <h5 className="font-semibold text-gray-700 mb-2">Alternatives:</h5>
              <ul className="list-disc list-inside space-y-1 text-gray-600">
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
              {latestRAG.latest_utterance && (
                <p>
                  <strong>Latest:</strong>{' '}
                  {latestRAG.latest_utterance.length > 80
                    ? `${latestRAG.latest_utterance.substring(0, 80)}...`
                    : latestRAG.latest_utterance}
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

