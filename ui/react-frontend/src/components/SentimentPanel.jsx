import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

export function SentimentPanel({ sentimentHistory }) {
  const latestSentiment = sentimentHistory[sentimentHistory.length - 1];
  
  const getSentimentLabel = (score) => {
    if (score > 0.2) return 'Positive';
    if (score < -0.2) return 'Negative';
    return 'Neutral';
  };

  const getSentimentColor = (score) => {
    if (score > 0.2) return '#4CAF50';
    if (score < -0.2) return '#f44336';
    return '#ff9800';
  };

  const chartData = sentimentHistory.slice(-20).map((item, index) => ({
    time: index,
    sentiment: item.avg_sentiment_score || 0.0,
  }));

  return (
    <div className="space-y-4">
      {latestSentiment ? (
        <>
          <div className="bg-white p-6 rounded-lg border-2 border-gray-300 shadow-md">
            <div className="flex items-center justify-between mb-3">
              <span className="text-base font-semibold text-gray-700">Current Sentiment</span>
              <span
                className="px-4 py-2 rounded-lg text-white font-bold text-lg"
                style={{ backgroundColor: getSentimentColor(latestSentiment.avg_sentiment_score) }}
              >
                {getSentimentLabel(latestSentiment.avg_sentiment_score)}
              </span>
            </div>
            <div className="mt-3 text-4xl font-bold" style={{ color: getSentimentColor(latestSentiment.avg_sentiment_score) }}>
              {latestSentiment.avg_sentiment_score > 0 ? '+' : ''}
              {latestSentiment.avg_sentiment_score.toFixed(2)}
            </div>
          </div>

          {chartData.length > 1 && (
            <div className="bg-white p-4 rounded-lg border border-gray-200 shadow-sm">
              <h4 className="text-sm font-semibold text-gray-700 mb-3">Sentiment Trend</h4>
              <ResponsiveContainer width="100%" height={280}>
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="time" />
                  <YAxis domain={[-1, 1]} />
                  <Tooltip />
                  <Line
                    type="monotone"
                    dataKey="sentiment"
                    stroke="#667eea"
                    strokeWidth={2}
                    dot={{ r: 3 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}

          <details className="bg-white p-4 rounded-lg border border-gray-200 shadow-sm">
            <summary className="cursor-pointer text-sm font-semibold text-gray-700">
              📈 Latest Window Details
            </summary>
            <div className="mt-3 space-y-1 text-sm text-gray-600">
              <p>
                <strong>Window:</strong> {latestSentiment.window_start || 'N/A'} - {latestSentiment.window_end || 'N/A'}
              </p>
              <p>
                <strong>Utterances:</strong> {latestSentiment.utterance_count || 0}
              </p>
              <p>
                <strong>Score:</strong> {latestSentiment.avg_sentiment_score?.toFixed(3) || '0.000'}
              </p>
            </div>
          </details>
        </>
      ) : (
        <div className="bg-blue-50 p-4 rounded-lg border border-blue-200">
          <p className="text-blue-800">Waiting for sentiment data...</p>
          <p className="text-sm text-blue-600 mt-1">Sentiment analysis appears every ~10 seconds</p>
        </div>
      )}
    </div>
  );
}

