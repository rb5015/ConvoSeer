import { useMemo } from 'react';
import { Accordion, AccordionDetails, AccordionSummary, Box, Card, CardContent, Stack, Typography } from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

export function SentimentPanel({ sentimentHistory }) {
  const latestSentiment = sentimentHistory[sentimentHistory.length - 1];

  const getSentimentLabel = (score) => {
    if (score > 0.1) return 'Positive';
    if (score < -0.1) return 'Negative';
    return 'Neutral';
  };

  const getSentimentColor = (score) => {
    if (score > 0.1) return '#22c55e';
    if (score < -0.1) return '#f87171';
    return '#fbbf24';
  };

  const chartData = useMemo(
    () =>
      sentimentHistory.map((item, index) => ({
        time: index + 1,
        sentiment: item.avg_sentiment_score || 0.0,
      })),
    [sentimentHistory],
  );

  if (!latestSentiment) {
    return (
      <Card>
        <CardContent>
          <Typography variant="body2" color="text.secondary">
            Awaiting sentiment data... updates arrive every ~10 seconds.
          </Typography>
        </CardContent>
      </Card>
    );
  }

  const score = latestSentiment.avg_sentiment_score || 0;
  const sentimentColor = getSentimentColor(score);

  return (
    <Card sx={{ height: '100%', width: '100%', overflow: 'hidden' }}>
      <CardContent>
        <Stack spacing={3}>
          <Stack direction="row" alignItems="center" justifyContent="space-between">
            <Stack>
              <Typography variant="subtitle2" color="text.secondary">
                Current sentiment
              </Typography>
              <Typography variant="h6" sx={{ color: sentimentColor, fontWeight: 600 }}>
                {score > 0 ? '+' : ''}
                {score.toFixed(2)}
              </Typography>
            </Stack>
            <Typography
              variant="button"
              sx={{
                px: 2,
                py: 0.75,
                borderRadius: 10,
                backgroundColor: sentimentColor,
                color: '#030712',
              }}
            >
              {getSentimentLabel(score)}
            </Typography>
          </Stack>

          {chartData.length > 1 && (
            <Box sx={{ width: '100%', height: 260 }}>
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData}>
                  <CartesianGrid opacity={0.3} strokeDasharray="3 3" />
                  <XAxis dataKey="time" stroke="#94a3b8" />
                  <YAxis domain={[-1, 1]} stroke="#94a3b8" />
                  <Tooltip />
                  <Line type="monotone" dataKey="sentiment" stroke="#94a3b8" strokeWidth={2} dot={{ r: 3 }} />
                </LineChart>
              </ResponsiveContainer>
            </Box>
          )}

          <Accordion sx={{ bgcolor: 'rgba(148, 163, 184, 0.08)', boxShadow: 'none' }}>
            <AccordionSummary expandIcon={<ExpandMoreIcon sx={{ color: 'text.secondary' }} />}>
              <Typography variant="body2" color="text.secondary">
                📈 Latest window details
              </Typography>
            </AccordionSummary>
            <AccordionDetails>
              <Stack spacing={0.5}>
                <Typography variant="caption" color="text.primary">
                  <strong>Window:</strong>{' '}
                  {latestSentiment.window_start || 'N/A'} - {latestSentiment.window_end || 'N/A'}
                </Typography>
                <Typography variant="caption" color="text.primary">
                  <strong>Utterances:</strong> {latestSentiment.utterance_count || 0}
                </Typography>
                <Typography variant="caption" color="text.primary">
                  <strong>Score:</strong> {score.toFixed(3)}
                </Typography>
              </Stack>
            </AccordionDetails>
          </Accordion>
        </Stack>
      </CardContent>
    </Card>
  );
}

