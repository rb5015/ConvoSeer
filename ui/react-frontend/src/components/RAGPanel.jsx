import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Card,
  CardContent,
  Divider,
  Stack,
  Typography,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

export function RAGPanel({ ragSuggestions }) {
  const latestRAG = ragSuggestions[ragSuggestions.length - 1];
  const ragResponse = latestRAG?.rag_response || {};

  if (!latestRAG) {
    return (
      <Card>
        <CardContent>
          <Typography variant="body2" color="text.secondary">
            Suggestions appear every ~10 seconds once transcription data flows to the RAG worker.
          </Typography>
        </CardContent>
      </Card>
    );
  }

  const contextSnippet =
    (latestRAG.query_text || latestRAG.latest_utterance || '').length > 80
      ? `${(latestRAG.query_text || latestRAG.latest_utterance).substring(0, 80)}...`
      : latestRAG.query_text || latestRAG.latest_utterance || 'N/A';

  return (
    <Card sx={{ height: '100%' }}>
      <CardContent>
        <Stack spacing={3}>
          <Box
            sx={{
              p: 3,
              borderRadius: 3,
              background: 'linear-gradient(135deg, #1e1b3b 0%, #403c7b 100%)',
              color: '#f8fafc',
            }}
          >
            <Typography variant="subtitle2" sx={{ letterSpacing: 0.5 }}>
              💡 Latest suggestion
            </Typography>
            <Typography variant="body1" sx={{ mt: 1 }}>
              {ragResponse.suggestion || 'No suggestion available yet.'}
            </Typography>
          </Box>

          {ragResponse.alternatives && ragResponse.alternatives.length > 0 && (
            <Box>
              <Typography variant="subtitle2" color="text.secondary">
                Alternatives
              </Typography>
              <Stack
                component="ol"
                spacing={1}
                sx={{
                  mt: 1,
                  pl: 3,
                  color: 'text.primary',
                }}
              >
                {ragResponse.alternatives.map((alt, index) => (
                  <Typography component="li" key={index} variant="body2">
                    {alt}
                  </Typography>
                ))}
              </Stack>
            </Box>
          )}

          <Accordion sx={{ bgcolor: 'rgba(148, 163, 184, 0.08)', boxShadow: 'none' }}>
            <AccordionSummary expandIcon={<ExpandMoreIcon sx={{ color: 'text.secondary' }} />}>
              <Typography variant="body2" color="text.secondary">
                🔍 Context
              </Typography>
            </AccordionSummary>
            <AccordionDetails>
              <Stack spacing={1}>
                <Typography variant="caption" color="text.primary">
                  <strong>Query:</strong> {contextSnippet}
                </Typography>
                <Typography variant="caption" color="text.primary">
                  <strong>Retrieved:</strong> {ragResponse.retrieved_count || 0} similar conversations
                </Typography>
                {latestRAG.sentiment && (
                  <Typography variant="caption" color="text.primary">
                    <strong>Sentiment:</strong> {latestRAG.sentiment.avg_score?.toFixed(2) || '0.00'}
                  </Typography>
                )}
                {latestRAG.window_start && latestRAG.window_end && (
                  <Typography variant="caption" color="text.primary">
                    <strong>Window:</strong>{' '}
                    {new Date(latestRAG.window_start).toLocaleTimeString()} -{' '}
                    {new Date(latestRAG.window_end).toLocaleTimeString()}
                  </Typography>
                )}
              </Stack>
            </AccordionDetails>
          </Accordion>
        </Stack>
      </CardContent>
    </Card>
  );
}

