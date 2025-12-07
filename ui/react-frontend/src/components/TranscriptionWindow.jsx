import { useEffect, useRef } from 'react';
import { Box, Paper, Stack, Typography } from '@mui/material';

export function TranscriptionWindow({ transcriptions }) {
  const scrollRef = useRef(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [transcriptions]);

  const getSentimentColor = (sentiment) => {
    switch (sentiment) {
      case 'POS':
        return '#22c55e';
      case 'NEG':
        return '#f87171';
      default:
        return '#fbbf24';
    }
  };

  return (
    <Paper
      className="transcription-scroll"
      ref={scrollRef}
      sx={{
        p: 2,
        maxHeight: 320,
        overflowY: 'auto',
        overflowX: 'hidden',
        backgroundColor: 'background.paper',
        width: '100%',
      }}
    >
      {transcriptions.length ? (
        <Stack spacing={1}>
          {transcriptions.map((trans, index) => (
            <Box
              key={`${trans.utterance_id}-${index}`}
              sx={{
                borderLeft: 4,
                borderColor: getSentimentColor(trans.sentiment),
                borderRadius: 2,
                backgroundColor: 'rgba(148, 163, 184, 0.08)',
                px: 2,
                py: 1.5,
              }}
            >
              <Typography variant="caption" color="text.secondary">
                [{trans.timestamp || '—'}]
              </Typography>
              <Typography variant="body1" color="text.primary" sx={{ wordBreak: 'break-word', overflowWrap: 'break-word' }}>
                {trans.text}
              </Typography>
            </Box>
          ))}
        </Stack>
      ) : (
        <Box textAlign="center" py={6}>
          <Typography variant="body2" color="text.secondary">
            No transcriptions yet. Start recording to begin.
          </Typography>
          <Typography variant="caption" color="text.secondary">
            Transcriptions will appear here as audio is processed...
          </Typography>
        </Box>
      )}
    </Paper>
  );
}
