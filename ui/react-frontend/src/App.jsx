import { useState, useEffect } from 'react';
import axios from 'axios';
import { useSSEStream } from './hooks/useSSEStream';
import { AudioRecorder } from './components/AudioRecorder';
import { TranscriptionWindow } from './components/TranscriptionWindow';
import { SentimentPanel } from './components/SentimentPanel';
import { RAGPanel } from './components/RAGPanel';
import {
  Box,
  Button,
  Card,
  CardContent,
  CardHeader,
  Chip,
  Container,
  Divider,
  Grid,
  Stack,
  Typography,
} from '@mui/material';
import FiberManualRecordIcon from '@mui/icons-material/FiberManualRecord';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';

function App() {
  const [callId, setCallId] = useState(`call-${Math.floor(Date.now() / 1000)}`);
  const [isRecording, setIsRecording] = useState(false);
  const [backendStatus, setBackendStatus] = useState({ audio: false, stream: false });
  const [localTranscriptions, setLocalTranscriptions] = useState([]);

  const {
    transcriptions: sseTranscriptions,
    sentimentHistory,
    ragSuggestions,
    isConnected: streamConnected,
    error: streamError,
    clearData,
  } = useSSEStream(callId, isRecording);

  const allTranscriptionsMap = new Map();
  localTranscriptions.forEach((trans) => {
    if (trans.utterance_id) {
      allTranscriptionsMap.set(trans.utterance_id, trans);
    }
  });
  sseTranscriptions.forEach((trans) => {
    if (trans.utterance_id) {
      allTranscriptionsMap.set(trans.utterance_id, trans);
    }
  });

  const allTranscriptions = Array.from(allTranscriptionsMap.values())
    .sort((a, b) => (a.index || 0) - (b.index || 0))
    .slice(-100);

  useEffect(() => {
    const checkHealth = async () => {
      const audioServiceUrl = import.meta.env.VITE_AUDIO_SERVICE_URL || 'http://localhost:8004';
      const streamUrl = import.meta.env.VITE_STREAM_URL || 'http://localhost:8003';

      try {
        const [audioRes, streamRes] = await Promise.all([
          axios.get(`${audioServiceUrl}/health`, { timeout: 2000 }),
          axios.get(`${streamUrl}/health`, { timeout: 2000 }),
        ]);

        setBackendStatus({
          audio: audioRes.status === 200,
          stream: streamRes.status === 200,
        });
      } catch (err) {
        setBackendStatus({ audio: false, stream: false });
      }
    };

    checkHealth();
    const interval = setInterval(checkHealth, 10000);
    return () => clearInterval(interval);
  }, []);

  const handleTranscription = (transcription) => {
    setLocalTranscriptions((prev) => [...prev, transcription].slice(-100));
  };

  const handleClear = () => {
    setLocalTranscriptions([]);
    clearData();
  };

  const getStatusChipProps = () => {
    if (isRecording) {
      return {
        label: 'Recording',
        color: 'error',
        icon: <FiberManualRecordIcon fontSize="small" />,
        sx: { animation: 'pulse 1.6s infinite' },
      };
    }

    if (backendStatus.audio && backendStatus.stream) {
      return {
        label: 'Connected',
        color: 'success',
        icon: <CheckCircleIcon fontSize="small" />,
      };
    }

    return {
      label: 'Idle',
      color: 'warning',
      icon: <WarningAmberIcon fontSize="small" />,
    };
  };

  return (
    <Box sx={{ minHeight: '100vh', bgcolor: 'background.default', color: 'text.primary' }}>
      <Box
        component="section"
        sx={{
          bgcolor: 'primary.main',
          borderBottom: 1,
          borderColor: 'divider',
          py: { xs: 4, md: 5 },
          boxShadow: '0 20px 35px rgba(2, 6, 23, 0.65)',
        }}
      >
        <Container maxWidth="lg">
          <Stack direction="row" alignItems="flex-start" justifyContent="space-between" spacing={4}>
            <Box>
              <Typography variant="h2" color="text.secondary" sx={{ fontWeight: 700, letterSpacing: 0.5 }}>
                ConvoSeer
              </Typography>
              <Typography variant="body2" color="grey" fontWeight="bold" sx={{ mt: 1 }}>
                Real-time audio intelligence with unified status, sentiment, and contextual recommendations.
              </Typography>
            </Box>

            <Stack direction="row" spacing={2} alignItems="center" justifyContent="flex-end">
              <Chip {...getStatusChipProps()} />
              <Stack direction="row" spacing={1} alignItems="center">
                <Typography variant="body2" color="text.secondary">
                  Utterances
                </Typography>
                <Typography variant="h6" sx={{ letterSpacing: 0.5 }}>
                  {allTranscriptions.length}
                </Typography>
              </Stack>
              <Button variant="outlined" color="secondary" onClick={handleClear}>
                🔄 Clear feed
              </Button>
            </Stack>
          </Stack>
        </Container>
      </Box>

      <Container maxWidth="lg" sx={{ py: { xs: 4, md: 8 } }}>
        <Grid container spacing={4}>
          <Grid item xs={12} lg={5}>
            <Card>
              <CardHeader
                title="Live Transcript"
                subheader="Capture audio and preview the latest utterances below."
                titleTypographyProps={{ variant: 'h5', fontWeight: 600 }}
                subheaderTypographyProps={{ color: 'text.secondary' }}
              />
              <CardContent>
                <Stack spacing={3}>
                  <AudioRecorder
                    callId={callId}
                    onTranscription={handleTranscription}
                    onRecordingChange={setIsRecording}
                  />
                  <Divider />
                  <TranscriptionWindow transcriptions={allTranscriptions} />
                </Stack>
              </CardContent>
            </Card>
          </Grid>

          <Grid item xs={12} lg={7}>
            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <SentimentPanel sentimentHistory={sentimentHistory} />
              </Grid>
              <Grid item xs={12} md={6}>
                <RAGPanel ragSuggestions={ragSuggestions} />
              </Grid>
            </Grid>
          </Grid>
        </Grid>
      </Container>

      <Box
        component="footer"
        sx={{
          borderTop: 1,
          borderColor: 'divider',
          py: 4,
          bgcolor: 'background.paper',
          mt: 6,
        }}
      >
        <Container maxWidth="lg">
          <Stack
            direction={{ xs: 'column', sm: 'row' }}
            spacing={2}
            justifyContent="space-between"
            alignItems="center"
          >
            <Stack direction="row" spacing={1} alignItems="center">
              <Typography variant="body2">Audio Service</Typography>
              <Typography variant="body2" color={backendStatus.audio ? 'success.main' : 'error.main'}>
                {backendStatus.audio ? '✔️ Healthy' : '⚠️ Offline'}
              </Typography>
            </Stack>
            <Stack direction="row" spacing={1} alignItems="center">
              <Typography variant="body2">Stream Service</Typography>
              <Typography variant="body2" color={backendStatus.stream ? 'success.main' : 'error.main'}>
                {backendStatus.stream ? '✔️ Healthy' : '⚠️ Offline'}
              </Typography>
              {streamConnected && (
                <Typography variant="body2" color="success.main">
                  (Listening)
                </Typography>
              )}
              {streamError && (
                <Typography variant="body2" color="warning.main">
                  ({streamError})
                </Typography>
              )}
            </Stack>
            <Typography variant="body2">
              Call ID: <strong>{callId}</strong>
            </Typography>
          </Stack>
        </Container>
      </Box>
    </Box>
  );
}

export default App;

