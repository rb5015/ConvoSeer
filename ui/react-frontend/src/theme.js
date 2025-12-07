import { createTheme } from '@mui/material/styles';

const monochromeTheme = createTheme({
  palette: {
    mode: 'dark',
    primary: {
      main: '#e2e8f0',
      contrastText: '#030712',
    },
    secondary: {
      main: '#94a3b8',
      contrastText: '#030712',
    },
    background: {
      default: '#030712',
      paper: '#0b1120',
    },
    text: {
      primary: '#f8fafc',
      secondary: '#94a3b8',
    },
    divider: '#1e293b',
  },
  typography: {
    fontFamily: "'Inter', 'Segoe UI', 'Helvetica Neue', sans-serif",
    button: {
      textTransform: 'none',
    },
  },
  shape: {
    borderRadius: 16,
  },
  components: {
    MuiButton: {
      styleOverrides: {
        root: {
          borderRadius: 999,
          paddingInline: '1.75rem',
        },
      },
    },
    MuiTextField: {
      defaultProps: {
        variant: 'filled',
        margin: 'dense',
        InputProps: {
          disableUnderline: true,
        },
      },
    },
    MuiFilledInput: {
      styleOverrides: {
        root: {
          borderRadius: 12,
          backgroundColor: '#0f172a',
          '&:hover': {
            backgroundColor: '#111c33',
          },
        },
        input: {
          color: '#f8fafc',
        },
      },
    },
    MuiPaper: {
      defaultProps: {
        elevation: 4,
      },
    },
  },
});

export default monochromeTheme;

