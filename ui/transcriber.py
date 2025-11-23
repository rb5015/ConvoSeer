"""
Whisper transcription module for Streamlit UI.
Provides simple transcription function for audio files.
"""
import whisper
import numpy as np
import torch
from typing import Optional
import tempfile
import os


# Global model cache
_model_cache = {}


def get_transcriber(model_size: str = "base", device: Optional[str] = None):
    """Get or load Whisper model (cached)."""
    if model_size not in _model_cache:
        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"
        _model_cache[model_size] = whisper.load_model(model_size, device=device)
    return _model_cache[model_size]


def transcribe_audio(audio_bytes: bytes, model_size: str = "base") -> str:
    """
    Transcribe audio bytes to text using Whisper.
    
    Args:
        audio_bytes: Audio data as bytes (WAV format from streamlit-audio-recorder)
        model_size: Whisper model size (tiny, base, small, etc.)
    
    Returns:
        Transcribed text
    """
    if not audio_bytes:
        raise ValueError("No audio data provided")
    
    # Save audio bytes to temporary file
    # streamlit-audio-recorder provides WAV format
    with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp:
        tmp.write(audio_bytes)
        tmp_path = tmp.name
    
    try:
        # Load model (cached)
        model = get_transcriber(model_size)
        
        # Load audio (whisper handles resampling automatically)
        audio = whisper.load_audio(tmp_path)
        audio = whisper.pad_or_trim(audio)
        
        # Transcribe
        result = model.transcribe(
            audio,
            language="en",
            task="transcribe",
            fp16=torch.cuda.is_available(),
            temperature=0.0,  # Deterministic
        )
        
        text = result["text"].strip()
        
        # Return empty string if transcription is just punctuation/noise
        if not text or text in ["", ".", ",", "!", "?"]:
            return ""
        
        return text
        
    except Exception as e:
        raise Exception(f"Transcription failed: {str(e)}")
    finally:
        # Cleanup temp file
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except:
                pass

