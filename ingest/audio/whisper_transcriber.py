#!/usr/bin/env python3
"""
Whisper transcription service for real-time audio.
Uses OpenAI Whisper (local) to transcribe audio chunks.
"""
import whisper
import numpy as np
import torch
from typing import Optional, Dict, Any
from dataclasses import dataclass
import time


@dataclass
class TranscriptionResult:
    """Result of transcription."""
    text: str
    language: str
    duration: float  # seconds
    confidence: Optional[float] = None


class WhisperTranscriber:
    """Real-time transcription using Whisper."""
    
    def __init__(self, model_size: str = "base", device: Optional[str] = None):
        """
        Initialize Whisper model.
        
        Args:
            model_size: Whisper model size (tiny, base, small, medium, large)
                       - tiny: fastest, least accurate (~1GB RAM)
                       - base: good balance (~1GB RAM) [RECOMMENDED]
                       - small: better quality (~2GB RAM)
                       - medium: high quality (~5GB RAM)
                       - large: best quality (~10GB RAM)
            device: 'cuda' or 'cpu' (auto-detect if None)
        """
        self.model_size = model_size
        
        # Auto-detect device
        if device is None:
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
        else:
            self.device = device
        
        print(f"Loading Whisper model '{model_size}' on {self.device}...")
        start = time.time()
        self.model = whisper.load_model(model_size, device=self.device)
        load_time = time.time() - start
        print(f"✓ Model loaded in {load_time:.1f}s")
        
        # Transcription options
        self.options = {
            "language": "en",  # Force English (faster)
            "task": "transcribe",
            "fp16": self.device == "cuda",  # Use FP16 on GPU
            "beam_size": 5,  # Default beam search
            "best_of": 5,  # Number of candidates
            "temperature": 0.0,  # Deterministic
        }
    
    def transcribe(self, audio: np.ndarray, **kwargs) -> TranscriptionResult:
        """
        Transcribe audio chunk.
        
        Args:
            audio: Audio data as float32 numpy array (16kHz)
            **kwargs: Override default transcription options
        
        Returns:
            TranscriptionResult with text and metadata
        """
        start = time.time()
        
        # Merge options
        options = {**self.options, **kwargs}
        
        # Transcribe
        result = self.model.transcribe(audio, **options)
        
        duration = time.time() - start
        
        # Extract text and metadata
        text = result["text"].strip()
        language = result.get("language", "en")
        
        return TranscriptionResult(
            text=text,
            language=language,
            duration=duration,
        )
    
    def transcribe_with_timestamps(self, audio: np.ndarray, **kwargs) -> Dict[str, Any]:
        """
        Transcribe with word-level timestamps.
        
        Returns full Whisper result dict with segments.
        """
        options = {**self.options, **kwargs}
        options["word_timestamps"] = True
        
        result = self.model.transcribe(audio, **options)
        return result


def benchmark_model(model_size: str = "base", duration: float = 5.0) -> None:
    """Benchmark transcription speed for a model."""
    print(f"\nBenchmarking Whisper '{model_size}' model...")
    print("=" * 60)
    
    transcriber = WhisperTranscriber(model_size)
    
    # Generate dummy audio (silence)
    sample_rate = 16000
    audio = np.zeros(int(sample_rate * duration), dtype=np.float32)
    
    # Warm-up
    print("Warming up...")
    transcriber.transcribe(audio)
    
    # Benchmark
    print(f"Transcribing {duration}s of audio...")
    result = transcriber.transcribe(audio)
    
    rtf = result.duration / duration  # Real-time factor
    print(f"\nResults:")
    print(f"  Audio duration: {duration:.1f}s")
    print(f"  Transcription time: {result.duration:.2f}s")
    print(f"  Real-time factor: {rtf:.2f}x")
    print(f"  {'✓ FASTER than real-time' if rtf < 1.0 else '✗ SLOWER than real-time'}")
    
    if rtf < 1.0:
        print(f"  Can process audio {1/rtf:.1f}x faster than real-time")
    else:
        print(f"  ⚠️  Consider using a smaller model (tiny/base)")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Whisper transcription")
    parser.add_argument("--model", default="base", choices=["tiny", "base", "small", "medium", "large"],
                       help="Whisper model size")
    parser.add_argument("--benchmark", action="store_true", help="Run benchmark")
    parser.add_argument("--audio", type=str, help="Audio file to transcribe (WAV)")
    
    args = parser.parse_args()
    
    if args.benchmark:
        benchmark_model(args.model)
    elif args.audio:
        import soundfile as sf
        
        print(f"Transcribing {args.audio}...")
        audio, sr = sf.read(args.audio)
        
        # Resample to 16kHz if needed
        if sr != 16000:
            print(f"Resampling from {sr}Hz to 16000Hz...")
            import librosa
            audio = librosa.resample(audio, orig_sr=sr, target_sr=16000)
        
        transcriber = WhisperTranscriber(args.model)
        result = transcriber.transcribe(audio)
        
        print(f"\nTranscription:")
        print(f"  Text: {result.text}")
        print(f"  Language: {result.language}")
        print(f"  Duration: {result.duration:.2f}s")
    else:
        # Just load model and show info
        transcriber = WhisperTranscriber(args.model)
        print(f"\n✓ Whisper '{args.model}' model ready")
        print(f"  Device: {transcriber.device}")
        print(f"\nRun with --benchmark to test speed")

