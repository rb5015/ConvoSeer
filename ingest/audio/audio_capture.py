#!/usr/bin/env python3
"""
Audio capture module for real-time microphone input.
Captures audio chunks and queues them for transcription.
"""
import pyaudio
import numpy as np
import wave
import queue
import threading
from typing import Optional, Callable
from dataclasses import dataclass


@dataclass
class AudioConfig:
    """Audio capture configuration."""
    sample_rate: int = 16000  # Whisper expects 16kHz
    channels: int = 1  # Mono
    chunk_duration: float = 5.0  # seconds per chunk
    format: int = pyaudio.paInt16
    
    @property
    def chunk_size(self) -> int:
        """Frames per chunk."""
        return int(self.sample_rate * self.chunk_duration)
    
    @property
    def bytes_per_sample(self) -> int:
        """Bytes per audio sample."""
        return 2  # paInt16 = 2 bytes


class AudioCapture:
    """Real-time audio capture from microphone."""
    
    def __init__(self, config: Optional[AudioConfig] = None):
        self.config = config or AudioConfig()
        self.audio = pyaudio.PyAudio()
        self.stream: Optional[pyaudio.Stream] = None
        self.audio_queue: queue.Queue = queue.Queue()
        self.is_running = False
        self._capture_thread: Optional[threading.Thread] = None
        
    def list_devices(self) -> None:
        """List available audio input devices."""
        print("\nAvailable audio input devices:")
        for i in range(self.audio.get_device_count()):
            info = self.audio.get_device_info_by_index(i)
            if info['maxInputChannels'] > 0:
                print(f"  [{i}] {info['name']} (channels: {info['maxInputChannels']})")
    
    def start(self, device_index: Optional[int] = None) -> None:
        """Start capturing audio from microphone."""
        if self.is_running:
            print("Already capturing audio")
            return
        
        self.stream = self.audio.open(
            format=self.config.format,
            channels=self.config.channels,
            rate=self.config.sample_rate,
            input=True,
            input_device_index=device_index,
            frames_per_buffer=1024,  # Small buffer for low latency
            stream_callback=self._audio_callback,
        )
        
        self.is_running = True
        self.stream.start_stream()
        print(f"🎤 Audio capture started (device: {device_index or 'default'})")
        
        # Start chunk aggregation thread
        self._capture_thread = threading.Thread(target=self._aggregate_chunks, daemon=True)
        self._capture_thread.start()
    
    def _audio_callback(self, in_data, frame_count, time_info, status):
        """Callback for audio stream (runs in separate thread)."""
        if status:
            print(f"Audio callback status: {status}")
        self.audio_queue.put(in_data)
        return (None, pyaudio.paContinue)
    
    def _aggregate_chunks(self) -> None:
        """Aggregate small audio buffers into larger chunks for transcription."""
        buffer = []
        buffer_size = 0
        target_size = self.config.chunk_size * self.config.bytes_per_sample
        
        while self.is_running:
            try:
                data = self.audio_queue.get(timeout=0.1)
                buffer.append(data)
                buffer_size += len(data)
                
                # When we have enough data, yield a chunk
                if buffer_size >= target_size:
                    chunk = b''.join(buffer)
                    # Convert to numpy array for Whisper
                    audio_np = np.frombuffer(chunk, dtype=np.int16).astype(np.float32) / 32768.0
                    
                    # Put in processed queue (will be consumed by transcriber)
                    if hasattr(self, '_chunk_callback') and self._chunk_callback:
                        self._chunk_callback(audio_np)
                    
                    buffer = []
                    buffer_size = 0
                    
            except queue.Empty:
                continue
    
    def set_chunk_callback(self, callback: Callable[[np.ndarray], None]) -> None:
        """Set callback function for audio chunks."""
        self._chunk_callback = callback
    
    def stop(self) -> None:
        """Stop capturing audio."""
        if not self.is_running:
            return
        
        self.is_running = False
        
        if self.stream:
            self.stream.stop_stream()
            self.stream.close()
        
        if self._capture_thread:
            self._capture_thread.join(timeout=2.0)
        
        print("🎤 Audio capture stopped")
    
    def __del__(self):
        """Cleanup."""
        self.stop()
        self.audio.terminate()


def save_audio_chunk(audio_data: np.ndarray, filename: str, sample_rate: int = 16000) -> None:
    """Save audio chunk to WAV file (for debugging)."""
    # Convert float32 back to int16
    audio_int16 = (audio_data * 32768.0).astype(np.int16)
    
    with wave.open(filename, 'wb') as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)  # 2 bytes for int16
        wf.setframerate(sample_rate)
        wf.writeframes(audio_int16.tobytes())


if __name__ == "__main__":
    # Test audio capture
    import time
    
    config = AudioConfig(chunk_duration=3.0)
    capture = AudioCapture(config)
    
    print("Audio Capture Test")
    print("=" * 50)
    capture.list_devices()
    print("\nStarting capture for 10 seconds...")
    
    chunk_count = [0]
    
    def on_chunk(audio: np.ndarray):
        chunk_count[0] += 1
        print(f"Captured chunk {chunk_count[0]}: {len(audio)} samples, {len(audio)/16000:.1f}s")
        # Save first chunk for inspection
        if chunk_count[0] == 1:
            save_audio_chunk(audio, "test_chunk.wav")
            print("  Saved to test_chunk.wav")
    
    capture.set_chunk_callback(on_chunk)
    capture.start()
    
    try:
        time.sleep(10)
    except KeyboardInterrupt:
        print("\nInterrupted")
    finally:
        capture.stop()
        print(f"\nCaptured {chunk_count[0]} chunks total")

