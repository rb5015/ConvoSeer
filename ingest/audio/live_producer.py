#!/usr/bin/env python3
"""
Live Kafka producer for real-time transcribed utterances.
Captures audio, transcribes with Whisper, and publishes to Kafka.
"""
import os
import sys
import time
import uuid
import signal
from typing import Optional, Dict, Any
from datetime import datetime
import numpy as np
from kafka import KafkaProducer
import orjson
from dotenv import load_dotenv

from audio_capture import AudioCapture, AudioConfig
from whisper_transcriber import WhisperTranscriber


load_dotenv()


def json_dumps(data: Dict[str, Any]) -> bytes:
    """Serialize to JSON bytes."""
    return orjson.dumps(data)


class LiveTranscriptionProducer:
    """Real-time audio → transcription → Kafka pipeline."""
    
    def __init__(
        self,
        kafka_brokers: str,
        kafka_topic: str,
        whisper_model: str = "tiny",
        call_id: Optional[str] = None,
        speaker_role: str = "customer",
        chunk_duration: float = 5.0,
    ):
        self.kafka_brokers = kafka_brokers
        self.kafka_topic = kafka_topic
        self.call_id = call_id or f"live-{uuid.uuid4().hex[:8]}"
        self.speaker_role = speaker_role
        self.utterance_index = 0
        
        # Initialize components
        print("Initializing live transcription pipeline...")
        
        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_brokers,
            value_serializer=json_dumps,
            key_serializer=lambda x: x.encode("utf-8"),
        )
        print(f"✓ Kafka producer connected to {kafka_brokers}")
        
        # Whisper transcriber
        self.transcriber = WhisperTranscriber(model_size=whisper_model)
        
        # Audio capture
        audio_config = AudioConfig(chunk_duration=chunk_duration)
        self.audio_capture = AudioCapture(audio_config)
        self.audio_capture.set_chunk_callback(self._on_audio_chunk)
        
        self.is_running = False
        self.start_time = None
    
    def _on_audio_chunk(self, audio: np.ndarray) -> None:
        """Callback when audio chunk is ready."""
        if not self.is_running:
            return
        
        try:
            # Transcribe
            print(f"🎤 Transcribing chunk {self.utterance_index}...")
            result = self.transcriber.transcribe(audio)
            
            if not result.text:
                print("  (silence detected, skipping)")
                return
            
            # Create utterance record
            elapsed_ms = int((time.time() - self.start_time) * 1000)
            current_timestamp = int(time.time() * 1000)  # Unix timestamp in ms
            chunk_id = uuid.uuid4().hex[:8]
            
            utterance = {
                "call_id": self.call_id,
                "utterance_id": f"{self.call_id}:{self.utterance_index}:{chunk_id}",
                "utterance_index": self.utterance_index,
                "timestamp_ms": elapsed_ms,
                "event_time": current_timestamp,  # For windowing
                "chunk_id": chunk_id,
                "speaker_role": self.speaker_role,
                "text": result.text,
                "metadata": {
                    "source": "live_audio",
                    "whisper_model": self.transcriber.model_size,
                    "transcription_duration": result.duration,
                    "language": result.language,
                    "audio_energy": float(np.abs(audio).mean()),  # Lightweight voice energy
                },
            }
            
            # Send to Kafka
            self.producer.send(
                self.kafka_topic,
                key=self.call_id,
                value=utterance,
            )
            self.producer.flush()
            
            print(f"✓ Sent to Kafka: \"{result.text}\"")
            print(f"  (transcribed in {result.duration:.2f}s)")
            
            self.utterance_index += 1
            
        except Exception as e:
            print(f"❌ Error processing audio chunk: {e}")
    
    def start(self, device_index: Optional[int] = None) -> None:
        """Start live transcription pipeline."""
        if self.is_running:
            print("Already running")
            return
        
        print("\n" + "=" * 60)
        print("Live Transcription → Kafka Pipeline")
        print("=" * 60)
        print(f"Call ID: {self.call_id}")
        print(f"Kafka topic: {self.kafka_topic}")
        print(f"Speaker role: {self.speaker_role}")
        print(f"Whisper model: {self.transcriber.model_size}")
        print("=" * 60)
        
        self.is_running = True
        self.start_time = time.time()
        self.audio_capture.start(device_index=device_index)
        
        print("\n🎙️  Speak into your microphone...")
        print("   Press Ctrl+C to stop\n")
    
    def stop(self) -> None:
        """Stop pipeline."""
        if not self.is_running:
            return
        
        print("\n\nStopping pipeline...")
        self.is_running = False
        self.audio_capture.stop()
        self.producer.flush()
        self.producer.close()
        
        print(f"✓ Sent {self.utterance_index} utterances to Kafka")
        print(f"✓ Pipeline stopped")
    
    def run(self, device_index: Optional[int] = None) -> None:
        """Run pipeline until interrupted."""
        self.start(device_index=device_index)
        
        try:
            while self.is_running:
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("\n\nReceived interrupt signal")
        finally:
            self.stop()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Live audio transcription to Kafka using Whisper"
    )
    parser.add_argument(
        "--brokers", "-b",
        default=os.getenv("KAFKA_BROKERS", "localhost:9092"),
        help="Kafka broker addresses"
    )
    parser.add_argument(
        "--topic", "-t",
        default=os.getenv("KAFKA_TOPIC_RAW", "calls.raw"),
        help="Kafka topic to publish to"
    )
    parser.add_argument(
        "--model", "-m",
        default="base",
        choices=["tiny", "base", "small", "medium", "large"],
        help="Whisper model size (base recommended)"
    )
    parser.add_argument(
        "--call-id",
        help="Call ID (auto-generated if not provided)"
    )
    parser.add_argument(
        "--speaker",
        default="customer",
        choices=["customer", "agent"],
        help="Speaker role"
    )
    parser.add_argument(
        "--chunk-duration",
        type=float,
        default=5.0,
        help="Audio chunk duration in seconds"
    )
    parser.add_argument(
        "--device",
        type=int,
        help="Audio input device index (list with --list-devices)"
    )
    parser.add_argument(
        "--list-devices",
        action="store_true",
        help="List available audio devices and exit"
    )
    
    args = parser.parse_args()
    
    # List devices and exit
    if args.list_devices:
        capture = AudioCapture()
        capture.list_devices()
        return 0
    
    # Create and run pipeline
    pipeline = LiveTranscriptionProducer(
        kafka_brokers=args.brokers,
        kafka_topic=args.topic,
        whisper_model=args.model,
        call_id=args.call_id,
        speaker_role=args.speaker,
        chunk_duration=args.chunk_duration,
    )
    
    pipeline.run(device_index=args.device)
    return 0


if __name__ == "__main__":
    sys.exit(main())

