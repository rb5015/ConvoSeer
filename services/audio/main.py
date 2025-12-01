#!/usr/bin/env python3
"""
Audio Ingestion Service - Receives audio from UI, transcribes, and publishes to Kafka.
"""
import os
import time
import uuid
import threading
import subprocess
import json
from typing import Optional
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka import KafkaProducer
import orjson
import numpy as np
import torch
import tempfile
from dotenv import load_dotenv
from faster_whisper import WhisperModel


load_dotenv()


app = FastAPI(title="Audio Ingestion Service", version="0.1.0")

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Preload Whisper model at startup to avoid crashes during first request."""
    print("🚀 Starting audio service...")
    try:
        # Preload the model at startup
        print("Preloading Whisper model...")
        get_whisper_model()
        print("✅ Audio service ready!")
    except Exception as e:
        print(f"⚠️  Warning: Could not preload Whisper model: {e}")
        print("Model will be loaded on first request (may cause delay)")

# Configuration
# Default to Docker internal address; can be overridden via KAFKA_BROKERS env var
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092")
RAW_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "calls.raw")
WHISPER_MODEL_SIZE = os.getenv("WHISPER_MODEL", "tiny")  # Using tiny model to avoid segfaults and reduce memory

# Lazy Kafka producer initialization
_producer = None


def get_producer():
    """Get or create Kafka producer (lazy initialization)."""
    global _producer
    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: orjson.dumps(v),
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                api_version=(0, 10, 1)  # Explicit API version
            )
            print(f"✓ Kafka producer connected to {KAFKA_BROKERS}")
        except Exception as e:
            print(f"⚠️  Kafka producer initialization failed: {e}")
            raise
    return _producer

# Whisper model cache with thread safety
_whisper_model = None
_model_loading = False
_model_load_error = None
_model_lock = threading.Lock()  # Lock for thread-safe model access
_transcription_lock = threading.Lock()  # Lock for transcription operations


def get_whisper_model():
    """Get or load faster-whisper model (cached, thread-safe)."""
    global _whisper_model, _model_loading, _model_load_error
    
    # Wait for another thread to finish loading if in progress
    import time
    max_wait_time = 300  # 30 seconds max wait
    wait_time = 0
    while _model_loading and wait_time < max_wait_time:
        time.sleep(0.1)
        wait_time += 0.1
    
    with _model_lock:
        if _whisper_model is not None:
            return _whisper_model
        
        if _model_load_error is not None:
            raise RuntimeError(f"Failed to load Whisper model: {_model_load_error}")
        
        # Check again after acquiring lock (another thread might have loaded it)
        if _whisper_model is not None:
            return _whisper_model
        
        if _model_loading:
            # Still loading, wait a bit more
            _model_lock.release()
            try:
                time.sleep(0.5)
            finally:
                _model_lock.acquire()
            if _whisper_model is not None:
                return _whisper_model
        
        try:
            _model_loading = True
            device = "cuda" if torch.cuda.is_available() else "cpu"
            compute_type = "float16" if device == "cuda" else "int8"  # Use int8 on CPU for better performance
            print(f"Loading faster-whisper model '{WHISPER_MODEL_SIZE}' on {device} (compute_type: {compute_type})...")
            
            # Load faster-whisper model - more stable on ARM64
            _whisper_model = WhisperModel(
                WHISPER_MODEL_SIZE, 
                device=device,
                compute_type=compute_type,
                num_workers=1,  # Single worker for stability
            )
            print(f"✓ faster-whisper model loaded successfully")
            return _whisper_model
        except Exception as e:
            _model_load_error = str(e)
            print(f"❌ Error loading Whisper model: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            _model_loading = False


class TranscriptionResponse(BaseModel):
    text: str
    call_id: str
    utterance_id: str
    published: bool
    transcription_time: float


@app.get("/health")
def health():
    return {"status": "ok", "kafka_brokers": KAFKA_BROKERS, "topic": RAW_TOPIC}


@app.post("/transcribe", response_model=TranscriptionResponse)
async def transcribe_audio(
    audio_file: UploadFile = File(...),
    call_id: str = Form(...),
    speaker_role: str = Form("customer"),
    utterance_index: Optional[int] = Form(None),
):
    """
    Receive audio file, transcribe with Whisper, and publish to Kafka.
    """
    start_time = time.time()
    
    print(f"📥 POST /transcribe received - call_id: {call_id}, speaker_role: {speaker_role}, utterance_index: {utterance_index}")
    
    # Generate utterance ID if not provided
    if utterance_index is None:
        utterance_index = int(time.time() * 1000) % 1000000
    
    utterance_id = f"{call_id}:{utterance_index}:{uuid.uuid4().hex[:8]}"
    
    try:
        # Read audio file
        audio_bytes = await audio_file.read()
        
        print(f"📥 Read audio file: {len(audio_bytes)} bytes, content_type: {audio_file.content_type}, filename: {audio_file.filename}")
        
        if not audio_bytes or len(audio_bytes) < 100:  # Very small files are likely empty/invalid
            print(f"❌ Empty or too small audio file: {len(audio_bytes)} bytes")
            raise HTTPException(status_code=400, detail=f"Empty or invalid audio file ({len(audio_bytes)} bytes)")
        
        # Limit audio file size to prevent memory issues (5MB max for real-time)
        MAX_AUDIO_SIZE = 5 * 1024 * 1024  # 5MB
        if len(audio_bytes) > MAX_AUDIO_SIZE:
            print(f"❌ Audio file too large: {len(audio_bytes)} bytes (max: {MAX_AUDIO_SIZE})")
            raise HTTPException(
                status_code=400, 
                detail=f"Audio file too large ({len(audio_bytes) / 1024 / 1024:.1f}MB). Maximum size is 5MB. Please send smaller chunks (recommended: 3-10 seconds of audio)."
            )
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp:
            tmp.write(audio_bytes)
            tmp_path = tmp.name
        
        print(f"💾 Saved audio to temp file: {tmp_path}")
        
        # Track original file path for cleanup
        original_tmp_path = tmp_path
        
        try:
            # Load and transcribe with thread safety
            print(f"🎤 Starting transcription for {len(audio_bytes)} byte audio file...")
            
            # Validate audio file and get duration
            try:
                print("📂 Validating audio file...")
                # Use ffprobe to get audio duration (more reliable than loading into memory)
                try:
                    probe_result = subprocess.run(
                        ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", tmp_path],
                        capture_output=True,
                        text=True,
                        timeout=5,
                        check=True,
                    )
                    duration_seconds = float(probe_result.stdout.strip())
                    print(f"✓ Audio validated: ~{duration_seconds:.1f} seconds")
                except:
                    # Fallback: estimate from file size (rough approximation)
                    file_size_mb = len(audio_bytes) / (1024 * 1024)
                    duration_seconds = file_size_mb * 0.5  # Rough estimate: 0.5 seconds per MB
                    print(f"✓ Audio validated: ~{duration_seconds:.1f} seconds (estimated)")
                
                # Limit audio duration to prevent memory issues (20 seconds max for real-time)
                MAX_DURATION_SECONDS = 20
                if duration_seconds > MAX_DURATION_SECONDS:
                    print(f"⚠️  Audio too long ({duration_seconds:.1f}s), truncating to {MAX_DURATION_SECONDS}s")
                    # Use ffmpeg to trim if available
                    try:
                        trimmed_path = tmp_path + "_trimmed.wav"
                        subprocess.run([
                            "ffmpeg", "-i", tmp_path, "-t", str(MAX_DURATION_SECONDS),
                            "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1", "-y", trimmed_path
                        ], check=True, capture_output=True, timeout=5)
                        # Use trimmed version for transcription
                        tmp_path = trimmed_path
                        print(f"✓ Audio trimmed using ffmpeg")
                    except Exception as ffmpeg_error:
                        print(f"⚠️  ffmpeg trimming failed: {ffmpeg_error}, will process as-is")
                        # Continue with original file - faster-whisper will handle it
            except Exception as e:
                print(f"❌ Error validating audio: {e}")
                import traceback
                traceback.print_exc()
                raise HTTPException(status_code=400, detail=f"Failed to validate audio file: {str(e)}")
            
            # Use faster-whisper - more stable on ARM64 and faster
            print("🎙️  Starting Whisper transcription (using faster-whisper)...")
            
            try:
                with _transcription_lock:
                    model = get_whisper_model()
                    
                    # Clear cache before transcription
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()
                    
                    # Transcribe using faster-whisper
                    # faster-whisper handles file loading internally and is more stable
                    segments, info = model.transcribe(
                        tmp_path,
                        language="en",
                        task="transcribe",
                        beam_size=5,
                        temperature=0.0,
                        condition_on_previous_text=False,
                        no_speech_threshold=0.6,
                        compression_ratio_threshold=2.4,
                        vad_filter=True,  # Voice activity detection filter
                    )
                    
                    # Collect all segments into text
                    text_parts = []
                    detected_language = info.language
                    
                    for segment in segments:
                        text_parts.append(segment.text.strip())
                    
                    text = " ".join(text_parts).strip()
                    
                    # Create result in expected format
                    result = {
                        "text": text,
                        "language": detected_language,
                    }
                    
                    # Clear cache after transcription
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()
                
                print("✓ Transcription completed via faster-whisper")
                
            except Exception as e:
                print(f"❌ Transcription error: {e}")
                import traceback
                traceback.print_exc()
                raise HTTPException(
                    status_code=500,
                    detail=f"Transcription failed: {str(e)}"
                )
            except RuntimeError as e:
                error_msg = str(e)
                if "out of memory" in error_msg.lower() or "segmentation" in error_msg.lower():
                    print(f"❌ Memory error during transcription: {e}")
                    raise HTTPException(
                        status_code=500, 
                        detail="Audio file too large or insufficient memory. Please send smaller audio chunks (max 30 seconds)."
                    )
                else:
                    print(f"❌ Error during transcription: {e}")
                    import traceback
                    traceback.print_exc()
                    raise HTTPException(status_code=500, detail=f"Transcription failed: {str(e)}")
            except Exception as e:
                print(f"❌ Error during transcription: {e}")
                import traceback
                traceback.print_exc()
                raise HTTPException(status_code=500, detail=f"Transcription failed: {str(e)}")
            
            text = result["text"].strip()
            transcription_time = time.time() - start_time
            
            print(f"🎤 Transcription result: '{text}' (length: {len(text)}, time: {transcription_time:.2f}s)")
            
            if not text or text in ["", ".", ",", "!", "?", "Thank you."]:
                print(f"⚠️  No significant speech detected (text: '{text}')")
                return TranscriptionResponse(
                    text="",
                    call_id=call_id,
                    utterance_id=utterance_id,
                    published=False,
                    transcription_time=transcription_time
                )
            
            # Calculate audio energy (simple metric) - use a simple estimate
            # faster-whisper doesn't expose the raw audio, so we estimate from file size
            try:
                # Rough estimate: larger files typically have more audio energy
                file_size_mb = len(audio_bytes) / (1024 * 1024)
                audio_energy = min(file_size_mb * 0.1, 1.0)  # Normalize to 0-1 range
            except:
                audio_energy = 0.0
            
            # Create utterance record
            elapsed_ms = int(time.time() * 1000)  # Simple timestamp for now
            utterance = {
                "call_id": call_id,
                "utterance_id": utterance_id,
                "utterance_index": utterance_index,
                "timestamp_ms": elapsed_ms,
                "event_time": elapsed_ms,
                "chunk_id": uuid.uuid4().hex[:8],
                "speaker_role": speaker_role,
                "text": text,
                "metadata": {
                    "source": "ui_audio",
                    "whisper_model": WHISPER_MODEL_SIZE,
                    "transcription_duration": transcription_time,
                    "language": result.get("language", "en"),
                    "audio_energy": audio_energy,
                },
            }
            
            # Publish to Kafka
            try:
                producer = get_producer()
                producer.send(RAW_TOPIC, key=call_id, value=utterance)
                producer.flush()
            except Exception as e:
                print(f"⚠️  Failed to publish to Kafka: {e}")
                # Reset producer to retry next time
                global _producer
                _producer = None
                raise HTTPException(status_code=503, detail=f"Kafka unavailable: {str(e)}")
            
            print(f"✓ Transcribed and published: call_id={call_id}, text=\"{text[:50]}...\"")
            
            return TranscriptionResponse(
                text=text,
                call_id=call_id,
                utterance_id=utterance_id,
                published=True,
                transcription_time=transcription_time
            )
            
        finally:
            # Cleanup temp files (original and trimmed if created)
            # os is already imported at the top of the file
            files_to_cleanup = [original_tmp_path]
            # If we created a trimmed file, clean it up too
            if tmp_path != original_tmp_path and os.path.exists(tmp_path):
                files_to_cleanup.append(tmp_path)
            
            for file_path in files_to_cleanup:
                if os.path.exists(file_path):
                    try:
                        os.unlink(file_path)
                    except:
                        pass
                    
    except HTTPException:
        # Re-raise HTTP exceptions (like empty file)
        raise
    except MemoryError as e:
        print(f"❌ Memory error during transcription: {e}")
        # Try to free memory
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        raise HTTPException(
            status_code=500, 
            detail="Audio file too large or insufficient memory. Please send smaller audio chunks (max 30 seconds, 5MB)."
        )
    except RuntimeError as e:
        error_msg = str(e).lower()
        if "out of memory" in error_msg or "cuda" in error_msg:
            print(f"❌ GPU/CPU memory error: {e}")
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            raise HTTPException(
                status_code=500,
                detail="Insufficient memory for transcription. Please send smaller audio chunks."
        )
        else:
            print(f"❌ Runtime error: {e}")
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Transcription failed: {str(e)}")
    except Exception as e:
        print(f"❌ Error transcribing audio: {e}")
        import traceback
        traceback.print_exc()
        # Try to free memory on any error
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        raise HTTPException(status_code=500, detail=f"Transcription failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)

