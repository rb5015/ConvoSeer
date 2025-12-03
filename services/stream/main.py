#!/usr/bin/env python3
"""
Streaming API Service - Exposes sentiment and RAG updates via SSE.
"""
import os
import json
import asyncio
from typing import AsyncGenerator, Dict, Any
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
import threading
import queue


app = FastAPI(title="Streaming API Service", version="0.1.0")

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
SENTIMENT_TOPIC = os.getenv("KAFKA_TOPIC_SENTIMENT", "calls.sentiment")
RAG_TOPIC = os.getenv("KAFKA_TOPIC_RAG", "calls.rag")
ENRICHED_TOPIC = os.getenv("KAFKA_TOPIC_ENRICHED", "calls.enriched")
RAW_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "calls.raw")

# Global queues for streaming updates
sentiment_queues: Dict[str, queue.Queue] = {}
rag_queues: Dict[str, queue.Queue] = {}
transcription_queues: Dict[str, queue.Queue] = {}


def sentiment_consumer_thread():
    """Background thread to consume sentiment updates."""
    while True:
        try:
            consumer = KafkaConsumer(
                SENTIMENT_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
                group_id="stream-api-sentiment",
                consumer_timeout_ms=5000
            )
            print(f"✅ Sentiment consumer connected to {SENTIMENT_TOPIC}")
            
            for message in consumer:
                call_id = message.key
                data = message.value
                
                # Ensure call_id is a string
                if call_id:
                    call_id = call_id.decode("utf-8") if isinstance(call_id, bytes) else str(call_id)
                else:
                    # Fallback to call_id from message value
                    call_id = data.get("call_id", "unknown") if isinstance(data, dict) else "unknown"
                
                # Log received message for debugging
                print(f"📥 Received sentiment message for call_id: {call_id}")
                
                # Broadcast to all queues for this call_id
                if call_id in sentiment_queues:
                    try:
                        sentiment_queues[call_id].put_nowait(data)
                        print(f"✅ Queued sentiment data for call_id: {call_id}")
                    except queue.Full:
                        print(f"⚠️ Queue full for call_id: {call_id}")
                        pass  # Drop if queue is full
                else:
                    print(f"⚠️ No active SSE connection for call_id: {call_id} (active: {list(sentiment_queues.keys())})")
        except Exception as e:
            print(f"❌ Error in sentiment consumer: {e}")
            import traceback
            traceback.print_exc()
            import time
            time.sleep(5)  # Wait before retrying


def rag_consumer_thread():
    """Background thread to consume RAG updates."""
    while True:
        try:
            consumer = KafkaConsumer(
                RAG_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
                group_id="stream-api-rag",
                consumer_timeout_ms=5000
            )
            print(f"✅ RAG consumer connected to {RAG_TOPIC}")
            
            for message in consumer:
                call_id = message.key
                data = message.value
                
                # Ensure call_id is a string
                if call_id:
                    call_id = call_id.decode("utf-8") if isinstance(call_id, bytes) else str(call_id)
                else:
                    # Fallback to call_id from message value
                    call_id = data.get("call_id", "unknown") if isinstance(data, dict) else "unknown"
                
                # Log received message for debugging
                print(f"📥 Received RAG message for call_id: {call_id}")
                
                # Broadcast to all queues for this call_id
                if call_id in rag_queues:
                    try:
                        rag_queues[call_id].put_nowait(data)
                        print(f"✅ Queued RAG data for call_id: {call_id}")
                    except queue.Full:
                        print(f"⚠️ Queue full for call_id: {call_id}")
                        pass  # Drop if queue is full
                else:
                    print(f"⚠️ No active SSE connection for call_id: {call_id} (active: {list(rag_queues.keys())})")
        except Exception as e:
            print(f"❌ Error in RAG consumer: {e}")
            import traceback
            traceback.print_exc()
            import time
            time.sleep(5)  # Wait before retrying


def transcription_consumer_thread():
    """Background thread to consume transcription updates from raw and enriched topics."""
    while True:
        try:
            # Try to consume from both topics, but handle missing topics gracefully
            topics = []
            if RAW_TOPIC:
                topics.append(RAW_TOPIC)
            # Only add enriched topic if it exists (will be created when streaming worker starts)
            
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=KAFKA_BROKERS,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
                group_id="stream-api-transcription",
                consumer_timeout_ms=5000
            )
            
            print(f"✅ Transcription consumer connected to topics: {', '.join(topics)}")
            
            for message in consumer:
                call_id = message.key or message.value.get("call_id", "unknown")
                data = message.value
                
                # Broadcast to all queues for this call_id
                if call_id in transcription_queues:
                    try:
                        transcription_queues[call_id].put_nowait(data)
                    except queue.Full:
                        pass  # Drop if queue is full
        except Exception as e:
            print(f"❌ Error in transcription consumer: {e}")
            import traceback
            traceback.print_exc()
            import time
            time.sleep(5)  # Wait before retrying


@app.on_event("startup")
async def startup_event():
    """Start background Kafka consumers."""
    sentiment_thread = threading.Thread(target=sentiment_consumer_thread, daemon=True)
    sentiment_thread.start()
    
    rag_thread = threading.Thread(target=rag_consumer_thread, daemon=True)
    rag_thread.start()
    
    transcription_thread = threading.Thread(target=transcription_consumer_thread, daemon=True)
    transcription_thread.start()
    
    print("✅ Background Kafka consumers started")


@app.get("/health")
def health():
    return {"status": "ok"}


async def sentiment_event_generator(call_id: str) -> AsyncGenerator[str, None]:
    """Generate SSE events for sentiment updates."""
    # Create queue for this client
    q = queue.Queue(maxsize=100)
    sentiment_queues[call_id] = q
    
    try:
        while True:
            try:
                # Wait for new data with timeout
                data = q.get(timeout=30)
                
                # Format as SSE
                event_data = json.dumps(data)
                yield f"event: sentiment\ndata: {event_data}\n\n"
                
            except queue.Empty:
                # Send keepalive
                yield f": keepalive\n\n"
                
            await asyncio.sleep(0.1)
            
    finally:
        # Cleanup
        if call_id in sentiment_queues:
            del sentiment_queues[call_id]


async def rag_event_generator(call_id: str) -> AsyncGenerator[str, None]:
    """Generate SSE events for RAG updates."""
    # Create queue for this client
    q = queue.Queue(maxsize=100)
    rag_queues[call_id] = q
    
    try:
        while True:
            try:
                # Wait for new data with timeout
                data = q.get(timeout=30)
                
                # Format as SSE
                event_data = json.dumps(data)
                yield f"event: rag\ndata: {event_data}\n\n"
                
            except queue.Empty:
                # Send keepalive
                yield f": keepalive\n\n"
                
            await asyncio.sleep(0.1)
            
    finally:
        # Cleanup
        if call_id in rag_queues:
            del rag_queues[call_id]


async def transcription_event_generator(call_id: str) -> AsyncGenerator[str, None]:
    """Generate SSE events for transcription updates."""
    # Create queue for this client
    q = queue.Queue(maxsize=100)
    transcription_queues[call_id] = q
    
    try:
        while True:
            try:
                # Wait for new data with timeout
                data = q.get(timeout=30)
                
                # Format as SSE
                event_data = json.dumps(data)
                yield f"event: transcription\ndata: {event_data}\n\n"
                
            except queue.Empty:
                # Send keepalive
                yield f": keepalive\n\n"
                
            await asyncio.sleep(0.1)
            
    finally:
        # Cleanup
        if call_id in transcription_queues:
            del transcription_queues[call_id]


async def combined_event_generator(call_id: str) -> AsyncGenerator[str, None]:
    """Generate SSE events for transcription, sentiment, and RAG updates."""
    # Create queues for this client
    sentiment_q = queue.Queue(maxsize=100)
    rag_q = queue.Queue(maxsize=100)
    transcription_q = queue.Queue(maxsize=100)
    sentiment_queues[call_id] = sentiment_q
    rag_queues[call_id] = rag_q
    transcription_queues[call_id] = transcription_q
    
    try:
        while True:
            has_data = False
            
            # Check transcription queue (highest priority)
            try:
                data = transcription_q.get_nowait()
                event_data = json.dumps(data)
                yield f"event: transcription\ndata: {event_data}\n\n"
                has_data = True
            except queue.Empty:
                pass
            
            # Check sentiment queue
            try:
                data = sentiment_q.get_nowait()
                event_data = json.dumps(data)
                yield f"event: sentiment\ndata: {event_data}\n\n"
                has_data = True
            except queue.Empty:
                pass
            
            # Check RAG queue
            try:
                data = rag_q.get_nowait()
                event_data = json.dumps(data)
                yield f"event: rag\ndata: {event_data}\n\n"
                has_data = True
            except queue.Empty:
                pass
            
            if not has_data:
                # Send keepalive every 30 seconds
                await asyncio.sleep(30)
                yield f": keepalive\n\n"
            else:
                await asyncio.sleep(0.1)
            
    finally:
        # Cleanup
        if call_id in sentiment_queues:
            del sentiment_queues[call_id]
        if call_id in rag_queues:
            del rag_queues[call_id]
        if call_id in transcription_queues:
            del transcription_queues[call_id]


@app.get("/stream/sentiment/{call_id}")
async def stream_sentiment(call_id: str, request: Request):
    """Stream sentiment updates for a specific call."""
    return StreamingResponse(
        sentiment_event_generator(call_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.get("/stream/rag/{call_id}")
async def stream_rag(call_id: str, request: Request):
    """Stream RAG updates for a specific call."""
    return StreamingResponse(
        rag_event_generator(call_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.get("/stream/transcription/{call_id}")
async def stream_transcription(call_id: str, request: Request):
    """Stream transcription updates for a specific call."""
    return StreamingResponse(
        transcription_event_generator(call_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.get("/stream/{call_id}")
async def stream_all(call_id: str, request: Request):
    """Stream transcription, sentiment, and RAG updates for a specific call."""
    return StreamingResponse(
        combined_event_generator(call_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)

