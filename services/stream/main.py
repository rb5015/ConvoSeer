#!/usr/bin/env python3
"""
Streaming API Service - Exposes transcription, sentiment and RAG updates via SSE.
"""
import os
import json
import asyncio
from typing import AsyncGenerator, Dict, Any, List
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
import threading
import queue
import time
import traceback

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

# Cache recent messages (last 50 per call_id)
recent_rag_messages: Dict[str, List[Dict[str, Any]]] = {}
recent_sentiment_messages: Dict[str, List[Dict[str, Any]]] = {}


# ---------------------------------------------------------------------------
# Kafka Consumers
# ---------------------------------------------------------------------------

def sentiment_consumer_thread():
    """Background thread to consume sentiment updates."""
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                SENTIMENT_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
                group_id="stream-api-sentiment",   # ✅ back to a stable group id
                # ❌ NO consumer_timeout_ms
            )
            print(
                f"✅ Sentiment consumer connected to {SENTIMENT_TOPIC}, "
                f"group_id={consumer.config.get('group_id')}"
            )

            for message in consumer:
                call_id = message.key
                data = message.value

                # Ensure call_id is a string
                if call_id:
                    call_id = call_id.decode("utf-8") if isinstance(call_id, bytes) else str(call_id)
                else:
                    call_id = data.get("call_id", "unknown") if isinstance(data, dict) else "unknown"

                print(f"📥 Received sentiment message for call_id: {call_id}")

                # Cache recent messages
                if call_id not in recent_sentiment_messages:
                    recent_sentiment_messages[call_id] = []
                recent_sentiment_messages[call_id].append(data)
                recent_sentiment_messages[call_id] = recent_sentiment_messages[call_id][-50:]

                # Broadcast to all queues for this call_id
                if call_id in sentiment_queues:
                    try:
                        sentiment_queues[call_id].put_nowait(data)
                        print(f"✅ Queued sentiment data for call_id: {call_id}")
                    except queue.Full:
                        print(f"⚠️ Queue full for call_id: {call_id}")
                else:
                    print(
                        f"⚠️ No active SSE connection for call_id: {call_id} "
                        f"(active: {list(sentiment_queues.keys())})"
                    )

        except Exception as e:
            print(f"❌ Error in sentiment consumer: {e}")
            traceback.print_exc()
            time.sleep(5)  # Retry after short delay
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass


def rag_consumer_thread():
    """Background thread to consume RAG updates."""
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                RAG_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
                group_id="stream-api-rag",   # ✅ stable group id again
            )
            print(
                f"✅ RAG consumer connected to {RAG_TOPIC}, "
                f"group_id={consumer.config.get('group_id')}"
            )

            for message in consumer:
                call_id = message.key
                data = message.value

                if call_id:
                    call_id = call_id.decode("utf-8") if isinstance(call_id, bytes) else str(call_id)
                else:
                    call_id = data.get("call_id", "unknown") if isinstance(data, dict) else "unknown"

                print(f"📥 Received RAG message for call_id: {call_id}")

                # Cache recent messages
                if call_id not in recent_rag_messages:
                    recent_rag_messages[call_id] = []
                recent_rag_messages[call_id].append(data)
                recent_rag_messages[call_id] = recent_rag_messages[call_id][-50:]

                # Broadcast to all queues for this call_id
                if call_id in rag_queues:
                    try:
                        rag_queues[call_id].put_nowait(data)
                        print(f"✅ Queued RAG data for call_id: {call_id}")
                    except queue.Full:
                        print(f"⚠️ Queue full for call_id: {call_id}")
                else:
                    print(
                        f"⚠️ No active SSE connection for call_id: {call_id} "
                        f"(active: {list(rag_queues.keys())})"
                    )

        except Exception as e:
            print(f"❌ Error in RAG consumer: {e}")
            traceback.print_exc()
            time.sleep(5)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass


def transcription_consumer_thread():
    """Background thread to consume transcription updates (raw and/or enriched)."""
    while True:
        consumer = None
        try:
            topics: List[str] = []
            if RAW_TOPIC:
                topics.append(RAW_TOPIC)
            if ENRICHED_TOPIC:
                topics.append(ENRICHED_TOPIC)

            if not topics:
                print("⚠️ No transcription topics configured; sleeping 10s")
                time.sleep(10)
                continue

            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=KAFKA_BROKERS,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
                group_id="stream-api-transcription",   # ✅ stable group id
            )

            print(
                f"✅ Transcription consumer connected to topics: {', '.join(topics)}, "
                f"group_id={consumer.config.get('group_id')}"
            )

            for message in consumer:
                call_id = message.key
                data = message.value

                if call_id:
                    call_id = call_id.decode("utf-8") if isinstance(call_id, bytes) else str(call_id)
                else:
                    call_id = data.get("call_id", "unknown") if isinstance(data, dict) else "unknown"

                # Mark which topic this came from (optional, but handy)
                if isinstance(data, dict) and "source_topic" not in data:
                    data.setdefault("source_topic", message.topic)

                if call_id in transcription_queues:
                    try:
                        transcription_queues[call_id].put_nowait(data)
                    except queue.Full:
                        print(f"⚠️ Transcription queue full for call_id: {call_id}")
                else:
                    # No current SSE listener for this call_id
                    pass

        except Exception as e:
            print(f"❌ Error in transcription consumer: {e}")
            traceback.print_exc()
            time.sleep(5)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass



# ---------------------------------------------------------------------------
# FastAPI lifecycle
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup_event():
    """Start background Kafka consumers."""
    threading.Thread(target=sentiment_consumer_thread, daemon=True).start()
    threading.Thread(target=rag_consumer_thread, daemon=True).start()
    threading.Thread(target=transcription_consumer_thread, daemon=True).start()
    print("✅ Background Kafka consumers started")


@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Recent messages APIs
# ---------------------------------------------------------------------------

@app.get("/recent/rag/{call_id}")
def get_recent_rag(call_id: str):
    """Get recent RAG messages for a call_id."""
    messages = recent_rag_messages.get(call_id, [])
    return {"call_id": call_id, "messages": messages, "count": len(messages)}


@app.get("/recent/sentiment/{call_id}")
def get_recent_sentiment(call_id: str):
    """Get recent sentiment messages for a call_id."""
    messages = recent_sentiment_messages.get(call_id, [])
    return {"call_id": call_id, "messages": messages, "count": len(messages)}


# ---------------------------------------------------------------------------
# SSE generators
# ---------------------------------------------------------------------------

async def sentiment_event_generator(call_id: str) -> AsyncGenerator[str, None]:
    """Generate SSE events for sentiment updates."""
    q = queue.Queue(maxsize=100)
    sentiment_queues[call_id] = q
    print(f"🔌 SSE Sentiment connection opened for call_id: {call_id}")
    print(f"📋 Active Sentiment queues: {list(sentiment_queues.keys())}")

    try:
        while True:
            try:
                data = q.get(timeout=30)
                event_data = json.dumps(data)
                print(f"📤 Sending Sentiment SSE event to call_id: {call_id}")
                yield f"event: sentiment\ndata: {event_data}\n\n"
            except queue.Empty:
                # keepalive
                yield f": keepalive\n\n"

            await asyncio.sleep(0.1)
    finally:
        sentiment_queues.pop(call_id, None)


async def rag_event_generator(call_id: str) -> AsyncGenerator[str, None]:
    """Generate SSE events for RAG updates."""
    q = queue.Queue(maxsize=100)
    rag_queues[call_id] = q
    print(f"🔌 SSE RAG connection opened for call_id: {call_id}")
    print(f"📋 Active RAG queues: {list(rag_queues.keys())}")

    try:
        # Send cached messages first (last 10)
        cached_messages = recent_rag_messages.get(call_id, [])
        if cached_messages:
            print(f"📤 Sending {len(cached_messages)} cached RAG messages to call_id: {call_id}")
            for data in cached_messages[-10:]:
                event_data = json.dumps(data)
                yield f"event: rag\ndata: {event_data}\n\n"
                await asyncio.sleep(0.05)

        while True:
            try:
                data = q.get(timeout=30)
                event_data = json.dumps(data)
                print(f"📤 Sending RAG SSE event to call_id: {call_id}")
                yield f"event: rag\ndata: {event_data}\n\n"
            except queue.Empty:
                yield f": keepalive\n\n"

            await asyncio.sleep(0.1)
    finally:
        rag_queues.pop(call_id, None)


async def transcription_event_generator(call_id: str) -> AsyncGenerator[str, None]:
    """Generate SSE events for transcription updates."""
    q = queue.Queue(maxsize=100)
    transcription_queues[call_id] = q
    print(f"🔌 SSE Transcription connection opened for call_id: {call_id}")

    try:
        while True:
            try:
                data = q.get(timeout=30)
                event_data = json.dumps(data)
                yield f"event: transcription\ndata: {event_data}\n\n"
            except queue.Empty:
                yield f": keepalive\n\n"

            await asyncio.sleep(0.1)
    finally:
        transcription_queues.pop(call_id, None)


async def combined_event_generator(call_id: str) -> AsyncGenerator[str, None]:
    """Generate SSE events for transcription, sentiment, and RAG updates."""
    sentiment_q = queue.Queue(maxsize=100)
    rag_q = queue.Queue(maxsize=100)
    transcription_q = queue.Queue(maxsize=100)

    sentiment_queues[call_id] = sentiment_q
    rag_queues[call_id] = rag_q
    transcription_queues[call_id] = transcription_q

    print(f"🔌 SSE Combined connection opened for call_id: {call_id}")
    print(
        f"📋 Active queues - Sentiment: {list(sentiment_queues.keys())}, "
        f"RAG: {list(rag_queues.keys())}"
    )

    try:
        # Cached RAG
        cached_rag = recent_rag_messages.get(call_id, [])
        if cached_rag:
            print(f"📤 Sending {len(cached_rag)} cached RAG messages to call_id: {call_id}")
            for data in cached_rag[-10:]:
                event_data = json.dumps(data)
                yield f"event: rag\ndata: {event_data}\n\n"
                await asyncio.sleep(0.05)

        # Cached sentiment
        cached_sentiment = recent_sentiment_messages.get(call_id, [])
        if cached_sentiment:
            print(f"📤 Sending {len(cached_sentiment)} cached sentiment messages to call_id: {call_id}")
            for data in cached_sentiment[-10:]:
                event_data = json.dumps(data)
                yield f"event: sentiment\ndata: {event_data}\n\n"
                await asyncio.sleep(0.05)

        while True:
            has_data = False

            # Transcription
            try:
                data = transcription_q.get_nowait()
                event_data = json.dumps(data)
                print(f"📤 Sending Transcription SSE event to call_id: {call_id}")
                yield f"event: transcription\ndata: {event_data}\n\n"
                has_data = True
            except queue.Empty:
                pass

            # Sentiment
            try:
                data = sentiment_q.get_nowait()
                event_data = json.dumps(data)
                print(f"📤 Sending Sentiment SSE event to call_id: {call_id}")
                yield f"event: sentiment\ndata: {event_data}\n\n"
                has_data = True
            except queue.Empty:
                pass

            # RAG
            try:
                data = rag_q.get_nowait()
                event_data = json.dumps(data)
                print(f"📤 Sending RAG SSE event to call_id: {call_id}")
                yield f"event: rag\ndata: {event_data}\n\n"
                has_data = True
            except queue.Empty:
                pass

            if not has_data:
                await asyncio.sleep(30)
                yield f": keepalive\n\n"
            else:
                await asyncio.sleep(0.1)
    finally:
        sentiment_queues.pop(call_id, None)
        rag_queues.pop(call_id, None)
        transcription_queues.pop(call_id, None)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/stream/sentiment/{call_id}")
async def stream_sentiment(call_id: str, request: Request):
    """Stream sentiment updates for a specific call."""
    return StreamingResponse(
        sentiment_event_generator(call_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
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
            "X-Accel-Buffering": "no",
        },
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
            "X-Accel-Buffering": "no",
        },
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
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
