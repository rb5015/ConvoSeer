#!/usr/bin/env python3
"""
RAG Stream Worker - Consumes sentiment windows and produces RAG responses.
"""
import os
import json
import time
from typing import Dict, Any, List
import requests
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv


load_dotenv()


KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
SENTIMENT_TOPIC = os.getenv("KAFKA_TOPIC_SENTIMENT", "calls.sentiment")
RAG_TOPIC = os.getenv("KAFKA_TOPIC_RAG", "calls.rag")
RAG_URL = os.getenv("RAG_URL", "http://localhost:8002")


def json_dumps(data: Dict[str, Any]) -> bytes:
    """Serialize to JSON bytes."""
    return json.dumps(data).encode("utf-8")


def call_rag_service(latest_utterance: str, call_id: str, k: int = 5) -> Dict[str, Any]:
    """Call the RAG service to get suggestions."""
    try:
        response = requests.post(
            f"{RAG_URL}/assist",
            json={
                "latest_utterance": latest_utterance,
                "k": k,
                "call_id": call_id
            },
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"❌ Error calling RAG service: {e}")
        return {
            "suggestion": "Error: Unable to get suggestion",
            "alternatives": [],
            "retrieved": []
        }


def process_sentiment_window(window_data: Dict[str, Any], producer: KafkaProducer) -> None:
    """Process a sentiment window and generate RAG response."""
    call_id = window_data.get("call_id")
    utterances = window_data.get("utterances", [])
    avg_sentiment = window_data.get("avg_sentiment_score", 0.0)
    utterance_count = window_data.get("utterance_count", 0)
    
    if not utterances:
        print(f"⚠️  No utterances in window for call {call_id}")
        return
    
    # Get the most recent customer utterance
    customer_utterances = [u for u in utterances if u.get("speaker_role") == "customer"]
    if not customer_utterances:
        print(f"⚠️  No customer utterances in window for call {call_id}")
        return
    
    latest_utterance = customer_utterances[-1]
    latest_text = latest_utterance.get("text", "")
    
    if not latest_text:
        print(f"⚠️  Empty text in latest utterance for call {call_id}")
        return
    
    print(f"\n📊 Processing window for call {call_id}")
    print(f"   Utterances: {utterance_count}, Avg sentiment: {avg_sentiment:.2f}")
    print(f"   Latest customer: \"{latest_text[:50]}...\"")
    
    # Call RAG service
    print(f"🤖 Calling RAG service...")
    rag_response = call_rag_service(latest_text, call_id)
    
    # Create response message
    response_msg = {
        "call_id": call_id,
        "window_start": window_data.get("window_start"),
        "window_end": window_data.get("window_end"),
        "timestamp": int(time.time() * 1000),
        "latest_utterance": latest_text,
        "utterance_id": latest_utterance.get("utterance_id"),
        "sentiment": {
            "avg_score": avg_sentiment,
            "utterance_count": utterance_count
        },
        "rag_response": {
            "suggestion": rag_response.get("suggestion", ""),
            "alternatives": rag_response.get("alternatives", []),
            "retrieved_count": len(rag_response.get("retrieved", []))
        }
    }
    
    # Publish to RAG topic
    producer.send(
        RAG_TOPIC,
        key=call_id.encode("utf-8"),
        value=json_dumps(response_msg)
    )
    producer.flush()
    
    print(f"✅ Published RAG response: \"{rag_response.get('suggestion', '')[:60]}...\"")


def main() -> None:
    """Main worker loop."""
    print("Starting RAG Stream Worker...")
    print(f"Sentiment topic: {SENTIMENT_TOPIC}")
    print(f"RAG topic: {RAG_TOPIC}")
    print(f"RAG service: {RAG_URL}")
    
    # Initialize Kafka consumer and producer
    consumer = KafkaConsumer(
        SENTIMENT_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
        group_id="rag-stream-worker"
    )
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k
    )
    
    print("✅ Connected to Kafka")
    print("🎧 Listening for sentiment windows...\n")
    
    try:
        for message in consumer:
            try:
                window_data = message.value
                process_sentiment_window(window_data, producer)
            except Exception as e:
                print(f"❌ Error processing message: {e}")
                import traceback
                traceback.print_exc()
    except KeyboardInterrupt:
        print("\n\n⏹️  Shutting down...")
    finally:
        consumer.close()
        producer.close()
        print("✅ Shutdown complete")


if __name__ == "__main__":
    main()

