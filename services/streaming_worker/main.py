#!/usr/bin/env python3
"""
Streaming Worker - Processes calls.raw, adds sentiment, and publishes to calls.enriched and calls.sentiment
Replaces Spark streaming with a simpler Python-based approach.
"""
import os
import json
import time
import sys
import requests
from typing import Dict, List, Any
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from collections import defaultdict

# Disable Python output buffering
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)


KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
RAW_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "calls.raw")
ENRICHED_TOPIC = os.getenv("KAFKA_TOPIC_ENRICHED", "calls.enriched")
SENTIMENT_TOPIC = os.getenv("KAFKA_TOPIC_SENTIMENT", "calls.sentiment")
SENTIMENT_URL = os.getenv("SENTIMENT_URL", "http://sentiment-service:8000")
WINDOW_SECONDS = int(os.getenv("STREAM_WINDOW_SECONDS", "10"))


def clean_text(txt: str) -> str:
    """Clean text for sentiment analysis."""
    if not txt:
        return ""
    t = txt.strip().replace("\n", " ")
    return " ".join(t.split())


def analyze_sentiment(texts: List[str]) -> List[Dict[str, Any]]:
    """Call sentiment service to analyze texts."""
    if not texts:
        return []
    
    try:
        response = requests.post(
            f"{SENTIMENT_URL}/analyze",
            json={"texts": texts},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        results = []
        for result in data.get("results", []):
            results.append({
                "score": float(result.get("score", 0.0)),
                "label": str(result.get("label", "NEU")).upper()
            })
        
        return results
    except Exception as e:
        print(f"❌ Error calling sentiment service: {e}")
        # Return neutral sentiment for all texts
        return [{"score": 0.0, "label": "NEU"} for _ in texts]


def process_enriched(producer: KafkaProducer, message_data: Dict[str, Any], sentiment: Dict[str, Any]) -> Dict[str, Any]:
    """Process and publish enriched utterance."""
    # Clean text
    clean_txt = clean_text(message_data.get("text", ""))
    
    # Create enriched message
    enriched_data = {
        "call_id": message_data.get("call_id"),
        "utterance_id": message_data.get("utterance_id"),
        "utterance_index": message_data.get("utterance_index", 0),
        "timestamp_ms": message_data.get("timestamp_ms"),
        "event_time": message_data.get("event_time"),
        "event_timestamp": message_data.get("event_time") / 1000 if message_data.get("event_time") else None,
        "chunk_id": message_data.get("chunk_id"),
        "text": clean_txt,
        "metadata": message_data.get("metadata", {}),
        "sentiment": sentiment
    }
    
    # Publish to enriched topic
    call_id = message_data.get("call_id", "unknown")
    producer.send(
        ENRICHED_TOPIC,
        key=call_id.encode("utf-8") if call_id else None,
        value=json.dumps(enriched_data).encode("utf-8")
    )
    
    print(f"✅ Published enriched utterance: {clean_txt[:50]}... (sentiment: {sentiment['label']})", flush=True)
    
    return enriched_data


def process_window(producer: KafkaProducer, call_id: str, window_data: List[Dict[str, Any]], 
                   window_start: int, window_end: int) -> None:
    """Process a time window and publish aggregated sentiment."""
    if not window_data:
        return
    
    # Calculate average sentiment
    scores = [item["sentiment"]["score"] for item in window_data if "sentiment" in item]
    avg_score = sum(scores) / len(scores) if scores else 0.0
    
    # Collect utterances
    utterances = []
    for item in window_data:
        utterances.append({
            "utterance_id": item.get("utterance_id"),
            "text": item.get("text", ""),
            "sentiment": item.get("sentiment", {"score": 0.0, "label": "NEU"})
        })
    
    # Combine texts
    combined_text = " | ".join([item.get("text", "") for item in window_data])
    
    # Create windowed sentiment message
    window_msg = {
        "call_id": call_id,
        "window_start": window_start,
        "window_end": window_end,
        "window_start_time": min([item.get("event_time", 0) for item in window_data]),
        "window_end_time": max([item.get("event_time", 0) for item in window_data]),
        "avg_sentiment_score": avg_score,
        "utterance_count": len(window_data),
        "combined_text": combined_text,
        "utterances": utterances
    }
    
    # Publish to sentiment topic
    producer.send(
        SENTIMENT_TOPIC,
        key=call_id.encode("utf-8") if call_id else None,
        value=json.dumps(window_msg).encode("utf-8")
    )
    
    print(f"📊 Published sentiment window: {call_id} | score: {avg_score:.3f} | utterances: {len(window_data)}", flush=True)


def main() -> None:
    """Main worker loop."""
    print("🚀 Starting Streaming Worker...")
    print(f"   Kafka brokers: {KAFKA_BROKERS}")
    print(f"   Raw topic: {RAW_TOPIC}")
    print(f"   Enriched topic: {ENRICHED_TOPIC}")
    print(f"   Sentiment topic: {SENTIMENT_TOPIC}")
    print(f"   Sentiment URL: {SENTIMENT_URL}")
    print(f"   Window size: {WINDOW_SECONDS} seconds\n")
    
    # Initialize Kafka consumer and producer with retry logic
    backoff = 1
    consumer = None
    producer = None
    
    while consumer is None or producer is None:
        try:
            consumer = KafkaConsumer(
                RAW_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
                group_id="streaming-worker"
            )
            
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k
            )
            
            print("✅ Connected to Kafka", flush=True)
            print("🎧 Listening for messages...\n", flush=True)
            break
        except NoBrokersAvailable:
            print(f"⚠️  Kafka brokers not available at '{KAFKA_BROKERS}', retrying in {backoff}s...", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as e:
            print(f"⚠️  Error connecting to Kafka: {e}, retrying in {backoff}s...", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    
    # Window tracking: call_id -> list of messages in current window
    windows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    window_start_times: Dict[str, int] = {}
    
    try:
        for message in consumer:
            try:
                message_data = message.value
                call_id = message.key or message_data.get("call_id", "unknown")
                event_time = message_data.get("event_time", int(time.time() * 1000))
                
                # Clean text and analyze sentiment once
                clean_txt = clean_text(message_data.get("text", ""))
                sentiment_results = analyze_sentiment([clean_txt])
                sentiment = sentiment_results[0] if sentiment_results else {"score": 0.0, "label": "NEU"}
                
                # Process and publish enriched message immediately
                enriched_data = process_enriched(producer, message_data, sentiment)
                
                # Add to window
                current_time_ms = event_time
                current_time_s = current_time_ms / 1000
                
                # Initialize window start time if needed
                if call_id not in window_start_times:
                    window_start_times[call_id] = int(current_time_s)
                
                window_start_s = window_start_times[call_id]
                window_end_s = window_start_s + WINDOW_SECONDS
                
                # If current time exceeds window end, process and reset window
                if current_time_s >= window_end_s:
                    if windows[call_id]:
                        # Process window
                        process_window(
                            producer,
                            call_id,
                            windows[call_id],
                            window_start_s * 1000,
                            window_end_s * 1000
                        )
                    
                    # Reset window
                    windows[call_id] = []
                    window_start_times[call_id] = int(current_time_s)
                    window_start_s = window_start_times[call_id]
                
                # Add message to current window (with enriched sentiment)
                window_item = {
                    **enriched_data,
                    "event_time": event_time
                }
                windows[call_id].append(window_item)
                
            except Exception as e:
                print(f"❌ Error processing message: {e}")
                import traceback
                traceback.print_exc()
                
    except KeyboardInterrupt:
        print("\n\n⏹️  Shutting down...")
    finally:
        # Process any remaining windows
        for call_id, window_data in windows.items():
            if window_data:
                window_start_s = window_start_times.get(call_id, 0)
                window_end_s = window_start_s + WINDOW_SECONDS
                process_window(
                    producer,
                    call_id,
                    window_data,
                    window_start_s * 1000,
                    window_end_s * 1000
                )
        
        consumer.close()
        producer.close()
        print("✅ Shutdown complete")


if __name__ == "__main__":
    main()

