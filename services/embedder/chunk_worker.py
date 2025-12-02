#!/usr/bin/env python3
"""
Chunk Worker - Consumes raw call transcripts, embeds them, and stores in MongoDB.
Processes messages from calls.raw topic and stores in my_database.call_chunks collection.
"""
import os
import json
from typing import List, Dict, Any, Optional
import requests
from kafka import KafkaConsumer
from pymongo import MongoClient, ReplaceOne


EMBEDDER_URL = os.getenv("EMBEDDER_URL", "http://localhost:8001")
BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
RAW_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "calls.raw")
MONGODB_URI = os.getenv("MONGODB_URI", "")
MONGODB_DB = os.getenv("MONGODB_DB", "my_database")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "call_chunks")
BATCH_SIZE = int(os.getenv("EMBED_BATCH", "32"))


def embed_batch(texts: List[str]) -> List[List[float]]:
    """Call embedder service to generate embeddings for a batch of texts."""
    resp = requests.post(f"{EMBEDDER_URL}/embed", json={"texts": texts}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data["embeddings"]


def extract_product_from_metadata(metadata: Dict[str, Any]) -> str:
    """Extract product from metadata, with fallback to default."""
    if not metadata:
        return "UNKNOWN"
    
    # Try various metadata fields
    product = metadata.get("product") or metadata.get("Product") or metadata.get("PRODUCT")
    if product:
        return str(product).upper()
    
    # Try to infer from text or other fields
    industry = metadata.get("industry") or metadata.get("Industry") or metadata.get("INDUSTRY")
    if industry:
        return str(industry).upper()
    
    return "UNKNOWN"


def extract_sentiment_from_metadata(metadata: Dict[str, Any]) -> str:
    """Extract sentiment from metadata, with fallback to neutral."""
    if not metadata:
        return "neutral"
    
    sentiment = metadata.get("sentiment") or metadata.get("Sentiment") or metadata.get("SENTIMENT")
    if sentiment:
        # Normalize sentiment values
        sent_str = str(sentiment).lower()
        if "positive" in sent_str:
            return "positive"
        elif "negative" in sent_str:
            return "negative"
        elif "neutral" in sent_str:
            return "neutral"
        return sent_str
    
    return "neutral"


def process_message_to_chunk(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Process a single message from calls.raw topic and create a chunk document.
    
    Args:
        msg: Raw message from calls.raw topic
        
    Returns:
        Chunk document ready for MongoDB insertion, or None if invalid
    """
    chunk_text = msg.get("text", "").strip()
    if not chunk_text:
        return None
    
    call_id = msg.get("call_id", "unknown")
    filename = f"{call_id}_transcript.json"
    
    # Use utterance_index as chunk_id, fallback to 0
    chunk_id = msg.get("utterance_index", 0)
    
    # Extract product and sentiment from metadata
    metadata = msg.get("metadata", {})
    product = extract_product_from_metadata(metadata)
    sentiment = extract_sentiment_from_metadata(metadata)
    
    # For now, use chunk_text as full_text
    # TODO: Could query MongoDB to aggregate all chunks for this call_id to build full_text
    full_text = chunk_text
    
    chunk_doc = {
        "filename": filename,
        "full_text": full_text,
        "chunk_id": chunk_id,
        "chunk_text": chunk_text,
        "sentiment": sentiment,
        "product": product,
        # embedding will be added after embedding generation
    }
    
    return chunk_doc


def main() -> None:
    """Main worker loop."""
    if not MONGODB_URI:
        raise RuntimeError("MONGODB_URI is required")
    
    print("🚀 Starting Chunk Worker...")
    print(f"   Kafka brokers: {BROKERS}")
    print(f"   Topic: {RAW_TOPIC}")
    print(f"   MongoDB: {MONGODB_DB}.{MONGODB_COLLECTION}")
    print(f"   Embedder URL: {EMBEDDER_URL}")
    print(f"   Batch size: {BATCH_SIZE}")
    
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=BROKERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
        group_id="chunk-worker-consumer",
    )
    
    # Initialize MongoDB
    mongo = MongoClient(MONGODB_URI)
    col = mongo[MONGODB_DB][MONGODB_COLLECTION]
    
    # Buffer to accumulate chunks before embedding
    chunk_buffer: List[Dict[str, Any]] = []
    
    print("✅ Connected to Kafka and MongoDB")
    print("🎧 Listening for raw call messages...\n")
    
    try:
        while True:
            # Poll for messages
            msg_pack = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
            records = []
            for tp, messages in msg_pack.items():
                records.extend(messages)
            
            if not records and not chunk_buffer:
                continue
            
            # Process new messages into chunks
            for msg in records:
                chunk = process_message_to_chunk(msg.value)
                if chunk:
                    chunk_buffer.append(chunk)
                    
                    # If buffer is full, flush it
                    if len(chunk_buffer) >= BATCH_SIZE:
                        flush_chunk_buffer(chunk_buffer, col)
                        chunk_buffer.clear()
            
            # Flush remaining chunks periodically
            if chunk_buffer:
                flush_chunk_buffer(chunk_buffer, col)
                chunk_buffer.clear()
                
    except KeyboardInterrupt:
        print("\n\n⏹️  Shutting down...")
        # Flush any remaining chunks
        if chunk_buffer:
            flush_chunk_buffer(chunk_buffer, col)
    except Exception as e:
        print(f"❌ Error in main loop: {e}")
        import traceback
        traceback.print_exc()
    finally:
        consumer.close()
        mongo.close()
        print("✅ Shutdown complete")


def flush_chunk_buffer(buf: List[Dict[str, Any]], col) -> None:
    """Embed chunks and write to MongoDB."""
    if not buf:
        return
    
    print(f"📦 Processing {len(buf)} chunks...")
    
    # Extract texts for embedding
    texts = [chunk["chunk_text"] for chunk in buf]
    
    try:
        # Generate embeddings
        print(f"🔮 Generating embeddings...")
        embeddings = embed_batch(texts)
        
        # Add embeddings to chunks
        for chunk, embedding in zip(buf, embeddings):
            chunk["embedding"] = embedding
        
        # Prepare MongoDB operations
        ops = []
        for chunk in buf:
            # Use filename + chunk_id as unique key
            key = {
                "filename": chunk["filename"],
                "chunk_id": chunk["chunk_id"]
            }
            ops.append(ReplaceOne(key, chunk, upsert=True))
        
        # Bulk write to MongoDB
        if ops:
            result = col.bulk_write(ops, ordered=False)
            print(f"✅ Stored {result.upserted_count + result.modified_count} chunks in MongoDB")
            if result.upserted_count > 0:
                print(f"   (New: {result.upserted_count}, Updated: {result.modified_count})")
    
    except Exception as e:
        print(f"❌ Error flushing buffer: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

