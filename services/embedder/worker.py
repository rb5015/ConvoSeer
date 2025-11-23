import os
import json
from typing import List, Dict, Any
import requests
from kafka import KafkaConsumer
from pymongo import MongoClient, ReplaceOne


EMBEDDER_URL = os.getenv("EMBEDDER_URL", "http://localhost:8001")
BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
ENRICHED_TOPIC = os.getenv("KAFKA_TOPIC_ENRICHED", "calls.enriched")
MONGODB_URI = os.getenv("MONGODB_URI", "")
MONGODB_DB = os.getenv("MONGODB_DB", "agent_assist")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "utterances")
BATCH_SIZE = int(os.getenv("EMBED_BATCH", "64"))


def embed_batch(texts: List[str]) -> List[List[float]]:
    resp = requests.post(f"{EMBEDDER_URL}/embed", json={"texts": texts}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data["embeddings"]


def main() -> None:
    if not MONGODB_URI:
        raise RuntimeError("MONGODB_URI is required")

    consumer = KafkaConsumer(
        ENRICHED_TOPIC,
        bootstrap_servers=BROKERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
        group_id="embedder-consumer",
    )
    mongo = MongoClient(MONGODB_URI)
    col = mongo[MONGODB_DB][MONGODB_COLLECTION]

    buffer: List[Dict[str, Any]] = []
    while True:
        msg_pack = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
        records = []
        for tp, messages in msg_pack.items():
            records.extend(messages)
        if not records and not buffer:
            continue

        for msg in records:
            buffer.append(msg.value)
            if len(buffer) >= BATCH_SIZE:
                flush_buffer(buffer, col)
                buffer.clear()

        # flush leftovers
        if buffer:
            flush_buffer(buffer, col)
            buffer.clear()


def flush_buffer(buf: List[Dict[str, Any]], col) -> None:
    texts = [rec.get("text", "") for rec in buf]
    embeddings = embed_batch(texts)
    ops = []
    for rec, vec in zip(buf, embeddings):
        doc = dict(rec)
        doc["embedding"] = vec
        key = {"utterance_id": doc.get("utterance_id")}
        ops.append(ReplaceOne(key, doc, upsert=True))
    if ops:
        col.bulk_write(ops, ordered=False)


if __name__ == "__main__":
    main()


