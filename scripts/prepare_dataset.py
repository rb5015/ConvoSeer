#!/usr/bin/env python3
"""
Prepare dataset: convert locally preprocessed chunks.jsonl into utterance-level JSONL
compatible with the pipeline (Kafka -> Spark -> MongoDB).
"""
import os
import sys
import uuid
import json
from pathlib import Path
from typing import Dict, Any


def to_utterance_record(chunk: Dict[str, Any], idx: int) -> Dict[str, Any]:
    transcript_id = str(chunk.get("transcript_id") or "unknown_call")
    speaker = str(chunk.get("speaker") or "unknown").lower()
    if speaker not in ("agent", "customer"):
        speaker = "unknown"
    text = chunk.get("chunk_text") or chunk.get("text") or ""
    # No reliable timestamps available: create synthetic monotonic ms
    ts_ms = (idx + 1) * 1000
    metadata = infer_metadata(text)
    return {
        "call_id": transcript_id,
        "utterance_id": f"{transcript_id}:{idx}:{uuid.uuid4().hex[:8]}",
        "utterance_index": int(chunk.get("chunk_sequence") or chunk.get("turn_index") or idx),
        "timestamp_ms": ts_ms,
        "speaker_role": speaker,
        "text": text,
        "metadata": metadata,
    }

def infer_metadata(text: str) -> Dict[str, Any]:
    t = (text or "").lower()
    industry = None
    product = None
    intent = None

    # industry
    if any(k in t for k in ["policy", "claim", "premium", "coverage", "deductible", "quote"]):
        industry = "insurance"
    if any(k in t for k in ["medicare", "appointment", "medical", "equipment", "clinic"]):
        industry = industry or "healthcare"
    if any(k in t for k in ["internet", "bundle", "plan", "mobile", "cable"]):
        industry = industry or "telecom"
    if any(k in t for k in ["hvac", "plumbing", "electrician", "home service", "roof"]):
        industry = industry or "home_services"
    if any(k in t for k in ["vehicle", "auto", "car", "vin"]):
        industry = industry or "automotive"

    # product
    if "auto" in t or "car" in t or "vehicle" in t:
        product = "auto_insurance"
    elif "home" in t and any(k in t for k in ["policy", "claim", "roof", "flood"]):
        product = "home_insurance"
    elif "medicare" in t or "dme" in t or "equipment" in t:
        product = "medical_equipment"
    elif any(k in t for k in ["internet", "mobile", "data plan", "cable"]):
        product = "telecom_plan"

    # intent
    if any(k in t for k in ["quote", "rate", "price estimate"]):
        intent = "quote"
    elif any(k in t for k in ["cancel", "terminate", "close account"]):
        intent = "cancel"
    elif any(k in t for k in ["upgrade", "change plan", "switch"]):
        intent = "upgrade"
    elif any(k in t for k in ["billing", "charge", "payment", "due"]):
        intent = "billing"
    elif any(k in t for k in ["claim", "accident", "incident"]):
        intent = "file_claim"
    elif any(k in t for k in ["complaint", "issue", "problem", "frustrated"]):
        intent = "complaint"

    meta: Dict[str, Any] = {}
    if industry:
        meta["industry"] = industry
    if product:
        meta["product"] = product
    if intent:
        meta["intent"] = intent
    return meta


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    chunks_path = root / "chunks.jsonl"
    if not chunks_path.exists():
        print("Expected chunks.jsonl at project root. Run preprocess.py first.")
        return 1

    out_dir = root / "datasets" / "prepared"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "utterances.jsonl"

    count = 0
    with open(chunks_path, "r", encoding="utf-8") as src, open(out_path, "w", encoding="utf-8") as dst:
        for idx, line in enumerate(src):
            line = line.strip()
            if not line:
                continue
            try:
                chunk = json.loads(line)
                rec = to_utterance_record(chunk, idx)
                dst.write(json.dumps(rec, ensure_ascii=False) + "\n")
                count += 1
            except Exception as e:
                print(f"Skipping malformed line {idx}: {e}", file=sys.stderr)
                continue

    print(f"Wrote {count} utterances to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


