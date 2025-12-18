import os
import json
import pandas as pd
import ollama
from pathlib import Path
import numpy as np
from pymongo import MongoClient
import re
from pathlib import Path
import time

# Allowed labels
ALLOWED_SENTIMENTS = {"joy", "sadness", "anger", "fear", "surprise", "disgust", "neutral"}

# Qwen model name (change to whatever you actually use)
QWEN_MODEL = "qwen2:0.5b"  # example placeholder


QWEN_SYSTEM_PROMPT = """
You are an emotion classification model.

Classify the overall emotional sentiment of the given text into ONE of:
joy, sadness, anger, fear, surprise, disgust, neutral.

Rules:
- Output ONLY the single word label in lowercase.
- No punctuation, no explanations, no extra text.
- If the text is factual or polite without strong emotion → output "neutral".
"""


def parse_sentiment_output(raw_output: str) -> str | None:
    """
    Try to extract a single-word sentiment from the model's raw output.
    Handles:
    - proper JSON: {"sentiment": "neutral"}
    - JSON string embedded: "{\n  \"sentiment\": \"neutral\"\n}"
    - plain single-word text: neutral
    Returns: one of ALLOWED_SENTIMENTS or None.
    """
    if not raw_output:
        return None

    raw_output = raw_output.strip()

    # 1) Try to parse as JSON object
    try:
        obj = json.loads(raw_output)
        if isinstance(obj, dict) and "sentiment" in obj:
            val = obj["sentiment"]
            if isinstance(val, str):
                label = val.strip().lower()
                if label in ALLOWED_SENTIMENTS:
                    return label
    except json.JSONDecodeError:
        pass

    # 2) Sometimes "sentiment" field itself holds the JSON-as-string:
    #    sentiment: "{\n  \"sentiment\": \"neutral\"\n}"
    #    Try to parse again if it looks like JSON.
    if raw_output.startswith("{") and "sentiment" in raw_output:
        try:
            obj = json.loads(raw_output)
            if isinstance(obj, dict) and "sentiment" in obj:
                val = obj["sentiment"]
                if isinstance(val, str):
                    label = val.strip().lower()
                    if label in ALLOWED_SENTIMENTS:
                        return label
        except json.JSONDecodeError:
            pass

    # 3) Fallback: maybe it's just the word itself
    label = raw_output.lower()
    # Sometimes models return quotes, punctuation, etc.
    label = label.strip().strip('"').strip("'").strip()
    if label in ALLOWED_SENTIMENTS:
        return label

    return None


def run_qwen_single_sentiment(text: str) -> str | None:
    """
    Call Qwen to get a single-word sentiment label.
    Expects Qwen to obey the system prompt and output just the word.
    """
    resp = ollama.chat(
        model=QWEN_MODEL,
        messages=[
            {"role": "system", "content": QWEN_SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ],
    )
    raw = resp["message"]["content"].strip()
    return parse_sentiment_output(raw) or None

## REPLACE
RESULTS_LOG = Path("PII_redacted_auto_insurance_script.jsonl")
INPUT_DIR = Path("PII_redacted_auto_insurance_script")  # directory that holds the call jsons


def load_results_jsonl(log_path: Path) -> list[dict]:
    records = []
    with log_path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                records.append(rec)
            except json.JSONDecodeError:
                # skip bad lines
                continue
    return records


results_records = load_results_jsonl(RESULTS_LOG)

# Clean + normalize
cleaned_records = []
for rec in results_records:
    file_path = rec.get("file", "")
    filename = os.path.basename(file_path)

    raw_sentiment_field = rec.get("sentiment")
    raw_output_field = rec.get("sentiment_output")

    # 1) Try to clean the existing "sentiment" field
    sentiment_clean = None
    if isinstance(raw_sentiment_field, str):
        sentiment_clean = parse_sentiment_output(raw_sentiment_field)

    # 2) If that fails, try from "sentiment_output"
    if sentiment_clean is None and isinstance(raw_output_field, str):
        sentiment_clean = parse_sentiment_output(raw_output_field)

    rec["filename"] = filename
    rec["sentiment_clean"] = sentiment_clean
    cleaned_records.append(rec)


# Index existing cleaned results by filename
results_by_filename = {r["filename"]: r for r in cleaned_records}

# List all transcript JSONs in the directory
all_files = sorted([p for p in INPUT_DIR.iterdir() if p.suffix == ".json"])

to_fix = []
for file_path in all_files:
    fn = file_path.name
    rec = results_by_filename.get(fn)
    if rec is None or rec.get("sentiment_clean") is None:
        to_fix.append(file_path)

print(f"Need to re-run Qwen on {len(to_fix)} files.")


# Re-run Qwen on problematic ones
for i, file_path in enumerate(to_fix, start=1):
    print(f"[{i}/{len(to_fix)}] Fixing {file_path.name} ...")

    # Load the transcript text
    with file_path.open("r") as f:
        data = json.load(f)
    text = data.get("text", "") or ""

    if not text.strip():
        print(f"  -> Skipping (empty text)")
        continue

    new_sentiment = run_qwen_single_sentiment(text)
    print(f"  -> Qwen returned: {new_sentiment}")

    # Update in-memory record structures
    rec = results_by_filename.get(file_path.name)
    if rec is None:
        rec = {
            "file": str(file_path),
            "filename": file_path.name,
            "sentiment_output": None,
            "sentiment": None,
            "error": None,
        }
        results_by_filename[file_path.name] = rec

    rec["sentiment_clean"] = new_sentiment

## REPLACE FILE NAME
FIXED_LOG = Path("PII_redacted_auto_insurance_script_fixed.jsonl")

with FIXED_LOG.open("w") as f:
    for rec in results_by_filename.values():
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ---------- CONFIG ---------- #

## REPLACE HERE
TRANSCRIPT_DIR = Path("PII_redacted_auto_insurance_script")
SENTIMENT_LOG = Path("PII_redacted_auto_insurance_script.jsonl")

# Where to read "product" from in each transcript JSON.
# - Simple: "product"
# - Nested path: "metadata.product" or "call_info.product_type"
PRODUCT_FIELD = "AUTO INSURANCE" ## REPLACE HERE

# Chunking params (word-based)
CHUNK_SIZE = 250      # words per chunk
CHUNK_OVERLAP = 50    # overlapping words between chunks

# Ollama embedding model
EMBED_MODEL_NAME = "mxbai-embed-large:latest"
MAX_EMBED_RETRIES = 3
EMBED_RETRY_DELAY = 2.0
EMBED_BATCH_SIZE = 64      # after this many chunks, pause briefly
EMBED_BATCH_PAUSE = 1.0 
# Optional: force GPU
os.environ.setdefault("OLLAMA_GPU", "1")


# ---------- HELPERS ---------- #

def load_sentiment_map(log_path: Path) -> dict:
    """
    Load the cleaned sentiment JSONL and map filename -> sentiment string.
    Tries 'sentiment_clean' then 'sentiment'.
    """
    sentiment_map = {}

    with log_path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            filename = rec.get("filename")
            if not filename and "file" in rec:
                filename = os.path.basename(rec["file"])
            if not filename:
                continue

            sentiment = rec.get("sentiment_clean") or rec.get("sentiment")
            if isinstance(sentiment, str):
                sentiment = sentiment.strip().lower()

            sentiment_map[filename] = sentiment

    return sentiment_map


def clean_text(text: str) -> str:
    """
    Basic cleaning:
    - normalize whitespace
    - strip leading/trailing spaces
    (Extend here if you want to remove [PERSON_NAME], [ORGANIZATION], etc.)
    """
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def chunk_text(text: str, chunk_size: int = 250, overlap: int = 50):
    """
    Chunk text by words with overlap.
    Returns a list of chunk strings.
    """
    words = text.split()
    chunks = []
    n = len(words)

    if n == 0:
        return []

    start = 0
    while start < n:
        end = min(start + chunk_size, n)
        chunk_words = words[start:end]
        chunks.append(" ".join(chunk_words))
        if end == n:
            break
        start = max(0, end - overlap)

    return chunks


def get_from_dotted_path(data: dict, path: str):
    """
    Get nested value using a dotted path like 'metadata.product'.
    Falls back to simple top-level lookup if no dot.
    """
    if "." not in path:
        return data.get(path)

    cur = data
    for key in path.split("."):
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return None
    return cur


def extract_product_field(data: dict):
    """
    Extract the product using PRODUCT_FIELD config.
    Also falls back to metadata.product if nothing found.
    """
    product = get_from_dotted_path(data, PRODUCT_FIELD)
    if product is not None:
        return product

    # Reasonable fallback
    return data.get("metadata", {}).get("product")


def embed_single_text_ollama(text: str):
    """
    Embed a single text with retries and backoff.
    Returns list[float] or None if all retries fail.
    """
    for attempt in range(1, MAX_EMBED_RETRIES + 1):
        try:
            res = ollama.embeddings(model=EMBED_MODEL_NAME, prompt=text)
            return res["embedding"]  # list[float]
        except Exception as e:
            print(f"[EMBED ERROR] attempt {attempt} for this chunk: {e}")
            if attempt == MAX_EMBED_RETRIES:
                print("[EMBED ERROR] giving up on this chunk.")
                return None
            # exponential backoff
            time.sleep(EMBED_RETRY_DELAY * attempt)


def embed_texts_ollama_batched(texts):
    """
    Embed a list of texts using Ollama, in mini-batches.
    - Still one request per text (API limitation)
    - But slows down after EMBED_BATCH_SIZE items to avoid overloading Ollama.
    """
    embeddings = []
    for idx, t in enumerate(texts):
        emb = embed_single_text_ollama(t)
        embeddings.append(emb)

        # simple throttling: pause after each mini-batch
        if (idx + 1) % EMBED_BATCH_SIZE == 0:
            print(f"[EMBED] processed {idx + 1} chunks, pausing {EMBED_BATCH_PAUSE}s...")
            time.sleep(EMBED_BATCH_PAUSE)

    return embeddings


def build_insurance_outbound_df():
    # 1. Load sentiment map
    sentiment_map = load_sentiment_map(SENTIMENT_LOG)
    print(f"Loaded {len(sentiment_map)} sentiment entries from JSONL.")

    rows = []

    # 2. Iterate transcripts
    transcript_files = sorted([p for p in TRANSCRIPT_DIR.iterdir() if p.suffix == ".json"])
    print(f"Found {len(transcript_files)} transcript files in {TRANSCRIPT_DIR}.")
    count=0
    for file_path in transcript_files:

        print("no. ",count,"\n")
        filename = file_path.name

        with file_path.open("r") as f:
            data = json.load(f)

        raw_text = data.get("text", "") or ""
        product = extract_product_field(data)

        full_text = clean_text(raw_text)
        sentiment = sentiment_map.get(filename)

        # 3. Chunk the text
        chunks = chunk_text(full_text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP)

        if not chunks:
            rows.append({
                "filename": filename,
                "full_text": full_text,
                "chunk_id": 0,
                "chunk_text": "",
                "sentiment": sentiment,
                "product": product,
                "embedding": None,
            })
            continue

        # 4. Embeddings via Ollama (one call per chunk)
        print(f"Embedding {len(chunks)} chunks for {filename} ...")
        embeddings = embed_texts_ollama_batched(chunks)

        # 5. Build rows (one per chunk)
        for idx, (chunk, emb) in enumerate(zip(chunks, embeddings)):
            
            rows.append({
                "filename": filename,
                "full_text": full_text,
                "chunk_id": idx,
                "chunk_text": chunk,
                "sentiment": sentiment,
                "product": product,
                "embedding": emb,  # already list[float]
            })
        count+=1

    df = pd.DataFrame(rows)
    return df


if __name__ == "__main__":
    df = build_insurance_outbound_df()
    print(df.head())
    print("Total rows (chunks):", len(df))
    df['product'] = "AUTO INSURANCE"  ## REPLACE PRODUCT NAME
    df_clean = df.copy()
    df_clean = df_clean.replace({np.nan: None})
    records = df_clean.to_dict(orient="records")
    df_clean.to_csv("PII_redacted_auto_insurance_script.csv", index=False)  ## REPLACE HERE

    # Connect to local MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["convoseer"]
    collection = db["call_chunks"]

    # Load CSV
    df = pd.read_csv("PII_redacted_auto_insurance_script.csv") ## REPLACE FILE

    # Drop the 'full_text' column
    if 'full_text' in df.columns:
        df = df.drop(columns=['full_text'])

    # Convert to list of dictionaries
    records = df.to_dict(orient="records")

    # Insert into MongoDB
    result = collection.insert_many(records)
    print(f"Inserted {len(result.inserted_ids)} documents.")

