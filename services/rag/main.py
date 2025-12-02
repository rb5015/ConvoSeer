from typing import Any, Dict, List, Optional
import os
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from cachetools import LRUCache
from google import genai
from google.genai import types
from pymongo import MongoClient
from pymongo.collection import Collection
from .prompts import ASSIST_SYSTEM_PROMPT, build_user_prompt


class AssistRequest(BaseModel):
    call_id: Optional[str] = Field(default=None, description="Current call id (optional)")
    latest_utterance: str = Field(..., description="Latest customer utterance")
    filters: Dict[str, Any] | None = Field(default=None, description="Optional filters e.g., {industry, product, sentiment_band}")
    k: int = Field(default=8, description="Top-k results")
    model: Optional[str] = Field(default=None, description="LLM model override")


class AssistResponse(BaseModel):
    suggestion: str
    alternatives: List[str]
    retrieved: List[Dict[str, Any]]


app = FastAPI(title="RAG Service", version="0.1.0")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
EMBEDDER_URL = os.getenv("EMBEDDER_URL", "http://embedder:8000")
GENERATION_MODEL = os.getenv("GENERATION_MODEL", "gemini-2.0-flash")
MONGODB_URI = os.getenv("MONGODB_URI", "")
MONGODB_DB = os.getenv("MONGODB_DB", "my_database")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "call_chunks")

_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None
_mongo: Optional[MongoClient] = MongoClient(MONGODB_URI) if MONGODB_URI else None
_col: Optional[Collection] = _mongo[MONGODB_DB][MONGODB_COLLECTION] if _mongo else None
_embed_cache = LRUCache(maxsize=50_000)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


def _embed_text(text: str) -> List[float]:
    if text in _embed_cache:
        return _embed_cache[text]
    # Use embedder service (which now uses Gemini embeddings)
    normalized_text = text.strip().replace("\n", " ")
    try:
        resp = requests.post(
            f"{EMBEDDER_URL}/embed",
            json={"texts": [normalized_text]},
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        vec = data["embeddings"][0]
        _embed_cache[text] = vec
        return vec
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Embedder service error: {str(e)}")


def _vector_search(query_vec: List[float], k: int, filters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not _col:
        return []
    vector_stage: Dict[str, Any] = {
        "$vectorSearch": {
            "index": "embedding_index",
            "path": "embedding",
            "queryVector": query_vec,
            "numCandidates": max(100, k * 20),
            "limit": k,
        }
    }
    pipeline: List[Dict[str, Any]] = [vector_stage]
    if filters:
        match_stage = {"$match": filters}
        pipeline.append(match_stage)
    project_stage = {
        "$project": {
            "_id": 0,
            "filename": 1,
            "full_text": 1,
            "chunk_id": 1,
            "chunk_text": 1,
            "sentiment": 1,
            "product": 1,
            "score": {"$meta": "vectorSearchScore"},
        }
    }
    pipeline.append(project_stage)
    return list(_col.aggregate(pipeline))


@app.post("/assist", response_model=AssistResponse)
def assist(req: AssistRequest) -> Any:
    if not req.latest_utterance:
        raise HTTPException(status_code=400, detail="latest_utterance required")
    qvec = _embed_text(req.latest_utterance)
    retrieved = _vector_search(qvec, req.k, req.filters or {})
    # Determine sentiment label from first retrieved (fallback to NEU)
    sentiment_label = "NEU"
    if retrieved:
        sent = retrieved[0].get("sentiment", "neutral")
        if isinstance(sent, dict):
            sentiment_label = sent.get("label", "NEU")
        else:
            # Map string sentiment to label format
            sent_str = str(sent).lower()
            if "positive" in sent_str:
                sentiment_label = "POS"
            elif "negative" in sent_str:
                sentiment_label = "NEG"
            else:
                sentiment_label = "NEU"
    user_prompt = build_user_prompt(req.latest_utterance, retrieved, sentiment_label)

    if not _client:
        raise HTTPException(status_code=500, detail="Gemini not configured")
    model = req.model or GENERATION_MODEL
    # Use Gemini API with system instructions
    response = _client.models.generate_content(
        model=model,
        contents=user_prompt,
        config=types.GenerateContentConfig(
            system_instruction=ASSIST_SYSTEM_PROMPT,
            temperature=0.4,
        )
    )
    text = response.text.strip()
    # Split alternatives if author provided bullets (very lightweight parsing)
    lines = [ln.strip("-• ").strip() for ln in text.split("\n") if ln.strip()]
    suggestion = lines[0] if lines else text
    alternatives = [ln for ln in lines[1:3]] if len(lines) > 1 else []
    return AssistResponse(suggestion=suggestion, alternatives=alternatives, retrieved=retrieved)


