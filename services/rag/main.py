from typing import Any, Dict, List, Optional
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from cachetools import LRUCache
from openai import OpenAI
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

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
EMBEDDER_URL = os.getenv("EMBEDDER_URL", "http://embedder:8000")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
GENERATION_MODEL = os.getenv("GENERATION_MODEL", "gpt-4o-mini")
MONGODB_URI = os.getenv("MONGODB_URI", "")
MONGODB_DB = os.getenv("MONGODB_DB", "agent_assist")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "utterances")

_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
_mongo: Optional[MongoClient] = MongoClient(MONGODB_URI) if MONGODB_URI else None
_col: Optional[Collection] = _mongo[MONGODB_DB][MONGODB_COLLECTION] if _mongo else None
_embed_cache = LRUCache(maxsize=50_000)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


def _embed_text(text: str) -> List[float]:
    if text in _embed_cache:
        return _embed_cache[text]
    if not _client:
        raise HTTPException(status_code=500, detail="OpenAI not configured")
    resp = _client.embeddings.create(model=EMBEDDING_MODEL, input=[text.strip().replace("\n", " ")])
    vec = resp.data[0].embedding
    _embed_cache[text] = vec
    return vec


def _vector_search(query_vec: List[float], k: int, filters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not _col:
        return []
    vector_stage: Dict[str, Any] = {
        "$vectorSearch": {
            "index": "vector_index",
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
            "text": 1,
            "speaker_role": 1,
            "metadata": 1,
            "sentiment": 1,
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
    if retrieved and isinstance(retrieved[0].get("sentiment"), dict):
        sentiment_label = retrieved[0]["sentiment"].get("label", "NEU")
    user_prompt = build_user_prompt(req.latest_utterance, retrieved, sentiment_label)

    if not _client:
        raise HTTPException(status_code=500, detail="OpenAI not configured")
    model = req.model or GENERATION_MODEL
    completion = _client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": ASSIST_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.4,
    )
    text = completion.choices[0].message.content.strip()
    # Split alternatives if author provided bullets (very lightweight parsing)
    lines = [ln.strip("-• ").strip() for ln in text.split("\n") if ln.strip()]
    suggestion = lines[0] if lines else text
    alternatives = [ln for ln in lines[1:3]] if len(lines) > 1 else []
    return AssistResponse(suggestion=suggestion, alternatives=alternatives, retrieved=retrieved)


