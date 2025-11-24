from typing import List, Dict, Any
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from cachetools import LRUCache
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from google import genai
from google.genai import types


class EmbedRequest(BaseModel):
    texts: List[str] = Field(..., description="Texts to embed")
    model: str | None = Field(default=None, description="Override embedding model")


class EmbedResponse(BaseModel):
    model: str
    embeddings: List[List[float]]


app = FastAPI(title="Embedder Service", version="0.1.0")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
DEFAULT_MODEL = os.getenv("EMBEDDING_MODEL", "gemini-embedding-001")

_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None
_cache = LRUCache(maxsize=100_000)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


def _normalize_text(text: str) -> str:
    return text.strip().replace("\n", " ")


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
    retry=retry_if_exception_type((Exception,)),
)
def _embed_batch(texts: List[str], model: str) -> List[List[float]]:
    assert _client is not None, "Gemini client not initialized"
    # Gemini API supports batch embedding with task_type for optimization
    result = _client.models.embed_content(
        model=model,
        contents=texts,
        config=types.EmbedContentConfig(
            task_type="SEMANTIC_SIMILARITY"
        )
    )
    # Gemini returns vectors in result.embeddings[i].values
    return [list(emb.values) for emb in result.embeddings]


@app.post("/embed", response_model=EmbedResponse)
def embed(req: EmbedRequest) -> Any:
    if not _client:
        raise HTTPException(status_code=500, detail="Gemini not configured")
    model = req.model or DEFAULT_MODEL
    inputs = [ _normalize_text(t or "") for t in req.texts ]

    # cache hits
    results: List[List[float]] = []
    to_query: List[str] = []
    to_query_indices: List[int] = []
    for idx, t in enumerate(inputs):
        if t in _cache:
            results.append(_cache[t])
        else:
            results.append([])  # placeholder
            to_query.append(t)
            to_query_indices.append(idx)

    if to_query:
        vectors = _embed_batch(to_query, model)
        for offset, vec in enumerate(vectors):
            idx = to_query_indices[offset]
            _cache[to_query[offset]] = vec
            results[idx] = vec

    return EmbedResponse(model=model, embeddings=results)


