from typing import List, Dict, Any
import os
import torch
import torch.nn.functional as F
from torch import Tensor
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from cachetools import LRUCache
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from transformers import AutoTokenizer, AutoModel


class EmbedRequest(BaseModel):
    texts: List[str] = Field(..., description="Texts to embed")
    model: str | None = Field(default=None, description="Override embedding model")


class EmbedResponse(BaseModel):
    model: str
    embeddings: List[List[float]]


app = FastAPI(title="Embedder Service", version="0.1.0")

DEFAULT_MODEL = os.getenv("EMBEDDING_MODEL", "mixedbread-ai/mxbai-embed-large-v1")
MAX_LENGTH = int(os.getenv("EMBEDDING_MAX_LENGTH", "8192"))
DEVICE = os.getenv("EMBEDDING_DEVICE", "cuda" if torch.cuda.is_available() else "cpu")

# Initialize model and tokenizer (lazy loading)
_tokenizer = None
_model = None
_cache = LRUCache(maxsize=100_000)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


def _normalize_text(text: str) -> str:
    return text.strip().replace("\n", " ")


def _get_model_and_tokenizer(model_name: str):
    """Lazy load model and tokenizer with memory optimization."""
    global _tokenizer, _model
    if _tokenizer is None or _model is None:
        print(f"Loading embedding model: {model_name} on {DEVICE}")
        _tokenizer = AutoTokenizer.from_pretrained(model_name)
        # Use half precision (float16) to reduce memory on CPU/Apple Silicon
        _model = AutoModel.from_pretrained(
            model_name,
            torch_dtype=torch.float16 if DEVICE == "cpu" else torch.float32
        )
        _model.to(DEVICE)
        _model.eval()
        print(f"✅ Model loaded successfully on {DEVICE} (dtype: float16)" if DEVICE == "cpu" else f"✅ Model loaded successfully on {DEVICE} (dtype: float32)")
    return _tokenizer, _model


def mean_pooling(last_hidden_states: Tensor, attention_mask: Tensor) -> Tensor:
    """Pool embeddings using mean pooling (used by MXBAI models)."""
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(last_hidden_states.size()).float()
    sum_embeddings = torch.sum(last_hidden_states * input_mask_expanded, 1)
    sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
    return sum_embeddings / sum_mask


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
    retry=retry_if_exception_type((Exception,)),
)
def _embed_batch(texts: List[str], model_name: str) -> List[List[float]]:
    """Generate embeddings using MXBAI-Embed-Large model."""
    tokenizer, model = _get_model_and_tokenizer(model_name)
    
    # Tokenize the input texts
    batch_dict = tokenizer(
        texts,
        padding=True,
        truncation=True,
        max_length=MAX_LENGTH,
        return_tensors="pt",
    )
    # Move all tensors in batch_dict to model device (fix for Apple Silicon meta device issues)
    batch_dict = {key: val.to(model.device) if hasattr(val, 'to') else val for key, val in batch_dict.items()}
    
    with torch.no_grad():
        outputs = model(**batch_dict)
        
        # MXBAI models return last_hidden_state
        if hasattr(outputs, 'last_hidden_state') and outputs.last_hidden_state is not None:
            embeddings = mean_pooling(outputs.last_hidden_state, batch_dict['attention_mask'])
        else:
            raise ValueError(f"Model output doesn't have expected 'last_hidden_state' attribute")
        
        # Normalize embeddings (L2 normalization)
        embeddings = F.normalize(embeddings, p=2, dim=1)
    
    # Convert to list of lists
    return embeddings.cpu().tolist()


@app.post("/embed", response_model=EmbedResponse)
def embed(req: EmbedRequest) -> Any:
    model_name = req.model or DEFAULT_MODEL
    inputs = [ _normalize_text(t or "") for t in req.texts ]
    
    if not inputs:
        raise HTTPException(status_code=400, detail="No texts provided")

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
        try:
            vectors = _embed_batch(to_query, model_name)
            for offset, vec in enumerate(vectors):
                idx = to_query_indices[offset]
                _cache[to_query[offset]] = vec
                results[idx] = vec
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"❌ Embedding generation error: {str(e)}")
            print(f"Traceback:\n{error_details}")
            raise HTTPException(status_code=500, detail=f"Embedding generation failed: {str(e)}")

    return EmbedResponse(model=model_name, embeddings=results)


