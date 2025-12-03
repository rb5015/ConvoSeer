from typing import List, Dict, Any
import os
import json
import re
import torch
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from cachetools import LRUCache
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from transformers import AutoModelForCausalLM, AutoTokenizer

try:
    from .prompts import SENTIMENT_SYSTEM_PROMPT, build_sentiment_prompt
except ImportError:
    # Fallback for direct execution
    from prompts import SENTIMENT_SYSTEM_PROMPT, build_sentiment_prompt


class SentimentRequest(BaseModel):
    texts: List[str] = Field(..., description="Texts to analyze for sentiment")
    model: str | None = Field(default=None, description="Override sentiment model")


class SentimentResult(BaseModel):
    score: float
    label: str


class SentimentResponse(BaseModel):
    model: str
    results: List[SentimentResult]


app = FastAPI(title="Sentiment Analysis Service", version="0.1.0")

DEFAULT_MODEL = os.getenv("SENTIMENT_MODEL", "Qwen/Qwen2.5-0.5B-Instruct")
MAX_LENGTH = int(os.getenv("SENTIMENT_MAX_LENGTH", "4096"))
DEVICE = os.getenv("SENTIMENT_DEVICE", "cuda" if torch.cuda.is_available() else "cpu")
MAX_NEW_TOKENS = int(os.getenv("SENTIMENT_MAX_NEW_TOKENS", "128"))

# Initialize model and tokenizer (lazy loading)
_tokenizer = None
_model = None
_cache = LRUCache(maxsize=10_000)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "model": DEFAULT_MODEL, "device": DEVICE}


def _get_model_and_tokenizer(model_name: str):
    """Lazy load model and tokenizer."""
    global _tokenizer, _model
    if _tokenizer is None or _model is None:
        print(f"Loading Qwen sentiment model: {model_name} on {DEVICE}")
        _tokenizer = AutoTokenizer.from_pretrained(model_name)
        _model = AutoModelForCausalLM.from_pretrained(
            model_name,
            torch_dtype=torch.float16 if DEVICE == "cuda" else torch.float32,
            device_map="auto" if DEVICE == "cuda" else None,
        )
        if DEVICE == "cpu":
            _model.to(DEVICE)
        _model.eval()
        print(f"✅ Sentiment model loaded successfully")
    return _tokenizer, _model


def _parse_sentiment_response(response_text: str, original_text: str = "") -> tuple[float, str]:
    """Parse sentiment response from Qwen LLM output."""
    # Keyword-based sentiment detection (run first as fallback/validation)
    keyword_score = None
    keyword_label = None
    if original_text:
        text_lower = original_text.lower()
        positive_keywords = ["thank", "thanks", "great", "good", "happy", "pleased", "excellent", "wonderful", "appreciate", "love", "perfect", "amazing", "fantastic"]
        negative_keywords = ["terrible", "bad", "unhappy", "angry", "frustrated", "disappointed", "worst", "horrible", "hate", "awful", "sucks", "not working"]
        
        has_positive = any(kw in text_lower for kw in positive_keywords)
        has_negative = any(kw in text_lower for kw in negative_keywords)
        
        if has_positive and not has_negative:
            keyword_score = 0.7
            keyword_label = "POS"
        elif has_negative and not has_positive:
            keyword_score = -0.7
            keyword_label = "NEG"
        elif has_positive and has_negative:
            # Mixed sentiment - slight positive bias
            keyword_score = 0.2
            keyword_label = "NEU"
    
    # Try to extract JSON from response
    json_match = re.search(r'\{[^}]*"score"[^}]*"label"[^}]*\}', response_text)
    if json_match:
        try:
            data = json.loads(json_match.group())
            score = float(data.get("score", 0.0))
            label = str(data.get("label", "NEU")).upper()
            # Validate label
            if label not in ["POS", "NEG", "NEU"]:
                label = "NEU"
            # Clamp score to [-1, 1]
            score = max(-1.0, min(1.0, score))
            
            # CRITICAL: If label contradicts score, trust the score and infer label
            # This handles cases where model returns correct score but wrong label
            if score < -0.3 and label in ["POS", "NEU"]:
                label = "NEG"
            elif score > 0.3 and label in ["NEG", "NEU"]:
                label = "POS"
            elif abs(score) <= 0.3:
                label = "NEU"
            
            # If keyword analysis contradicts model output, trust keywords for score adjustment
            # This handles cases where the model clearly misclassifies
            if keyword_score is not None and keyword_label is not None:
                # Check for contradiction: positive keywords but negative score
                if keyword_label == "POS" and score < -0.1:
                    # Keywords say positive but model says negative - trust keywords
                    score = keyword_score
                    label = keyword_label
                # Check for contradiction: negative keywords but positive score
                elif keyword_label == "NEG" and score > 0.1:
                    # Keywords say negative but model says positive - trust keywords
                    score = keyword_score
                    label = keyword_label
                # If model is uncertain (low absolute score) but keywords are clear, use keywords
                elif keyword_label == "POS" and abs(score) < 0.4:
                    score = keyword_score
                    label = keyword_label
                elif keyword_label == "NEG" and abs(score) < 0.4:
                    score = keyword_score
                    label = keyword_label
            
            return score, label
        except (json.JSONDecodeError, ValueError, KeyError):
            pass
    
    # If JSON parsing failed but we have keyword analysis, use that
    if keyword_score is not None and keyword_label is not None:
        return keyword_score, keyword_label
    
    # Fallback: try to infer from text
    response_lower = response_text.lower()
    if "pos" in response_lower or "positive" in response_lower:
        return 0.7, "POS"
    elif "neg" in response_lower or "negative" in response_lower:
        return -0.7, "NEG"
    else:
        # Infer from score if available in text
        score_match = re.search(r'"score"\s*:\s*(-?\d+\.?\d*)', response_text)
        if score_match:
            score = float(score_match.group(1))
            if score < -0.3:
                return score, "NEG"
            elif score > 0.3:
                return score, "POS"
        return 0.0, "NEU"


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
    retry=retry_if_exception_type((Exception,)),
)
def _analyze_sentiment_batch(texts: List[str], model_name: str) -> List[tuple[float, str]]:
    """Analyze sentiment for a batch of texts using Qwen LLM."""
    tokenizer, model = _get_model_and_tokenizer(model_name)
    results = []
    
    for text in texts:
        if not text or not text.strip():
            results.append((0.0, "NEU"))
            continue
        
        # Build prompt
        user_prompt = build_sentiment_prompt(text.strip())
        messages = [
            {"role": "system", "content": SENTIMENT_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ]
        
        # Apply chat template
        formatted_text = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True
        )
        
        # Tokenize
        model_inputs = tokenizer([formatted_text], return_tensors="pt", truncation=True, max_length=MAX_LENGTH)
        model_inputs = {k: v.to(model.device) for k, v in model_inputs.items()}
        
        # Generate
        with torch.no_grad():
            generated_ids = model.generate(
                **model_inputs,
                max_new_tokens=MAX_NEW_TOKENS,
                temperature=0.3,
                do_sample=True,
                pad_token_id=tokenizer.eos_token_id
            )
        
        # Decode response
        input_length = model_inputs["input_ids"].shape[1]
        output_ids = generated_ids[0][input_length:].tolist()
        response = tokenizer.decode(output_ids, skip_special_tokens=True)
        
        # Parse sentiment (pass original text for keyword validation)
        score, label = _parse_sentiment_response(response, original_text=text.strip())
        results.append((score, label))
    
    return results


@app.post("/analyze", response_model=SentimentResponse)
def analyze(req: SentimentRequest) -> Any:
    model_name = req.model or DEFAULT_MODEL
    texts = [t.strip() if t else "" for t in req.texts]
    
    if not texts:
        raise HTTPException(status_code=400, detail="No texts provided")
    
    # Check cache
    results: List[tuple[float, str]] = []
    to_analyze: List[str] = []
    to_analyze_indices: List[int] = []
    
    for idx, text in enumerate(texts):
        if not text:
            results.append((0.0, "NEU"))
            continue
        
        cache_key = f"{model_name}:{text}"
        if cache_key in _cache:
            cached_score, cached_label = _cache[cache_key]
            results.append((cached_score, cached_label))
        else:
            results.append((0.0, "NEU"))  # placeholder
            to_analyze.append(text)
            to_analyze_indices.append(idx)
    
    # Analyze uncached texts
    if to_analyze:
        try:
            analyzed = _analyze_sentiment_batch(to_analyze, model_name)
            for offset, (score, label) in enumerate(analyzed):
                idx = to_analyze_indices[offset]
                cache_key = f"{model_name}:{to_analyze[offset]}"
                _cache[cache_key] = (score, label)
                results[idx] = (score, label)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Sentiment analysis failed: {str(e)}")
    
    # Convert to response format
    sentiment_results = [SentimentResult(score=score, label=label) for score, label in results]
    
    return SentimentResponse(model=model_name, results=sentiment_results)


@app.post("/analyze/clear-cache")
def clear_cache():
    """Clear the sentiment analysis cache."""
    global _cache
    cache_size = len(_cache)
    _cache.clear()
    return {"status": "ok", "cleared": cache_size}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

