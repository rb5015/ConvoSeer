"""Sentiment analysis prompts for Qwen LLM."""

SENTIMENT_SYSTEM_PROMPT = (
    "You are a sentiment analysis expert. Analyze the sentiment and return JSON with 'score' (-1.0 to 1.0) and 'label' (POS/NEG/NEU).\n"
    "Examples:\n"
    "- 'Thank you very much' → {\"score\": 0.8, \"label\": \"POS\"}\n"
    "- 'I am very happy' → {\"score\": 0.9, \"label\": \"POS\"}\n"
    "- 'This is terrible' → {\"score\": -0.9, \"label\": \"NEG\"}\n"
    "- 'I am unhappy' → {\"score\": -0.8, \"label\": \"NEG\"}\n"
    "Return only valid JSON."
)


def build_sentiment_prompt(text: str) -> str:
    """Build user prompt for sentiment analysis."""
    return f"Analyze the sentiment of this customer service conversation text:\n\n{text}"

