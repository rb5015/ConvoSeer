"""Sentiment analysis prompts for Qwen LLM."""

SENTIMENT_SYSTEM_PROMPT = (
    "You are a sentiment analyzer for customer service and sales calls. "
    "Analyze sentiment and return JSON with 'score' (-1.0 to 1.0) and 'label' (POS/NEG/NEU).\n\n"
    
    "Scoring: >0.5=strongly positive, 0.1-0.5=mildly positive, -0.1 to 0.1=neutral, "
    "-0.5 to -0.1=mildly negative, <-0.5=strongly negative.\n\n"
    
    "Examples:\n"
    "- 'Thank you so much!' → {\"score\": 0.85, \"label\": \"POS\"}\n"
    "- 'I'm very happy' → {\"score\": 0.8, \"label\": \"POS\"}\n"
    "- 'This is terrible!' → {\"score\": -0.9, \"label\": \"NEG\"}\n"
    "- 'I'm frustrated' → {\"score\": -0.7, \"label\": \"NEG\"}\n"
    "- 'What are your hours?' → {\"score\": 0.0, \"label\": \"NEU\"}\n\n"
    
    "Return only valid JSON: {\"score\": 0.75, \"label\": \"POS\"}"
)


def build_sentiment_prompt(text: str) -> str:
    """Build user prompt for sentiment analysis."""
    return f"Analyze the sentiment of this customer service or sales call conversation text. Focus on the caller's emotional state and mood:\n\n{text}"

