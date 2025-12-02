from typing import List, Dict

ASSIST_SYSTEM_PROMPT = (
    "You are a helpful sales assistant that suggests the next reply for an agent. "
    "Use the retrieved similar utterances as guidance, stay concise and empathetic. "
    "Adhere to compliance and avoid making false promises."
)


def build_user_prompt(latest_utterance: str, retrieved: List[Dict], sentiment_label: str) -> str:
    context_lines = []
    for i, doc in enumerate(retrieved[:8], start=1):
        # Support both old schema (text) and new schema (chunk_text)
        txt = doc.get("chunk_text") or doc.get("text", "")
        product = doc.get("product", "UNKNOWN")
        s = doc.get("sentiment", "neutral")
        # Handle both string and dict sentiment formats
        if isinstance(s, dict):
            s_lab = s.get("label", "neutral")
        else:
            s_lab = str(s).lower()
        context_lines.append(f"{i}. [{product}][{s_lab}] {txt}")
    context = "\n".join(context_lines) if context_lines else "No similar utterances found."
    tone = "empathetic and reassuring" if sentiment_label == "NEG" else "confident and friendly"
    return (
        f"Customer latest message:\n\"{latest_utterance}\"\n\n"
        f"Similar past utterances:\n{context}\n\n"
        f"Customer sentiment appears {sentiment_label}. "
        f"Respond in a {tone} tone. Provide one succinct response (2-3 sentences) and optionally 2 bullet-point alternatives."
    )


