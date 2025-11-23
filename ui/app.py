import os
import time
import requests
import streamlit as st
import pandas as pd
from audio_recorder_streamlit import audio_recorder
from transcriber import transcribe_audio

RAG_URL = os.getenv("RAG_URL", "http://localhost:8002")

st.set_page_config(page_title="Agent Assist", layout="wide")
st.title("Agent Assist — Real-time Suggestions")

with st.sidebar:
    st.header("Settings")
    k = st.slider("Top-k retrieved", 1, 20, 8)
    industry = st.text_input("Filter: industry", value="")
    product = st.text_input("Filter: product", value="")
    sentiment_band = st.selectbox("Filter: sentiment", ["", "NEG", "NEU", "POS"], index=0)
    
    st.divider()
    st.header("Audio Settings")
    whisper_model = st.selectbox("Whisper Model", ["tiny", "base", "small"], index=1, 
                                 help="Larger models are more accurate but slower")

col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("Customer Input")
    call_id = st.text_input("Call ID", value="demo-call-1")
    
    # Input method selection
    input_method = st.radio(
        "Input Method",
        ["Type", "Record Audio"],
        horizontal=True,
        help="Choose to type text or record audio for transcription"
    )
    
    latest_utterance = ""
    
    if input_method == "Type":
        latest_utterance = st.text_area("Customer says...", height=120, value="", key="text_input")
    else:
        st.write("**Record Audio:**")
        st.caption("Click the microphone button below to start recording. Click again to stop.")
        
        # Audio recorder
        audio_bytes = audio_recorder(
            text="🎤 Click to record",
            recording_color="#e74c3c",
            neutral_color="#34495e",
            icon_name="microphone",
            icon_size="2x",
            pause_threshold=2.0,  # Pause after 2 seconds of silence
        )
        
        # Process audio if recorded
        if audio_bytes:
            with st.spinner("Transcribing audio with Whisper (this may take a moment on first run)..."):
                try:
                    transcribed = transcribe_audio(audio_bytes, model_size=whisper_model)
                    if transcribed:
                        st.session_state['transcribed_text'] = transcribed
                        st.success(f"✓ Transcription complete!")
                        st.audio(audio_bytes, format="audio/wav")
                    else:
                        st.warning("No speech detected in audio. Please try again.")
                except Exception as e:
                    st.error(f"Transcription error: {e}")
                    st.info("💡 Tip: The first run will download the Whisper model (~150MB for 'base'). This may take a few minutes.")
        
        # Display transcribed text
        if 'transcribed_text' in st.session_state:
            latest_utterance = st.session_state['transcribed_text']
            st.text_area("Transcribed text:", value=latest_utterance, height=120, key="transcribed_display")
        else:
            latest_utterance = st.text_area("Transcribed text will appear here...", height=120, value="", key="audio_input", disabled=True)
    
    go = st.button("Get Suggestions", type="primary", use_container_width=True)

with col2:
    st.subheader("Suggestions")
    suggestion_area = st.empty()
    alt_area = st.empty()

if go and latest_utterance.strip():
    filters = {}
    if industry: filters["metadata.industry"] = industry
    if product: filters["metadata.product"] = product
    if sentiment_band: filters["sentiment.label"] = sentiment_band

    payload = {
        "call_id": call_id,
        "latest_utterance": latest_utterance,
        "filters": filters,
        "k": k,
    }
    
    with st.spinner("Getting suggestions from RAG service..."):
        try:
            r = requests.post(f"{RAG_URL}/assist", json=payload, timeout=30)
            r.raise_for_status()
            data = r.json()
            
            suggestion_area.markdown(f"**Primary suggestion:**\n\n{data['suggestion']}")
            if data.get("alternatives"):
                alt_md = "\n".join([f"- {a}" for a in data["alternatives"]])
                alt_area.markdown(f"**Alternatives:**\n\n{alt_md}")
            else:
                alt_area.write("")

            st.subheader("Retrieved Context")
            if data.get("retrieved"):
                df = pd.DataFrame([
                    {
                        "score": round(doc.get("score", 0.0), 4),
                        "role": doc.get("speaker_role"),
                        "sentiment": (doc.get("sentiment") or {}).get("label"),
                        "text": doc.get("text"),
                    }
                    for doc in data["retrieved"]
                ])
                st.dataframe(df, use_container_width=True, height=240)
            else:
                st.write("No context retrieved.")
        except requests.exceptions.ConnectionError:
            st.error("❌ Cannot connect to RAG service. Make sure it's running:")
            st.code("docker compose up -d rag")
        except Exception as e:
            st.error(f"Request failed: {e}")

elif go and not latest_utterance.strip():
    st.warning("⚠️ Please enter some text or record audio before getting suggestions.")
