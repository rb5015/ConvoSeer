import os
import time
import requests
import streamlit as st
import pandas as pd
from audio_recorder_streamlit import audio_recorder
from transcriber import transcribe_audio
import io

RAG_URL = os.getenv("RAG_URL", "http://localhost:8002")
AUDIO_SERVICE_URL = os.getenv("AUDIO_SERVICE_URL", "http://localhost:8004")

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
    
    # Initialize utterance counter
    if 'utterance_index' not in st.session_state:
        st.session_state['utterance_index'] = 0
    if 'transcription_history' not in st.session_state:
        st.session_state['transcription_history'] = []
    
    # Input method selection
    input_method = st.radio(
        "Input Method",
        ["Type", "Record Audio (Stream to Backend)"],
        horizontal=True,
        help="Choose to type text or record audio for streaming transcription"
    )
    
    latest_utterance = ""
    
    if input_method == "Type":
        latest_utterance = st.text_area("Customer says...", height=120, value="", key="text_input")
    else:
        st.write("**Record Audio:**")
        st.caption("Click the microphone button below to start recording. Audio will be sent to backend, transcribed, and published to Kafka.")
        
        # Show backend status
        backend_status = st.empty()
        
        # Test backend connection
        if 'backend_checked' not in st.session_state:
            try:
                test_response = requests.get(f"{AUDIO_SERVICE_URL}/health", timeout=3)
                if test_response.status_code == 200:
                    backend_status.success("✅ Backend connected")
                else:
                    backend_status.warning("⚠️ Backend responded with error")
            except Exception as e:
                backend_status.error(f"❌ Backend not connected: {str(e)}")
            st.session_state['backend_checked'] = True
        
        # Audio recorder
        audio_bytes = audio_recorder(
            text="🎤 Click to record",
            recording_color="#e74c3c",
            neutral_color="#34495e",
            icon_name="microphone",
            icon_size="2x",
            pause_threshold=2.0,  # Pause after 2 seconds of silence
        )
        
        # Process audio if recorded - send to backend
        if audio_bytes:
            with st.spinner("Sending audio to backend for transcription and Kafka publishing..."):
                try:
                    # Send audio to backend service
                    files = {
                        'audio_file': ('audio.wav', io.BytesIO(audio_bytes), 'audio/wav')
                    }
                    data = {
                        'call_id': call_id,
                        'speaker_role': 'customer',
                        'utterance_index': st.session_state['utterance_index']
                    }
                    
                    response = requests.post(
                        f"{AUDIO_SERVICE_URL}/transcribe",
                        files=files,
                        data=data,
                        timeout=60
                    )
                    response.raise_for_status()
                    result = response.json()
                    
                    if result.get('published') and result.get('text'):
                        transcribed_text = result['text']
                        st.session_state['transcribed_text'] = transcribed_text
                        st.session_state['utterance_index'] += 1
                        
                        # Add to history
                        st.session_state['transcription_history'].append({
                            'text': transcribed_text,
                            'utterance_id': result['utterance_id'],
                            'time': time.time(),
                            'transcription_time': result['transcription_time']
                        })
                        
                        st.success(f"✓ Transcribed and published to Kafka!")
                        st.info(f"**Utterance ID:** {result['utterance_id']}\n**Transcription time:** {result['transcription_time']:.2f}s")
                        st.audio(audio_bytes, format="audio/wav")
                        
                        backend_status.success(f"✅ Backend connected - Published to Kafka")
                    elif result.get('text'):
                        st.warning(f"Transcribed but not published: {result['text']}")
                    else:
                        st.warning("No speech detected in audio. Please try again.")
                        
                except requests.exceptions.ConnectionError:
                    st.error("❌ Cannot connect to audio service. Make sure it's running:")
                    st.code("docker compose up -d audio-service")
                    backend_status.error("❌ Backend not connected")
                    # Fallback to local transcription (requires ffmpeg)
                    st.info("💡 **Note:** Local transcription requires ffmpeg. Install with:")
                    st.code("brew install ffmpeg  # macOS\n# or\nsudo apt-get install ffmpeg  # Linux")
                    try:
                        transcribed = transcribe_audio(audio_bytes, model_size=whisper_model)
                        if transcribed:
                            st.session_state['transcribed_text'] = transcribed
                            st.success(f"✓ Local transcription complete (not published to Kafka)")
                    except FileNotFoundError as e:
                        if 'ffmpeg' in str(e).lower():
                            st.warning("⚠️ ffmpeg not found. Local transcription unavailable. Please install ffmpeg or ensure the audio service is running.")
                        else:
                            st.error(f"Local transcription failed: {e}")
                    except Exception as e:
                        st.error(f"Local transcription failed: {e}")
                except Exception as e:
                    st.error(f"Backend transcription error: {e}")
                    backend_status.error(f"❌ Error: {str(e)}")
        
        # Display transcribed text
        if 'transcribed_text' in st.session_state:
            latest_utterance = st.session_state['transcribed_text']
            st.text_area("Transcribed text:", value=latest_utterance, height=120, key="transcribed_display")
        else:
            latest_utterance = st.text_area("Transcribed text will appear here...", height=120, value="", key="audio_input", disabled=True)
        
        # Show transcription history
        if st.session_state.get('transcription_history'):
            with st.expander(f"📜 Transcription History ({len(st.session_state['transcription_history'])} utterances)"):
                for i, item in enumerate(reversed(st.session_state['transcription_history'][-10:]), 1):
                    st.text(f"{len(st.session_state['transcription_history']) - i + 1}. {item['text']}")
                    st.caption(f"ID: {item['utterance_id']} | Time: {item['transcription_time']:.2f}s")
    
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
