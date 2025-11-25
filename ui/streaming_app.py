"""
Streaming Agent Assist UI - Real-time audio transcription with live sentiment and RAG suggestions.
Product-like interface with continuous streaming support.
"""
import os
import time
import json
import requests
import streamlit as st
import pandas as pd
from audio_recorder_streamlit import audio_recorder
import io
import threading
from sseclient import SSEClient
from datetime import datetime
from typing import Optional


# Configuration
RAG_URL = os.getenv("RAG_URL", "http://localhost:8002")
AUDIO_SERVICE_URL = os.getenv("AUDIO_SERVICE_URL", "http://localhost:8004")
STREAM_URL = os.getenv("STREAM_URL", "http://localhost:8003")

# Page config
st.set_page_config(
    page_title="Agent Assist - Live Stream",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for polished UI
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 1rem;
    }
    .status-indicator {
        padding: 0.5rem 1rem;
        border-radius: 0.5rem;
        font-weight: 600;
        display: inline-block;
        margin: 0.5rem 0;
    }
    .status-recording {
        background-color: #ff4444;
        color: white;
        animation: pulse 2s infinite;
    }
    .status-idle {
        background-color: #888;
        color: white;
    }
    .status-connected {
        background-color: #4CAF50;
        color: white;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }
    .transcription-window {
        background-color: #f8f9fa;
        border: 2px solid #dee2e6;
        border-radius: 0.5rem;
        padding: 1rem;
        max-height: 450px;
        overflow-y: auto;
        font-family: 'Courier New', monospace;
        font-size: 0.9rem;
    }
    .utterance-item {
        padding: 0.75rem;
        margin: 0.5rem 0;
        border-left: 4px solid #1f77b4;
        background-color: white;
        border-radius: 0.25rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    .utterance-item:hover {
        box-shadow: 0 2px 6px rgba(0,0,0,0.15);
    }
    .sentiment-positive { border-left-color: #4CAF50; }
    .sentiment-negative { border-left-color: #f44336; }
    .sentiment-neutral { border-left-color: #ff9800; }
    .suggestion-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #dee2e6;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .stButton>button {
        width: 100%;
        border-radius: 0.5rem;
        font-weight: 600;
        padding: 0.5rem 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'call_id' not in st.session_state:
    st.session_state['call_id'] = f"call-{int(time.time())}"
if 'is_recording' not in st.session_state:
    st.session_state['is_recording'] = False
if 'transcriptions' not in st.session_state:
    st.session_state['transcriptions'] = []
if 'sentiment_history' not in st.session_state:
    st.session_state['sentiment_history'] = []
if 'rag_suggestions' not in st.session_state:
    st.session_state['rag_suggestions'] = []
if 'utterance_index' not in st.session_state:
    st.session_state['utterance_index'] = 0
if 'last_audio_send' not in st.session_state:
    st.session_state['last_audio_send'] = 0
if 'last_audio_hash' not in st.session_state:
    st.session_state['last_audio_hash'] = None
if 'stream_thread' not in st.session_state:
    st.session_state['stream_thread'] = None
if 'backend_connected' not in st.session_state:
    st.session_state['backend_connected'] = False
if 'last_transcription_id' not in st.session_state:
    st.session_state['last_transcription_id'] = None


def check_backend_connection():
    """Check if backend services are available."""
    try:
        audio_health = requests.get(f"{AUDIO_SERVICE_URL}/health", timeout=2)
        stream_health = requests.get(f"{STREAM_URL}/health", timeout=2)
        return audio_health.status_code == 200 and stream_health.status_code == 200
    except:
        return False


def send_audio_chunk(audio_bytes: bytes, call_id: str, utterance_index: int):
    """Send audio chunk to backend for transcription."""
    try:
        # Validate audio bytes
        if not audio_bytes or len(audio_bytes) < 100:
            print(f"⚠️  Audio chunk too small: {len(audio_bytes)} bytes")
            return None
        
        # Check maximum size (5MB)
        MAX_AUDIO_SIZE = 5 * 1024 * 1024
        if len(audio_bytes) > MAX_AUDIO_SIZE:
            print(f"⚠️  Audio chunk too large: {len(audio_bytes) / (1024 * 1024):.2f} MB (max: 5 MB)")
            return None
        
        files = {
            'audio_file': ('audio.wav', io.BytesIO(audio_bytes), 'audio/wav')
        }
        data = {
            'call_id': call_id,
            'speaker_role': 'customer',
            'utterance_index': utterance_index
        }
        
        response = requests.post(
            f"{AUDIO_SERVICE_URL}/transcribe",
            files=files,
            data=data,
            timeout=90  # Increased timeout for larger chunks
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP error: {e}")
        if e.response is not None:
            try:
                error_detail = e.response.json()
                print(f"   Error detail: {error_detail}")
            except:
                print(f"   Error text: {e.response.text}")
        return None
    except requests.exceptions.Timeout:
        print(f"❌ Request timeout - audio chunk may be too large or backend is slow")
        return None
    except Exception as e:
        print(f"❌ Error sending audio: {e}")
        return None


def stream_updates_thread(call_id: str):
    """Background thread to stream updates via SSE."""
    try:
        stream_url = f"{STREAM_URL}/stream/{call_id}"
        messages = SSEClient(stream_url)
        
        for msg in messages:
            if msg.event == "transcription":
                data = json.loads(msg.data)
                utterance_id = data.get('utterance_id')
                # Avoid duplicates
                if utterance_id != st.session_state.get('last_transcription_id'):
                    transcription = {
                        'text': data.get('text', ''),
                        'utterance_id': utterance_id,
                        'timestamp': datetime.now().strftime("%H:%M:%S"),
                        'index': data.get('utterance_index', 0),
                        'sentiment': data.get('sentiment', {}).get('label', 'NEU')
                    }
                    st.session_state['transcriptions'].append(transcription)
                    st.session_state['last_transcription_id'] = utterance_id
                    # Keep only last 100 transcriptions
                    if len(st.session_state['transcriptions']) > 100:
                        st.session_state['transcriptions'] = st.session_state['transcriptions'][-100:]
            
            elif msg.event == "sentiment":
                data = json.loads(msg.data)
                st.session_state['sentiment_history'].append(data)
                # Keep only last 50 windows
                if len(st.session_state['sentiment_history']) > 50:
                    st.session_state['sentiment_history'] = st.session_state['sentiment_history'][-50:]
            
            elif msg.event == "rag":
                data = json.loads(msg.data)
                st.session_state['rag_suggestions'].append(data)
                # Keep only last 20 suggestions
                if len(st.session_state['rag_suggestions']) > 20:
                    st.session_state['rag_suggestions'] = st.session_state['rag_suggestions'][-20:]
    except Exception as e:
        pass  # Thread will handle errors silently


# Header
st.markdown('<div class="main-header">🎙️ Agent Assist - Live Streaming</div>', unsafe_allow_html=True)

# Top bar with status and controls
col_header1, col_header2, col_header3, col_header4 = st.columns([2, 1.5, 1, 1])

with col_header1:
    call_id = st.text_input("Call ID", value=st.session_state['call_id'], key="call_id_input", label_visibility="collapsed")
    st.session_state['call_id'] = call_id

with col_header2:
    # Status indicator
    if st.session_state.get('is_recording'):
        status_html = '<div class="status-indicator status-recording">🔴 RECORDING</div>'
    elif st.session_state.get('backend_connected'):
        status_html = '<div class="status-indicator status-connected">✅ Connected</div>'
    else:
        status_html = '<div class="status-indicator status-idle">⚪ Idle</div>'
    st.markdown(status_html, unsafe_allow_html=True)

with col_header3:
    st.metric("Utterances", len(st.session_state['transcriptions']))

with col_header4:
    if st.button("🔄 Clear", use_container_width=True):
        st.session_state['transcriptions'] = []
        st.session_state['sentiment_history'] = []
        st.session_state['rag_suggestions'] = []
        st.session_state['utterance_index'] = 0
        st.session_state['last_transcription_id'] = None
        st.rerun()

# Check backend connection
if not st.session_state.get('backend_checked', False):
    st.session_state['backend_connected'] = check_backend_connection()
    st.session_state['backend_checked'] = True

# Main layout - 3 columns
col1, col2, col3 = st.columns([2, 1.5, 1.5])

with col1:
    st.subheader("📝 Live Transcription")
    
    # Recording controls
    recording_col1, recording_col2 = st.columns([2, 1])
    
    with recording_col1:
        if st.session_state.get('is_recording'):
            if st.button("⏹️ Stop Recording", type="primary", use_container_width=True):
                st.session_state['is_recording'] = False
                st.rerun()
        else:
            if st.button("🎤 Start Recording", type="primary", use_container_width=True):
                st.session_state['is_recording'] = True
                st.session_state['backend_connected'] = check_backend_connection()
                # Start streaming thread
                if st.session_state['stream_thread'] is None or not st.session_state['stream_thread'].is_alive():
                    thread = threading.Thread(
                        target=stream_updates_thread,
                        args=(st.session_state['call_id'],),
                        daemon=True
                    )
                    thread.start()
                    st.session_state['stream_thread'] = thread
                st.rerun()
    
    with recording_col2:
        if st.session_state.get('is_recording'):
            st.caption(f"Index: {st.session_state['utterance_index']}")
    
    # Audio recorder (only show when recording)
    if st.session_state.get('is_recording'):
        st.caption("🎤 Recording... Speak clearly. Audio chunks are automatically sent every 2-3 seconds.")
        audio_bytes = audio_recorder(
            text="",
            recording_color="#ff4444",
            neutral_color="#888",
            icon_name="microphone",
            icon_size="3x",
            pause_threshold=2.0,  # Reduced from 3.0 to send chunks more frequently
        )
        
        # Debug info
        if audio_bytes:
            audio_size_mb = len(audio_bytes) / (1024 * 1024)
            st.caption(f"📊 Audio chunk size: {len(audio_bytes):,} bytes ({audio_size_mb:.2f} MB)")
        
        # Process audio chunk automatically (every 2+ seconds or when new audio detected)
        current_time = time.time()
        audio_hash = hash(audio_bytes) if audio_bytes else None
        
        # Maximum audio size (5MB = 5 * 1024 * 1024 bytes)
        MAX_AUDIO_SIZE = 5 * 1024 * 1024
        
        # Check if we have new audio to send
        if audio_bytes and audio_hash != st.session_state.get('last_audio_hash'):
            # Validate audio size before sending
            if len(audio_bytes) < 100:
                st.warning(f"⚠️ Audio chunk too small ({len(audio_bytes)} bytes). Please speak louder or check your microphone.")
                st.caption("💡 Tip: The audio recorder needs at least 2 seconds of continuous speech.")
            elif len(audio_bytes) > MAX_AUDIO_SIZE:
                st.error(f"⚠️ Audio chunk too large ({len(audio_bytes) / (1024 * 1024):.2f} MB). Maximum is 5 MB.")
                st.caption("💡 Tip: The audio recorder accumulated too much audio. Please pause briefly between sentences.")
                # Reset the hash so we don't keep showing this error
                st.session_state['last_audio_hash'] = audio_hash
            # Wait a bit to ensure we have a complete chunk (reduced to 2 seconds)
            elif current_time - st.session_state['last_audio_send'] > 2:
                with st.spinner("🔄 Transcribing audio chunk..."):
                    audio_size_mb = len(audio_bytes) / (1024 * 1024)
                    st.caption(f"Sending {len(audio_bytes):,} bytes ({audio_size_mb:.2f} MB) to backend...")
                    result = send_audio_chunk(
                        audio_bytes,
                        st.session_state['call_id'],
                        st.session_state['utterance_index']
                    )
                    
                    if result and result.get('published') and result.get('text'):
                        st.session_state['utterance_index'] += 1
                        st.session_state['last_audio_send'] = current_time
                        st.session_state['last_audio_hash'] = audio_hash
                        st.success(f"✓ Sent: \"{result['text'][:50]}...\"")
                        # Transcription will also appear via SSE stream
                    elif result and result.get('text'):
                        st.warning("Transcribed but not published to Kafka")
                    elif result is None:
                        st.error("❌ Failed to send audio. Check backend logs.")
                        st.caption("💡 Check: Is the audio service running? Run: docker compose logs -f audio-service")
                    else:
                        st.info("No speech detected in this chunk. Try speaking more clearly.")
    
    # Transcription window (scrolling)
    st.markdown('<div class="transcription-window">', unsafe_allow_html=True)
    
    if st.session_state['transcriptions']:
        for trans in st.session_state['transcriptions']:
            # Determine sentiment class
            sentiment = trans.get('sentiment', 'NEU')
            if sentiment == 'POS':
                sentiment_class = "sentiment-positive"
            elif sentiment == 'NEG':
                sentiment_class = "sentiment-negative"
            else:
                sentiment_class = "sentiment-neutral"
            
            st.markdown(f"""
            <div class="utterance-item {sentiment_class}">
                <strong>[{trans['timestamp']}]</strong> {trans['text']}
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No transcriptions yet. Start recording to begin.")
        st.caption("Transcriptions will appear here as audio is processed...")
    
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.subheader("📊 Sentiment Analysis")
    
    # Current sentiment
    if st.session_state['sentiment_history']:
        latest_sentiment = st.session_state['sentiment_history'][-1]
        avg_score = latest_sentiment.get('avg_sentiment_score', 0.0)
        
        # Sentiment gauge
        if avg_score > 0.2:
            sentiment_label = "Positive"
            sentiment_color = "#4CAF50"
            delta = f"+{avg_score:.2f}"
        elif avg_score < -0.2:
            sentiment_label = "Negative"
            sentiment_color = "#f44336"
            delta = f"{avg_score:.2f}"
        else:
            sentiment_label = "Neutral"
            sentiment_color = "#ff9800"
            delta = f"{avg_score:.2f}"
        
        st.metric("Current Sentiment", sentiment_label, delta=delta)
        
        # Sentiment chart
        if len(st.session_state['sentiment_history']) > 1:
            df = pd.DataFrame([
                {
                    'Time': i,
                    'Sentiment': d.get('avg_sentiment_score', 0.0)
                }
                for i, d in enumerate(st.session_state['sentiment_history'][-20:])
            ])
            st.line_chart(df.set_index('Time'), height=200, color="#667eea")
        
        # Latest window info
        with st.expander("📈 Latest Window Details"):
            st.write(f"**Window:** {latest_sentiment.get('window_start', 'N/A')} - {latest_sentiment.get('window_end', 'N/A')}")
            st.write(f"**Utterances:** {latest_sentiment.get('utterance_count', 0)}")
            st.write(f"**Score:** {avg_score:.3f}")
    else:
        st.info("Waiting for sentiment data...")
        st.caption("Sentiment analysis appears every ~10 seconds")

with col3:
    st.subheader("🤖 Agent Suggestions")
    
    # Latest RAG suggestion
    if st.session_state['rag_suggestions']:
        latest_rag = st.session_state['rag_suggestions'][-1]
        rag_response = latest_rag.get('rag_response', {})
        
        st.markdown(f"""
        <div class="suggestion-card">
            <h4>💡 Latest Suggestion</h4>
            <p style="font-size: 1.1rem; margin-top: 0.5rem;">{rag_response.get('suggestion', 'No suggestion')}</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Alternatives
        alternatives = rag_response.get('alternatives', [])
        if alternatives:
            st.write("**Alternatives:**")
            for alt in alternatives:
                st.write(f"• {alt}")
        
        # Context
        with st.expander("🔍 Context"):
            latest_utterance = latest_rag.get('latest_utterance', '')
            if latest_utterance:
                st.write(f"**Latest:** {latest_utterance[:80]}{'...' if len(latest_utterance) > 80 else ''}")
            st.write(f"**Retrieved:** {rag_response.get('retrieved_count', 0)} similar conversations")
            sentiment_info = latest_rag.get('sentiment', {})
            st.write(f"**Sentiment:** {sentiment_info.get('avg_score', 0.0):.2f}")
    else:
        st.info("Waiting for suggestions...")
        st.caption("Suggestions appear every ~10 seconds after transcription")

# Auto-refresh when recording (every 1.5 seconds for real-time feel)
if st.session_state.get('is_recording'):
    time.sleep(1.5)
    st.rerun()

# Footer with connection status
st.divider()
footer_col1, footer_col2, footer_col3 = st.columns(3)

with footer_col1:
    try:
        audio_status = "✅" if requests.get(f'{AUDIO_SERVICE_URL}/health', timeout=1).status_code == 200 else "❌"
    except:
        audio_status = "❌"
    st.caption(f"**Audio Service:** {audio_status}")

with footer_col2:
    try:
        stream_status = "✅" if requests.get(f'{STREAM_URL}/health', timeout=1).status_code == 200 else "❌"
    except:
        stream_status = "❌"
    st.caption(f"**Stream Service:** {stream_status}")

with footer_col3:
    st.caption(f"**Call ID:** {st.session_state['call_id']}")
