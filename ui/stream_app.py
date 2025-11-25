import os
import time
import json
import requests
import streamlit as st
import pandas as pd
from sseclient import SSEClient


RAG_URL = os.getenv("RAG_URL", "http://localhost:8002")
STREAM_URL = os.getenv("STREAM_URL", "http://localhost:8003")

st.set_page_config(page_title="Agent Assist - Live Stream", layout="wide")
st.title("Agent Assist — Live Streaming Dashboard")

# Initialize session state
if 'call_id' not in st.session_state:
    st.session_state['call_id'] = f"live-{int(time.time())}"
if 'sentiment_history' not in st.session_state:
    st.session_state['sentiment_history'] = []
if 'rag_history' not in st.session_state:
    st.session_state['rag_history'] = []

with st.sidebar:
    st.header("Live Stream Settings")
    call_id = st.text_input("Call ID", value=st.session_state['call_id'])
    st.session_state['call_id'] = call_id
    
    if st.button("Clear History"):
        st.session_state['sentiment_history'] = []
        st.session_state['rag_history'] = []
        st.rerun()

# Main layout
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("📊 Sentiment Analysis")
    sentiment_placeholder = st.empty()
    sentiment_chart_placeholder = st.empty()

with col2:
    st.subheader("🤖 RAG Suggestions")
    rag_placeholder = st.empty()

# Stream status
st.divider()
status_col1, status_col2 = st.columns(2)
with status_col1:
    stream_status = st.empty()
with status_col2:
    if st.button("Start Streaming", type="primary"):
        stream_status.success(f"🔴 Streaming call: {call_id}")
        
        # Connect to SSE stream
        try:
            stream_url = f"{STREAM_URL}/stream/{call_id}"
            messages = SSEClient(stream_url)
            
            for msg in messages:
                if msg.event == "sentiment":
                    data = json.loads(msg.data)
                    st.session_state['sentiment_history'].append(data)
                    
                    # Update sentiment display
                    with sentiment_placeholder.container():
                        st.metric(
                            "Average Sentiment",
                            f"{data['avg_sentiment_score']:.2f}",
                            delta=None
                        )
                        st.caption(f"Utterances: {data['utterance_count']}")
                        st.caption(f"Window: {data['window_start']} - {data['window_end']}")
                    
                    # Update chart
                    if len(st.session_state['sentiment_history']) > 1:
                        df = pd.DataFrame([
                            {
                                'time': d['window_end'],
                                'sentiment': d['avg_sentiment_score']
                            }
                            for d in st.session_state['sentiment_history'][-20:]
                        ])
                        sentiment_chart_placeholder.line_chart(df.set_index('time'))
                
                elif msg.event == "rag":
                    data = json.loads(msg.data)
                    st.session_state['rag_history'].append(data)
                    
                    # Update RAG display
                    with rag_placeholder.container():
                        st.markdown(f"**Latest utterance:**")
                        st.info(data['latest_utterance'])
                        
                        st.markdown(f"**Suggestion:**")
                        st.success(data['rag_response']['suggestion'])
                        
                        if data['rag_response'].get('alternatives'):
                            st.markdown("**Alternatives:**")
                            for alt in data['rag_response']['alternatives']:
                                st.markdown(f"- {alt}")
                        
                        st.caption(f"Retrieved: {data['rag_response']['retrieved_count']} similar utterances")
                        st.caption(f"Sentiment: {data['sentiment']['avg_score']:.2f}")
                
        except Exception as e:
            stream_status.error(f"❌ Stream error: {e}")
            st.info("💡 Make sure the streaming service is running: `docker compose up -d stream`")

# Display history
st.divider()
st.subheader("📜 History")

tab1, tab2 = st.tabs(["Sentiment Windows", "RAG Responses"])

with tab1:
    if st.session_state['sentiment_history']:
        df = pd.DataFrame([
            {
                'Window End': d['window_end'],
                'Avg Sentiment': round(d['avg_sentiment_score'], 3),
                'Utterances': d['utterance_count']
            }
            for d in st.session_state['sentiment_history']
        ])
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No sentiment data yet. Start streaming to see updates.")

with tab2:
    if st.session_state['rag_history']:
        for i, item in enumerate(reversed(st.session_state['rag_history'][-10:])):
            with st.expander(f"Response {len(st.session_state['rag_history']) - i}: {item['latest_utterance'][:50]}..."):
                st.markdown(f"**Utterance:** {item['latest_utterance']}")
                st.markdown(f"**Suggestion:** {item['rag_response']['suggestion']}")
                if item['rag_response'].get('alternatives'):
                    st.markdown("**Alternatives:**")
                    for alt in item['rag_response']['alternatives']:
                        st.markdown(f"- {alt}")
    else:
        st.info("No RAG responses yet. Start streaming to see updates.")

