#!/bin/bash
# Test script for live audio pipeline

set -e

echo "Live Audio Pipeline Test"
echo "========================"
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Check if dependencies are installed
if ! python3 -c "import whisper" 2>/dev/null; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
fi

echo ""
echo "Step 1: Checking Whisper model..."
python3 whisper_transcriber.py --model base

echo ""
echo "Step 2: Listing audio devices..."
python3 live_producer.py --list-devices

echo ""
echo "Step 3: Running benchmark..."
python3 whisper_transcriber.py --model base --benchmark

echo ""
echo "========================"
echo "✓ Pipeline test complete!"
echo ""
echo "To start live transcription:"
echo "  python3 live_producer.py --brokers localhost:9092"
echo ""
echo "Make sure Kafka is running:"
echo "  docker compose up -d zookeeper kafka"

