#!/bin/bash
# Setup script for UI with audio recording

set -e

echo "Setting up UI environment..."
echo "=============================="
echo ""

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "✅ Setup complete!"
echo ""
echo "To run the UI:"
echo "  source venv/bin/activate"
echo "  streamlit run app.py"
echo ""
echo "Or use Docker:"
echo "  docker compose up -d ui"

