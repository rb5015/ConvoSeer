#!/bin/bash

# Setup script for React frontend

echo "🚀 Setting up ConvoSeer React Frontend..."

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "❌ Node.js is not installed. Please install Node.js 18+ first."
    exit 1
fi

# Check Node.js version
NODE_VERSION=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
if [ "$NODE_VERSION" -lt 18 ]; then
    echo "❌ Node.js version 18+ is required. Current version: $(node -v)"
    exit 1
fi

echo "✅ Node.js version: $(node -v)"

# Install dependencies
echo "📦 Installing dependencies..."
npm install

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from .env.example..."
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "✅ Created .env file. You can edit it to change backend URLs."
    else
        echo "⚠️  .env.example not found. Creating default .env..."
        cat > .env << EOF
# Backend service URLs
VITE_AUDIO_SERVICE_URL=http://localhost:8004
VITE_STREAM_URL=http://localhost:8003
VITE_RAG_URL=http://localhost:8002
EOF
    fi
else
    echo "✅ .env file already exists"
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "To start the development server, run:"
echo "  npm run dev"
echo ""
echo "The app will be available at http://localhost:3000"
echo ""
echo "Make sure your backend services are running:"
echo "  - Audio Service: http://localhost:8004"
echo "  - Stream Service: http://localhost:8003"
echo "  - RAG Service: http://localhost:8002"

