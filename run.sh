#!/bin/bash

# HTF Signal Bot - Quick Start Script

echo "🚀 Starting HTF Signal Bot..."
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  .env file not found!"
    echo "📝 Creating .env from template..."
    cp .env.example .env
    echo ""
    echo "✅ .env file created. Please edit it with your credentials:"
    echo "   - DHAN_CLIENT_ID"
    echo "   - DHAN_ACCESS_TOKEN"
    echo "   - TELEGRAM_BOT_TOKEN"
    echo "   - TELEGRAM_CHAT_ID"
    echo ""
    echo "Then run this script again."
    exit 1
fi

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
    echo "✅ Virtual environment created"
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source venv/bin/activate

# Install/upgrade dependencies
echo "📥 Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

echo ""
echo "✅ Setup complete!"
echo ""
echo "🌐 Starting Streamlit app..."
echo "   Access at: http://localhost:8501"
echo ""
echo "Press Ctrl+C to stop the app"
echo ""

# Run Streamlit app
streamlit run streamlit_app.py
