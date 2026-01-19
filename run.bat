@echo off
REM HTF Signal Bot - Quick Start Script for Windows

echo.
echo 🚀 Starting HTF Signal Bot...
echo.

REM Check if .env file exists
if not exist .env (
    echo ⚠️  .env file not found!
    echo 📝 Creating .env from template...
    copy .env.example .env
    echo.
    echo ✅ .env file created. Please edit it with your credentials:
    echo    - DHAN_CLIENT_ID
    echo    - DHAN_ACCESS_TOKEN
    echo    - TELEGRAM_BOT_TOKEN
    echo    - TELEGRAM_CHAT_ID
    echo.
    echo Then run this script again.
    pause
    exit /b 1
)

REM Check if virtual environment exists
if not exist venv (
    echo 📦 Creating virtual environment...
    python -m venv venv
    echo ✅ Virtual environment created
)

REM Activate virtual environment
echo 🔌 Activating virtual environment...
call venv\Scripts\activate.bat

REM Install/upgrade dependencies
echo 📥 Installing dependencies...
python -m pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

echo.
echo ✅ Setup complete!
echo.
echo 🌐 Starting Streamlit app...
echo    Access at: http://localhost:8501
echo.
echo Press Ctrl+C to stop the app
echo.

REM Run Streamlit app
streamlit run streamlit_app.py

pause
