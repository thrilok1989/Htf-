"""
Streamlit App for HTF Signal Bot
Real-time dashboard for monitoring signals and sending Telegram alerts
"""

import streamlit as st
import pandas as pd
import asyncio
from datetime import datetime, timedelta
import pytz
import time
import os
from dotenv import load_dotenv

from signal_detector import SignalDetector
from dhan_data_fetcher import DhanDataFetcher
from telegram import Bot
from telegram.constants import ParseMode

# Load environment variables
load_dotenv()

# Indian Standard Time
IST = pytz.timezone('Asia/Kolkata')

# Page config
st.set_page_config(
    page_title="HTF Signal Bot",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .signal-card {
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .buy-signal {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    .sell-signal {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        color: white;
    }
    .metric-card {
        background: #f0f2f6;
        padding: 1rem;
        border-radius: 8px;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)


class StreamlitSignalBot:
    """Streamlit-based signal monitoring bot"""
    
    def __init__(self):
        """Initialize bot"""
        self.dhan_fetcher = DhanDataFetcher()
        self.signal_detector = SignalDetector()
        
        # Telegram setup
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.bot = None
        
        if self.telegram_token and self.chat_id:
            self.bot = Bot(token=self.telegram_token)
        
        # Initialize session state
        if 'signals' not in st.session_state:
            st.session_state.signals = []
        if 'sent_signals' not in st.session_state:
            st.session_state.sent_signals = {}
        if 'monitoring' not in st.session_state:
            st.session_state.monitoring = False
    
    async def send_telegram_signal(self, signal: dict):
        """Send signal to Telegram"""
        if not self.bot:
            return False
        
        try:
            # Format message
            emoji = "🟢" if signal['signal_type'] == 'BUY' else "🔴"
            
            message = f"""
{emoji} <b>{signal['signal_type']} SIGNAL</b> {emoji}

📊 <b>Instrument:</b> {signal['instrument']}
💰 <b>Price:</b> {signal['current_price']:.2f}
⏰ <b>Time:</b> {signal['timestamp'].strftime('%H:%M:%S IST')}

🎯 <b>Signal Reason:</b> {signal['reason']}

📍 <b>HTF Level:</b>
• Type: {signal['level_type']}
• Price: {signal['level_price']:.2f}
• Distance: {signal['distance_pct']:.2f}%
• Timeframe: {signal['timeframe']}

✅ <b>Confirmations:</b>
{chr(10).join(['✓ ' + c for c in signal['confirmations']])}

📈 <b>Trade Setup:</b>
• Entry: {signal['entry_price']:.2f}
• Stop Loss: {signal['stop_loss']:.2f}
• Target 1: {signal['target1']:.2f}
• Target 2: {signal['target2']:.2f}
• R:R = 1:{signal['risk_reward']:.1f}

🔔 <b>Strength:</b> {signal['signal_strength']}/10
"""
            
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=ParseMode.HTML
            )
            
            return True
            
        except Exception as e:
            st.error(f"Error sending Telegram: {e}")
            return False
    
    def fetch_and_analyze(self, instrument: str):
        """Fetch data and analyze for signals"""
        try:
            # Fetch data
            to_date = datetime.now(IST)
            from_date = to_date - timedelta(hours=3)
            
            result = self.dhan_fetcher.fetch_intraday_data(
                instrument=instrument,
                interval='1',
                from_date=from_date.strftime('%Y-%m-%d'),
                to_date=to_date.strftime('%Y-%m-%d')
            )
            
            if not result.get('success'):
                return []
            
            df = result['data']
            
            if len(df) < 100:
                return []
            
            # Detect signals
            signals = self.signal_detector.detect_signals(df, instrument)
            
            return signals
            
        except Exception as e:
            st.error(f"Error analyzing {instrument}: {e}")
            return []


def main():
    """Main Streamlit app"""
    
    # Initialize bot
    if 'bot' not in st.session_state:
        st.session_state.bot = StreamlitSignalBot()
    
    bot = st.session_state.bot
    
    # Header
    st.markdown('<div class="main-header">📊 HTF Signal Bot Dashboard</div>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Instruments to monitor
        st.subheader("Instruments")
        nifty = st.checkbox("NIFTY 50", value=True)
        banknifty = st.checkbox("BANK NIFTY", value=True)
        sensex = st.checkbox("SENSEX", value=True)
        
        instruments = []
        if nifty:
            instruments.append("NIFTY")
        if banknifty:
            instruments.append("BANKNIFTY")
        if sensex:
            instruments.append("SENSEX")
        
        st.divider()
        
        # Settings
        st.subheader("Settings")
        auto_telegram = st.checkbox("Auto-send to Telegram", value=True)
        scan_interval = st.slider("Scan Interval (seconds)", 10, 120, 30)
        signal_cooldown = st.slider("Signal Cooldown (minutes)", 5, 60, 15)
        
        st.divider()
        
        # Telegram status
        st.subheader("📱 Telegram Status")
        if bot.bot:
            st.success("✅ Connected")
        else:
            st.error("❌ Not configured")
            st.info("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")
        
        st.divider()
        
        # Control buttons
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🚀 Start Monitoring", use_container_width=True):
                st.session_state.monitoring = True
                st.rerun()
        with col2:
            if st.button("⏸️ Stop", use_container_width=True):
                st.session_state.monitoring = False
                st.rerun()
        
        if st.button("🗑️ Clear Signals", use_container_width=True):
            st.session_state.signals = []
            st.session_state.sent_signals = {}
            st.rerun()
    
    # Main content
    current_time = datetime.now(IST)
    
    # Market status
    market_open = current_time.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = current_time.replace(hour=15, minute=30, second=0, microsecond=0)
    is_market_open = market_open <= current_time <= market_close
    
    # Status bar
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Market Status", "🟢 OPEN" if is_market_open else "🔴 CLOSED")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Bot Status", "🟢 Running" if st.session_state.monitoring else "⏸️ Stopped")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Total Signals", len(st.session_state.signals))
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Current Time", current_time.strftime('%H:%M:%S IST'))
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.divider()
    
    # Monitoring loop
    if st.session_state.monitoring:
        if not is_market_open:
            st.warning("⚠️ Market is currently closed. Monitoring will resume when market opens.")
        else:
            # Scan placeholder
            status_placeholder = st.empty()
            
            # Scan all instruments
            status_placeholder.info(f"🔍 Scanning {len(instruments)} instruments...")
            
            for instrument in instruments:
                signals = bot.fetch_and_analyze(instrument)
                
                for signal in signals:
                    # Check cooldown
                    key = f"{signal['instrument']}_{signal['signal_type']}"
                    
                    if key in st.session_state.sent_signals:
                        last_time = st.session_state.sent_signals[key]
                        elapsed = (datetime.now(IST) - last_time).total_seconds() / 60
                        
                        if elapsed < signal_cooldown:
                            continue
                    
                    # Add to signals
                    st.session_state.signals.insert(0, signal)
                    st.session_state.sent_signals[key] = datetime.now(IST)
                    
                    # Send to Telegram if enabled
                    if auto_telegram and bot.bot:
                        asyncio.run(bot.send_telegram_signal(signal))
                
                time.sleep(1)
            
            status_placeholder.success(f"✅ Scan complete. Next scan in {scan_interval} seconds...")
            
            # Auto-refresh
            time.sleep(scan_interval)
            st.rerun()
    
    # Display signals
    st.subheader("📋 Recent Signals")
    
    if len(st.session_state.signals) == 0:
        st.info("No signals detected yet. Start monitoring to see signals here.")
    else:
        # Display each signal
        for i, signal in enumerate(st.session_state.signals[:10]):  # Show last 10
            signal_type = signal['signal_type']
            card_class = "buy-signal" if signal_type == "BUY" else "sell-signal"
            
            with st.container():
                st.markdown(f'<div class="signal-card {card_class}">', unsafe_allow_html=True)
                
                col1, col2, col3 = st.columns([2, 2, 1])
                
                with col1:
                    st.markdown(f"### {signal['instrument']} - {signal_type}")
                    st.markdown(f"**Price:** {signal['current_price']:.2f}")
                    st.markdown(f"**Time:** {signal['timestamp'].strftime('%H:%M:%S IST')}")
                
                with col2:
                    st.markdown(f"**Level:** {signal['level_type']} @ {signal['level_price']:.2f}")
                    st.markdown(f"**Timeframe:** {signal['timeframe']}")
                    st.markdown(f"**Strength:** {signal['signal_strength']}/10")
                
                with col3:
                    st.markdown(f"**Entry:** {signal['entry_price']:.2f}")
                    st.markdown(f"**SL:** {signal['stop_loss']:.2f}")
                    st.markdown(f"**Target:** {signal['target1']:.2f}")
                
                # Confirmations
                st.markdown("**✅ Confirmations:**")
                for conf in signal['confirmations']:
                    st.markdown(f"• {conf}")
                
                st.markdown('</div>', unsafe_allow_html=True)
                st.markdown("<br>", unsafe_allow_html=True)
    
    # Footer
    st.divider()
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 1rem;'>
        <p>HTF Signal Bot - Real-time market signal detection with HTF levels and pattern recognition</p>
        <p>⚠️ For educational purposes only. Always do your own research before trading.</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
