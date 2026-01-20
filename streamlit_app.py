"""
HTF Signal Bot - Minimal Dashboard
Shows spot price, HTF levels, patterns, and signals with 30s auto-refresh
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

load_dotenv()
IST = pytz.timezone('Asia/Kolkata')

st.set_page_config(page_title="HTF Signal Bot", page_icon="📊", layout="wide")

@st.cache_resource
def get_fetcher():
    return DhanDataFetcher()

@st.cache_resource
def get_detector():
    return SignalDetector()

@st.cache_resource
def get_bot():
    token = None
    chat_id = None
    try:
        token = st.secrets.get('TELEGRAM_BOT_TOKEN')
        chat_id = st.secrets.get('TELEGRAM_CHAT_ID')
    except:
        pass
    if not token:
        token = os.getenv('TELEGRAM_BOT_TOKEN')
    if not chat_id:
        chat_id = os.getenv('TELEGRAM_CHAT_ID')
    if token and chat_id:
        return Bot(token=token), chat_id
    return None, None

def fetch_data(fetcher, instrument: str, use_mock: bool = False):
    """Fetch data - real or mock"""
    if use_mock:
        return fetcher.get_mock_data(instrument)

    # Use datetime format: "2024-09-11 09:30:00"
    now = datetime.now(IST)
    to_date = now.strftime('%Y-%m-%d %H:%M:%S')
    from_date = (now - timedelta(days=1)).strftime('%Y-%m-%d 09:15:00')

    return fetcher.fetch_intraday_data(
        instrument=instrument,
        interval='1',
        from_date=from_date,
        to_date=to_date
    )

async def send_telegram(bot, chat_id, signal):
    if not bot:
        return
    emoji = "🟢" if signal['signal_type'] == 'BUY' else "🔴"
    msg = f"""{emoji} <b>{signal['signal_type']} {signal['instrument']}</b>
💰 Price: {signal['current_price']:.2f}
📍 {signal['level_type']} @ {signal['level_price']:.2f} ({signal['timeframe']})
✅ {', '.join(signal['confirmations'])}
🎯 Entry: {signal['entry_price']:.2f} | SL: {signal['stop_loss']:.2f} | T1: {signal['target1']:.2f}"""
    try:
        await bot.send_message(chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML)
    except:
        pass

def main():
    st.title("📊 HTF Signal Bot")

    if 'signals' not in st.session_state:
        st.session_state.signals = []
    if 'sent_signals' not in st.session_state:
        st.session_state.sent_signals = {}
    if 'last_price' not in st.session_state:
        st.session_state.last_price = {}

    fetcher = get_fetcher()
    detector = get_detector()
    bot, chat_id = get_bot()

    # Sidebar
    with st.sidebar:
        st.header("⚙️ Settings")
        instrument = st.selectbox("Instrument", ["NIFTY", "BANKNIFTY", "SENSEX"])
        auto_telegram = st.checkbox("Auto Telegram", value=True)
        cooldown_mins = st.slider("Cooldown (min)", 5, 60, 15)
        use_mock = st.checkbox("Use Demo Data", value=False, help="Use mock data if API fails")
        st.divider()
        st.caption(f"Telegram: {'✅' if bot else '❌'}")
        if st.button("🗑️ Clear"):
            st.session_state.signals = []
            st.session_state.sent_signals = {}

    now = datetime.now(IST)
    is_open = now.replace(hour=9, minute=15, second=0) <= now <= now.replace(hour=15, minute=30, second=0)

    # Fetch live quote for spot price
    quote_result = fetcher.fetch_live_quote(instrument)
    if quote_result.get('success'):
        live_price = quote_result.get('ltp')
        st.session_state.last_price[instrument] = live_price
    else:
        # Use cached price if available
        live_price = st.session_state.last_price.get(instrument)

    # Fetch historical data for HTF levels
    result = fetch_data(fetcher, instrument, use_mock)

    if not result.get('success'):
        st.warning(f"⚠️ API Error: {result.get('error', 'Unknown')}")
        st.info("Enable 'Use Demo Data' in sidebar to test the app")
        time.sleep(10)
        st.rerun()
        return

    df = result['data']
    if df is None or len(df) < 10:
        st.warning("⚠️ Not enough data")
        time.sleep(10)
        st.rerun()
        return

    if result.get('is_mock'):
        st.info("📊 Using demo data (API unavailable)")

    # Use live price if available, else use last close
    price = live_price if live_price else df['close'].iloc[-1]
    prev = df['close'].iloc[-2]
    chg = price - prev
    chg_pct = (chg / prev) * 100

    # Metrics row
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Market", "🟢 OPEN" if is_open else "🔴 CLOSED")
    c2.metric(f"{instrument}", f"₹{price:,.2f}", f"{chg:+.2f} ({chg_pct:+.2f}%)")
    c3.metric("Time", now.strftime('%H:%M:%S'))
    c4.metric("Signals", len(st.session_state.signals))

    st.divider()

    # Main content
    left, right = st.columns([2, 1])

    with left:
        st.subheader("📐 HTF Levels")
        levels = detector.get_htf_levels_table(df, price)
        if levels:
            ldf = pd.DataFrame(levels)
            ldf['Price'] = ldf['Price'].map(lambda x: f"₹{x:,.2f}")
            ldf['Distance'] = ldf['Distance'].map(lambda x: f"{x:+.2f}")
            ldf['Distance%'] = ldf['Distance%'].map(lambda x: f"{x:+.3f}%")
            st.dataframe(ldf, use_container_width=True, hide_index=True)

    with right:
        st.subheader("📈 Indicators")
        ind = detector.get_indicators(df)
        if ind['rsi']:
            st.markdown(f"**RSI:** {'🟢' if 30<ind['rsi']<70 else '🔴'} {ind['rsi']:.1f}")
            st.markdown(f"**MACD:** {'🟢' if ind['macd']>ind['macd_signal'] else '🔴'} {ind['macd']:.2f}")
        avg_vol = df['volume'].tail(20).mean()
        vol = df['volume'].iloc[-1] / avg_vol if avg_vol > 0 else 1
        st.markdown(f"**Vol:** {vol:.1f}x {'🔥' if vol>=1.5 else ''}")
        st.divider()
        pattern = detector.detect_current_pattern(df)
        st.markdown(f"**Pattern:** {pattern or 'None'}")

    st.divider()

    # Signals
    st.subheader("🎯 Signals")
    st.caption("2+ confirms | Strength≥5 | ≤0.3% from level | 15min cooldown")

    if is_open or use_mock:
        for sig in detector.detect_signals(df, instrument):
            key = f"{sig['instrument']}_{sig['signal_type']}"
            if key in st.session_state.sent_signals:
                if (now - st.session_state.sent_signals[key]).seconds < cooldown_mins * 60:
                    continue
            st.session_state.signals.insert(0, sig)
            st.session_state.sent_signals[key] = now
            if auto_telegram and bot:
                asyncio.run(send_telegram(bot, chat_id, sig))

    if st.session_state.signals:
        for s in st.session_state.signals[:5]:
            st.markdown(f"**{'🟢' if s['signal_type']=='BUY' else '🔴'} {s['signal_type']} ₹{s['current_price']:.2f}** | {s['timestamp'].strftime('%H:%M:%S')} | {s['level_type']} @ ₹{s['level_price']:.2f} ({s['timeframe']}) | Str:{s['signal_strength']}")
            st.caption(f"Entry: ₹{s['entry_price']:.2f} | SL: ₹{s['stop_loss']:.2f} | T1: ₹{s['target1']:.2f} | T2: ₹{s['target2']:.2f}")
    else:
        st.info("No signals yet...")

    st.caption("⚠️ Educational only")

    time.sleep(30)
    st.rerun()

if __name__ == "__main__":
    main()
