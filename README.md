# 📊 HTF Signal Bot - Real-Time Trading Signal Detection

Automated trading signal bot that monitors **Higher Time Frame (HTF) Support & Resistance levels** and sends **Telegram alerts** when price approaches key levels with multiple confirmations.

## 🎯 Features

### Signal Detection
- ✅ **HTF Support/Resistance**: Multi-timeframe pivot levels (5min, 15min, 1hour)
- ✅ **Reversal Patterns**: Hammer, Shooting Star, Engulfing patterns
- ✅ **Level Hold Detection**: Price tests and holds at key levels
- ✅ **Indicator Confirmation**: RSI, MACD momentum confirmation
- ✅ **Volume Surge**: Volume confirmation for signal validation
- ✅ **Geometric Patterns**: Triangle, H&S, Flag patterns (ready to integrate)

### Confirmations Required
A signal is only generated when **at least 2 of these conditions** are met:
1. **Reversal Candle Close** (Hammer / Engulfing / Strong Rejection)
2. **Level Hold + Rejection Wick** (Price tests and bounces from level)
3. **Indicator Flip** (RSI/MACD confirmation)
4. **Volume Surge** (1.5x+ average volume)

### Platform Features
- 📱 **Telegram Integration**: Instant signal alerts with full trade setup
- 📊 **Streamlit Dashboard**: Real-time monitoring interface
- 🔄 **Auto-Refresh**: Continuous market scanning (30-second intervals)
- 🎨 **Visual Dashboard**: Clean UI with signal history and metrics
- ⚡ **DhanHQ Integration**: Real-time Indian market data (NIFTY, BANKNIFTY, SENSEX)

## 📋 Signal Example

```
🟢 BUY SIGNAL 🟢

📊 Instrument: NIFTY
💰 Price: 24,350.75
⏰ Time: 10:45:23 IST

🎯 Signal Reason: Price near SUPPORT at 15min timeframe

📍 HTF Level:
• Type: SUPPORT
• Price: 24,340.00
• Distance: 0.04%
• Timeframe: 15min

✅ Confirmations:
✓ Bullish Hammer detected
✓ Support tested and held with bounce
✓ RSI turning up from oversold + MACD bullish crossover
✓ Volume surge: 2.3x average

📈 Trade Setup:
• Entry: 24,350.75
• Stop Loss: 24,267.00
• Target 1: 24,510.00
• Target 2: 24,625.00
• R:R = 1:3.3

🔔 Strength: 8/10
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- DhanHQ account with API access
- Telegram Bot (for alerts)

### 1. Clone Repository
```bash
git clone https://github.com/yourusername/htf-signal-bot.git
cd htf-signal-bot
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Create a `.env` file in the root directory:

```env
# DhanHQ API Credentials
DHAN_CLIENT_ID=your_client_id_here
DHAN_ACCESS_TOKEN=your_access_token_here

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
```

#### Getting DhanHQ Credentials
1. Login to [Dhan](https://web.dhan.co)
2. Go to **My Profile** → **Access DhanHQ APIs**
3. Generate **Access Token** (valid for 24 hours)
4. Copy your **Client ID** and **Access Token**

#### Getting Telegram Credentials
1. Create a bot via [@BotFather](https://t.me/BotFather)
2. Copy the **Bot Token**
3. Get your **Chat ID** from [@userinfobot](https://t.me/userinfobot)

### 4. Run the Streamlit App
```bash
streamlit run streamlit_app.py
```

The app will open at `http://localhost:8501`

## 📊 Streamlit Dashboard

### Dashboard Features
- **Real-time Monitoring**: Live signal detection
- **Market Status**: Shows if market is open/closed
- **Signal History**: Last 10 signals with full details
- **Auto-Telegram**: Automatically send signals to Telegram
- **Customizable Settings**: Scan interval, signal cooldown, instruments

### Dashboard Controls
- 🚀 **Start Monitoring**: Begin scanning for signals
- ⏸️ **Stop**: Pause monitoring
- 🗑️ **Clear Signals**: Reset signal history
- ⚙️ **Settings**: Configure instruments, intervals, and alerts

## 🔧 Configuration

### Instruments
Select which instruments to monitor:
- **NIFTY 50**
- **BANK NIFTY**
- **SENSEX**

### Settings
- **Auto-send to Telegram**: Enable/disable automatic alerts
- **Scan Interval**: 10-120 seconds (default: 30s)
- **Signal Cooldown**: 5-60 minutes (default: 15min)

### HTF Timeframes
The bot monitors these timeframes:
- 5-minute
- 15-minute
- 1-hour

## 🏗️ Project Structure

```
htf-signal-bot/
├── streamlit_app.py          # Main Streamlit dashboard
├── signal_detector.py         # Signal detection logic
├── dhan_data_fetcher.py      # DhanHQ API integration
├── config.py                  # Configuration management
├── requirements.txt           # Python dependencies
├── .env.example              # Environment variables template
└── README.md                 # This file
```

## 📈 How It Works

### 1. Data Fetching
- Fetches 1-minute OHLCV data from DhanHQ API
- Maintains 3-hour lookback window
- Updates every 30 seconds during market hours

### 2. HTF Level Calculation
- Resamples data to multiple timeframes (5m, 15m, 1h)
- Calculates pivot highs (resistance) and pivot lows (support)
- Identifies strong S/R levels using swing analysis

### 3. Signal Detection
- Monitors distance to HTF levels (within 0.3%)
- Checks for reversal patterns on current candle
- Validates with indicator confirmations (RSI, MACD)
- Requires minimum 2 confirmations + strength score ≥5

### 4. Trade Setup Calculation
- **Entry**: Current market price
- **Stop Loss**: 0.3% beyond level (managed risk)
- **Targets**: ATR-based (1.5x and 2.5x ATR)
- **R:R Ratio**: Typically 1:2 to 1:4

### 5. Alert Delivery
- Formats signal with all details
- Sends to Telegram with HTML formatting
- Stores in dashboard history
- Implements cooldown to prevent spam

## ⚠️ Important Notes

### Market Hours
- Bot only scans during market hours: **9:15 AM to 3:30 PM IST**
- Outside market hours, monitoring pauses automatically

### Signal Cooldown
- Default 15-minute cooldown between signals for same instrument/direction
- Prevents duplicate alerts for the same setup
- Configurable in dashboard settings

### API Rate Limits
- DhanHQ limits: 1 request/second for quote APIs
- Bot uses 30-second scan interval to stay within limits
- Historical data API: 100,000 calls/day limit

### Token Expiry
- DhanHQ access tokens expire after 24 hours
- Need to regenerate token daily from Dhan web portal
- Consider using API Key authentication for longer validity

## 🔒 Security Best Practices

1. **Never commit `.env` file** to Git
2. **Use `.env.example`** as template only
3. **Rotate tokens regularly**
4. **Keep bot token private** (Telegram)
5. **Monitor API usage** to avoid rate limits

## 🐛 Troubleshooting

### "DhanHQ credentials not found"
- Ensure `.env` file exists in root directory
- Check variable names match exactly
- Verify no extra spaces in values

### "Telegram credentials not found"
- Get bot token from @BotFather
- Get chat ID from @userinfobot
- Ensure both are set in `.env`

### "No data returned from API"
- Verify DhanHQ token is valid (regenerate if expired)
- Check instrument is supported
- Ensure market is open during scan time

### "Insufficient data for analysis"
- Wait for more candles to form
- Check if market has been open long enough
- Verify API is returning data correctly

## 📝 Future Enhancements

- [ ] WebSocket integration for real-time data
- [ ] More geometric pattern detection
- [ ] Multi-asset support (stocks, commodities)
- [ ] Backtesting module
- [ ] Performance analytics dashboard
- [ ] Discord integration
- [ ] Email alerts option

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## ⚖️ Disclaimer

**This bot is for educational purposes only.** 

- Not financial advice
- Past performance doesn't guarantee future results
- Always do your own research before trading
- Use proper risk management
- Trade at your own risk

## 📄 License

MIT License - see LICENSE file for details

## 💬 Support

For issues, questions, or suggestions:
- Open an issue on GitHub
- Contact: [Your contact info]

---

**Built with ❤️ for Indian Markets** | Powered by DhanHQ API
