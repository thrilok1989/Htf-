# 🚀 Deployment Guide - Streamlit Cloud

This guide will help you deploy the HTF Signal Bot on **Streamlit Cloud** for 24/7 operation.

## 📋 Prerequisites

1. **GitHub Account**: Your code repository
2. **Streamlit Cloud Account**: Free at [share.streamlit.io](https://share.streamlit.io)
3. **DhanHQ Account**: With API access
4. **Telegram Bot**: Created via @BotFather

## 🔧 Step 1: Prepare Your Repository

### 1.1 Push Code to GitHub

```bash
# Initialize Git (if not already done)
git init

# Add all files
git add .

# Commit
git commit -m "Initial commit: HTF Signal Bot"

# Add remote
git remote add origin https://github.com/yourusername/htf-signal-bot.git

# Push to GitHub
git push -u origin main
```

### 1.2 Verify Required Files

Ensure these files are in your repository:
- ✅ `streamlit_app.py` (main app)
- ✅ `signal_detector.py`
- ✅ `dhan_data_fetcher.py`
- ✅ `config.py`
- ✅ `requirements.txt`
- ✅ `.streamlit/config.toml`
- ✅ `.gitignore` (to exclude .env and secrets)
- ✅ `README.md`

**Important**: Do NOT commit `.env` or `.streamlit/secrets.toml` files!

## 🌐 Step 2: Deploy on Streamlit Cloud

### 2.1 Sign Up / Login

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Click **"Sign up with GitHub"**
3. Authorize Streamlit to access your repositories

### 2.2 Create New App

1. Click **"New app"**
2. Select your repository: `yourusername/htf-signal-bot`
3. Set **Main file path**: `streamlit_app.py`
4. Choose **Python version**: 3.9 or higher
5. Click **"Advanced settings"**

### 2.3 Configure Secrets

In the **Advanced settings** → **Secrets** section, paste:

```toml
# DhanHQ API Credentials
DHAN_CLIENT_ID = "your_client_id_here"
DHAN_ACCESS_TOKEN = "your_access_token_here"

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = "your_telegram_bot_token_here"
TELEGRAM_CHAT_ID = "your_telegram_chat_id_here"
```

**Replace with your actual credentials!**

### 2.4 Deploy

1. Click **"Deploy!"**
2. Wait 2-3 minutes for deployment
3. Your app will be live at: `https://yourusername-htf-signal-bot.streamlit.app`

## 🔐 Getting Your Credentials

### DhanHQ Credentials

1. Login to [Dhan Web](https://web.dhan.co)
2. Go to **My Profile** → **Access DhanHQ APIs**
3. Generate **Access Token** (24-hour validity)
4. Copy:
   - `DHAN_CLIENT_ID`: Your client ID
   - `DHAN_ACCESS_TOKEN`: Generated token

**Note**: Token expires every 24 hours. You'll need to update it daily in Streamlit secrets.

### Telegram Bot Credentials

#### Create Bot
1. Open Telegram and search for **@BotFather**
2. Send `/newbot`
3. Follow prompts to name your bot
4. Copy the **Bot Token** (looks like: `1234567890:ABCdefGHIjklMNOpqrsTUVwxyz`)

#### Get Chat ID
1. Search for **@userinfobot** on Telegram
2. Start chat and send any message
3. Copy your **Chat ID** (numeric, like: `123456789`)

#### Test Bot
1. Search for your bot on Telegram
2. Click **Start** to activate it
3. Your bot can now send you messages!

## ⚙️ Step 3: Configure App Settings

### 3.1 Access Your App

Visit: `https://yourusername-htf-signal-bot.streamlit.app`

### 3.2 Initial Configuration

In the **sidebar**:

1. **Instruments**: Select NIFTY, BANKNIFTY, SENSEX
2. **Auto-send to Telegram**: Enable ✅
3. **Scan Interval**: 30 seconds (recommended)
4. **Signal Cooldown**: 15 minutes (recommended)

### 3.3 Start Monitoring

1. Click **"🚀 Start Monitoring"**
2. Bot will now scan for signals during market hours
3. Signals appear in dashboard AND Telegram

## 📱 Step 4: Verify Telegram Alerts

### Test Signal

When a signal is detected, you'll receive a message like:

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
✓ RSI turning up from oversold

📈 Trade Setup:
• Entry: 24,350.75
• Stop Loss: 24,267.00
• Target 1: 24,510.00
• Target 2: 24,625.00
• R:R = 1:3.3

🔔 Strength: 8/10
```

## 🔄 Maintenance

### Daily Token Refresh

DhanHQ tokens expire after 24 hours. To refresh:

1. Login to [Dhan Web](https://web.dhan.co)
2. Go to **My Profile** → **Access DhanHQ APIs**
3. Click **"Regenerate Access Token"**
4. Go to your Streamlit app settings
5. Update `DHAN_ACCESS_TOKEN` in secrets
6. Click **"Save"** - app will restart automatically

### Alternative: API Key Authentication

For longer validity (12 months), use API Key method:

1. In Dhan Web → **DhanHQ APIs**
2. Toggle to **"API Key"**
3. Enter app details
4. Follow OAuth flow (see README)
5. Use generated token in secrets

## 🐛 Troubleshooting

### App Won't Start

**Error**: `DhanHQ credentials not found`
- **Fix**: Check secrets are properly configured in Streamlit Cloud
- Verify no typos in variable names
- Ensure no extra spaces in values

**Error**: `Telegram credentials not found`
- **Fix**: Ensure both `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set
- Test bot with @BotFather first

### No Signals Detected

**Possible causes**:
1. Market is closed (signals only during 9:15 AM - 3:30 PM IST)
2. No valid setups at the moment (need all confirmations)
3. Token expired (regenerate DhanHQ token)
4. Signal cooldown active (15-minute default)

**Fix**:
- Check market hours
- Wait for better setups
- Refresh token if expired
- Lower signal strength threshold (not recommended)

### Telegram Not Receiving Messages

**Check**:
1. Bot is started on Telegram (click "Start")
2. Chat ID is correct (from @userinfobot)
3. Bot token is valid
4. "Auto-send to Telegram" is enabled in app

**Test**:
```python
# Run this locally to test
import asyncio
from telegram import Bot

async def test():
    bot = Bot(token="YOUR_BOT_TOKEN")
    await bot.send_message(chat_id="YOUR_CHAT_ID", text="Test message")

asyncio.run(test())
```

### App Performance Issues

If app is slow or timing out:
1. Increase scan interval (60 seconds)
2. Monitor fewer instruments
3. Check DhanHQ API rate limits

## 📊 Monitoring App Health

### Streamlit Cloud Dashboard

Access at: [share.streamlit.io](https://share.streamlit.io)

**Check**:
- ✅ App status (Running/Stopped)
- 📊 Resource usage
- 📝 Recent logs
- 🔄 Restart if needed

### App Logs

View logs in Streamlit Cloud:
1. Go to your app dashboard
2. Click **"Logs"**
3. Monitor for errors or warnings

## 🔒 Security Best Practices

### Secrets Management

1. **Never commit** `.env` or `secrets.toml` to Git
2. **Rotate tokens** regularly (every 30 days)
3. **Monitor usage** for unauthorized access
4. **Use separate tokens** for dev/prod

### API Security

1. Enable **IP whitelisting** in DhanHQ (if available)
2. Use **API rate limiting** awareness
3. Monitor **API usage** in Dhan dashboard
4. **Revoke tokens** immediately if compromised

## 🆙 Updating Your App

### Deploy Updates

```bash
# Make changes to code
git add .
git commit -m "Update: [description]"
git push origin main
```

Streamlit Cloud will **automatically redeploy** when you push to GitHub!

### Manual Restart

If needed, restart from Streamlit Cloud dashboard:
1. Go to app settings
2. Click **"Reboot app"**
3. Wait for restart (30 seconds)

## 💰 Cost Considerations

### Streamlit Cloud

**Free Tier**:
- ✅ 1 private app
- ✅ Unlimited public apps
- ✅ 1 GB RAM
- ✅ Community support

**Paid Plans** (if needed):
- Pro: $20/month
- Teams: Custom pricing

### DhanHQ API

**Costs**:
- Trading APIs: **FREE** for all Dhan users
- Data APIs: Check [Dhan pricing](https://dhan.co/pricing)

**Limits**:
- Quote API: 1 request/second
- Historical Data: 100,000 calls/day
- Order APIs: 25/second

### Telegram

**Free** for bots! No cost for sending messages.

## 📈 Optimization Tips

### Performance

1. **Scan Interval**: Start with 30s, increase if slow
2. **Instruments**: Monitor only needed indices
3. **Data Lookback**: 3 hours is optimal
4. **Signal Cooldown**: 15 minutes prevents spam

### Signal Quality

1. **Confirmations**: Keep minimum at 2
2. **Strength Threshold**: 5+ for quality signals
3. **Level Proximity**: 0.3% is balanced
4. **Volume Filter**: 1.5x average is good

### Resource Usage

1. Cache data where possible
2. Use connection pooling
3. Implement exponential backoff on errors
4. Log only important events

## 🎯 Next Steps

After successful deployment:

1. ✅ **Test thoroughly** during market hours
2. 📊 **Monitor performance** for 1-2 days
3. 🔧 **Adjust settings** based on results
4. 📈 **Track signal accuracy**
5. 🚀 **Add more features** as needed

## 💬 Support

Need help? 
- 📧 GitHub Issues
- 💬 Discussions tab
- 📚 Streamlit Docs: [docs.streamlit.io](https://docs.streamlit.io)

---

**Happy Trading! 🚀📊**
