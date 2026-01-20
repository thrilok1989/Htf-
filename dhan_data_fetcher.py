"""
DhanHQ Data Fetcher
Fetches real-time and historical data from DhanHQ API
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
from config import get_dhan_credentials

IST = pytz.timezone('Asia/Kolkata')


class DhanDataFetcher:
    """
    Fetch market data from DhanHQ API
    """

    def __init__(self):
        """Initialize DhanHQ Data Fetcher"""
        self.client_id, self.access_token = get_dhan_credentials()
        self.base_url = "https://api.dhan.co"

        # Correct instrument mapping for DhanHQ API (from IDX_I segment)
        self.instruments = {
            'NIFTY': {'exchange': 'IDX_I', 'security_id': '13'},
            'BANKNIFTY': {'exchange': 'IDX_I', 'security_id': '25'},
            'SENSEX': {'exchange': 'IDX_I', 'security_id': '51'}
        }

        print("✅ DhanHQ Data Fetcher initialized")

    def fetch_intraday_data(self, instrument: str, interval: str = '1',
                           from_date: str = None, to_date: str = None) -> dict:
        """
        Fetch intraday data for an instrument
        """
        try:
            if instrument not in self.instruments:
                return {'success': False, 'error': f"Unknown instrument: {instrument}"}

            inst_info = self.instruments[instrument]

            if not to_date:
                to_date = datetime.now(IST).strftime('%Y-%m-%d')
            if not from_date:
                from_date = to_date

            url = f"{self.base_url}/charts/intraday"

            headers = {
                'access-token': self.access_token,
                'Content-Type': 'application/json'
            }

            payload = {
                'securityId': inst_info['security_id'],
                'exchangeSegment': inst_info['exchange'],
                'instrument': 'INDEX',
                'interval': interval,
                'fromDate': from_date,
                'toDate': to_date
            }

            response = requests.post(url, json=payload, headers=headers, timeout=30)

            if response.status_code != 200:
                return {
                    'success': False,
                    'error': f"API {response.status_code}: {response.text[:300]}"
                }

            data = response.json()

            if data.get('status') == 'failure' or data.get('errorCode'):
                return {
                    'success': False,
                    'error': f"API: {data.get('message', data.get('errorCode', 'Unknown'))}"
                }

            # Handle different response structures
            candles = data.get('data') or data.get('candles') or data.get('ohlc') or []

            if not candles:
                return {'success': False, 'error': f'No data. Response: {str(data)[:200]}'}

            df = pd.DataFrame(candles)
            df.columns = [col.lower() for col in df.columns]

            # Handle timestamp
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
                df['timestamp'] = df['timestamp'].dt.tz_convert(IST)
                df.set_index('timestamp', inplace=True)
            elif 'start_time' in df.columns:
                df['timestamp'] = pd.to_datetime(df['start_time'])
                df.set_index('timestamp', inplace=True)

            # Map column names if needed
            col_map = {'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'}
            df.rename(columns=col_map, inplace=True)

            required = ['open', 'high', 'low', 'close']
            if not all(c in df.columns for c in required):
                return {'success': False, 'error': f'Missing columns. Got: {df.columns.tolist()}'}

            if 'volume' not in df.columns:
                df['volume'] = 0

            return {'success': True, 'data': df, 'instrument': instrument}

        except Exception as e:
            return {'success': False, 'error': f"Error: {str(e)}"}
