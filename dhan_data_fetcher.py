"""
DhanHQ Data Fetcher
Fetches real-time and historical data from DhanHQ API
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from config import get_dhan_credentials

IST = pytz.timezone('Asia/Kolkata')


class DhanDataFetcher:
    def __init__(self):
        self.client_id, self.access_token = get_dhan_credentials()
        self.base_url = "https://api.dhan.co"

        # Multiple security ID options to try
        self.instruments = {
            'NIFTY': {'exchange': 'IDX_I', 'security_id': '13'},
            'BANKNIFTY': {'exchange': 'IDX_I', 'security_id': '25'},
            'SENSEX': {'exchange': 'IDX_I', 'security_id': '51'}
        }

        print("DhanHQ Data Fetcher initialized")

    def fetch_intraday_data(self, instrument: str, interval: str = '1',
                           from_date: str = None, to_date: str = None) -> dict:
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
                'client-id': self.client_id,
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
                    'error': f"API {response.status_code}: {response.text[:200]}"
                }

            data = response.json()

            if data.get('status') == 'failure' or data.get('errorCode'):
                return {
                    'success': False,
                    'error': f"API: {data.get('message', data.get('errorCode', 'Unknown'))}"
                }

            candles = data.get('data') or data.get('candles') or data.get('ohlc') or []

            if not candles:
                return {'success': False, 'error': f'No data: {str(data)[:150]}'}

            df = pd.DataFrame(candles)
            df.columns = [col.lower() for col in df.columns]

            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
                df['timestamp'] = df['timestamp'].dt.tz_convert(IST)
                df.set_index('timestamp', inplace=True)
            elif 'start_time' in df.columns:
                df['timestamp'] = pd.to_datetime(df['start_time'])
                df.set_index('timestamp', inplace=True)

            col_map = {'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'}
            df.rename(columns=col_map, inplace=True)

            if 'volume' not in df.columns:
                df['volume'] = 0

            required = ['open', 'high', 'low', 'close']
            if not all(c in df.columns for c in required):
                return {'success': False, 'error': f'Missing cols. Got: {df.columns.tolist()}'}

            return {'success': True, 'data': df, 'instrument': instrument}

        except Exception as e:
            return {'success': False, 'error': f"Error: {str(e)}"}

    def get_mock_data(self, instrument: str) -> dict:
        """Generate mock data for testing when API is unavailable"""
        try:
            end_time = datetime.now(IST)
            start_time = end_time - timedelta(minutes=200)
            date_range = pd.date_range(start=start_time, end=end_time, freq='1min', tz=IST)

            base_price = {'NIFTY': 23500, 'BANKNIFTY': 50000, 'SENSEX': 77000}.get(instrument, 23500)
            volatility = base_price * 0.0005

            np.random.seed(int(datetime.now().timestamp()) % 1000)
            prices = []
            current = base_price

            for _ in range(len(date_range)):
                change = np.random.uniform(-volatility, volatility)
                current = current + change
                prices.append(current)

            df = pd.DataFrame(index=date_range)
            df['open'] = [p - np.random.uniform(0, volatility) for p in prices]
            df['high'] = [p + np.random.uniform(0, volatility*2) for p in prices]
            df['low'] = [p - np.random.uniform(0, volatility*2) for p in prices]
            df['close'] = prices
            df['volume'] = np.random.randint(1000, 50000, size=len(date_range))

            return {'success': True, 'data': df, 'instrument': instrument, 'is_mock': True}

        except Exception as e:
            return {'success': False, 'error': f"Mock data error: {str(e)}"}
