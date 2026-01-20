"""
DhanHQ Data Fetcher - Fixed for v2 API
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

        # Security IDs for indices (from IDX_I segment)
        self.instruments = {
            'NIFTY': {'security_id': '13', 'exchange': 'IDX_I'},
            'BANKNIFTY': {'security_id': '25', 'exchange': 'IDX_I'},
            'SENSEX': {'security_id': '51', 'exchange': 'IDX_I'}
        }

        print("DhanHQ Data Fetcher initialized")

    def fetch_live_quote(self, instrument: str) -> dict:
        """Fetch live LTP using Market Quote API"""
        try:
            if instrument not in self.instruments:
                return {'success': False, 'error': f"Unknown instrument: {instrument}"}

            inst = self.instruments[instrument]

            url = f"{self.base_url}/v2/marketfeed/ltp"

            headers = {
                'access-token': self.access_token,
                'Content-Type': 'application/json'
            }

            payload = {
                inst['exchange']: [inst['security_id']]
            }

            response = requests.post(url, json=payload, headers=headers, timeout=10)

            if response.status_code != 200:
                return {'success': False, 'error': f"Quote API {response.status_code}"}

            data = response.json()

            # Parse response - format: {"data": {"IDX_I": {"13": {"last_price": 23500}}}}
            if 'data' in data:
                exchange_data = data['data'].get(inst['exchange'], {})
                security_data = exchange_data.get(inst['security_id'], {})
                ltp = security_data.get('last_price') or security_data.get('LTP')
                if ltp:
                    return {'success': True, 'ltp': float(ltp), 'instrument': instrument}

            return {'success': False, 'error': f'No LTP in response: {str(data)[:150]}'}

        except Exception as e:
            return {'success': False, 'error': f"Quote error: {str(e)}"}

    def fetch_intraday_data(self, instrument: str, interval: str = '1',
                           from_date: str = None, to_date: str = None) -> dict:
        try:
            if instrument not in self.instruments:
                return {'success': False, 'error': f"Unknown instrument: {instrument}"}

            inst = self.instruments[instrument]

            # Date format with time for intraday: "2024-09-11 09:30:00"
            now = datetime.now(IST)
            if not to_date:
                to_date = now.strftime('%Y-%m-%d %H:%M:%S')
            if not from_date:
                from_date = (now - timedelta(days=1)).strftime('%Y-%m-%d 09:15:00')

            url = f"{self.base_url}/v2/charts/intraday"

            headers = {
                'access-token': self.access_token,
                'Content-Type': 'application/json'
            }

            payload = {
                'securityId': inst['security_id'],
                'exchangeSegment': inst['exchange'],
                'instrument': 'INDEX',
                'interval': interval,
                'fromDate': from_date,
                'toDate': to_date
            }

            response = requests.post(url, json=payload, headers=headers, timeout=30)

            if response.status_code != 200:
                return {'success': False, 'error': f"API {response.status_code}: {response.text[:200]}"}

            data = response.json()

            # Response format: {open: [], high: [], low: [], close: [], volume: [], timestamp: []}
            if 'open' not in data or not data['open']:
                return {'success': False, 'error': f'No data in response: {str(data)[:200]}'}

            df = pd.DataFrame({
                'open': data['open'],
                'high': data['high'],
                'low': data['low'],
                'close': data['close'],
                'volume': data.get('volume', [0] * len(data['open'])),
                'timestamp': data['timestamp']
            })

            # Convert timestamp (epoch seconds)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert(IST)
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)

            return {'success': True, 'data': df, 'instrument': instrument}

        except Exception as e:
            return {'success': False, 'error': f"Error: {str(e)}"}

    def get_mock_data(self, instrument: str) -> dict:
        """Generate mock data for testing"""
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

            return {'success': True, 'data': df, 'instrument': instrument, 'ltp': prices[-1]}

        except Exception as e:
            return {'success': False, 'error': f"Mock data error: {str(e)}"}
