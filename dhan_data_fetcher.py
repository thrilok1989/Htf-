"""
DhanHQ Data Fetcher using official dhanhq library
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from dhanhq import dhanhq
from config import get_dhan_credentials

IST = pytz.timezone('Asia/Kolkata')


class DhanDataFetcher:
    def __init__(self):
        self.client_id, self.access_token = get_dhan_credentials()
        self.dhan = dhanhq(self.client_id, self.access_token)

        # Security IDs for indices (from IDX_I segment)
        self.instruments = {
            'NIFTY': {'security_id': 13, 'exchange': 0},      # IDX_I = 0
            'BANKNIFTY': {'security_id': 25, 'exchange': 0},
            'SENSEX': {'security_id': 51, 'exchange': 0}
        }

        print("DhanHQ Data Fetcher initialized (using dhanhq library)")

    def fetch_intraday_data(self, instrument: str, interval: str = '1',
                           from_date: str = None, to_date: str = None) -> dict:
        try:
            if instrument not in self.instruments:
                return {'success': False, 'error': f"Unknown instrument: {instrument}"}

            inst = self.instruments[instrument]

            # Use dhanhq library method
            data = self.dhan.intraday_minute_data(
                security_id=str(inst['security_id']),
                exchange_segment='IDX_I',
                instrument_type='INDEX'
            )

            if data.get('status') == 'failure':
                return {'success': False, 'error': f"API: {data.get('remarks', 'Unknown error')}"}

            candles = data.get('data', {}).get('candles', [])

            if not candles:
                return {'success': False, 'error': f'No candles: {str(data)[:200]}'}

            # Convert to DataFrame - candles format: [timestamp, open, high, low, close, volume]
            df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

            # Convert timestamp
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert(IST)
            df.set_index('timestamp', inplace=True)

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

            return {'success': True, 'data': df, 'instrument': instrument, 'is_mock': True}

        except Exception as e:
            return {'success': False, 'error': f"Mock data error: {str(e)}"}
