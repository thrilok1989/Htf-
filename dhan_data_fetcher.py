"""
DhanHQ Data Fetcher
Fetches real-time and historical data from DhanHQ API
"""

import requests
import pandas as pd
from datetime import datetime
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
        
        # Instrument mapping - using IDX_I for index data
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
        
        Args:
            instrument: Instrument name (NIFTY, BANKNIFTY, SENSEX)
            interval: Time interval ('1', '5', '15', '60')
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            Dictionary with success status and data
        """
        try:
            if instrument not in self.instruments:
                return {
                    'success': False,
                    'error': f"Unknown instrument: {instrument}"
                }
            
            inst_info = self.instruments[instrument]
            
            # Default dates if not provided
            if not to_date:
                to_date = datetime.now(IST).strftime('%Y-%m-%d')
            if not from_date:
                from_date = (datetime.now(IST) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            
            # Build request
            url = f"{self.base_url}/v2/charts/intraday"
            
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
            
            # Make request
            response = requests.post(url, json=payload, headers=headers)
            
            if response.status_code != 200:
                return {
                    'success': False,
                    'error': f"API {response.status_code}: {response.text[:200]}"
                }

            data = response.json()

            # Check for API error response
            if data.get('status') == 'failure' or data.get('errorCode'):
                return {
                    'success': False,
                    'error': f"API: {data.get('message', data.get('errorCode', 'Unknown error'))}"
                }
            
            # Parse data into DataFrame
            if 'data' in data and len(data['data']) > 0:
                df = pd.DataFrame(data['data'])
                
                # Convert timestamp to datetime
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
                    df['timestamp'] = df['timestamp'].dt.tz_convert(IST)
                    df.set_index('timestamp', inplace=True)
                
                # Rename columns to lowercase
                df.columns = [col.lower() for col in df.columns]
                
                # Ensure required columns
                required_cols = ['open', 'high', 'low', 'close', 'volume']
                if not all(col in df.columns for col in required_cols):
                    return {
                        'success': False,
                        'error': 'Missing required columns in data'
                    }
                
                return {
                    'success': True,
                    'data': df,
                    'instrument': instrument
                }
            else:
                return {
                    'success': False,
                    'error': 'No data returned from API'
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': f"Error fetching data: {str(e)}"
            }
    
    def fetch_quote(self, instrument: str) -> dict:
        """
        Fetch current quote for an instrument
        
        Args:
            instrument: Instrument name
            
        Returns:
            Dictionary with quote data
        """
        try:
            if instrument not in self.instruments:
                return {
                    'success': False,
                    'error': f"Unknown instrument: {instrument}"
                }
            
            inst_info = self.instruments[instrument]
            
            url = f"{self.base_url}/v2/marketfeed/quote"
            
            headers = {
                'access-token': self.access_token,
                'client-id': self.client_id,
                'Content-Type': 'application/json'
            }
            
            payload = {
                inst_info['exchange']: [inst_info['security_id']]
            }
            
            response = requests.post(url, json=payload, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'success': True,
                    'data': data
                }
            else:
                return {
                    'success': False,
                    'error': f"API request failed: {response.status_code}"
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': f"Error fetching quote: {str(e)}"
            }
