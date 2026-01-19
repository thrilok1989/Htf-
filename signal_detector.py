"""
Signal Detector Module
Detects trading signals based on HTF levels, patterns, and confirmations
"""

import pandas as pd
import numpy as np
from datetime import datetime
import pytz

IST = pytz.timezone('Asia/Kolkata')


class SignalDetector:
    """
    Detects trading signals when price approaches HTF levels with confirmations
    """
    
    def __init__(self):
        """Initialize Signal Detector"""
        # HTF timeframes to check
        self.htf_timeframes = [
            {'timeframe': '5T', 'length': 5, 'name': '5min'},
            {'timeframe': '15T', 'length': 5, 'name': '15min'},
            {'timeframe': '60T', 'length': 5, 'name': '1hour'},
        ]
        
        # Distance threshold (percentage) to consider "near" a level
        self.level_proximity_pct = 0.3  # 0.3% from level
        
        print("✅ Signal Detector initialized")
    
    def detect_signals(self, df: pd.DataFrame, instrument: str) -> list:
        """
        Main signal detection logic
        
        Args:
            df: DataFrame with OHLC data
            instrument: Instrument name
            
        Returns:
            List of signal dictionaries
        """
        signals = []
        
        if len(df) < 100:
            return signals
        
        # Calculate HTF levels
        htf_levels = self._calculate_htf_levels(df)
        
        if not htf_levels:
            return signals
        
        # Get current price and candle
        current_price = df['close'].iloc[-1]
        current_candle = {
            'open': df['open'].iloc[-1],
            'high': df['high'].iloc[-1],
            'low': df['low'].iloc[-1],
            'close': df['close'].iloc[-1],
            'volume': df['volume'].iloc[-1]
        }
        
        # Check each HTF level
        for level in htf_levels:
            # Check if price is near this level
            distance = abs(current_price - level['price'])
            distance_pct = (distance / level['price']) * 100
            
            if distance_pct <= self.level_proximity_pct:
                # Price is near level - check for confirmations
                confirmations = self._check_confirmations(df, level, current_candle)
                
                if confirmations['has_signal']:
                    # Generate signal
                    signal = self._generate_signal(
                        instrument=instrument,
                        level=level,
                        current_price=current_price,
                        confirmations=confirmations,
                        df=df
                    )
                    signals.append(signal)
        
        return signals
    
    def _calculate_htf_levels(self, df: pd.DataFrame) -> list:
        """
        Calculate HTF support and resistance levels
        
        Args:
            df: DataFrame with OHLC data
            
        Returns:
            List of level dictionaries
        """
        levels = []
        
        for tf_config in self.htf_timeframes:
            timeframe = tf_config['timeframe']
            length = tf_config['length']
            name = tf_config['name']
            
            # Resample to HTF
            try:
                df_htf = df.resample(timeframe).agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna()
                
                if len(df_htf) < length + 2:
                    continue
                
                # Find pivot high (resistance)
                pivot_high = self._find_pivot_high(df_htf, length)
                if pivot_high:
                    levels.append({
                        'price': pivot_high,
                        'type': 'RESISTANCE',
                        'timeframe': name,
                        'timeframe_code': timeframe
                    })
                
                # Find pivot low (support)
                pivot_low = self._find_pivot_low(df_htf, length)
                if pivot_low:
                    levels.append({
                        'price': pivot_low,
                        'type': 'SUPPORT',
                        'timeframe': name,
                        'timeframe_code': timeframe
                    })
                    
            except Exception as e:
                print(f"Error calculating HTF levels for {timeframe}: {e}")
                continue
        
        return levels
    
    def _find_pivot_high(self, df: pd.DataFrame, length: int) -> float:
        """Find most recent pivot high"""
        highs = df['high'].values
        
        for i in range(len(highs) - 2, length, -1):
            is_pivot = True
            for j in range(1, length + 1):
                if i - j >= 0 and highs[i] <= highs[i - j]:
                    is_pivot = False
                    break
                if i + j < len(highs) and highs[i] <= highs[i + j]:
                    is_pivot = False
                    break
            if is_pivot:
                return float(highs[i])
        
        return float(df['high'].tail(length * 2).max())
    
    def _find_pivot_low(self, df: pd.DataFrame, length: int) -> float:
        """Find most recent pivot low"""
        lows = df['low'].values
        
        for i in range(len(lows) - 2, length, -1):
            is_pivot = True
            for j in range(1, length + 1):
                if i - j >= 0 and lows[i] >= lows[i - j]:
                    is_pivot = False
                    break
                if i + j < len(lows) and lows[i] >= lows[i + j]:
                    is_pivot = False
                    break
            if is_pivot:
                return float(lows[i])
        
        return float(df['low'].tail(length * 2).min())
    
    def _check_confirmations(self, df: pd.DataFrame, level: dict, current_candle: dict) -> dict:
        """
        Check for signal confirmations
        
        Returns:
            Dictionary with confirmation results
        """
        confirmations = {
            'has_signal': False,
            'signal_type': None,
            'confirmations': [],
            'strength': 0
        }
        
        # Determine expected signal direction based on level type
        if level['type'] == 'SUPPORT':
            expected_direction = 'BUY'
        else:  # RESISTANCE
            expected_direction = 'SELL'
        
        # Check 1: Reversal Candle Pattern
        reversal_pattern = self._detect_reversal_pattern(df, expected_direction)
        if reversal_pattern:
            confirmations['confirmations'].append(reversal_pattern)
            confirmations['strength'] += 3
        
        # Check 2: Level Hold + Rejection Wick
        level_hold = self._check_level_hold(df, level, expected_direction)
        if level_hold:
            confirmations['confirmations'].append(level_hold)
            confirmations['strength'] += 3
        
        # Check 3: Indicator Confirmation
        indicator_confirm = self._check_indicator_flip(df, expected_direction)
        if indicator_confirm:
            confirmations['confirmations'].append(indicator_confirm)
            confirmations['strength'] += 2
        
        # Check 4: Volume Confirmation
        volume_confirm = self._check_volume_surge(df)
        if volume_confirm:
            confirmations['confirmations'].append(volume_confirm)
            confirmations['strength'] += 2
        
        # Need at least 2 confirmations and minimum strength of 5
        if len(confirmations['confirmations']) >= 2 and confirmations['strength'] >= 5:
            confirmations['has_signal'] = True
            confirmations['signal_type'] = expected_direction
        
        return confirmations
    
    def _detect_reversal_pattern(self, df: pd.DataFrame, direction: str) -> str:
        """
        Detect 5 types of reversal candlestick patterns

        Returns:
            Description of pattern found, or None
        """
        if len(df) < 3:
            return None

        current = df.iloc[-1]
        prev = df.iloc[-2]
        prev2 = df.iloc[-3] if len(df) >= 3 else None

        c_open = current['open']
        c_high = current['high']
        c_low = current['low']
        c_close = current['close']

        body = abs(c_close - c_open)
        upper_wick = c_high - max(c_open, c_close)
        lower_wick = min(c_open, c_close) - c_low
        candle_range = c_high - c_low if c_high != c_low else 0.01

        if direction == 'BUY':
            # Pattern 1: Hammer
            if body < candle_range * 0.3 and lower_wick > body * 2 and upper_wick < body * 0.5:
                return "Bullish Hammer"

            # Pattern 2: Bullish Engulfing
            if (c_close > c_open and prev['close'] < prev['open'] and
                c_open < prev['close'] and c_close > prev['open']):
                return "Bullish Engulfing"

            # Pattern 3: Bullish Rejection Wick
            if c_close > c_open and lower_wick > body * 1.5:
                return "Bullish Rejection Wick"

            # Pattern 4: Doji at Support
            if body < candle_range * 0.1 and candle_range > 0:
                return "Doji at Support"

            # Pattern 5: Morning Star
            if prev2 is not None:
                p2_body = abs(prev2['close'] - prev2['open'])
                p1_body = abs(prev['close'] - prev['open'])
                if (prev2['close'] < prev2['open'] and p2_body > candle_range * 0.5 and
                    p1_body < p2_body * 0.3 and c_close > c_open and body > p1_body):
                    return "Morning Star"

        else:  # SELL
            # Pattern 1: Shooting Star
            if body < candle_range * 0.3 and upper_wick > body * 2 and lower_wick < body * 0.5:
                return "Bearish Shooting Star"

            # Pattern 2: Bearish Engulfing
            if (c_close < c_open and prev['close'] > prev['open'] and
                c_open > prev['close'] and c_close < prev['open']):
                return "Bearish Engulfing"

            # Pattern 3: Bearish Rejection Wick
            if c_close < c_open and upper_wick > body * 1.5:
                return "Bearish Rejection Wick"

            # Pattern 4: Doji at Resistance
            if body < candle_range * 0.1 and candle_range > 0:
                return "Doji at Resistance"

            # Pattern 5: Evening Star
            if prev2 is not None:
                p2_body = abs(prev2['close'] - prev2['open'])
                p1_body = abs(prev['close'] - prev['open'])
                if (prev2['close'] > prev2['open'] and p2_body > candle_range * 0.5 and
                    p1_body < p2_body * 0.3 and c_close < c_open and body > p1_body):
                    return "Evening Star"

        return None
    
    def _check_level_hold(self, df: pd.DataFrame, level: dict, direction: str) -> str:
        """
        Check if price tested and held the level
        
        Returns:
            Description if level held, or None
        """
        if len(df) < 5:
            return None
        
        level_price = level['price']
        recent_candles = df.tail(5)
        
        if direction == 'BUY':
            # Check if recent lows tested support but held
            lowest_low = recent_candles['low'].min()
            distance_to_level = abs(lowest_low - level_price)
            distance_pct = (distance_to_level / level_price) * 100
            
            # Price came within 0.5% of support
            if distance_pct <= 0.5:
                # Check if price bounced back
                current_close = df['close'].iloc[-1]
                if current_close > lowest_low * 1.002:  # Closed 0.2% above low
                    return "Support tested and held with bounce"
        
        else:  # SELL
            # Check if recent highs tested resistance but held
            highest_high = recent_candles['high'].max()
            distance_to_level = abs(highest_high - level_price)
            distance_pct = (distance_to_level / level_price) * 100
            
            # Price came within 0.5% of resistance
            if distance_pct <= 0.5:
                # Check if price rejected
                current_close = df['close'].iloc[-1]
                if current_close < highest_high * 0.998:  # Closed 0.2% below high
                    return "Resistance tested and held with rejection"
        
        return None
    
    def _check_indicator_flip(self, df: pd.DataFrame, direction: str) -> str:
        """
        Check if momentum indicators confirm the direction
        
        Returns:
            Description of indicator confirmation, or None
        """
        if len(df) < 50:
            return None
        
        # Calculate RSI
        close = df['close']
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(window=14).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        current_rsi = rsi.iloc[-1]
        prev_rsi = rsi.iloc[-2]
        
        # Calculate MACD
        exp1 = close.ewm(span=12, adjust=False).mean()
        exp2 = close.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        current_macd = macd.iloc[-1]
        current_signal = signal.iloc[-1]
        prev_macd = macd.iloc[-2]
        prev_signal = signal.iloc[-2]
        
        if direction == 'BUY':
            # Bullish confirmations
            confirmations = []
            
            # RSI oversold and turning up
            if current_rsi < 40 and current_rsi > prev_rsi:
                confirmations.append("RSI turning up from oversold")
            
            # MACD bullish crossover
            if current_macd > current_signal and prev_macd <= prev_signal:
                confirmations.append("MACD bullish crossover")
            
            # MACD both positive
            if current_macd > 0 and current_signal > 0:
                confirmations.append("MACD bullish momentum")
            
            if confirmations:
                return " + ".join(confirmations)
        
        else:  # SELL
            # Bearish confirmations
            confirmations = []
            
            # RSI overbought and turning down
            if current_rsi > 60 and current_rsi < prev_rsi:
                confirmations.append("RSI turning down from overbought")
            
            # MACD bearish crossover
            if current_macd < current_signal and prev_macd >= prev_signal:
                confirmations.append("MACD bearish crossover")
            
            # MACD both negative
            if current_macd < 0 and current_signal < 0:
                confirmations.append("MACD bearish momentum")
            
            if confirmations:
                return " + ".join(confirmations)
        
        return None
    
    def _check_volume_surge(self, df: pd.DataFrame) -> str:
        """
        Check for volume surge confirming the move
        
        Returns:
            Description of volume confirmation, or None
        """
        if len(df) < 20:
            return None
        
        current_volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].tail(20).mean()
        
        # Volume surge (1.5x average)
        if current_volume > avg_volume * 1.5:
            return f"Volume surge: {current_volume / avg_volume:.1f}x average"
        
        return None
    
    def _generate_signal(self, instrument: str, level: dict, current_price: float,
                        confirmations: dict, df: pd.DataFrame) -> dict:
        """
        Generate complete signal with entry, stop loss, and targets
        
        Returns:
            Signal dictionary
        """
        signal_type = confirmations['signal_type']
        level_price = level['price']
        
        # Calculate distance to level
        distance = abs(current_price - level_price)
        distance_pct = (distance / level_price) * 100
        
        # Calculate entry, SL, and targets
        if signal_type == 'BUY':
            entry = current_price
            stop_loss = level_price * 0.997  # 0.3% below support
            
            # Calculate ATR for dynamic targets
            atr = self._calculate_atr(df, period=14)
            
            target1 = entry + (atr * 1.5)
            target2 = entry + (atr * 2.5)
            
        else:  # SELL
            entry = current_price
            stop_loss = level_price * 1.003  # 0.3% above resistance
            
            # Calculate ATR for dynamic targets
            atr = self._calculate_atr(df, period=14)
            
            target1 = entry - (atr * 1.5)
            target2 = entry - (atr * 2.5)
        
        # Calculate risk:reward
        risk = abs(entry - stop_loss)
        reward = abs(target2 - entry)
        risk_reward = reward / risk if risk > 0 else 0
        
        # Build reason string
        reason = f"Price near {level['type']} at {level['timeframe']} timeframe"
        
        # Generate signal
        signal = {
            'instrument': instrument,
            'signal_type': signal_type,
            'current_price': current_price,
            'timestamp': datetime.now(IST),
            'reason': reason,
            'level_type': level['type'],
            'level_price': level_price,
            'distance_to_level': distance,
            'distance_pct': distance_pct,
            'timeframe': level['timeframe'],
            'confirmations': confirmations['confirmations'],
            'signal_strength': min(confirmations['strength'], 10),
            'entry_price': entry,
            'stop_loss': stop_loss,
            'target1': target1,
            'target2': target2,
            'risk_reward': risk_reward
        }
        
        return signal
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate Average True Range"""
        high = df['high']
        low = df['low']
        close = df['close']

        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean().iloc[-1]

        return atr

    def get_htf_levels_table(self, df: pd.DataFrame, current_price: float) -> list:
        """
        Get HTF levels with distance from current price for display

        Args:
            df: DataFrame with OHLC data
            current_price: Current spot price

        Returns:
            List of level dictionaries with distance info
        """
        levels = self._calculate_htf_levels(df)
        result = []

        for level in levels:
            distance = current_price - level['price']
            distance_pct = (distance / level['price']) * 100
            proximity = "NEAR" if abs(distance_pct) <= self.level_proximity_pct else ""

            result.append({
                'Timeframe': level['timeframe'],
                'Type': level['type'],
                'Price': level['price'],
                'Distance': distance,
                'Distance%': distance_pct,
                'Status': proximity
            })

        return sorted(result, key=lambda x: abs(x['Distance']))

    def get_indicators(self, df: pd.DataFrame) -> dict:
        """Get current RSI and MACD values"""
        if len(df) < 50:
            return {'rsi': None, 'macd': None, 'macd_signal': None}

        close = df['close']
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(window=14).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        exp1 = close.ewm(span=12, adjust=False).mean()
        exp2 = close.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()

        return {
            'rsi': rsi.iloc[-1],
            'macd': macd.iloc[-1],
            'macd_signal': signal.iloc[-1]
        }

    def detect_current_pattern(self, df: pd.DataFrame) -> str:
        """Detect pattern at current candle (direction-agnostic for display)"""
        buy_pattern = self._detect_reversal_pattern(df, 'BUY')
        if buy_pattern:
            return buy_pattern
        sell_pattern = self._detect_reversal_pattern(df, 'SELL')
        return sell_pattern
