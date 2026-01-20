"""
Liquidity Drain Detection System
Simple detector that monitors DEX pools for liquidity drains using relative changes only.
No USD conversions - just track percentage drops and slippage changes.
"""

import uuid
import os
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List
from dataclasses import dataclass, field, asdict
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError
from evm import dex_pool_block_message_pb2
import logging
import config
import json
import requests
from detection_config import DetectionConfig

# ============================================================================
# Data Structures
# ============================================================================

@dataclass
class PoolState:
    """Tracks current state of a liquidity pool"""
    pool_id: str
    pool_address: str  # Pool contract address
    currency_a: str
    currency_b: str
    currency_a_symbol: str
    currency_b_symbol: str
    dex_protocol: str
    
    # Token decimals
    decimals_a: int = 18
    decimals_b: int = 18
    
    # Current liquidity (human-readable amounts - schema now sends floats directly)
    amount_a: float = 0.0
    amount_b: float = 0.0
    
    # Current slippage data for A->B and B->A directions
    # Key: basis points (e.g., 100)
    # Value: dict with 'max_amount_in' (human-readable float), 'min_amount_out' (human-readable float), 'price' (exchange rate)
    slippage_at_bps_a_to_b: Dict[int, Dict] = field(default_factory=dict)
    slippage_at_bps_b_to_a: Dict[int, Dict] = field(default_factory=dict)
    
    # Current average prices
    price_a_to_b: float = 0.0
    price_b_to_a: float = 0.0
    
    # Timestamp
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def amount_a_human(self) -> float:
        """Get currency A amount in human-readable format (already stored as human-readable)"""
        return max(0.0, self.amount_a)
    
    def amount_b_human(self) -> float:
        """Get currency B amount in human-readable format (already stored as human-readable)"""
        return max(0.0, self.amount_b)

@dataclass
class PoolHistory:
    """Maintains historical data for a pool to detect drains"""
    pool_id: str
    
    # Historical amounts for each currency separately (they're different units!)
    # Track as human-readable amounts after decimal conversion
    amount_a_history: deque = field(default_factory=lambda: deque(maxlen=1000))
    amount_b_history: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    # Track MaxAmountIn at 100bp over time (both directions) - used for alert detection
    max_amount_history_a_to_b_100bp: deque = field(default_factory=lambda: deque(maxlen=1000))
    max_amount_history_b_to_a_100bp: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    # Track MaxAmountIn for all slippage levels over time (for baseline calculation and display)
    # Key: basis points (e.g., 10, 50, 100, 200, 500, 1000)
    max_amount_history_a_to_b: Dict[int, deque] = field(default_factory=dict)
    max_amount_history_b_to_a: Dict[int, deque] = field(default_factory=dict)
    
    # Baseline metrics (track each currency separately)
    baseline_amount_a: Optional[float] = None
    baseline_amount_b: Optional[float] = None
    baseline_max_amount_a_to_b_100bp: Optional[float] = None
    baseline_max_amount_b_to_a_100bp: Optional[float] = None
    
    # Baselines for all slippage levels (for display only, not used for detection)
    baseline_max_amount_a_to_b_all: Dict[int, Optional[float]] = field(default_factory=dict)
    baseline_max_amount_b_to_a_all: Dict[int, Optional[float]] = field(default_factory=dict)
    
    # Alert management
    last_alert_time: Optional[datetime] = None
    last_alert_types: List[str] = field(default_factory=list)  # Track which alert types were sent
    alert_cooldown_minutes = DetectionConfig.ALERT_COOLDOWN_MINUTES
    
    # Pre-alert confirmation tracking (to prevent false positives)
    # Track recent drop measurements - only alert if drop persists across multiple measurements
    recent_drop_measurements_a: deque = field(default_factory=lambda: deque(maxlen=10))  # Store (timestamp, drop_percent)
    recent_drop_measurements_b: deque = field(default_factory=lambda: deque(maxlen=10))
    
    def add_measurement(self, amount_a_human: float, amount_b_human: float,
                       max_amount_a_to_b_100bp: Optional[float],
                       max_amount_b_to_a_100bp: Optional[float],
                       max_amounts_a_to_b_all: Dict[int, float],
                       max_amounts_b_to_a_all: Dict[int, float],
                       timestamp: datetime):
        """Add a new measurement to history"""
        if amount_a_human <= 0 and amount_b_human <= 0:
            return
        
        self.amount_a_history.append((timestamp, amount_a_human))
        self.amount_b_history.append((timestamp, amount_b_human))
        
        if max_amount_a_to_b_100bp is not None and max_amount_a_to_b_100bp > 0:
            self.max_amount_history_a_to_b_100bp.append((timestamp, max_amount_a_to_b_100bp))
        
        if max_amount_b_to_a_100bp is not None and max_amount_b_to_a_100bp > 0:
            self.max_amount_history_b_to_a_100bp.append((timestamp, max_amount_b_to_a_100bp))
        
        # Store all slippage levels for baseline calculation (display only)
        for bps, max_amount in max_amounts_a_to_b_all.items():
            if max_amount > 0:
                if bps not in self.max_amount_history_a_to_b:
                    self.max_amount_history_a_to_b[bps] = deque(maxlen=1000)
                self.max_amount_history_a_to_b[bps].append((timestamp, max_amount))
        
        for bps, max_amount in max_amounts_b_to_a_all.items():
            if max_amount > 0:
                if bps not in self.max_amount_history_b_to_a:
                    self.max_amount_history_b_to_a[bps] = deque(maxlen=1000)
                self.max_amount_history_b_to_a[bps].append((timestamp, max_amount))
        
        self.update_baseline(timestamp)
    
    def update_baseline(self, current_time: datetime):
        """Update baseline from recent history (using mean for better sensitivity)"""
        cutoff_time = current_time - timedelta(hours=DetectionConfig.BASELINE_WINDOW_HOURS)
        
        # Baseline for currency A (mean - better for gradual drain detection)
        recent_amount_a = [amt for ts, amt in self.amount_a_history if ts >= cutoff_time and amt > 0]
        if recent_amount_a:
            self.baseline_amount_a = sum(recent_amount_a) / len(recent_amount_a)
        
        # Baseline for currency B (mean)
        recent_amount_b = [amt for ts, amt in self.amount_b_history if ts >= cutoff_time and amt > 0]
        if recent_amount_b:
            self.baseline_amount_b = sum(recent_amount_b) / len(recent_amount_b)
        
        # Baseline MaxAmountIn at 100bp (A->B) - used for detection
        recent_max_a = [amt for ts, amt in self.max_amount_history_a_to_b_100bp if ts >= cutoff_time and amt > 0]
        if recent_max_a:
            self.baseline_max_amount_a_to_b_100bp = sum(recent_max_a) / len(recent_max_a)
        
        # Baseline MaxAmountIn at 100bp (B->A) - used for detection
        recent_max_b = [amt for ts, amt in self.max_amount_history_b_to_a_100bp if ts >= cutoff_time and amt > 0]
        if recent_max_b:
            self.baseline_max_amount_b_to_a_100bp = sum(recent_max_b) / len(recent_max_b)
        
        # Calculate baselines for all slippage levels (for display only)
        for bps, history_queue in self.max_amount_history_a_to_b.items():
            recent = [amt for ts, amt in history_queue if ts >= cutoff_time and amt > 0]
            if recent:
                self.baseline_max_amount_a_to_b_all[bps] = sum(recent) / len(recent)
        
        for bps, history_queue in self.max_amount_history_b_to_a.items():
            recent = [amt for ts, amt in history_queue if ts >= cutoff_time and amt > 0]
            if recent:
                self.baseline_max_amount_b_to_a_all[bps] = sum(recent) / len(recent)
    
    def amount_drop_percent(self, current_amount_a: float, current_amount_b: float) -> tuple[Optional[float], Optional[float]]:
        """Calculate drop percentage for each currency from baseline"""
        drop_a = None
        drop_b = None
        
        if self.baseline_amount_a is not None and self.baseline_amount_a > 0:
            drop_a = ((self.baseline_amount_a - current_amount_a) / self.baseline_amount_a) * 100
        
        if self.baseline_amount_b is not None and self.baseline_amount_b > 0:
            drop_b = ((self.baseline_amount_b - current_amount_b) / self.baseline_amount_b) * 100
        
        return drop_a, drop_b
    
    def max_amount_decrease_percent(self, current_max: float, direction: str) -> Optional[float]:
        """Calculate MaxAmountIn decrease percentage from baseline"""
        baseline = self.baseline_max_amount_a_to_b_100bp if direction == 'a_to_b' else self.baseline_max_amount_b_to_a_100bp
        if baseline is None or baseline == 0:
            return None
        return ((baseline - current_max) / baseline) * 100
    
    def is_rapid_drain(self, current_amount_a: float, current_amount_b: float, current_time: datetime) -> bool:
        """Check if liquidity dropped rapidly (within RAPID_DRAIN_WINDOW)"""
        cutoff_time = current_time - timedelta(minutes=DetectionConfig.RAPID_DRAIN_WINDOW_MINUTES)
        
        recent_a = [amt for ts, amt in self.amount_a_history if ts >= cutoff_time and amt > 0]
        recent_b = [amt for ts, amt in self.amount_b_history if ts >= cutoff_time and amt > 0]
        
        # Check currency A
        if len(recent_a) >= 2:
            max_a = max(recent_a)
            if max_a > 0:
                drop_a = ((max_a - current_amount_a) / max_a) * 100
                if drop_a >= DetectionConfig.LIQUIDITY_DROP_WARNING:
                    return True
        
        # Check currency B
        if len(recent_b) >= 2:
            max_b = max(recent_b)
            if max_b > 0:
                drop_b = ((max_b - current_amount_b) / max_b) * 100
                if drop_b >= DetectionConfig.LIQUIDITY_DROP_WARNING:
                    return True
        
        return False
    
    def check_recovery(self, current_amount_a: float, current_amount_b: float, current_time: datetime) -> bool:
        """Check if liquidity recovered after a recent drop (false positive filter)"""
        if self.last_alert_time is None:
            return False
        
        recovery_window_end = self.last_alert_time + timedelta(minutes=DetectionConfig.LOOKBACK_WINDOW_MINUTES)
        if self.last_alert_time <= current_time <= recovery_window_end:
            # Check if liquidity recovered (either currency close to baseline = likely false positive)
            # If drop was real, both should stay low. If temporary, at least one should recover.
            recovered_a = self.baseline_amount_a and current_amount_a >= self.baseline_amount_a * 0.9
            recovered_b = self.baseline_amount_b and current_amount_b >= self.baseline_amount_b * 0.9
            
            # Consider it recovered if either currency is close to baseline (was likely temporary fluctuation)
            if recovered_a or recovered_b:
                # Additional check: if both were significantly below, require both to recover
                # But if only one was affected, recovery of that one is enough
                return True
        return False
    
    def can_alert(self, current_time: datetime) -> bool:
        """Check if we can send a new alert (respect cooldown)"""
        if self.last_alert_time is None:
            return True
        time_since_alert = (current_time - self.last_alert_time).total_seconds() / 60
        return time_since_alert >= self.alert_cooldown_minutes
    
    def has_sufficient_baseline(self, current_time: datetime) -> bool:
        """Check if we have enough data to trust the baseline"""
        # Need minimum number of measurements
        min_measurements = min(len(self.amount_a_history), len(self.amount_b_history))
        if min_measurements < DetectionConfig.MIN_MEASUREMENTS_FOR_BASELINE:
            return False
        
        # Need minimum time span
        if min_measurements < 2:
            return False
        
        all_times = [ts for ts, _ in self.amount_a_history] + [ts for ts, _ in self.amount_b_history]
        if not all_times:
            return False
        
        oldest_time = min(all_times)
        time_span_minutes = (current_time - oldest_time).total_seconds() / 60
        
        if time_span_minutes < DetectionConfig.MIN_TIME_FOR_BASELINE_MINUTES:
            return False
        
        # Need valid baselines for at least one currency
        if (self.baseline_amount_a is None or self.baseline_amount_a <= 0) and \
           (self.baseline_amount_b is None or self.baseline_amount_b <= 0):
            return False
        
        return True

@dataclass
class DrainAlert:
    """Represents a liquidity drain alert"""
    pool_id: str
    severity: str  # 'warning' or 'critical'
    alert_type: str
    message: str
    metrics: Dict
    timestamp: datetime
    pool_info: Dict
    
    def to_dict(self) -> Dict:
        """Convert alert to dictionary for API"""
        return {
            'pool_id': self.pool_id,
            'severity': self.severity,
            'alert_type': self.alert_type,
            'message': self.message,
            'metrics': self.metrics,
            'timestamp': self.timestamp.isoformat(),
            'pool_info': self.pool_info
        }

# ============================================================================
# Detection Engine
# ============================================================================

class LiquidityDrainDetector:
    """Simple liquidity drain detector - tracks relative changes only"""
    
    def __init__(self):
        self.pool_states: Dict[str, PoolState] = {}
        self.pool_histories: Dict[str, PoolHistory] = {}
        self.logger = logging.getLogger(__name__)
        
    def process_pool_update(self, dex_pool_event) -> List[DrainAlert]:
        """Process a DEX pool event and return any alerts"""
        alerts = []
        current_time = datetime.now(timezone.utc)
        
        try:
            pool = dex_pool_event.Pool
            liquidity = dex_pool_event.Liquidity
            price_table = dex_pool_event.PoolPriceTable
            dex_info = dex_pool_event.Dex
            
            # Extract transaction hash if available
            transaction_hash = None
            if hasattr(dex_pool_event, 'TransactionHeader') and dex_pool_event.TransactionHeader:
                if hasattr(dex_pool_event.TransactionHeader, 'Hash') and dex_pool_event.TransactionHeader.Hash:
                    transaction_hash = convert_bytes_to_hex(dex_pool_event.TransactionHeader.Hash)
            
            pool_address = convert_bytes_to_hex(pool.SmartContract) if hasattr(pool, 'SmartContract') else 'unknown'
            currency_a = convert_bytes_to_hex(pool.CurrencyA.SmartContract) if hasattr(pool.CurrencyA, 'SmartContract') else 'unknown'
            currency_b = convert_bytes_to_hex(pool.CurrencyB.SmartContract) if hasattr(pool.CurrencyB, 'SmartContract') else 'unknown'
            protocol_name = dex_info.ProtocolName if hasattr(dex_info, 'ProtocolName') else 'unknown'
            
            # For Uniswap V4, use PoolId to identify individual pools
            # (Liquidity amounts are aggregated across all pools in poolManager, but MaxAmountIn is pool-specific)
            # For other protocols, use composite key: pool_address + currency_pair
            if protocol_name == 'uniswap_v4' and hasattr(pool, 'PoolId') and pool.PoolId:
                # PoolId is bytes, convert to hex string
                if isinstance(pool.PoolId, bytes) and len(pool.PoolId) > 0:
                    pool_id = convert_bytes_to_hex(pool.PoolId).lower()
                else:
                    # Fallback if PoolId is invalid/empty
                    currency_a_lower = currency_a.lower()
                    currency_b_lower = currency_b.lower()
                    if currency_a_lower < currency_b_lower:
                        pool_id = f"{pool_address.lower()}_{currency_a_lower}_{currency_b_lower}"
                    else:
                        pool_id = f"{pool_address.lower()}_{currency_b_lower}_{currency_a_lower}"
                    self.logger.warning(f"Uniswap V4 pool with invalid/empty PoolId, using composite key: {pool_id}")
            else:
                # For non-V4 protocols, use composite key: pool_address + currency_pair (sorted for consistency)
                # Same pool address can have different token pairs (e.g., Uniswap V3)
                currency_a_lower = currency_a.lower()
                currency_b_lower = currency_b.lower()
                if currency_a_lower < currency_b_lower:
                    pool_id = f"{pool_address.lower()}_{currency_a_lower}_{currency_b_lower}"
                else:
                    pool_id = f"{pool_address.lower()}_{currency_b_lower}_{currency_a_lower}"
            
            # Get or create pool state
            if pool_id not in self.pool_states:
                self.pool_states[pool_id] = PoolState(
                    pool_id=pool_id,
                    pool_address=pool_address,
                    currency_a=currency_a,
                    currency_b=currency_b,
                    currency_a_symbol=pool.CurrencyA.Symbol,
                    currency_b_symbol=pool.CurrencyB.Symbol,
                    dex_protocol=dex_info.ProtocolName
                )
                self.pool_histories[pool_id] = PoolHistory(pool_id=pool_id)
            
            # Get references to state and history (must be after creation check above)
            state = self.pool_states[pool_id]
            history = self.pool_histories[pool_id]
            
            # Update pool address in case it changed
            state.pool_address = pool_address
            
            # Update pool state (schema now sends floats directly - human-readable)
            amount_a_raw = liquidity.AmountCurrencyA
            amount_b_raw = liquidity.AmountCurrencyB
            
            # Handle float values (new schema)
            if isinstance(amount_a_raw, (float, int)):
                state.amount_a = float(amount_a_raw)
            elif isinstance(amount_a_raw, bytes):
                # Backward compatibility: convert bytes to int, then to human-readable
                amount_a_int = int.from_bytes(amount_a_raw, byteorder='big')
                state.amount_a = amount_a_int / (10 ** state.decimals_a) if state.decimals_a > 0 else float(amount_a_int)
            else:
                state.amount_a = 0.0
            
            if isinstance(amount_b_raw, (float, int)):
                state.amount_b = float(amount_b_raw)
            elif isinstance(amount_b_raw, bytes):
                # Backward compatibility: convert bytes to int, then to human-readable
                amount_b_int = int.from_bytes(amount_b_raw, byteorder='big')
                state.amount_b = amount_b_int / (10 ** state.decimals_b) if state.decimals_b > 0 else float(amount_b_int)
            else:
                state.amount_b = 0.0
            
            state.price_a_to_b = price_table.AtoBPrice if hasattr(price_table, 'AtoBPrice') else 0.0
            state.price_b_to_a = price_table.BtoAPrice if hasattr(price_table, 'BtoAPrice') else 0.0
            state.decimals_a = pool.CurrencyA.Decimals if hasattr(pool.CurrencyA, 'Decimals') else 18
            state.decimals_b = pool.CurrencyB.Decimals if hasattr(pool.CurrencyB, 'Decimals') else 18
            state.last_updated = current_time
            
            # Process slippage data (A->B direction)
            max_amount_a_to_b_100bp = None
            if hasattr(price_table, 'AtoBPrices') and price_table.AtoBPrices:
                for price_info in price_table.AtoBPrices:
                    bps = price_info.SlippageBasisPoints
                    
                    # Handle float values (new schema - already human-readable)
                    max_in = price_info.MaxAmountIn
                    if isinstance(max_in, (float, int)):
                        max_in_human = float(max_in)
                    elif isinstance(max_in, bytes):
                        # Backward compatibility: convert bytes to int, then to human-readable
                        max_in_int = int.from_bytes(max_in, byteorder='big')
                        max_in_human = max_in_int / (10 ** state.decimals_a) if state.decimals_a > 0 else float(max_in_int)
                    else:
                        continue
                    
                    min_out = price_info.MinAmountOut
                    if isinstance(min_out, (float, int)):
                        min_out_human = float(min_out)
                    elif isinstance(min_out, bytes):
                        # Backward compatibility: convert bytes to int, then to human-readable
                        min_out_int = int.from_bytes(min_out, byteorder='big')
                        min_out_human = min_out_int / (10 ** state.decimals_b) if state.decimals_b > 0 else float(min_out_int)
                    else:
                        min_out_human = 0.0
                    
                    if max_in_human <= 0:
                        continue
                    
                    # Store slippage data (Price is exchange rate, not normalized)
                    state.slippage_at_bps_a_to_b[bps] = {
                        'max_amount_in': max_in_human,
                        'min_amount_out': min_out_human,
                        'price': price_info.Price,  # Exchange rate
                        'slippage_basis_points': bps
                    }
                    
                    if bps == 100:
                        max_amount_a_to_b_100bp = max_in_human
            
            # Process slippage data (B->A direction)
            max_amount_b_to_a_100bp = None
            if hasattr(price_table, 'BtoAPrices') and price_table.BtoAPrices:
                for price_info in price_table.BtoAPrices:
                    bps = price_info.SlippageBasisPoints
                    
                    # Handle float values (new schema - already human-readable)
                    max_in = price_info.MaxAmountIn
                    if isinstance(max_in, (float, int)):
                        max_in_human = float(max_in)
                    elif isinstance(max_in, bytes):
                        # Backward compatibility: convert bytes to int, then to human-readable
                        max_in_int = int.from_bytes(max_in, byteorder='big')
                        max_in_human = max_in_int / (10 ** state.decimals_b) if state.decimals_b > 0 else float(max_in_int)
                    else:
                        continue
                    
                    min_out = price_info.MinAmountOut
                    if isinstance(min_out, (float, int)):
                        min_out_human = float(min_out)
                    elif isinstance(min_out, bytes):
                        # Backward compatibility: convert bytes to int, then to human-readable
                        min_out_int = int.from_bytes(min_out, byteorder='big')
                        min_out_human = min_out_int / (10 ** state.decimals_a) if state.decimals_a > 0 else float(min_out_int)
                    else:
                        min_out_human = 0.0
                    
                    if max_in_human <= 0:
                        continue
                    
                    state.slippage_at_bps_b_to_a[bps] = {
                        'max_amount_in': max_in_human,
                        'min_amount_out': min_out_human,
                        'price': price_info.Price,
                        'slippage_basis_points': bps
                    }
                    
                    if bps == 100:
                        max_amount_b_to_a_100bp = max_in_human
            
            # Get human-readable amounts for each currency
            amount_a_human = state.amount_a_human()
            amount_b_human = state.amount_b_human()
            
            if amount_a_human <= 0 and amount_b_human <= 0:
                return alerts
            
            # Extract all slippage levels for baseline tracking (display only)
            max_amounts_a_to_b_all = {}
            max_amounts_b_to_a_all = {}
            for bps, data in state.slippage_at_bps_a_to_b.items():
                max_amounts_a_to_b_all[bps] = data['max_amount_in']
            for bps, data in state.slippage_at_bps_b_to_a.items():
                max_amounts_b_to_a_all[bps] = data['max_amount_in']
            
            # Add to history (track each currency separately)
            # Note: Detection still uses 100bp only, but we track all levels for display
            history.add_measurement(amount_a_human, amount_b_human, max_amount_a_to_b_100bp, max_amount_b_to_a_100bp, 
                                  max_amounts_a_to_b_all, max_amounts_b_to_a_all, current_time)
            
            # Skip very small pools (simple check using both currencies)
            if amount_a_human + amount_b_human < DetectionConfig.MIN_LIQUIDITY_TOKENS:
                return alerts
            
            # Run detection checks if we have sufficient baseline data
            if history.has_sufficient_baseline(current_time):
                # Check liquidity drop (for each currency separately)
                alerts.extend(self._check_liquidity_drop(state, history, amount_a_human, amount_b_human, current_time, transaction_hash))
                
                # Check MaxAmountIn decreases (liquidity depth shrinking)
                if DetectionConfig.ENABLE_MAX_AMOUNT_DECREASE_ALERTS:
                    alerts.extend(self._check_max_amount_decrease(
                        state, history, max_amount_a_to_b_100bp, max_amount_b_to_a_100bp, current_time, transaction_hash
                    ))
                
                # Check rapid drain
                alerts.extend(self._check_rapid_drain(state, history, amount_a_human, amount_b_human, current_time, transaction_hash))
            else:
                # Log that we're still building baseline (debug info)
                min_measurements = min(len(history.amount_a_history), len(history.amount_b_history))
                if min_measurements % 10 == 0:
                    self.logger.debug(
                        f"Pool {pool_id[:16]}... building baseline: "
                        f"{min_measurements} measurements, "
                        f"need {DetectionConfig.MIN_MEASUREMENTS_FOR_BASELINE}"
                    )
            
            # Mark alert times and track alert types
            if alerts:
                history.last_alert_time = current_time
                # Clear drop measurements after alert (reset for next detection cycle)
                history.recent_drop_measurements_a.clear()
                history.recent_drop_measurements_b.clear()
                # Track alert types (keep last 5)
                for alert in alerts:
                    if alert.alert_type not in history.last_alert_types:
                        history.last_alert_types.append(alert.alert_type)
                        if len(history.last_alert_types) > 5:
                            history.last_alert_types.pop(0)
            
        except Exception as e:
            self.logger.error(f"Error processing pool update: {e}", exc_info=True)
        
        return alerts
    
    def _check_liquidity_drop(self, state: PoolState, history: PoolHistory, 
                              current_amount_a: float, current_amount_b: float, 
                              current_time: datetime, transaction_hash: Optional[str] = None) -> List[DrainAlert]:
        """Check for significant liquidity drops (track each currency separately)"""
        alerts = []
        
        if not history.can_alert(current_time):
            return alerts
        
        drop_a, drop_b = history.amount_drop_percent(current_amount_a, current_amount_b)
        
        # Check for recovery (false positive filter)
        if DetectionConfig.RECOVERY_CHECK_ENABLED and history.check_recovery(current_amount_a, current_amount_b, current_time):
            last_types = ', '.join(history.last_alert_types[-3:]) if history.last_alert_types else 'unknown'
            self.logger.info(f"Pool {state.pool_id[:16]}... recovered after drop (previous alerts: {last_types}), likely false positive")
            return alerts
        
        # Pre-alert confirmation: Track recent drops and only alert if drop persists
        # This prevents false positives from temporary fluctuations
        
        # Check currency A drop
        if drop_a is not None:
            # Track this measurement if it shows a drop
            if drop_a >= DetectionConfig.LIQUIDITY_DROP_WARNING:
                history.recent_drop_measurements_a.append((current_time, drop_a))
            else:
                # If drop is below threshold, clear recent measurements (drop didn't persist)
                history.recent_drop_measurements_a.clear()
            
            # Pre-alert confirmation: Require multiple consecutive measurements showing drop
            if len(history.recent_drop_measurements_a) >= DetectionConfig.DROP_CONFIRMATION_COUNT:
                # Drop has persisted across multiple measurements - proceed with alert
                severity = None
                if drop_a >= DetectionConfig.LIQUIDITY_DROP_CRITICAL:
                    severity = 'critical'
                elif drop_a >= DetectionConfig.LIQUIDITY_DROP_WARNING:
                    severity = 'warning'
                
                if severity:
                    alerts.append(DrainAlert(
                    pool_id=state.pool_id,
                    severity=severity,
                    alert_type='liquidity_drop',
                    message=f"{state.currency_a_symbol} dropped {drop_a:.1f}% from baseline",
                    metrics={
                        'currency': state.currency_a_symbol,
                        'current_amount': current_amount_a,
                        'baseline_amount': history.baseline_amount_a,
                        'drop_percent': drop_a,
                    },
                    timestamp=current_time,
                    pool_info={
                        'pool_id': state.pool_id,
                        'pool_address': state.pool_address,
                        'currency_pair': f"{state.currency_a_symbol}/{state.currency_b_symbol}",
                        'dex': state.dex_protocol,
                        'transaction_hash': transaction_hash
                    }
                ))
        
        # Check currency B drop
        if drop_b is not None:
            # Track this measurement if it shows a drop
            if drop_b >= DetectionConfig.LIQUIDITY_DROP_WARNING:
                history.recent_drop_measurements_b.append((current_time, drop_b))
            else:
                # If drop is below threshold, clear recent measurements (drop didn't persist)
                history.recent_drop_measurements_b.clear()
            
            # Pre-alert confirmation: Require multiple consecutive measurements showing drop
            if len(history.recent_drop_measurements_b) >= DetectionConfig.DROP_CONFIRMATION_COUNT:
                # Drop has persisted across multiple measurements - proceed with alert
                severity = None
                if drop_b >= DetectionConfig.LIQUIDITY_DROP_CRITICAL:
                    severity = 'critical'
                elif drop_b >= DetectionConfig.LIQUIDITY_DROP_WARNING:
                    severity = 'warning'
                
                if severity:
                    alerts.append(DrainAlert(
                        pool_id=state.pool_id,
                        severity=severity,
                        alert_type='liquidity_drop',
                        message=f"{state.currency_b_symbol} dropped {drop_b:.1f}% from baseline",
                        metrics={
                            'currency': state.currency_b_symbol,
                            'current_amount': current_amount_b,
                            'baseline_amount': history.baseline_amount_b,
                            'drop_percent': drop_b,
                        },
                        timestamp=current_time,
                        pool_info={
                            'pool_id': state.pool_id,
                            'pool_address': state.pool_address,
                            'currency_pair': f"{state.currency_a_symbol}/{state.currency_b_symbol}",
                            'dex': state.dex_protocol,
                            'transaction_hash': transaction_hash
                        }
                    ))
        
        return alerts
    
    def _check_max_amount_decrease(self, state: PoolState, history: PoolHistory,
                                  max_amount_a_to_b: Optional[float],
                                  max_amount_b_to_a: Optional[float],
                                  current_time: datetime, transaction_hash: Optional[str] = None) -> List[DrainAlert]:
        """Check if MaxAmountIn decreased significantly (liquidity depth shrinking)"""
        alerts = []
        
        if not history.can_alert(current_time):
            return alerts
        
        # Check both directions, use worst-case
        worst_direction = None
        worst_decrease = None
        worst_max_amount = None
        
        if max_amount_a_to_b is not None:
            decrease_a = history.max_amount_decrease_percent(max_amount_a_to_b, 'a_to_b')
            if decrease_a is not None and decrease_a > 0:
                if worst_decrease is None or decrease_a > worst_decrease:
                    worst_decrease = decrease_a
                    worst_direction = 'a_to_b'
                    worst_max_amount = max_amount_a_to_b
        
        if max_amount_b_to_a is not None:
            decrease_b = history.max_amount_decrease_percent(max_amount_b_to_a, 'b_to_a')
            if decrease_b is not None and decrease_b > 0:
                if worst_decrease is None or decrease_b > worst_decrease:
                    worst_decrease = decrease_b
                    worst_direction = 'b_to_a'
                    worst_max_amount = max_amount_b_to_a
        
        if worst_decrease is None or worst_decrease <= 0:
            return alerts
        
        severity = None
        if worst_decrease >= 50.0:  # 50% drop in max trade size
            severity = 'critical'
        elif worst_decrease >= 30.0:  # 30% drop
            severity = 'warning'
        
        if severity:
            direction_label = f"{state.currency_a_symbol}→{state.currency_b_symbol}" if worst_direction == 'a_to_b' else f"{state.currency_b_symbol}→{state.currency_a_symbol}"
            
            alerts.append(DrainAlert(
                pool_id=state.pool_id,
                severity=severity,
                alert_type='max_amount_decrease',
                message=f"Max trade size dropped {worst_decrease:.1f}% from baseline (direction: {direction_label})",
                metrics={
                    'max_amount_decrease_percent': worst_decrease,
                    'current_max_amount': worst_max_amount,
                    'direction': worst_direction,
                    'baseline_max_amount_a_to_b': history.baseline_max_amount_a_to_b_100bp,
                    'baseline_max_amount_b_to_a': history.baseline_max_amount_b_to_a_100bp,
                    'current_max_amount_a_to_b': max_amount_a_to_b,
                    'current_max_amount_b_to_a': max_amount_b_to_a,
                },
                timestamp=current_time,
                pool_info={
                    'pool_id': state.pool_id,
                    'pool_address': state.pool_address,
                    'currency_pair': f"{state.currency_a_symbol}/{state.currency_b_symbol}",
                    'dex': state.dex_protocol,
                    'transaction_hash': transaction_hash
                }
            ))
        
        return alerts
    
    def _check_rapid_drain(self, state: PoolState, history: PoolHistory, 
                           current_amount_a: float, current_amount_b: float,
                           current_time: datetime, transaction_hash: Optional[str] = None) -> List[DrainAlert]:
        """Check for rapid drains (sudden drops)"""
        alerts = []
        
        if not history.can_alert(current_time):
            return alerts
        
        if history.is_rapid_drain(current_amount_a, current_amount_b, current_time):
            alerts.append(DrainAlert(
                pool_id=state.pool_id,
                severity='critical',
                alert_type='rapid_drain',
                message="Rapid liquidity drain detected in last 5 minutes",
                metrics={
                    'current_amount_a': current_amount_a,
                    'current_amount_b': current_amount_b,
                    'baseline_amount_a': history.baseline_amount_a,
                    'baseline_amount_b': history.baseline_amount_b
                },
                timestamp=current_time,
                pool_info={
                    'pool_id': state.pool_id,
                    'pool_address': state.pool_address,
                    'currency_pair': f"{state.currency_a_symbol}/{state.currency_b_symbol}",
                    'dex': state.dex_protocol,
                    'transaction_hash': transaction_hash
                }
            ))
        
        return alerts

# ============================================================================
# Helper Functions
# ============================================================================

def convert_bytes_to_hex(value) -> str:
    """Convert bytes to hexadecimal string"""
    return '0x' + value.hex()

def format_alert(alert: DrainAlert, state: Optional[PoolState] = None, history: Optional[PoolHistory] = None) -> str:
    """Format an alert for display with full details"""
    output = f"""
{'='*80}
🚨 LIQUIDITY DRAIN ALERT - {alert.severity.upper()}
{'='*80}
Type: {alert.alert_type}
Pool ID: {alert.pool_info.get('pool_id', 'N/A')}
Pool Address: {alert.pool_info.get('pool_address', 'N/A')}
Pair: {alert.pool_info.get('currency_pair', 'N/A')}
DEX: {alert.pool_info.get('dex', 'N/A')}
Transaction Hash: {alert.pool_info.get('transaction_hash', 'N/A')}
Time: {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}

{alert.message}

Current State:
"""
    
    if state and history:
        # Convert amounts to human-readable
        amount_a_human = state.amount_a_human()
        amount_b_human = state.amount_b_human()
        
        # Calculate liquidity drops
        drop_a, drop_b = history.amount_drop_percent(amount_a_human, amount_b_human)
        
        baseline_a_str = f"{history.baseline_amount_a:,.6f} {state.currency_a_symbol}" if history.baseline_amount_a is not None else 'N/A'
        baseline_b_str = f"{history.baseline_amount_b:,.6f} {state.currency_b_symbol}" if history.baseline_amount_b is not None else 'N/A'
        drop_a_str = f"{drop_a:+.1f}%" if drop_a is not None else 'N/A'
        drop_b_str = f"{drop_b:+.1f}%" if drop_b is not None else 'N/A'
        
        output += f"""
  Currency A: {state.currency_a_symbol} ({state.currency_a})
    - Current Liquidity: {amount_a_human:,.6f} {state.currency_a_symbol}
    - Baseline Liquidity: {baseline_a_str}
    - Drop: {drop_a_str}
  
  Currency B: {state.currency_b_symbol} ({state.currency_b})
    - Current Liquidity: {amount_b_human:,.6f} {state.currency_b_symbol}
    - Baseline Liquidity: {baseline_b_str}
    - Drop: {drop_b_str}
  
  Price A->B: {state.price_a_to_b:.10f}
  Price B->A: {state.price_b_to_a:.10f}
"""
        
        # Add MaxAmountIn data at multiple slippage levels (convert to human-readable)
        # Common slippage levels to display
        slippage_levels = [10, 50, 100, 200, 500, 1000]
        
        # A->B direction
        output += "\n  A->B:\n"
        for bps in slippage_levels:
            if bps in state.slippage_at_bps_a_to_b:
                data_a = state.slippage_at_bps_a_to_b[bps]
                max_a_human = data_a['max_amount_in']  # Already human-readable
                
                # Show baseline and drop for all levels
                baseline_max_a_human = history.baseline_max_amount_a_to_b_all.get(bps)
                drop_max_a = None
                if baseline_max_a_human is not None and baseline_max_a_human > 0:
                    drop_max_a = ((baseline_max_a_human - max_a_human) / baseline_max_a_human) * 100
                
                baseline_max_a_str = f"{baseline_max_a_human:,.6f} {state.currency_a_symbol}" if baseline_max_a_human is not None else 'N/A'
                drop_max_a_str = f"{drop_max_a:+.1f}%" if drop_max_a is not None else 'N/A'
                
                output += f"""    ({bps}bp):
      - Current MaxAmountIn: {max_a_human:,.6f} {state.currency_a_symbol}
      - Baseline MaxAmountIn: {baseline_max_a_str}
      - Drop: {drop_max_a_str}
      - Exchange Rate: {data_a['price']:.10f}
"""
        
        # B->A direction
        output += "\n  B->A:\n"
        for bps in slippage_levels:
            if bps in state.slippage_at_bps_b_to_a:
                data_b = state.slippage_at_bps_b_to_a[bps]
                max_b_human = data_b['max_amount_in']  # Already human-readable
                
                # Show baseline and drop for all levels
                baseline_max_b_human = history.baseline_max_amount_b_to_a_all.get(bps)
                drop_max_b = None
                if baseline_max_b_human is not None and baseline_max_b_human > 0:
                    drop_max_b = ((baseline_max_b_human - max_b_human) / baseline_max_b_human) * 100
                
                baseline_max_b_str = f"{baseline_max_b_human:,.6f} {state.currency_b_symbol}" if baseline_max_b_human is not None else 'N/A'
                drop_max_b_str = f"{drop_max_b:+.1f}%" if drop_max_b is not None else 'N/A'
                
                output += f"""    ({bps}bp):
      - Current MaxAmountIn: {max_b_human:,.6f} {state.currency_b_symbol}
      - Baseline MaxAmountIn: {baseline_max_b_str}
      - Drop: {drop_max_b_str}
      - Exchange Rate: {data_b['price']:.10f}
"""
    
    output += f"""
History Stats:
"""
    
    if history:
        last_alert_str = history.last_alert_time.strftime('%Y-%m-%d %H:%M:%S UTC') if history.last_alert_time else 'Never'
        
        # Calculate time ranges for baseline validation
        baseline_cutoff = alert.timestamp - timedelta(hours=DetectionConfig.BASELINE_WINDOW_HOURS)
        
        # Get baseline measurement counts and time ranges
        baseline_max_a_100bp = [amt for ts, amt in history.max_amount_history_a_to_b_100bp if ts >= baseline_cutoff and amt > 0]
        baseline_max_b_100bp = [amt for ts, amt in history.max_amount_history_b_to_a_100bp if ts >= baseline_cutoff and amt > 0]
        
        baseline_times_a = [ts for ts, amt in history.max_amount_history_a_to_b_100bp if ts >= baseline_cutoff and amt > 0]
        baseline_times_b = [ts for ts, amt in history.max_amount_history_b_to_a_100bp if ts >= baseline_cutoff and amt > 0]
        
        baseline_start_a = min(baseline_times_a).strftime('%Y-%m-%d %H:%M:%S UTC') if baseline_times_a else 'N/A'
        baseline_start_b = min(baseline_times_b).strftime('%Y-%m-%d %H:%M:%S UTC') if baseline_times_b else 'N/A'
        
        output += f"""
    - Liquidity A measurements: {len(history.amount_a_history)}
    - Liquidity B measurements: {len(history.amount_b_history)}
    - MaxAmountIn A->B (100bp) measurements: {len(history.max_amount_history_a_to_b_100bp)}
      * Baseline window measurements: {len(baseline_max_a_100bp)}
      * Baseline start time: {baseline_start_a}
    - MaxAmountIn B->A (100bp) measurements: {len(history.max_amount_history_b_to_a_100bp)}
      * Baseline window measurements: {len(baseline_max_b_100bp)}
      * Baseline start time: {baseline_start_b}
    - Last alert time: {last_alert_str}
    
    ⚠️  VALIDATION NOTES:
    - For Uniswap V4: Liquidity amounts are AGGREGATED across all pools (may not change much)
    - MaxAmountIn is POOL-SPECIFIC (drops reflect this specific pool's drain)
    - Baseline uses last {DetectionConfig.BASELINE_WINDOW_HOURS} hours of data
    - Check transaction hash on Etherscan to verify on-chain events
"""
    
    output += f"""
Alert Metrics:
{json.dumps(alert.metrics, indent=2, default=str)}
{'='*80}
"""
    
    return output

# ============================================================================
# Main Consumer Integration
# ============================================================================

def send_alert_to_api(alert: DrainAlert, api_url: str = 'http://localhost:5000', logger=None) -> bool:
    """Send alert to API server"""
    try:
        response = requests.post(
            f'{api_url}/api/alerts',
            json=alert.to_dict(),
            timeout=2
        )
        if response.status_code == 200:
            if logger:
                logger.debug(f"Alert sent to API successfully: {alert.alert_type}")
            return True
        else:
            if logger:
                logger.warning(f"API returned status {response.status_code} for alert")
            return False
    except requests.exceptions.ConnectionError as e:
        if logger:
            logger.warning(f"Could not connect to API at {api_url} - is the server running?")
        return False
    except Exception as e:
        if logger:
            logger.warning(f"Error sending alert to API: {e}")
        return False

def main():
    """Main execution"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - [%(levelname)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)
    
    # API configuration (optional)
    api_url = os.environ.get('API_URL', 'http://localhost:5001')  # Default to 5001 to match API server
    enable_api = os.environ.get('ENABLE_API', 'true').lower() == 'true'
    
    detector = LiquidityDrainDetector()
    
    group_id_suffix = uuid.uuid4().hex
    conf = {
        'bootstrap.servers': 'rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092',
        'group.id': f'{config.username}-liquidity-drain-{group_id_suffix}',
        'session.timeout.ms': 30000,
        'security.protocol': 'SASL_PLAINTEXT',
        'ssl.endpoint.identification.algorithm': 'none',
        'sasl.mechanisms': 'SCRAM-SHA-512',
        'sasl.username': config.username,
        'sasl.password': config.password,
        'auto.offset.reset': 'latest',
    }
    
    consumer = Consumer(conf)
    topic = 'eth.dexpools.proto'
    consumer.subscribe([topic])
    
    logger.info(f"Starting liquidity drain detector for topic: {topic}")
    logger.info(f"Consumer group ID: {conf['group.id']}")
    logger.info("Monitoring for liquidity drains...")
    logger.info(f"Thresholds: Warning={DetectionConfig.LIQUIDITY_DROP_WARNING}%, Critical={DetectionConfig.LIQUIDITY_DROP_CRITICAL}%")
    logger.info(f"Kafka offset reset: 'latest' (will only process new messages from now)")
    logger.info("Note: Building baseline data takes a few minutes before alerts can be generated")
    if enable_api:
        logger.info(f"API integration enabled: {api_url}")
    logger.info("Press Ctrl+C to stop\n")
    
    processed_count = 0
    alert_count = 0
    poll_count = 0
    last_message_time = None
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            poll_count += 1
            
            # Log heartbeat every 60 seconds to show it's alive
            if poll_count % 60 == 0:
                pools_tracked = len(detector.pool_states)
                if last_message_time:
                    time_since_msg = (datetime.now(timezone.utc) - last_message_time).total_seconds()
                    logger.info(f"Heartbeat: polled {poll_count} times, processed {processed_count} messages, {pools_tracked} pools tracked, last message {time_since_msg:.0f}s ago")
                else:
                    logger.info(f"Heartbeat: polled {poll_count} times, processed {processed_count} messages, {pools_tracked} pools tracked, waiting for messages...")
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition")
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    raise KafkaException(msg.error())
            
            try:
                dex_pool_block = dex_pool_block_message_pb2.DexPoolBlockMessage()
                dex_pool_block.ParseFromString(msg.value())
                last_message_time = datetime.now(timezone.utc)
                
                # Log when we receive messages (first message and every 10th)
                num_pool_events = len(dex_pool_block.PoolEvents) if dex_pool_block.PoolEvents else 0
                if processed_count == 0 or processed_count % 10 == 0:
                    logger.info(f"Received message #{processed_count + 1} with {num_pool_events} pool events")
                
                for pool_event in dex_pool_block.PoolEvents:
                    alerts = detector.process_pool_update(pool_event)
                    
                    for alert in alerts:
                        alert_count += 1
                        # Get pool state and history for detailed output
                        pool_id = alert.pool_id
                        state = detector.pool_states.get(pool_id)
                        history = detector.pool_histories.get(pool_id)
                        print(format_alert(alert, state=state, history=history))
                        logger.warning(f"ALERT #{alert_count}: {alert.alert_type} - {alert.message}")
                        
                        # Send to API if enabled
                        if enable_api:
                            send_alert_to_api(alert, api_url, logger)
                
                processed_count += 1
                
                if processed_count % 100 == 0:
                    logger.info(f"Processed {processed_count} messages, {alert_count} alerts generated")
                
            except DecodeError as err:
                logger.warning(f"Protobuf decoding error: {err}")
            except Exception as err:
                logger.error(f"Error processing message: {err}", exc_info=True)
                
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.exception(f"Error in main loop: {e}")
    finally:
        consumer.close()
        logger.info(f"Shutdown complete . Processed: {processed_count}, Alerts: {alert_count}")

if __name__ == "__main__":
    main()
