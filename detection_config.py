"""
Simple configuration for liquidity drain detection.
No USD conversions - just relative percentage thresholds.
"""

class DetectionConfig:
    """Configurable thresholds for liquidity drain detection"""
    
    # ========================================================================
    # LIQUIDITY DROP THRESHOLDS
    # ========================================================================
    # Percentage drop from baseline that triggers alerts
    LIQUIDITY_DROP_WARNING = 20.0   # 20% drop = warning level alert
    LIQUIDITY_DROP_CRITICAL = 40.0  # 40% drop = critical level alert
    
    # ========================================================================
    # TIME WINDOWS
    # ========================================================================
    BASELINE_WINDOW_HOURS = 24           # Hours of data to build baseline
    LOOKBACK_WINDOW_MINUTES = 30         # Minutes to check for recovery (false positive filter)
    RAPID_DRAIN_WINDOW_MINUTES = 5       # Window for detecting sudden drains
    
    # ========================================================================
    # MAX AMOUNT IN DECREASE THRESHOLDS
    # ========================================================================
    # Percentage decrease in MaxAmountIn (max trade size) that triggers alerts
    MAX_AMOUNT_DECREASE_WARNING = 30.0   # 30% decrease = warning
    MAX_AMOUNT_DECREASE_CRITICAL = 50.0  # 50% decrease = critical
    
    # ========================================================================
    # FILTERING & ALERT MANAGEMENT
    # ========================================================================
    ALERT_COOLDOWN_MINUTES = 10          # Minutes between alerts for same pool
    RECOVERY_CHECK_ENABLED = True        # Check if liquidity recovers quickly (false positive filter)
    
    # ========================================================================
    # FALSE POSITIVE PREVENTION
    # ========================================================================
    DROP_CONFIRMATION_COUNT = 2          # Number of consecutive measurements showing drop required before alerting (prevents false positives from temporary fluctuations)
    ENABLE_MAX_AMOUNT_DECREASE_ALERTS = False  # Set to False to disable max_amount_decrease alerts (they cause many false positives)
    
    # ========================================================================
    # POOL SIZE FILTER
    # ========================================================================
    # Minimum liquidity (sum of token amounts after decimals) to monitor
    # This filters out very small pools to avoid noise
    MIN_LIQUIDITY_TOKENS = 1000          # Rough threshold (sum of both tokens after decimal conversion)
    
    # ========================================================================
    # BASELINE RELIABILITY
    # ========================================================================
    # Minimum number of measurements required before trusting baseline enough to alert
    # Prevents false positives from early baseline calculations
    MIN_MEASUREMENTS_FOR_BASELINE = 10   # Need at least 10 measurements before alerting
    MIN_TIME_FOR_BASELINE_MINUTES = 5    # Need at least 5 minutes of data before alerting
