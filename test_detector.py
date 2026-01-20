#!/usr/bin/env python3
"""
Quick test script for liquidity drain detector
Tests with sample data structure without requiring Kafka
"""

import json
from datetime import datetime, timezone
from liquidity_drain_detector import LiquidityDrainDetector, PoolState

def create_mock_pool_event():
    """Create a mock pool event based on sample_data structure"""
    class MockCurrency:
        def __init__(self, address, symbol, decimals):
            self.SmartContract = bytes.fromhex(address.replace('0x', ''))
            self.Symbol = symbol
            self.Decimals = decimals
    
    class MockLiquidity:
        def __init__(self, amount_a, amount_b):
            self.AmountCurrencyA = amount_a
            self.AmountCurrencyB = amount_b
    
    class MockPriceInfo:
        def __init__(self, bps, max_in, min_out, price):
            self.SlippageBasisPoints = bps
            self.MaxAmountIn = max_in
            self.MinAmountOut = min_out
            self.Price = price
    
    class MockPriceTable:
        def __init__(self):
            # A->B prices (USDC -> USDT)
            self.AtoBPrices = [
                MockPriceInfo(10, 3308994052208, 3298540775778, 0.9968489408493042),
                MockPriceInfo(50, 3822070896088, 3791392015233, 0.9919811487197876),
                MockPriceInfo(100, 3829692933970, 3774341858482, 0.9855547547340393),
                MockPriceInfo(200, 3849129130571, 3736273942164, 0.9706881046295166),
                MockPriceInfo(500, 3887239319982, 3641470057817, 0.9367828369140625),
                MockPriceInfo(1000, 3925349509394, 3508127382522, 0.8937180042266846),
            ]
            # B->A prices (USDT -> USDC)
            self.BtoAPrices = [
                MockPriceInfo(10, 3085892575047, 3074063801539, 0.9961748123168945),
                MockPriceInfo(50, 3832912030758, 3799555487647, 0.9913052916526794),
                MockPriceInfo(100, 3850168607938, 3791933510741, 0.9848825335502625),  # WORSE: 1.51% slippage
                MockPriceInfo(200, 3888715873170, 3772878568476, 0.9702196717262268),
                MockPriceInfo(500, 3984574634540, 3734768683946, 0.9373142719268799),
                MockPriceInfo(1000, 4118279905096, 3696658799416, 0.8976292014122009),
            ]
            self.AtoBPrice = 0.999634861946106
            self.BtoAPrice = 1.0003652572631836
    
    class MockPool:
        def __init__(self):
            self.PoolId = bytes.fromhex('395f91b34aa34a477ce3bc6505639a821b286a62b1a164fc1887fa3a5ef713a5'.replace('0x', ''))
            self.CurrencyA = MockCurrency('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 'USDC', 6)
            self.CurrencyB = MockCurrency('0xdac17f958d2ee523a2206206994597c13d831ec7', 'USDT', 6)
    
    class MockDex:
        def __init__(self):
            self.ProtocolName = 'uniswap_v4'
    
    class MockEvent:
        def __init__(self):
            self.Pool = MockPool()
            self.Liquidity = MockLiquidity(57164826795117, 70341152498093)
            self.PoolPriceTable = MockPriceTable()
            self.Dex = MockDex()
    
    return MockEvent()

def test_detector():
    """Test the detector with mock data"""
    print("=" * 80)
    print("🧪 TESTING LIQUIDITY DRAIN DETECTOR")
    print("=" * 80)
    print()
    
    # Initialize detector
    detector = LiquidityDrainDetector()
    print("✅ Detector initialized")
    
    # Create mock event
    mock_event = create_mock_pool_event()
    print("✅ Mock pool event created (USDC/USDT pool)")
    print(f"   - Pool ID: 0x{mock_event.Pool.PoolId.hex()[:20]}...")
    print(f"   - Currency A: {mock_event.Pool.CurrencyA.Symbol} ({mock_event.Pool.CurrencyA.Decimals} decimals)")
    print(f"   - Currency B: {mock_event.Pool.CurrencyB.Symbol} ({mock_event.Pool.CurrencyB.Decimals} decimals)")
    print(f"   - Liquidity: {mock_event.Liquidity.AmountCurrencyA:,} {mock_event.Pool.CurrencyA.Symbol} / {mock_event.Liquidity.AmountCurrencyB:,} {mock_event.Pool.CurrencyB.Symbol}")
    print()
    
    # Process first event (baseline)
    print("📊 Processing initial pool state (baseline)...")
    alerts1 = detector.process_pool_update(mock_event)
    print(f"   Alerts: {len(alerts1)}")
    
    if alerts1:
        print("   ⚠️  Initial alerts (may be pool imbalance checks):")
        for alert in alerts1:
            print(f"      - {alert.alert_type}: {alert.message[:60]}...")
    
    # Check pool state
    pool_id = f"0x{mock_event.Pool.PoolId.hex()}"
    if pool_id in detector.pool_states:
        state = detector.pool_states[pool_id]
        print(f"   ✅ Pool state created")
        print(f"   - TVL: ${state.total_liquidity_usd():,.2f}")
        
        # Check slippage data for BOTH directions
        if 100 in state.slippage_at_bps_a_to_b:
            data_a = state.slippage_at_bps_a_to_b[100]
            print(f"   - A->B (100bp): {data_a.get('actual_slippage_pct', 0):.2f}% slippage, ${data_a.get('max_amount_in_usd', 0):,.0f} max")
        
        if 100 in state.slippage_at_bps_b_to_a:
            data_b = state.slippage_at_bps_b_to_a[100]
            print(f"   - B->A (100bp): {data_b.get('actual_slippage_pct', 0):.2f}% slippage, ${data_b.get('max_amount_in_usd', 0):,.0f} max ⚠️ WORSE")
        else:
            print(f"   - B->A (100bp): ❌ NOT PROCESSED!")
    
    print()
    
    # Process multiple events with slight variations to build baseline
    print("📈 Processing 25 events to build baseline (simulating time passing)...")
    import time
    from datetime import timedelta
    
    # Create multiple events with slight variations
    base_amount_a = mock_event.Liquidity.AmountCurrencyA
    base_amount_b = mock_event.Liquidity.AmountCurrencyB
    
    for i in range(25):
        # Add small random variations to simulate real-world fluctuations
        import random
        variation = 1.0 + (random.random() - 0.5) * 0.05  # ±2.5% variation
        mock_event.Liquidity.AmountCurrencyA = int(base_amount_a * variation)
        mock_event.Liquidity.AmountCurrencyB = int(base_amount_b * variation)
        detector.process_pool_update(mock_event)
    
    print("   ✅ Baseline established")
    
    # Check baseline
    if pool_id in detector.pool_histories:
        history = detector.pool_histories[pool_id]
        print(f"   - Baseline liquidity: ${history.baseline_liquidity:,.2f}" if history.baseline_liquidity else "   - Baseline liquidity: None")
        print(f"   - Baseline slippage (100bp): {history.baseline_actual_slippage_100bp:.2f}%" if history.baseline_actual_slippage_100bp else "   - Baseline slippage: None")
        print(f"   - Baseline MaxAmountIn: ${history.baseline_max_amount_in_usd_100bp:,.0f}" if history.baseline_max_amount_in_usd_100bp else "   - Baseline MaxAmountIn: None")
    
    print()
    
    # Simulate a drain - reduce liquidity by 50%
    print("🚨 SIMULATING LIQUIDITY DRAIN (50% reduction)...")
    mock_event.Liquidity.AmountCurrencyA = int(base_amount_a * 0.5)
    mock_event.Liquidity.AmountCurrencyB = int(base_amount_b * 0.5)
    
    # Also worsen slippage significantly (simulate drain)
    # Make prices much worse = higher slippage
    mock_event.PoolPriceTable.AtoBPrices[2].Price = 0.95  # 5% slippage (was 1.44%)
    mock_event.PoolPriceTable.BtoAPrices[2].Price = 0.94  # 6% slippage (was 1.51%) - WORSE
    # Reduce MaxAmountIn significantly
    mock_event.PoolPriceTable.AtoBPrices[2].MaxAmountIn = int(3829692933970 * 0.3)  # 70% drop
    mock_event.PoolPriceTable.BtoAPrices[2].MaxAmountIn = int(3850168607938 * 0.25)  # 75% drop - WORSE
    
    alerts2 = detector.process_pool_update(mock_event)
    
    print(f"   ✅ Processed drain event")
    
    # Check current state after drain
    if pool_id in detector.pool_states:
        state = detector.pool_states[pool_id]
        print(f"   - Current TVL: ${state.total_liquidity_usd():,.2f}")
        
        if 100 in state.slippage_at_bps_a_to_b:
            data_a = state.slippage_at_bps_a_to_b[100]
            print(f"   - A->B (100bp): {data_a.get('actual_slippage_pct', 0):.2f}% slippage, ${data_a.get('max_amount_in_usd', 0):,.0f} max")
        
        if 100 in state.slippage_at_bps_b_to_a:
            data_b = state.slippage_at_bps_b_to_a[100]
            print(f"   - B->A (100bp): {data_b.get('actual_slippage_pct', 0):.2f}% slippage, ${data_b.get('max_amount_in_usd', 0):,.0f} max ⚠️ WORSE")
    
    print(f"   Alerts generated: {len(alerts2)}")
    print()
    
    if alerts2:
        print("🚨 ALERTS GENERATED:")
        print()
        for i, alert in enumerate(alerts2, 1):
            print(f"Alert #{i}:")
            print(f"  Type: {alert.alert_type}")
            print(f"  Severity: {alert.severity.upper()}")
            print(f"  Message: {alert.message}")
            if 'a_to_b' in alert.metrics or 'b_to_a' in alert.metrics:
                print(f"  Both Directions:")
                if 'a_to_b_slippage_pct' in alert.metrics:
                    print(f"    A->B: {alert.metrics.get('a_to_b_slippage_pct')}% slippage, ${alert.metrics.get('a_to_b_max_amount_usd', 0):,.0f} max")
                if 'b_to_a_slippage_pct' in alert.metrics:
                    print(f"    B->A: {alert.metrics.get('b_to_a_slippage_pct')}% slippage, ${alert.metrics.get('b_to_a_max_amount_usd', 0):,.0f} max ⚠️")
            print()
    else:
        print("⚠️  No alerts generated")
        # Debug: check why
        if pool_id in detector.pool_histories:
            history = detector.pool_histories[pool_id]
            current_tvl = detector.pool_states[pool_id].total_liquidity_usd()
            drop_pct = history.liquidity_drop_percent(current_tvl)
            print(f"   Debug:")
            print(f"     - Current TVL: ${current_tvl:,.2f}")
            print(f"     - Baseline TVL: ${history.baseline_liquidity:,.2f}")
            print(f"     - Drop %: {drop_pct:.1f}%" if drop_pct else "     - Drop %: None")
            print(f"     - Can alert: {history.can_alert(datetime.now(timezone.utc))}")
            print(f"     - Last alert: {history.last_alert_time}")
    
    print("=" * 80)
    print("✅ TEST COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    try:
        test_detector()
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

