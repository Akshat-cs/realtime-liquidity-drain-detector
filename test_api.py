#!/usr/bin/env python3
"""
Quick test script to check if the API is receiving alerts
"""

import requests
import json
from datetime import datetime, timezone

# Test alert data
test_alert = {
    'pool_id': 'test-pool-123',
    'severity': 'critical',
    'alert_type': 'rapid_drain',
    'message': 'Test alert - Rapid liquidity drain detected',
    'timestamp': datetime.now(timezone.utc).isoformat(),
    'pool_info': {
        'pool_id': 'test-pool-123',
        'pool_address': '0x1234567890123456789012345678901234567890',
        'currency_pair': 'WETH/USDC',
        'dex': 'uniswap_v3',
        'transaction_hash': '0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890'
    },
    'metrics': {
        'current_amount_a': 100.0,
        'current_amount_b': 200.0,
        'baseline_amount_a': 150.0,
        'baseline_amount_b': 250.0
    }
}

def test_api():
    api_url = 'http://localhost:5001'
    
    print("Testing API connection...")
    print(f"API URL: {api_url}")
    
    # Test 1: Send alert
    print("\n1. Sending test alert...")
    try:
        response = requests.post(
            f'{api_url}/api/alerts',
            json=test_alert,
            timeout=5
        )
        if response.status_code == 200:
            print("✓ Alert sent successfully!")
            print(f"  Response: {response.json()}")
        else:
            print(f"✗ Failed to send alert. Status: {response.status_code}")
            print(f"  Response: {response.text}")
            return
    except requests.exceptions.ConnectionError:
        print("✗ Could not connect to API server!")
        print("  Make sure the API server is running: python api_server.py")
        return
    except Exception as e:
        print(f"✗ Error: {e}")
        return
    
    # Test 2: Get alerts
    print("\n2. Fetching alerts...")
    try:
        response = requests.get(f'{api_url}/api/alerts', timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Retrieved {data['total']} alert(s)")
            if data['alerts']:
                print(f"  First alert: {data['alerts'][0]['message']}")
            else:
                print("  No alerts found")
        else:
            print(f"✗ Failed to get alerts. Status: {response.status_code}")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    # Test 3: Get stats
    print("\n3. Fetching stats...")
    try:
        response = requests.get(f'{api_url}/api/alerts/stats', timeout=5)
        if response.status_code == 200:
            stats = response.json()
            print(f"✓ Stats retrieved:")
            print(f"  Total alerts: {stats['total_alerts']}")
            print(f"  By severity: {stats['by_severity']}")
        else:
            print(f"✗ Failed to get stats. Status: {response.status_code}")
    except Exception as e:
        print(f"✗ Error: {e}")

if __name__ == '__main__':
    test_api()
