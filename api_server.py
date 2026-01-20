"""
Flask API server for liquidity drain alerts frontend
Provides endpoints for viewing alerts, filtering by pool, and configuring thresholds
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
import json
import threading
from collections import deque
import os

# Application root path for subpath deployment
# On Vercel, app is always mounted at /, so use empty string
# For local subpath deployment, use the subpath
APPLICATION_ROOT = '' if os.environ.get('VERCEL') == '1' else os.environ.get('SCRIPT_NAME', '/realtime-liquidity-drain-detector')

# Determine base directory - handle both local and Vercel deployments
# When imported from api/index.py on Vercel, cwd should be project root
# When run locally, __file__ is api_server.py in project root
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Get current working directory (may be different on Vercel)
CWD = os.getcwd()

# Try to find frontend directory
# First, try relative to this file
FRONTEND_DIR = os.path.join(BASE_DIR, 'frontend')

# If not found, try relative to current working directory
if not os.path.exists(FRONTEND_DIR):
    FRONTEND_DIR = os.path.join(CWD, 'frontend')

# If still not found, try parent of BASE_DIR (in case BASE_DIR is in a subdirectory)
if not os.path.exists(FRONTEND_DIR):
    parent_dir = os.path.dirname(BASE_DIR)
    FRONTEND_DIR = os.path.join(parent_dir, 'frontend')

# Don't create directory in production - it should exist
# If it doesn't exist, the routes will handle the error gracefully
if not os.path.exists(FRONTEND_DIR) and os.environ.get('VERCEL') != '1':
    try:
        os.makedirs(FRONTEND_DIR, exist_ok=True)
    except:
        pass  # Silently fail if we can't create

# Create Flask app instance
# Simplified - we're manually serving static files via routes, so we don't need static_folder
app = Flask(__name__)
CORS(app)

# Configure application root for subpath deployment
app.config['APPLICATION_ROOT'] = APPLICATION_ROOT

# In-memory storage for alerts (max 1000 alerts, or 24 hours, whichever is larger)
alerts_storage: deque = deque(maxlen=1000)
alerts_lock = threading.Lock()
ALERT_RETENTION_HOURS = 24  # Keep alerts for 24 hours

# Configuration storage
config_data = {
    'liquidity_drop_warning': 20.0,
    'liquidity_drop_critical': 40.0,
    'max_amount_decrease_warning': 30.0,
    'max_amount_decrease_critical': 50.0,
    'rapid_drain_threshold': 20.0,
}

def alert_to_dict(alert_dict: Dict) -> Dict:
    """Convert alert dict to JSON-serializable format"""
    result = alert_dict.copy()
    
    # Convert datetime to ISO string
    if 'timestamp' in result and isinstance(result['timestamp'], datetime):
        result['timestamp'] = result['timestamp'].isoformat()
    elif 'timestamp' in result and isinstance(result['timestamp'], str):
        # Already a string, keep it
        pass
    
    return result

def get_base_path():
    """Get the base path from request or use default."""
    try:
        # Try to get from SCRIPT_NAME (set by reverse proxy)
        script_name = request.environ.get('SCRIPT_NAME', '')
        if script_name:
            return script_name
        # Try to get from HTTP_X_SCRIPT_NAME (some proxies use this)
        http_script_name = request.environ.get('HTTP_X_SCRIPT_NAME', '')
        if http_script_name:
            return http_script_name
        # Try to detect from request path
        path = request.path
        if path.startswith('/realtime-liquidity-drain-detector'):
            return '/realtime-liquidity-drain-detector'
    except:
        pass
    # Fallback to configured APPLICATION_ROOT
    return APPLICATION_ROOT

@app.route(f'{APPLICATION_ROOT}/')
@app.route('/')
def index():
    """Serve the frontend"""
    try:
        return send_from_directory(FRONTEND_DIR, 'index.html')
    except Exception as e:
        return f"Error loading index.html: {str(e)}", 500

@app.route(f'{APPLICATION_ROOT}/styles.css')
@app.route('/styles.css')
def serve_css():
    """Serve CSS file"""
    try:
        return send_from_directory(FRONTEND_DIR, 'styles.css', mimetype='text/css')
    except Exception as e:
        return f"Error loading styles.css: {str(e)}", 500

@app.route(f'{APPLICATION_ROOT}/app.js')
@app.route('/app.js')
def serve_js():
    """Serve JavaScript file"""
    try:
        return send_from_directory(FRONTEND_DIR, 'app.js', mimetype='application/javascript')
    except Exception as e:
        return f"Error loading app.js: {str(e)}", 500

@app.route(f'{APPLICATION_ROOT}/api/alerts', methods=['GET'])
@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    """Get all alerts, optionally filtered by pool address/PoolId"""
    pool_filter = request.args.get('pool', '').strip().lower()
    alert_type_filter = request.args.get('type', '').strip().lower()
    severity_filter = request.args.get('severity', '').strip().lower()
    limit = request.args.get('limit', type=int, default=100)
    
    with alerts_lock:
        alerts_list = list(alerts_storage)
    
    # Filter out alerts older than retention period
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=ALERT_RETENTION_HOURS)
    filtered_by_time = []
    for alert in alerts_list:
        alert_time = None
        if isinstance(alert.get('timestamp'), datetime):
            alert_time = alert['timestamp']
        elif isinstance(alert.get('timestamp'), str):
            try:
                alert_time = datetime.fromisoformat(alert['timestamp'].replace('Z', '+00:00'))
            except:
                pass  # Keep if we can't parse timestamp
        
        if alert_time is None or alert_time >= cutoff_time:
            filtered_by_time.append(alert)
    
    alerts_list = filtered_by_time
    
    # Apply filters
    filtered = []
    for alert in alerts_list:
        # Pool filter: check pool_address or pool_id (case-insensitive)
        if pool_filter:
            pool_address = alert.get('pool_info', {}).get('pool_address', '').lower()
            pool_id = alert.get('pool_info', {}).get('pool_id', '').lower()
            if pool_filter not in pool_address and pool_filter not in pool_id:
                continue
        
        # Alert type filter
        if alert_type_filter and alert.get('alert_type', '').lower() != alert_type_filter:
            continue
        
        # Severity filter
        if severity_filter and alert.get('severity', '').lower() != severity_filter:
            continue
        
        filtered.append(alert_to_dict(alert))
    
    # Sort by timestamp (newest first) and limit
    filtered.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
    filtered = filtered[:limit]
    
    return jsonify({
        'alerts': filtered,
        'total': len(filtered),
        'total_all': len(alerts_list)
    })

@app.route(f'{APPLICATION_ROOT}/api/alerts/stats', methods=['GET'])
@app.route('/api/alerts/stats', methods=['GET'])
def get_stats():
    """Get statistics about alerts"""
    with alerts_lock:
        alerts_list = list(alerts_storage)
    
    stats = {
        'total_alerts': len(alerts_list),
        'by_severity': {},
        'by_type': {},
        'by_dex': {},
        'unique_pools': set(),
    }
    
    for alert in alerts_list:
        severity = alert.get('severity', 'unknown')
        alert_type = alert.get('alert_type', 'unknown')
        dex = alert.get('pool_info', {}).get('dex', 'unknown')
        pool_id = alert.get('pool_info', {}).get('pool_id', '')
        
        stats['by_severity'][severity] = stats['by_severity'].get(severity, 0) + 1
        stats['by_type'][alert_type] = stats['by_type'].get(alert_type, 0) + 1
        stats['by_dex'][dex] = stats['by_dex'].get(dex, 0) + 1
        if pool_id:
            stats['unique_pools'].add(pool_id)
    
    stats['unique_pools'] = len(stats['unique_pools'])
    stats['by_severity'] = dict(stats['by_severity'])
    stats['by_type'] = dict(stats['by_type'])
    stats['by_dex'] = dict(stats['by_dex'])
    
    return jsonify(stats)

@app.route(f'{APPLICATION_ROOT}/api/config', methods=['GET'])
@app.route('/api/config', methods=['GET'])
def get_config():
    """Get current configuration"""
    return jsonify(config_data)

@app.route(f'{APPLICATION_ROOT}/api/config', methods=['POST'])
@app.route('/api/config', methods=['POST'])
def update_config():
    """Update configuration"""
    global config_data
    
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    # Validate and update config
    allowed_keys = {
        'liquidity_drop_warning', 'liquidity_drop_critical',
        'max_amount_decrease_warning', 'max_amount_decrease_critical',
        'rapid_drain_threshold'
    }
    
    for key, value in data.items():
        if key in allowed_keys:
            try:
                float_value = float(value)
                if float_value < 0 or float_value > 100:
                    return jsonify({'error': f'{key} must be between 0 and 100'}), 400
                config_data[key] = float_value
            except (ValueError, TypeError):
                return jsonify({'error': f'{key} must be a number'}), 400
    
    # Update the actual DetectionConfig if the module is imported
    try:
        import detection_config
        if hasattr(detection_config, 'DetectionConfig'):
            DetectionConfig = detection_config.DetectionConfig
            if 'liquidity_drop_warning' in config_data:
                DetectionConfig.LIQUIDITY_DROP_WARNING = config_data['liquidity_drop_warning']
            if 'liquidity_drop_critical' in config_data:
                DetectionConfig.LIQUIDITY_DROP_CRITICAL = config_data['liquidity_drop_critical']
            if 'max_amount_decrease_warning' in config_data:
                DetectionConfig.MAX_AMOUNT_DECREASE_WARNING = config_data['max_amount_decrease_warning']
            if 'max_amount_decrease_critical' in config_data:
                DetectionConfig.MAX_AMOUNT_DECREASE_CRITICAL = config_data['max_amount_decrease_critical']
    except (ImportError, AttributeError, Exception):
        pass  # Config module not available or not needed, that's okay
    
    return jsonify({'success': True, 'config': config_data})

@app.route(f'{APPLICATION_ROOT}/api/alerts', methods=['POST'])
@app.route('/api/alerts', methods=['POST'])
def add_alert():
    """Add a new alert (called by the detector)"""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    # Validate required fields
    required_fields = ['pool_id', 'severity', 'alert_type', 'message', 'timestamp', 'pool_info']
    for field in required_fields:
        if field not in data:
            return jsonify({'error': f'Missing required field: {field}'}), 400
    
    # Convert timestamp string to datetime if needed
    if isinstance(data['timestamp'], str):
        try:
            data['timestamp'] = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
        except:
            pass
    
    with alerts_lock:
        alerts_storage.append(data)
    
    return jsonify({'success': True, 'alert_id': data.get('pool_id', 'unknown')})

@app.route(f'{APPLICATION_ROOT}/api/pools', methods=['GET'])
@app.route('/api/pools', methods=['GET'])
def get_pools():
    """Get list of unique pools"""
    with alerts_lock:
        alerts_list = list(alerts_storage)
    
    pools = {}
    for alert in alerts_list:
        pool_info = alert.get('pool_info', {})
        pool_id = pool_info.get('pool_id', '')
        pool_address = pool_info.get('pool_address', '')
        currency_pair = pool_info.get('currency_pair', '')
        dex = pool_info.get('dex', '')
        
        if pool_id:
            # For Uniswap V4, pool_id is the PoolId
            # For others, it's the composite key
            if pool_id not in pools:
                pools[pool_id] = {
                    'pool_id': pool_id,
                    'pool_address': pool_address,
                    'currency_pair': currency_pair,
                    'dex': dex,
                    'alert_count': 0
                }
            pools[pool_id]['alert_count'] += 1
    
    return jsonify({
        'pools': list(pools.values()),
        'total': len(pools)
    })

# Note: For Vercel, the handler is exported in api/index.py
# Don't export handler here to avoid conflicts
# handler = app  # Only export if running directly, not when imported

if __name__ == '__main__':
    # Create frontend directory if it doesn't exist
    os.makedirs('frontend', exist_ok=True)
    
    print("Starting API server on http://localhost:5001")
    print("Frontend will be available at http://localhost:5001")
    app.run(host='0.0.0.0', port=5001, debug=True, threaded=True)
