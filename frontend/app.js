// API base URL - auto-detect for subpath deployment
(function() {
    // Auto-detect base path from current location
    const path = window.location.pathname;
    if (path.includes('/realtime-liquidity-drain-detector')) {
        window.APP_BASE_PATH = '/realtime-liquidity-drain-detector';
    } else {
        // Extract first path segment if present, otherwise use empty string
        const parts = path.split('/').filter(p => p);
        window.APP_BASE_PATH = parts.length > 0 ? '/' + parts[0] : '';
    }
})();

const API_BASE = window.APP_BASE_PATH || '';

// State
let currentFilters = {
    pool: '',
    type: '',
    severity: ''
};

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadConfig();
    loadAlerts();
    setupEventListeners();
    
    // Auto-refresh every 3 seconds
    setInterval(loadAlerts, 3000);
});

function setupEventListeners() {
    // Config button
    document.getElementById('configBtn').addEventListener('click', () => {
        openConfigModal();
    });

    // Close modal
    document.getElementById('closeModal').addEventListener('click', closeConfigModal);
    document.getElementById('cancelConfig').addEventListener('click', closeConfigModal);
    document.getElementById('saveConfig').addEventListener('click', saveConfig);

    // Filters
    document.getElementById('poolFilter').addEventListener('input', (e) => {
        currentFilters.pool = e.target.value;
        loadAlerts();
    });

    document.getElementById('typeFilter').addEventListener('change', (e) => {
        currentFilters.type = e.target.value;
        loadAlerts();
    });

    document.getElementById('severityFilter').addEventListener('change', (e) => {
        currentFilters.severity = e.target.value;
        loadAlerts();
    });

    document.getElementById('clearFiltersBtn').addEventListener('click', () => {
        currentFilters = { pool: '', type: '', severity: '' };
        document.getElementById('poolFilter').value = '';
        document.getElementById('typeFilter').value = '';
        document.getElementById('severityFilter').value = '';
        loadAlerts();
    });

    // Close modal on background click
    document.getElementById('configModal').addEventListener('click', (e) => {
        if (e.target.id === 'configModal') {
            closeConfigModal();
        }
    });
}

async function loadAlerts() {
    const container = document.getElementById('alertsContainer');
    const loading = document.getElementById('loading');
    
    if (!container) {
        console.error('alertsContainer element not found');
        return;
    }
    
    // Save scroll position before updating
    const scrollPosition = window.pageYOffset || document.documentElement.scrollTop;
    const containerScroll = container.scrollTop || window.pageYOffset;
    
    if (loading) {
        loading.style.display = 'block';
    }
    container.innerHTML = '';

    try {
        const params = new URLSearchParams();
        if (currentFilters.pool) params.append('pool', currentFilters.pool);
        if (currentFilters.type) params.append('type', currentFilters.type);
        if (currentFilters.severity) params.append('severity', currentFilters.severity);

        const url = `${API_BASE}/api/alerts?${params.toString()}`;
        
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();

        if (loading) {
            loading.style.display = 'none';
        }

        if (data.alerts && data.alerts.length > 0) {
            data.alerts.forEach((alert, index) => {
                try {
                    const card = createAlertCard(alert);
                    container.appendChild(card);
                } catch (err) {
                    console.error(`Error creating alert card ${index}:`, err, alert);
                }
            });
            // Refresh stats after loading alerts
            loadStats();
        } else {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>No alerts found</h3>
                    <p>${currentFilters.pool || currentFilters.type || currentFilters.severity 
                        ? 'Try adjusting your filters' 
                        : 'Alerts will appear here when detected'}</p>
                </div>
            `;
        }
        
        // Restore scroll position after DOM update
        // Use requestAnimationFrame to ensure DOM is fully rendered
        requestAnimationFrame(() => {
            window.scrollTo(0, scrollPosition);
            if (container.scrollTop !== undefined) {
                container.scrollTop = containerScroll;
            }
        });
    } catch (error) {
        if (loading) {
            loading.style.display = 'none';
        }
        console.error('Error loading alerts:', error);
        container.innerHTML = `
            <div class="empty-state">
                <h3>Error loading alerts</h3>
                <p>${error.message}</p>
            </div>
        `;
        // Restore scroll even on error
        requestAnimationFrame(() => {
            window.scrollTo(0, scrollPosition);
        });
    }
}

async function loadStats() {
    try {
        const response = await fetch(`${API_BASE}/api/alerts/stats`);
        const data = await response.json();

        document.getElementById('totalAlerts').textContent = data.total_alerts || 0;
        document.getElementById('criticalAlerts').textContent = data.by_severity?.critical || 0;
        document.getElementById('warningAlerts').textContent = data.by_severity?.warning || 0;
        document.getElementById('uniquePools').textContent = data.unique_pools || 0;
    } catch (error) {
        console.error('Error loading stats:', error);
    }
}

function createAlertCard(alert) {
    const card = document.createElement('div');
    card.className = `alert-card ${alert.severity}`;

    const poolInfo = alert.pool_info || {};
    const timestamp = new Date(alert.timestamp).toLocaleString();
    
    // Determine pool identifier for display
    const poolIdentifier = poolInfo.pool_id || poolInfo.pool_address || 'N/A';
    const isUniswapV4 = poolInfo.dex === 'uniswap_v4' && poolInfo.pool_id && poolInfo.pool_id.startsWith('0x');
    // For non-V4 pools, pool_id is a composite key (pool_address + currency_pair), so don't show it separately
    const showPoolId = isUniswapV4;
    
    card.innerHTML = `
        <div class="alert-header">
            <div class="alert-title">
                <span class="alert-badge ${alert.severity}">${alert.severity}</span>
                <span class="alert-type">${formatAlertType(alert.alert_type)}</span>
            </div>
            <div class="alert-time">${timestamp}</div>
        </div>
        <div class="alert-body">
            <div class="alert-message">${alert.message}</div>
            <div class="alert-info">
                ${showPoolId ? `
                <div class="info-item">
                    <span class="info-label">Pool ID</span>
                    <span class="info-value">${createCopyableAddress(poolIdentifier)}</span>
                </div>
                ` : ''}
                <div class="info-item">
                    <span class="info-label">Pool Address</span>
                    <span class="info-value">${createCopyableAddress(poolInfo.pool_address)}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Currency Pair</span>
                    <span class="info-value">${poolInfo.currency_pair || 'N/A'}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">DEX Protocol</span>
                    <span class="info-value">${formatDexName(poolInfo.dex || 'N/A')}</span>
                </div>
                ${poolInfo.transaction_hash ? `
                <div class="info-item">
                    <span class="info-label">Transaction</span>
                    <span class="info-value">
                        <span class="copyable-address" data-address="${poolInfo.transaction_hash}">
                            <a href="https://etherscan.io/tx/${poolInfo.transaction_hash}" 
                               target="_blank" 
                               class="transaction-link"
                               onclick="event.stopPropagation();">
                                <span class="address-text">${truncateAddress(poolInfo.transaction_hash)}</span>
                            </a>
                            <span class="copy-icon" title="Copy to clipboard">📋</span>
                        </span>
                    </span>
                </div>
                ` : ''}
            </div>
            ${alert.metrics ? createMetricsSection(alert.metrics, alert.alert_type) : ''}
        </div>
    `;

    // Attach copy handlers to copyable addresses (click anywhere to copy, or click icon)
    card.querySelectorAll('.copyable-address').forEach(el => {
        const fullAddress = el.getAttribute('data-address');
        const icon = el.querySelector('.copy-icon');
        const hasLink = el.querySelector('.transaction-link');
        
        // For transaction links, only the icon is clickable for copy
        // For regular addresses, the whole element is clickable
        const clickTarget = hasLink ? icon : el;
        
        if (clickTarget) {
            clickTarget.addEventListener('click', async (e) => {
                if (hasLink && e.target.closest('.transaction-link')) {
                    // Let the link handle the click
                    return;
                }
                e.preventDefault();
                e.stopPropagation();
                const success = await copyToClipboard(fullAddress);
                if (success && icon) {
                    const originalText = icon.textContent;
                    icon.textContent = '✓';
                    icon.classList.add('copied');
                    setTimeout(() => {
                        icon.textContent = originalText;
                        icon.classList.remove('copied');
                    }, 2000);
                }
            });
        }
    });

    return card;
}

function createMetricsSection(metrics, alertType) {
    let html = '<div class="alert-metrics"><div class="metrics-title">Metrics</div>';

    if (alertType === 'liquidity_drop') {
        html += `
            <div class="metric-item">
                <span class="metric-label">Currency</span>
                <span class="metric-value">${metrics.currency || 'N/A'}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Current Amount</span>
                <span class="metric-value">${formatNumber(metrics.current_amount)}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Baseline Amount</span>
                <span class="metric-value">${formatNumber(metrics.baseline_amount)}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Drop Percentage</span>
                <span class="metric-value">${formatNumber(metrics.drop_percent)}%</span>
            </div>
        `;
    } else if (alertType === 'max_amount_decrease') {
        html += `
            <div class="metric-item">
                <span class="metric-label">Direction</span>
                <span class="metric-value">${metrics.direction || 'N/A'}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Current Max Amount</span>
                <span class="metric-value">${formatNumber(metrics.current_max_amount)}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Baseline Max Amount</span>
                <span class="metric-value">${formatNumber(metrics.baseline_max_amount_a_to_b || metrics.baseline_max_amount_b_to_a)}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Decrease Percentage</span>
                <span class="metric-value">${formatNumber(metrics.max_amount_decrease_percent)}%</span>
            </div>
        `;
    } else if (alertType === 'rapid_drain') {
        html += `
            <div class="metric-item">
                <span class="metric-label">Current Amount A</span>
                <span class="metric-value">${formatNumber(metrics.current_amount_a)}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Current Amount B</span>
                <span class="metric-value">${formatNumber(metrics.current_amount_b)}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Baseline Amount A</span>
                <span class="metric-value">${formatNumber(metrics.baseline_amount_a)}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Baseline Amount B</span>
                <span class="metric-value">${formatNumber(metrics.baseline_amount_b)}</span>
            </div>
        `;
    }

    html += '</div>';
    return html;
}

function formatAlertType(type) {
    const types = {
        'liquidity_drop': 'Liquidity Drop',
        'max_amount_decrease': 'Max Amount Decrease',
        'rapid_drain': 'Rapid Drain'
    };
    return types[type] || type;
}

function formatDexName(dex) {
    const names = {
        'uniswap_v4': 'Uniswap V4',
        'uniswap_v3': 'Uniswap V3',
        'uniswap_v2': 'Uniswap V2'
    };
    return names[dex.toLowerCase()] || dex;
}

function formatNumber(num) {
    if (num === null || num === undefined) return 'N/A';
    if (typeof num === 'number') {
        if (num > 1000000) {
            return (num / 1000000).toFixed(2) + 'M';
        } else if (num > 1000) {
            return (num / 1000).toFixed(2) + 'K';
        }
        return num.toFixed(6);
    }
    return num.toString();
}

function truncateAddress(address) {
    if (!address || address === 'N/A' || address.length <= 8) {
        return address;
    }
    return address.slice(0, 4) + '...' + address.slice(-4);
}

async function copyToClipboard(text) {
    try {
        await navigator.clipboard.writeText(text);
        return true;
    } catch (err) {
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.opacity = '0';
        document.body.appendChild(textArea);
        textArea.select();
        try {
            document.execCommand('copy');
            document.body.removeChild(textArea);
            return true;
        } catch (e) {
            document.body.removeChild(textArea);
            return false;
        }
    }
}

function createCopyableAddress(fullAddress, label = '') {
    if (!fullAddress || fullAddress === 'N/A') {
        return fullAddress;
    }
    
    const truncated = truncateAddress(fullAddress);
    const id = 'addr-' + Math.random().toString(36).substr(2, 9);
    
    return `
        <span class="copyable-address" data-address="${fullAddress}" id="${id}">
            <span class="address-text">${truncated}</span>
            <span class="copy-icon" title="Copy to clipboard">📋</span>
        </span>
    `;
}

async function loadConfig() {
    try {
        const response = await fetch(`${API_BASE}/api/config`);
        const config = await response.json();

        document.getElementById('liquidityDropWarning').value = config.liquidity_drop_warning || 20.0;
        document.getElementById('liquidityDropCritical').value = config.liquidity_drop_critical || 40.0;
        document.getElementById('maxAmountWarning').value = config.max_amount_decrease_warning || 30.0;
        document.getElementById('maxAmountCritical').value = config.max_amount_decrease_critical || 50.0;
        document.getElementById('rapidDrainThreshold').value = config.rapid_drain_threshold || 20.0;
    } catch (error) {
        console.error('Error loading config:', error);
    }
}

function openConfigModal() {
    loadConfig();
    document.getElementById('configModal').classList.add('active');
}

function closeConfigModal() {
    document.getElementById('configModal').classList.remove('active');
}

async function saveConfig() {
    const config = {
        liquidity_drop_warning: parseFloat(document.getElementById('liquidityDropWarning').value),
        liquidity_drop_critical: parseFloat(document.getElementById('liquidityDropCritical').value),
        max_amount_decrease_warning: parseFloat(document.getElementById('maxAmountWarning').value),
        max_amount_decrease_critical: parseFloat(document.getElementById('maxAmountCritical').value),
        rapid_drain_threshold: parseFloat(document.getElementById('rapidDrainThreshold').value)
    };

    try {
        const response = await fetch(`${API_BASE}/api/config`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(config)
        });

        if (response.ok) {
            closeConfigModal();
            alert('Configuration saved successfully!');
        } else {
            const error = await response.json();
            alert(`Error: ${error.error || 'Failed to save configuration'}`);
        }
    } catch (error) {
        alert(`Error: ${error.message}`);
        console.error('Error saving config:', error);
    }
}

// Load stats on page load
loadStats();
