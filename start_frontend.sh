#!/bin/bash
# Start the API server for the liquidity drain detection frontend

echo "Starting Liquidity Drain Detection Frontend..."
echo "API server will be available at http://localhost:5000"
echo ""
echo "Make sure the detector is running with API integration enabled:"
echo "  ENABLE_API=true python liquidity_drain_detector.py"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

python api_server.py
