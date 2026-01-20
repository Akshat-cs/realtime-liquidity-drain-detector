# Vercel entrypoint for Flask app
# Vercel expects top-level variable named 'app', NOT 'handler'

import sys
import os

# Setup paths
api_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(api_dir)

# Add to Python path
sys.path.insert(0, project_root)

# Change to project root
try:
    os.chdir(project_root)
except:
    pass

# Mark as Vercel environment
os.environ['VERCEL'] = '1'

# Import Flask app - Vercel expects 'app' variable at top level
from api_server import app

# Do NOT export as 'handler' - Vercel looks for 'app' directly
