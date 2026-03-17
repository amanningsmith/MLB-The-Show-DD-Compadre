"""
Configuration Management
"""

import os
from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
BACKUP_DIR = DATA_DIR / 'backups'
LOGS_DIR = BASE_DIR / 'logs'
STATIC_DIR = BASE_DIR / 'static'
TEMPLATES_DIR = BASE_DIR / 'templates'

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
BACKUP_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Data files
MISSIONS_DB = DATA_DIR / 'missions.db'
CARDS_DB = DATA_DIR / 'cards.db'
LOCAL_API_CONFIG_PATH = DATA_DIR / 'local_api_config.json'

# API Configuration
MLB_API_BASE_URL = 'https://mlb26.theshow.com'
API_DELAY = 0.5  # Delay between API calls in seconds

# Optional authenticated inventory sync settings (Beta 3.0)
MLB_API_INVENTORY_ENDPOINT = os.getenv('MLB_API_INVENTORY_ENDPOINT', '/apis/profile/inventory.json')
MLB_API_AUTH_TOKEN = os.getenv('MLB_API_AUTH_TOKEN', '').strip()
MLB_API_AUTH_HEADER = os.getenv('MLB_API_AUTH_HEADER', 'Authorization').strip() or 'Authorization'
MLB_API_AUTH_PREFIX = os.getenv('MLB_API_AUTH_PREFIX', 'Bearer ').strip()

# Backup settings
MAX_BACKUPS = 10  # Keep last 10 backups per file type

# Flask Configuration
SECRET_KEY = 'mlb-the-show-26-dd-tracker-secret-key'
DEBUG = True

# Pagination
CARDS_PER_PAGE = 20
