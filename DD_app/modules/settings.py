"""
Runtime-local settings helpers.
Stores optional API auth configuration in a local untracked JSON file.
"""

import json
from pathlib import Path

from config import (
    LOCAL_API_CONFIG_PATH,
    MLB_API_INVENTORY_ENDPOINT,
    MLB_API_AUTH_TOKEN,
    MLB_API_AUTH_HEADER,
    MLB_API_AUTH_PREFIX,
)
from modules.logger import logger

_ALLOWED_KEYS = {
    'inventory_endpoint',
    'auth_header',
    'auth_prefix',
    'auth_token',
}


def _default_auth_settings():
    return {
        'inventory_endpoint': MLB_API_INVENTORY_ENDPOINT,
        'auth_header': MLB_API_AUTH_HEADER,
        'auth_prefix': MLB_API_AUTH_PREFIX,
        'auth_token': MLB_API_AUTH_TOKEN,
    }


def _normalize_endpoint(value):
    endpoint = (value or '').strip()
    if not endpoint:
        endpoint = '/apis/profile/inventory.json'
    if not endpoint.startswith('/'):
        endpoint = f'/{endpoint}'
    return endpoint


def _normalize_header(value):
    return (value or '').strip() or 'Authorization'


def _normalize_prefix(value):
    return value or 'Bearer '


def _normalize_token(value):
    return (value or '').strip()


def load_local_api_auth_settings():
    path = Path(LOCAL_API_CONFIG_PATH)
    if not path.exists():
        return {}

    try:
        with path.open('r', encoding='utf-8') as file:
            data = json.load(file)
            if isinstance(data, dict):
                return {k: data.get(k) for k in _ALLOWED_KEYS if k in data}
    except Exception as exc:
        logger.warning(f'Failed to read local API auth settings: {exc}')

    return {}


def get_effective_api_auth_settings():
    settings = _default_auth_settings()
    local = load_local_api_auth_settings()

    if local.get('inventory_endpoint'):
        settings['inventory_endpoint'] = _normalize_endpoint(local.get('inventory_endpoint'))
    else:
        settings['inventory_endpoint'] = _normalize_endpoint(settings['inventory_endpoint'])

    if local.get('auth_header'):
        settings['auth_header'] = _normalize_header(local.get('auth_header'))
    else:
        settings['auth_header'] = _normalize_header(settings['auth_header'])

    if 'auth_prefix' in local:
        settings['auth_prefix'] = _normalize_prefix(local.get('auth_prefix'))
    else:
        settings['auth_prefix'] = _normalize_prefix(settings['auth_prefix'])

    if local.get('auth_token'):
        settings['auth_token'] = _normalize_token(local.get('auth_token'))
    else:
        settings['auth_token'] = _normalize_token(settings['auth_token'])

    return settings


def get_masked_api_auth_settings():
    effective = get_effective_api_auth_settings()
    token = effective.get('auth_token', '')
    if token:
        suffix = token[-4:] if len(token) >= 4 else token
        masked_token = f'***{suffix}'
    else:
        masked_token = ''

    return {
        'inventory_endpoint': effective.get('inventory_endpoint', ''),
        'auth_header': effective.get('auth_header', ''),
        'auth_prefix': effective.get('auth_prefix', ''),
        'has_token': bool(token),
        'token_masked': masked_token,
    }


def save_api_auth_settings(payload):
    current = get_effective_api_auth_settings()
    local_existing = load_local_api_auth_settings()

    next_values = {
        'inventory_endpoint': _normalize_endpoint(payload.get('inventory_endpoint', current['inventory_endpoint'])),
        'auth_header': _normalize_header(payload.get('auth_header', current['auth_header'])),
        'auth_prefix': _normalize_prefix(payload.get('auth_prefix', current['auth_prefix'])),
        'auth_token': current.get('auth_token', ''),
    }

    # Keep current token when empty string is submitted from the UI.
    if 'auth_token' in payload:
        token_candidate = _normalize_token(payload.get('auth_token'))
        if token_candidate:
            next_values['auth_token'] = token_candidate
        elif local_existing.get('auth_token'):
            next_values['auth_token'] = local_existing.get('auth_token', '')

    path = Path(LOCAL_API_CONFIG_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open('w', encoding='utf-8') as file:
        json.dump(next_values, file, indent=2)

    logger.info('Saved local API auth settings')
    return get_masked_api_auth_settings()
