"""
The Show API Client
Handles all API calls to mlb26.theshow.com with rate limiting
"""

import requests
import time
import json
import threading
from config import (
    MLB_API_BASE_URL,
    API_DELAY,
)
from modules.logger import logger
from modules.settings import get_effective_api_auth_settings

# Global sync state — shared across all threads
_sync_state = {
    'is_syncing': False,
    'current_page': 0,
    'total_pages': 0,
    'cards_synced': 0,
    'error': None,
    'finished_at': None,
}
_sync_lock = threading.Lock()

class TheShowAPIClient:
    """
    Client for The Show 26 Public API
    """
    
    def __init__(self):
        self.base_url = MLB_API_BASE_URL
        self.delay = API_DELAY
        self.last_call_time = 0
    
    def _rate_limit(self):
        """
        Ensure polite delay between API calls
        """
        elapsed = time.time() - self.last_call_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_call_time = time.time()
    
    def _make_request(self, endpoint, params=None, headers=None):
        """
        Make a GET request to the API with error handling
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            Response JSON or None if failed
        """
        self._rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            logger.info(f"API Request: {endpoint} | Params: {params}")
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"API Success: {endpoint} - Status: {response.status_code}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API Request failed: {endpoint} - Error: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"API Response JSON decode failed: {endpoint} - Error: {e}")
            return None
    
    def search_cards_by_name(self, name):
        """
        Search for cards by player name
        
        Args:
            name: Player name to search
            
        Returns:
            List of matching cards
        """
        endpoint = '/apis/listings.json'
        params = {'name': name}
        data = self._make_request(endpoint, params)
        
        if data and 'listings' in data:
            return data['listings']
        return []
    
    def search_cards_advanced(self, name=None, rarity=None, position=None, 
                              team=None, series_id=None, 
                              min_sell_price=None, max_sell_price=None,
                              min_buy_price=None, max_buy_price=None,
                              sort='rank', order='desc', page=1):
        """
        Advanced card search with all Listings API filters
        
        Args:
            name: Player name to search (optional)
            rarity: Card rarity (diamond/gold/silver/bronze/common)
            position: Display position (SP, C, 1B, etc.)
            team: Team code (NYY, LAD, etc.)
            series_id: Series ID (1337=Live, 10001=Rookie, etc.)
            min_sell_price: Minimum sell price in stubs
            max_sell_price: Maximum sell price in stubs
            min_buy_price: Minimum buy price in stubs
            max_buy_price: Maximum buy price in stubs
            sort: Sort field (rank, best_sell_price, best_buy_price)
            order: Sort order (desc, asc)
            page: Page number (default 1)
            
        Returns:
            Dictionary with listings data including pagination info
        """
        endpoint = '/apis/listings.json'
        params = {'page': page, 'sort': sort, 'order': order}
        
        if name:
            params['name'] = name
        if rarity:
            params['rarity'] = rarity.lower()
        if position:
            params['display_position'] = position
        if team:
            params['team'] = team
        if series_id:
            params['series_id'] = series_id
        if min_sell_price is not None:
            params['min_best_sell_price'] = min_sell_price
        if max_sell_price is not None:
            params['max_best_sell_price'] = max_sell_price
        if min_buy_price is not None:
            params['min_best_buy_price'] = min_buy_price
        if max_buy_price is not None:
            params['max_best_buy_price'] = max_buy_price
        
        data = self._make_request(endpoint, params)
        
        if data:
            return data
        return {'listings': [], 'page': page, 'total_pages': 0}
    
    def search_cards_by_page(self, page=0, card_type='mlb_card'):
        """
        Browse cards by page
        
        Args:
            page: Page number (0-indexed)
            card_type: Type of card (default: mlb_card)
            
        Returns:
            List of cards on that page
        """
        endpoint = '/apis/items.json'
        params = {'type': card_type, 'page': page}
        data = self._make_request(endpoint, params)
        
        if data and 'items' in data:
            return data['items']
        return []
    
    def get_card_details(self, uuid):
        """
        Get full card details by UUID
        
        Args:
            uuid: Card UUID
            
        Returns:
            Card detail dictionary or None
        """
        endpoint = '/apis/item.json'
        params = {'uuid': uuid}
        data = self._make_request(endpoint, params)
        
        return data
    
    def get_card_listing(self, uuid):
        """
        Get card market listing (prices, history, image)
        
        Args:
            uuid: Card UUID
            
        Returns:
            Listing data dictionary or None
        """
        endpoint = '/apis/listing.json'
        params = {'uuid': uuid}
        data = self._make_request(endpoint, params)
        
        return data
    
    def get_roster_updates(self):
        """
        Get list of roster updates
        
        Returns:
            List of roster updates or None
        """
        endpoint = '/apis/roster_updates.json'
        data = self._make_request(endpoint)
        
        if data and 'roster_updates' in data:
            return data['roster_updates']
        return []

    def _extract_inventory_items(self, payload):
        """Extract inventory card rows from varied auth endpoint response shapes."""
        if payload is None:
            return []

        candidates = []
        if isinstance(payload, list):
            candidates = payload
        elif isinstance(payload, dict):
            for key in ('items', 'inventory', 'cards', 'owned_cards', 'roster'):
                value = payload.get(key)
                if isinstance(value, list):
                    candidates = value
                    break
                if isinstance(value, dict):
                    for nested_key in ('items', 'cards', 'owned_cards'):
                        nested = value.get(nested_key)
                        if isinstance(nested, list):
                            candidates = nested
                            break
                    if candidates:
                        break

        normalized = []
        for raw in candidates:
            if not isinstance(raw, dict):
                continue

            nested_item = raw.get('item') if isinstance(raw.get('item'), dict) else {}
            uuid = (
                raw.get('uuid')
                or raw.get('card_uuid')
                or raw.get('item_uuid')
                or nested_item.get('uuid')
            )
            if not uuid:
                continue

            quantity = raw.get('quantity', raw.get('qty', raw.get('count', 1)))
            try:
                quantity = int(quantity)
            except (TypeError, ValueError):
                quantity = 1

            on_team_raw = raw.get('on_team', raw.get('in_lineup', raw.get('is_active', False)))
            on_team = bool(on_team_raw)

            normalized.append(
                {
                    'uuid': uuid,
                    'quantity': max(quantity, 0),
                    'on_team': on_team,
                }
            )

        return normalized

    def get_owned_inventory_cards(self):
        """
        Attempt to fetch owned inventory cards from an authenticated profile endpoint.

        Returns:
            dict: {
                success: bool,
                source: str,
                cards: [{uuid, quantity, on_team}],
                warning: str|None,
                error: str|None
            }
        """
        runtime_settings = get_effective_api_auth_settings()
        auth_token = runtime_settings.get('auth_token', '')
        inventory_endpoint = runtime_settings.get('inventory_endpoint', '/apis/profile/inventory.json')
        auth_header = runtime_settings.get('auth_header', 'Authorization')
        auth_prefix = runtime_settings.get('auth_prefix', 'Bearer ')

        if not auth_token:
            return {
                'success': False,
                'source': 'auth_unavailable',
                'cards': [],
                'warning': 'Authenticated inventory token not configured',
                'error': None,
            }

        auth_value = f"{auth_prefix}{auth_token}" if auth_prefix else auth_token
        headers = {auth_header: auth_value}

        data = self._make_request(inventory_endpoint, headers=headers)
        if data is None:
            return {
                'success': False,
                'source': 'auth_failed',
                'cards': [],
                'warning': 'Authenticated inventory request failed',
                'error': 'Inventory endpoint unavailable or unauthorized',
            }

        cards = self._extract_inventory_items(data)
        if not cards:
            return {
                'success': False,
                'source': 'auth_empty',
                'cards': [],
                'warning': 'Authenticated response returned no parseable inventory cards',
                'error': None,
            }

        return {
            'success': True,
            'source': 'authenticated_api',
            'cards': cards,
            'warning': None,
            'error': None,
        }
    
    def refresh_card_data(self, uuid):
        """
        Refresh all data for a single card (details + listing)
        
        Args:
            uuid: Card UUID
            
        Returns:
            Dictionary with 'details' and 'listing' keys, or None if failed
        """
        details = self.get_card_details(uuid)
        if not details:
            logger.warning(f"Card refresh failed - no details returned for UUID: {uuid}")
            return None
        
        listing = self.get_card_listing(uuid)
        
        return {
            'details': details,
            'listing': listing
        }

    def get_sync_progress(self):
        """
        Return the current catalog sync progress state.
        Safe to call from any thread.
        """
        with _sync_lock:
            return dict(_sync_state)

    def sync_card_catalog_background(self):
        """
        Start a background thread to sync the card catalog.
        Safe to call multiple times — ignores the call if sync is already running.

        Returns:
            True if the sync was started, False if already in progress.
        """
        with _sync_lock:
            if _sync_state['is_syncing']:
                return False
            _sync_state.update({
                'is_syncing': True,
                'current_page': 0,
                'total_pages': 0,
                'cards_synced': 0,
                'error': None,
                'finished_at': None,
            })

        thread = threading.Thread(target=self._run_sync, daemon=True)
        thread.start()
        return True

    def _run_sync(self):
        """Internal: runs in a background thread, updates _sync_state as it goes."""
        from modules.database import get_db_connection

        try:
            logger.info('Card catalog background sync started')

            all_cards = []
            page = 1
            safety_limit = 300

            # Probe page 1 to get total_pages
            first_data = self._make_request('/apis/items.json', {'type': 'mlb_card', 'page': 1})
            if not first_data or 'items' not in first_data:
                # Try page 0 (some versions are 0-indexed)
                first_data = self._make_request('/apis/items.json', {'type': 'mlb_card', 'page': 0})
                if not first_data or 'items' not in first_data:
                    raise RuntimeError('Items API returned no data on page 1 or 0')
                page = 0

            discovered_total = first_data.get('total_pages', 0)
            with _sync_lock:
                _sync_state['total_pages'] = discovered_total
                _sync_state['current_page'] = page

            all_cards.extend(first_data.get('items', []))

            while True:
                page += 1
                if page > safety_limit:
                    break

                with _sync_lock:
                    _sync_state['current_page'] = page
                    _sync_state['cards_synced'] = len(all_cards)

                data = self._make_request('/apis/items.json', {'type': 'mlb_card', 'page': page})
                if not data or not data.get('items'):
                    break

                all_cards.extend(data['items'])

                total_pages = data.get('total_pages', discovered_total)
                with _sync_lock:
                    _sync_state['total_pages'] = total_pages

                if page >= total_pages:
                    break

            if not all_cards:
                raise RuntimeError('No card data returned from Items API')

            # Write to database
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM card_catalog')

            rows = []
            for card in all_cards:
                rows.append((
                    card.get('uuid'),
                    card.get('name'),
                    card.get('rarity'),
                    card.get('team') or card.get('team_short_name'),
                    card.get('ovr'),
                    card.get('series'),
                    card.get('display_position'),
                    card.get('display_secondary_positions'),
                    1 if card.get('is_sellable') else 0,
                ))

            cursor.executemany('''
                INSERT OR REPLACE INTO card_catalog (
                    uuid, name, rarity, team, ovr, series, display_position, display_secondary_positions, is_sellable
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', rows)

            conn.commit()
            conn.close()

            import datetime as _dt
            with _sync_lock:
                _sync_state['is_syncing'] = False
                _sync_state['cards_synced'] = len(rows)
                _sync_state['current_page'] = page
                _sync_state['error'] = None
                _sync_state['finished_at'] = _dt.datetime.now().isoformat()

            logger.info(f'Card catalog sync complete: {len(rows)} cards')

        except Exception as exc:
            logger.error(f'Card catalog background sync failed: {exc}')
            with _sync_lock:
                _sync_state['is_syncing'] = False
                _sync_state['error'] = str(exc)

    def search_items_api_live(self, name, max_pages=8):
        """
        Live partial search of the Items API for cards matching ``name``.

        Because the Items API has no name filter, this paginates up to
        ``max_pages`` pages and returns any card whose name contains the
        search string (case-insensitive).  Use this as a fallback when the
        local catalog cache is empty.

        Args:
            name: Player name substring to match
            max_pages: Maximum pages to scan before stopping (default 8)

        Returns:
            List of matching card dicts, plus a ``_partial`` flag
        """
        name_lower = name.lower()
        matches = []
        scanned = 0

        for page in range(1, max_pages + 1):
            data = self._make_request('/apis/items.json', {'type': 'mlb_card', 'page': page})
            if not data or not data.get('items'):
                break

            scanned += 1
            for card in data['items']:
                card_name = (card.get('name') or '').lower()
                if name_lower in card_name:
                    matches.append(card)

            total_pages = data.get('total_pages', page)
            if page >= total_pages:
                break

        return {
            'cards': matches,
            'pages_scanned': scanned,
            'is_partial': scanned < (max_pages if max_pages else 8),
        }

    def sync_card_catalog(self):
        """
        Synchronous catalog sync — kept for backwards compatibility.
        Prefer calling sync_card_catalog_background() from HTTP handlers.
        """
        started = self.sync_card_catalog_background()
        if not started:
            return {'success': False, 'error': 'Sync already in progress', 'card_count': 0, 'pages_synced': 0}

        # Wait for it to finish (blocking — only safe in non-request context)
        while True:
            with _sync_lock:
                if not _sync_state['is_syncing']:
                    break
            time.sleep(0.3)

        with _sync_lock:
            state = dict(_sync_state)

        if state.get('error'):
            return {'success': False, 'error': state['error'], 'card_count': 0, 'pages_synced': state['current_page']}

        return {'success': True, 'card_count': state['cards_synced'], 'pages_synced': state['current_page']}


# Create singleton instance
api_client = TheShowAPIClient()
