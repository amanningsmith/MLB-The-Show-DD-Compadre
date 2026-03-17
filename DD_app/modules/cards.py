"""
Card Management Module
Handles all card CRUD operations and business logic
"""

import json
from datetime import datetime
from modules.database import get_db_connection
from modules.api_client import api_client
from modules.logger import logger

SERIES_ID_MAP = {
    1337: 'Live',
    10001: 'Rookie',
    10002: 'Breakout',
    10003: 'Veteran',
    10004: 'All-Star',
    10005: 'Awards',
    10006: 'Postseason',
    10009: 'Signature',
    10013: 'Prime',
    10017: 'Topps Now',
    10020: '2nd Half Heroes',
    10022: 'Milestone',
    10028: 'World Baseball Classic',
    10034: 'Standout',
    10035: 'The Negro Leagues',
    10044: 'Contributor',
    10045: 'Last Ride',
    10046: 'Jolt',
    10049: 'Cornerstone',
    10052: 'Ranked 1000',
    10067: 'WBC'
}

ACTUAL_INVENTORY_SORT_FIELDS = {
    'team': 'team',
    'series': 'series',
    'position': 'display_position',
    'ovr': 'ovr',
    'player_name': 'player_name',
}


def _to_int(value, default=0):
    try:
        if value is None or str(value).strip() == '':
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _build_player_id(player_name, series):
    player_name = (player_name or '').strip()
    if not player_name:
        return None
    player_series = (series or 'Unknown').strip() or 'Unknown'
    return f"{player_name}::{player_series}"

def parse_api_card_data(item_data, listing_data=None):
    """
    Parse API response into card data dictionary
    
    Args:
        item_data: Card details from /apis/item.json
        listing_data: Market data from /apis/listing.json (optional)
        
    Returns:
        Dictionary with card data ready for database insertion
    """
    try:
        card_data = {
            'uuid': item_data.get('uuid'),
            'player_name': item_data.get('name'),
            'display_position': item_data.get('display_position'),
            'display_secondary_positions': item_data.get('display_secondary_positions'),
            'team': item_data.get('team'),
            'team_short_name': item_data.get('team_short_name'),
            'ovr': item_data.get('ovr'),
            'rarity': item_data.get('rarity'),
            'series': item_data.get('series'),
            'series_year': item_data.get('series_year'),
            'card_type': 'Live' if item_data.get('series', '').lower() == 'live' else 'Legend',
            
            # Player Bio
            'jersey_number': item_data.get('jersey_number'),
            'age': item_data.get('age'),
            'bat_hand': item_data.get('bat_hand'),
            'throw_hand': item_data.get('throw_hand'),
            'height': item_data.get('height'),
            'weight': item_data.get('weight'),
            'born': item_data.get('born'),
            'is_hitter': item_data.get('is_hitter', True),
            
            # Hitting Attributes
            'contact_right': item_data.get('contact_right'),
            'contact_left': item_data.get('contact_left'),
            'power_right': item_data.get('power_right'),
            'power_left': item_data.get('power_left'),
            'plate_vision': item_data.get('plate_vision'),
            'plate_discipline': item_data.get('plate_discipline'),
            'batting_clutch': item_data.get('batting_clutch'),
            'bunting_ability': item_data.get('bunting_ability'),
            'drag_bunting_ability': item_data.get('drag_bunting_ability'),
            
            # Fielding Attributes
            'fielding_ability': item_data.get('fielding_ability'),
            'arm_strength': item_data.get('arm_strength'),
            'arm_accuracy': item_data.get('arm_accuracy'),
            'reaction_time': item_data.get('reaction_time'),
            'blocking': item_data.get('blocking'),
            'fielding_durability': item_data.get('fielding_durability'),
            
            # Running Attributes
            'speed': item_data.get('speed'),
            'baserunning_ability': item_data.get('baserunning_ability'),
            'baserunning_aggression': item_data.get('baserunning_aggression'),
            'steal': item_data.get('steal'),
            
            # Pitching Attributes
            'stamina': item_data.get('stamina'),
            'pitching_clutch': item_data.get('pitching_clutch'),
            'hits_per_9': item_data.get('hits_per_bf'),
            'k_per_9': item_data.get('k_per_bf'),
            'bb_per_9': item_data.get('bb_per_bf'),
            'hr_per_9': item_data.get('hr_per_bf'),
            'pitch_velocity': item_data.get('pitch_velocity'),
            'pitch_control': item_data.get('pitch_control'),
            'pitch_movement': item_data.get('pitch_movement'),
            'hitting_durability': item_data.get('hitting_durability'),
            
            # Pitch Repertoire and Quirks (store as JSON)
            'pitches': json.dumps(item_data.get('pitches', [])),
            'quirks': json.dumps(item_data.get('quirks', [])),
            
            # Card Metadata
            'locations': json.dumps(item_data.get('locations', [])),
            'event_eligible': False,  # From community data if available
            'supercharged': False,    # From community data if available
            'inside_edge_stars': None,
        }
        
        # Add listing data if provided
        if listing_data:
            card_data['current_sell_price'] = listing_data.get('best_sell_price', 0)
            card_data['current_buy_price'] = listing_data.get('best_buy_price', 0)
            
            # Try to get image from listing
            if 'item' in listing_data and 'img' in listing_data['item']:
                card_data['card_image_url'] = listing_data['item']['img']
            elif 'img' in item_data:
                card_data['card_image_url'] = item_data.get('img')
        
        return card_data
        
    except Exception as e:
        logger.error(f"Failed to parse card data: {e}")
        return None

def create_card(uuid, user_data):
    """
    Create a new card in the database
    
    Args:
        uuid: Card UUID from The Show API
        user_data: Dictionary with user-entered fields
        
    Returns:
        Created card UUID or None if failed
    """
    try:
        # Fetch card data from API
        api_data = api_client.refresh_card_data(uuid)
        if not api_data or not api_data['details']:
            logger.error(f"Cannot create card - API data unavailable for UUID: {uuid}")
            return None
        
        # Parse API data
        card_data = parse_api_card_data(api_data['details'], api_data.get('listing'))
        if not card_data:
            return None
        
        # Add user-entered data
        card_data.update({
            'purchased_price': user_data.get('purchased_price'),
            'quantity': user_data.get('quantity', 1),
            'on_team': user_data.get('on_team', False),
            'grind_card': user_data.get('grind_card', False),
            'pxp': user_data.get('pxp', 0),
            'comments': user_data.get('comments', ''),
            'inside_edge': user_data.get('inside_edge', ''),
        })
        
        # Calculate initial investment
        if card_data['purchased_price']:
            card_data['total_investment'] = card_data['purchased_price'] * card_data['quantity']
        
        # Calculate potential profit
        if card_data.get('current_sell_price'):
            card_data['potential_profit'] = (card_data['current_sell_price'] * card_data['quantity']) * 0.9
        
        card_data['last_refreshed'] = datetime.now().isoformat()
        
        # Insert into database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build INSERT query dynamically
        columns = list(card_data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        column_names = ', '.join(columns)
        
        query = f"INSERT INTO cards ({column_names}) VALUES ({placeholders})"
        cursor.execute(query, list(card_data.values()))
        
        # Log initial price if available
        if card_data.get('current_sell_price'):
            cursor.execute('''
                INSERT INTO price_history (card_uuid, best_sell_price, best_buy_price)
                VALUES (?, ?, ?)
            ''', (uuid, card_data['current_sell_price'], card_data.get('current_buy_price', 0)))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Card created: {card_data['player_name']} ({uuid})")
        return uuid
        
    except Exception as e:
        logger.error(f"Failed to create card: {e}")
        return None

def create_card_manual(card_data):
    """
    Create a new card manually (without API) for cards not in the marketplace
    
    Args:
        card_data: Dictionary with all card fields including user data
        
    Returns:
        Created card UUID or None if failed
    """
    try:
        import uuid as uuid_module
        
        # Generate a UUID for manual cards (prefixed with 'manual-')
        new_uuid = f"manual-{uuid_module.uuid4()}"
        
        # Build complete card object with defaults
        complete_card_data = {
            'uuid': new_uuid,
            'player_name': card_data['player_name'],
            'display_position': card_data['display_position'],
            'display_secondary_positions': None,
            'team': card_data.get('team', ''),
            'team_short_name': card_data.get('team', ''),
            'ovr': card_data['ovr'],
            'rarity': card_data['rarity'],
            'series': card_data['series'],
            'series_year': None,
            'card_image_url': card_data.get('card_image_url'),
            'card_type': 'manual',
            
            # Player Bio - all null for manual entry
            'jersey_number': None,
            'age': None,
            'bat_hand': None,
            'throw_hand': None,
            'height': None,
            'weight': None,
            'born': None,
            'is_hitter': 1 if card_data['display_position'] not in ['SP', 'RP', 'CP'] else 0,
            
            # All attributes set to 0 for manual cards
            'contact_right': 0,
            'contact_left': 0,
            'power_right': 0,
            'power_left': 0,
            'plate_vision': 0,
            'plate_discipline': 0,
            'batting_clutch': 0,
            'bunting_ability': 0,
            'drag_bunting_ability': 0,
            'fielding_ability': 0,
            'arm_strength': 0,
            'arm_accuracy': 0,
            'reaction_time': 0,
            'blocking': 0,
            'fielding_durability': 0,
            'speed': 0,
            'baserunning_ability': 0,
            'baserunning_aggression': 0,
            'steal': 0,
            'stamina': 0,
            'pitching_clutch': 0,
            'hits_per_9': 0,
            'k_per_9': 0,
            'bb_per_9': 0,
            'hr_per_9': 0,
            'pitch_velocity': 0,
            'pitch_control': 0,
            'pitch_movement': 0,
            'hitting_durability': 0,
            
            # Empty JSON fields
            'pitches': None,
            'quirks': None,
            'locations': None,
            
            # Metadata
            'event_eligible': 0,
            'supercharged': 0,
            'inside_edge_stars': 0,
            
            # User-entered fields
            'purchased_price': card_data.get('purchased_price'),
            'current_sell_price': 0,
            'current_buy_price': 0,
            'sold_price': None,
            'quantity': card_data.get('quantity', 1),
            'on_team': 1 if card_data.get('on_team') else 0,
            'grind_card': 1 if card_data.get('grind_card') else 0,
            'pxp': card_data.get('pxp', 0),
            'comments': card_data.get('comments', ''),
            'inside_edge': card_data.get('inside_edge', ''),
            'card_status': 'Active',
            
            # Calculated fields
            'profit_generated': 0,
            'potential_profit': 0,
            'total_investment': 0,
            
            # Timestamps
            'date_acquired': datetime.now().isoformat(),
            'last_refreshed': None,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        # Calculate total investment
        if complete_card_data['purchased_price']:
            complete_card_data['total_investment'] = complete_card_data['purchased_price'] * complete_card_data['quantity']
        
        # Insert into database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build INSERT query dynamically
        columns = list(complete_card_data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        column_names = ', '.join(columns)
        
        query = f"INSERT INTO cards ({column_names}) VALUES ({placeholders})"
        cursor.execute(query, list(complete_card_data.values()))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Manual card created: {complete_card_data['player_name']} ({new_uuid})")
        return new_uuid
        
    except Exception as e:
        logger.error(f"Failed to create manual card: {e}")
        return None

def get_all_cards(filters=None, sort_by='ovr', sort_order='desc', page=1, per_page=20):
    """
    Get all cards with optional filtering, sorting, and pagination
    
    Args:
        filters: Dictionary of filter criteria
        sort_by: Column to sort by
        sort_order: 'asc' or 'desc'
        page: Page number (1-indexed)
        per_page: Results per page
        
    Returns:
        Dictionary with 'cards' and 'total' keys
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build WHERE clause
        where_clauses = []
        params = []
        
        if filters:
            if filters.get('rarity'):
                where_clauses.append("rarity = ?")
                params.append(filters['rarity'])
            
            if filters.get('position'):
                where_clauses.append("display_position = ?")
                params.append(filters['position'])
            
            if filters.get('team'):
                where_clauses.append("team = ?")
                params.append(filters['team'])
            
            if filters.get('series'):
                where_clauses.append("series = ?")
                params.append(filters['series'])
            
            if filters.get('on_team') is not None:
                where_clauses.append("on_team = ?")
                params.append(1 if filters['on_team'] else 0)
            
            if filters.get('grind_card') is not None:
                where_clauses.append("grind_card = ?")
                params.append(1 if filters['grind_card'] else 0)
            
            if filters.get('card_status'):
                where_clauses.append("card_status = ?")
                params.append(filters['card_status'])
            
            if filters.get('search'):
                where_clauses.append("player_name LIKE ?")
                params.append(f"%{filters['search']}%")
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        # Get total count
        count_query = f"SELECT COUNT(*) as total FROM cards WHERE {where_sql}"
        cursor.execute(count_query, params)
        total = cursor.fetchone()['total']
        
        # Get paginated results
        offset = (page - 1) * per_page
        query = f'''
            SELECT * FROM cards 
            WHERE {where_sql}
            ORDER BY {sort_by} {sort_order.upper()}
            LIMIT ? OFFSET ?
        '''
        
        cursor.execute(query, params + [per_page, offset])
        rows = cursor.fetchall()
        
        cards = [dict(row) for row in rows]
        
        conn.close()
        
        return {
            'cards': cards,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page
        }
        
    except Exception as e:
        logger.error(f"Failed to get cards: {e}")
        return {'cards': [], 'total': 0, 'page': 1, 'per_page': per_page, 'total_pages': 0}

def get_card_by_uuid(uuid):
    """
    Get a single card by UUID
    
    Args:
        uuid: Card UUID
        
    Returns:
        Card dictionary or None
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM cards WHERE uuid = ?", (uuid,))
        row = cursor.fetchone()
        
        conn.close()
        
        if row:
            return dict(row)
        return None
        
    except Exception as e:
        logger.error(f"Failed to get card: {e}")
        return None

def update_card(uuid, updates):
    """
    Update card fields
    
    Args:
        uuid: Card UUID
        updates: Dictionary of fields to update
        
    Returns:
        True if successful, False otherwise
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build UPDATE query
        set_clauses = []
        params = []
        
        for key, value in updates.items():
            set_clauses.append(f"{key} = ?")
            params.append(value)
        
        set_clauses.append("updated_at = ?")
        params.append(datetime.now().isoformat())
        
        params.append(uuid)
        
        query = f"UPDATE cards SET {', '.join(set_clauses)} WHERE uuid = ?"
        cursor.execute(query, params)
        
        conn.commit()
        conn.close()
        
        logger.info(f"Card updated: {uuid}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to update card: {e}")
        return False

def delete_cards(uuids):
    """
    Delete cards by UUIDs
    
    Args:
        uuids: List of card UUIDs to delete
        
    Returns:
        Number of cards deleted
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        placeholders = ', '.join(['?' for _ in uuids])
        query = f"DELETE FROM cards WHERE uuid IN ({placeholders})"
        cursor.execute(query, uuids)
        
        deleted_count = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        logger.info(f"Deleted {deleted_count} cards")
        return deleted_count
        
    except Exception as e:
        logger.error(f"Failed to delete cards: {e}")
        return 0


def get_actual_inventory_cards(filters=None, sort_by='ovr', sort_order='desc', page=1, per_page=20):
    """Read owned inventory cards for the Actual Card Tracker page."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        where_clauses = ["quantity > 0", "card_status = 'Active'"]
        params = []

        if filters:
            team = (filters.get('team') or '').strip()
            series = (filters.get('series') or '').strip()
            position = (filters.get('position') or '').strip()
            search = (filters.get('search') or '').strip()

            min_ovr = _to_int(filters.get('min_ovr'), None)
            max_ovr = _to_int(filters.get('max_ovr'), None)

            if team:
                where_clauses.append('team = ?')
                params.append(team)
            if series:
                where_clauses.append('series = ?')
                params.append(series)
            if position:
                where_clauses.append('display_position = ?')
                params.append(position)
            if search:
                where_clauses.append('player_name LIKE ?')
                params.append(f'%{search}%')
            if min_ovr is not None:
                where_clauses.append('ovr >= ?')
                params.append(min_ovr)
            if max_ovr is not None:
                where_clauses.append('ovr <= ?')
                params.append(max_ovr)

        where_sql = ' AND '.join(where_clauses)

        count_query = f'SELECT COUNT(*) as total FROM cards WHERE {where_sql}'
        cursor.execute(count_query, params)
        total = cursor.fetchone()['total']

        safe_sort_column = ACTUAL_INVENTORY_SORT_FIELDS.get(sort_by, 'ovr')
        safe_sort_order = 'ASC' if (sort_order or '').lower() == 'asc' else 'DESC'
        page = max(1, _to_int(page, 1))
        per_page = max(1, _to_int(per_page, 20))
        offset = (page - 1) * per_page

        query = f'''
            SELECT * FROM cards
            WHERE {where_sql}
            ORDER BY {safe_sort_column} {safe_sort_order}, player_name ASC
            LIMIT ? OFFSET ?
        '''
        cursor.execute(query, params + [per_page, offset])
        rows = [dict(row) for row in cursor.fetchall()]

        conn.close()

        return {
            'cards': rows,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page,
        }
    except Exception as e:
        logger.error(f'Failed to get actual inventory cards: {e}')
        return {'cards': [], 'total': 0, 'page': 1, 'per_page': per_page, 'total_pages': 0}


def get_actual_inventory_filter_options():
    """Return distinct filter options for owned inventory view."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        base_where = "quantity > 0 AND card_status = 'Active'"

        cursor.execute(
            f"SELECT DISTINCT team FROM cards WHERE {base_where} AND team IS NOT NULL AND TRIM(team) <> '' ORDER BY team ASC"
        )
        teams = [row['team'] for row in cursor.fetchall()]

        cursor.execute(
            f"SELECT DISTINCT series FROM cards WHERE {base_where} AND series IS NOT NULL AND TRIM(series) <> '' ORDER BY series ASC"
        )
        series = [row['series'] for row in cursor.fetchall()]

        cursor.execute(
            f"SELECT DISTINCT display_position FROM cards WHERE {base_where} AND display_position IS NOT NULL AND TRIM(display_position) <> '' ORDER BY display_position ASC"
        )
        positions = [row['display_position'] for row in cursor.fetchall()]

        conn.close()
        return {'teams': teams, 'series': series, 'positions': positions}
    except Exception as e:
        logger.error(f'Failed to get actual inventory filter options: {e}')
        return {'teams': [], 'series': [], 'positions': []}


def sync_actual_inventory(sync_source='actual_card_tracker'):
    """Sync owned inventory cards and apply mission automation for exact player_id matches."""
    try:
        sync_result = api_client.get_owned_inventory_cards()
        cards_to_sync = []
        source = sync_result.get('source', 'unknown')
        warning = sync_result.get('warning')
        error = sync_result.get('error')

        if sync_result.get('success'):
            cards_to_sync = sync_result.get('cards', [])
        else:
            source = 'local_inventory_fallback'
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT uuid, quantity, on_team FROM cards WHERE quantity > 0 AND card_status = 'Active'"
            )
            cards_to_sync = [dict(row) for row in cursor.fetchall()]
            conn.close()
            if not warning:
                warning = 'Auth inventory unavailable; used local tracked inventory fallback'

        now_iso = datetime.now().isoformat()
        conn = get_db_connection()
        cursor = conn.cursor()

        cards_created = 0
        cards_updated = 0
        synced_uuids = set()
        owned_player_ids = set()

        for raw in cards_to_sync:
            uuid = (raw.get('uuid') or '').strip()
            if not uuid:
                continue

            quantity = max(0, _to_int(raw.get('quantity'), 0))
            if quantity <= 0:
                continue

            on_team = 1 if raw.get('on_team') else 0

            cursor.execute('SELECT uuid, player_name, series FROM cards WHERE uuid = ?', (uuid,))
            existing = cursor.fetchone()

            if existing:
                cursor.execute(
                    '''
                    UPDATE cards
                    SET quantity = ?,
                        on_team = ?,
                        card_status = 'Active',
                        updated_at = ?
                    WHERE uuid = ?
                    ''',
                    (quantity, on_team, now_iso, uuid),
                )
                cards_updated += 1
                player_id = _build_player_id(existing['player_name'], existing['series'])
                if player_id:
                    owned_player_ids.add(player_id)
                synced_uuids.add(uuid)
                continue

            api_data = api_client.refresh_card_data(uuid)
            details = api_data.get('details') if isinstance(api_data, dict) else None
            if not details:
                logger.warning(f'Skipping inventory sync for unknown UUID with no API details: {uuid}')
                continue

            parsed = parse_api_card_data(details, api_data.get('listing') if isinstance(api_data, dict) else None)
            if not parsed:
                continue

            parsed.update(
                {
                    'quantity': quantity,
                    'on_team': on_team,
                    'grind_card': 0,
                    'pxp': 0,
                    'comments': '',
                    'inside_edge': '',
                    'card_status': 'Active',
                    'last_refreshed': now_iso,
                }
            )

            columns = list(parsed.keys())
            placeholders = ', '.join(['?' for _ in columns])
            column_names = ', '.join(columns)
            query = f'INSERT INTO cards ({column_names}) VALUES ({placeholders})'
            cursor.execute(query, list(parsed.values()))

            if parsed.get('current_sell_price'):
                cursor.execute(
                    '''
                    INSERT INTO price_history (card_uuid, best_sell_price, best_buy_price)
                    VALUES (?, ?, ?)
                    ''',
                    (uuid, parsed.get('current_sell_price', 0), parsed.get('current_buy_price', 0)),
                )

            cards_created += 1
            synced_uuids.add(uuid)
            player_id = _build_player_id(parsed.get('player_name'), parsed.get('series'))
            if player_id:
                owned_player_ids.add(player_id)

        conn.commit()
        conn.close()

        from modules import missions

        mission_result = missions.apply_owned_player_sync(
            list(owned_player_ids),
            sync_source=(sync_source or source or 'actual_card_tracker'),
        )

        return {
            'success': True,
            'source': source,
            'warning': warning,
            'error': error,
            'cards_received': len(cards_to_sync),
            'cards_synced': len(synced_uuids),
            'cards_created': cards_created,
            'cards_updated': cards_updated,
            'missions_updated': mission_result.get('missions_updated', 0),
            'audit_entries': mission_result.get('audit_entries', 0),
            'mission_summary': mission_result,
        }
    except Exception as e:
        logger.error(f'Failed to sync actual inventory: {e}')
        return {
            'success': False,
            'source': 'sync_error',
            'warning': None,
            'error': str(e),
            'cards_received': 0,
            'cards_synced': 0,
            'cards_created': 0,
            'cards_updated': 0,
            'missions_updated': 0,
            'audit_entries': 0,
            'mission_summary': {
                'players_matched': 0,
                'missions_linked': 0,
                'missions_updated': 0,
                'audit_entries': 0,
            },
        }

def search_local_catalog(name=None, rarity=None, position=None, team=None, series_id=None, limit=100):
    """
    Search the local card catalog cache (fast, offline-friendly).

    Args:
        name: Player name contains match
        rarity: Rarity exact match (case-insensitive)
        position: Display position exact match
        team: Team exact match
        series_id: Optional series ID to map into series text
        limit: Max number of rows to return

    Returns:
        List of catalog card dictionaries
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query_parts = ["SELECT * FROM card_catalog WHERE 1=1"]
        params = []

        if name:
            query_parts.append("AND name LIKE ?")
            params.append(f"%{name}%")

        if rarity:
            query_parts.append("AND LOWER(rarity) = LOWER(?)")
            params.append(rarity)

        if position:
            query_parts.append("AND display_position = ?")
            params.append(position)

        if team:
            query_parts.append("AND team = ?")
            params.append(team)

        if series_id is not None:
            series_name = SERIES_ID_MAP.get(series_id)
            if series_name:
                query_parts.append("AND series LIKE ?")
                params.append(f"%{series_name}%")

        query_parts.append("ORDER BY ovr DESC, name ASC LIMIT ?")
        params.append(limit)

        query = " ".join(query_parts)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    except Exception as e:
        logger.error(f"Failed to search local catalog: {e}")
        return []

def get_card_catalog_status():
    """
    Get catalog cache health/status data.

    Returns:
        Dictionary with count and last cached timestamp
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT
                COUNT(*) as card_count,
                MAX(cached_at) as last_cached_at
            FROM card_catalog
        ''')
        row = cursor.fetchone()
        conn.close()

        return {
            'card_count': row['card_count'] if row else 0,
            'last_cached_at': row['last_cached_at'] if row else None,
            'is_ready': (row['card_count'] > 0) if row else False
        }

    except Exception as e:
        logger.error(f"Failed to get card catalog status: {e}")
        return {
            'card_count': 0,
            'last_cached_at': None,
            'is_ready': False
        }

# Additional helper functions will be added for sell workflow, refresh, etc.
