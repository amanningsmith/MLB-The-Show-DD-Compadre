"""
MLB The Show 26 Diamond Dynasty Tracker
Flask Application Entry Point
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for
import sqlite3
from config import SECRET_KEY, DEBUG, MISSIONS_DB, CARDS_DB
from modules.logger import logger
from modules import missions, cards
from modules.backup import backup_missions, backup_cards
from modules.api_client import api_client
from modules import settings as app_settings
from modules import scores
import json

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY
app.config['DEBUG'] = DEBUG

# ========================
# Template Filters
# ========================

@app.template_filter('pxp_level')
def pxp_level_filter(pxp):
    """Calculate PXP level from total PXP"""
    if pxp is None:
        return 1
    pxp = int(pxp)
    if pxp >= 10000:
        return 5
    elif pxp >= 5000:
        return 4
    elif pxp >= 2500:
        return 3
    elif pxp >= 1000:
        return 2
    else:
        return 1

@app.template_filter('roman_numeral')
def roman_numeral_filter(num):
    """Convert number to Roman numeral (1-5)"""
    numerals = {1: 'I', 2: 'II', 3: 'III', 4: 'IV', 5: 'V'}
    return numerals.get(int(num), 'I')

# ========================
# Mission Tracker Routes
# ========================

@app.route('/')
def home():
    """Features Home Page — Beta 4.1 MVP 7."""
    return render_template('home.html')

@app.route('/missions')
def mission_tracker():
    """Mission Tracker View"""
    try:
        all_missions = missions.read_missions()
        program_cards = missions.get_program_progress(all_missions)
        program_categories = missions.get_program_categories()
        program_type_options = missions.get_program_type_options()
        sample_players = missions.search_sync_players(limit=150)

        return render_template(
            'missions.html',
            missions=all_missions,
            program_cards=program_cards,
            program_categories=program_categories,
            program_type_options=program_type_options,
            sample_players=sample_players,
        )
    except Exception as e:
        logger.error(f"Mission tracker view error: {e}")
        return render_template('error.html', error=str(e))


@app.route('/scores')
def scores_dashboard():
    """Scores dashboard page for Beta 4.0 MVP 6."""
    try:
        return render_template('scores.html', default_date=scores.get_et_today_date_str())
    except Exception as e:
        logger.error(f"Scores dashboard view error: {e}")
        return render_template('error.html', error=str(e))

@app.route('/api/missions', methods=['GET'])
def get_missions():
    """API endpoint to get all missions"""
    try:
        program = request.args.get('program_category', '').strip()
        include_players_value = (request.args.get('include_players', 'false') or '').strip().lower()
        include_players = include_players_value in {'1', 'true', 'yes'}

        if include_players:
            all_missions = missions.read_missions_with_players(program if program else None)
        else:
            all_missions = missions.read_missions(program if program else None)

        return jsonify({'success': True, 'missions': all_missions})
    except Exception as e:
        logger.error(f"Get missions API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/missions', methods=['POST'])
def create_mission():
    """API endpoint to create a new mission"""
    try:
        data = request.json
        new_mission = missions.add_mission(data)
        
        if new_mission:
            return jsonify({'success': True, 'mission': new_mission})
        else:
            return jsonify({'success': False, 'error': 'Failed to create mission'}), 500
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Create mission API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/missions/<int:mission_id>', methods=['PUT'])
def update_mission_api(mission_id):
    """API endpoint to update a mission"""
    try:
        data = request.json
        success = missions.update_mission(mission_id, data)
        
        if success:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Failed to update mission'}), 400
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Update mission API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/missions/recalculate-priority', methods=['POST'])
def recalculate_priority_api():
    """Manual priority recalculation endpoint."""
    try:
        data = request.json or {}
        program_category = (data.get('program_category') or '').strip() or None
        mission_ids = data.get('mission_ids', [])
        if not isinstance(mission_ids, list):
            mission_ids = []

        result = missions.recalculate_mission_priorities(
            program_category=program_category,
            mission_ids=mission_ids,
        )
        return jsonify({'success': True, **result})
    except Exception as e:
        logger.error(f"Recalculate priority API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/missions/backfill-acquired-inventory', methods=['POST'])
def backfill_acquired_inventory_api():
    """Backfill inventory for all players linked to Acquired missions."""
    try:
        result = missions.backfill_acquired_missions_to_inventory()
        return jsonify({'success': True, **result})
    except Exception as e:
        logger.error(f"Backfill acquired inventory API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/missions/program-progress', methods=['GET'])
def get_program_progress_api():
    """API endpoint to get weighted mission progress cards."""
    try:
        program_type = request.args.get('program_type', '').strip()
        all_missions = missions.read_missions()
        progress = missions.get_program_progress(all_missions, program_type if program_type else None)
        return jsonify({'success': True, 'program_cards': progress})
    except Exception as e:
        logger.error(f"Program progress API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/missions/program/<path:program_category>', methods=['GET'])
def get_program_missions_api(program_category):
    """API endpoint to get all missions for a specific program category."""
    try:
        program_missions = missions.get_program_missions(program_category)
        return jsonify({'success': True, 'missions': program_missions})
    except Exception as e:
        logger.error(f"Program missions API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/missions/program/<path:program_category>/type', methods=['PATCH'])
def update_program_type_api(program_category):
    """API endpoint to update a program's type."""
    try:
        data = request.json or {}
        new_type = (data.get('program_type') or '').strip()
        if not new_type:
            return jsonify({'success': False, 'error': 'program_type is required'}), 400
        missions.update_program_type(program_category, new_type)
        return jsonify({'success': True})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Update program type API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/missions/players', methods=['GET'])
def mission_player_picker_api():
    """API endpoint for searchable mission player picker from full synced catalog."""
    try:
        query = request.args.get('query', '').strip()
        limit = int(request.args.get('limit', 120))
        limit = max(1, min(limit, 500))
        players = missions.search_sync_players(query=query, limit=limit)
        return jsonify({'success': True, 'players': players})
    except Exception as e:
        logger.error(f"Mission player picker API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/missions/<int:mission_id>/players', methods=['POST'])
def set_mission_players_api(mission_id):
    """API endpoint to update mission-player associations."""
    try:
        data = request.json or {}
        player_ids = data.get('player_ids', [])
        missions.set_mission_players(mission_id, player_ids)
        players = missions.get_mission_players(mission_id)
        return jsonify({'success': True, 'players': players})
    except Exception as e:
        logger.error(f"Set mission players API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/missions/grind-players', methods=['GET'])
def grind_players_api():
    """API endpoint for mission-related player list with ownership labels."""
    try:
        mission_ids_raw = request.args.get('mission_ids', '').strip()
        mission_ids = []
        if mission_ids_raw:
            mission_ids = [mid.strip() for mid in mission_ids_raw.split(',') if mid.strip()]

        players = missions.get_grind_player_list(mission_ids)
        return jsonify({'success': True, 'players': players})
    except Exception as e:
        logger.error(f"Grind players API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/missions/grind-lineup', methods=['GET'])
def grind_lineup_api():
    """API endpoint for initial lineup suggestion from mission players."""
    try:
        mission_ids_raw = request.args.get('mission_ids', '').strip()
        mission_ids = []
        if mission_ids_raw:
            mission_ids = [mid.strip() for mid in mission_ids_raw.split(',') if mid.strip()]

        lineup = missions.get_lineup_suggestion(mission_ids)
        return jsonify({'success': True, 'lineup': lineup})
    except Exception as e:
        logger.error(f"Grind lineup API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/missions/auto-grind-diamond', methods=['GET'])
def auto_grind_diamond_api():
    """API endpoint for Beta 2.1 auto-grind diamond depth chart."""
    try:
        include_completed_value = (request.args.get('include_completed', 'false') or '').strip().lower()
        include_completed = include_completed_value in {'1', 'true', 'yes'}
        limit_per_position = int(request.args.get('limit_per_position', 10))
        limit_per_position = max(1, min(limit_per_position, 50))

        result = missions.get_auto_grind_diamond(
            limit_per_position=limit_per_position,
            include_completed=include_completed,
        )
        return jsonify({'success': True, 'meta': result['meta'], 'positions': result['positions']})
    except Exception as e:
        logger.error(f"Auto grind diamond API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/missions/delete', methods=['POST'])
def delete_missions_api():
    """API endpoint to delete missions"""
    try:
        data = request.json
        mission_ids = data.get('mission_ids', [])
        
        # Create backup before deletion
        backup_missions('bulk_delete')
        
        deleted_count = missions.delete_missions(mission_ids)
        
        return jsonify({'success': True, 'deleted_count': deleted_count})
    except Exception as e:
        logger.error(f"Delete missions API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ========================
# Card Tracker Routes
# ========================

@app.route('/cards')
def card_tracker():
    """Card Tracker View"""
    try:
        # Get filter parameters
        filters = {}
        if request.args.get('rarity'):
            filters['rarity'] = request.args.get('rarity')
        if request.args.get('position'):
            filters['position'] = request.args.get('position')
        if request.args.get('team'):
            filters['team'] = request.args.get('team')
        if request.args.get('series'):
            filters['series'] = request.args.get('series')
        if request.args.get('on_team'):
            filters['on_team'] = request.args.get('on_team') == 'true'
        if request.args.get('grind_card'):
            filters['grind_card'] = request.args.get('grind_card') == 'true'
        if request.args.get('card_status'):
            filters['card_status'] = request.args.get('card_status')
        if request.args.get('search'):
            filters['search'] = request.args.get('search')
        
        # Get sort parameters
        sort_by = request.args.get('sort_by', 'ovr')
        sort_order = request.args.get('sort_order', 'desc')
        page = int(request.args.get('page', 1))
        
        # Get cards
        result = cards.get_all_cards(filters, sort_by, sort_order, page)
        
        return render_template('cards.html', 
                             cards_data=result['cards'],
                             pagination=result)
    except Exception as e:
        logger.error(f"Card tracker view error: {e}")
        return render_template('error.html', error=str(e))


@app.route('/cards/actual')
def actual_card_tracker():
    """Actual Card Tracker View (Beta 3.0)."""
    try:
        filters = {}
        if request.args.get('team'):
            filters['team'] = request.args.get('team')
        if request.args.get('series'):
            filters['series'] = request.args.get('series')
        if request.args.get('position'):
            filters['position'] = request.args.get('position')
        if request.args.get('search'):
            filters['search'] = request.args.get('search')
        if request.args.get('min_ovr'):
            filters['min_ovr'] = request.args.get('min_ovr')
        if request.args.get('max_ovr'):
            filters['max_ovr'] = request.args.get('max_ovr')

        sort_by = request.args.get('sort_by', 'ovr')
        sort_order = request.args.get('sort_order', 'desc')
        page = int(request.args.get('page', 1))

        result = cards.get_actual_inventory_cards(filters, sort_by, sort_order, page)
        filter_options = cards.get_actual_inventory_filter_options()
        recent_audit = missions.get_mission_auto_update_audit(limit=15)

        return render_template(
            'actual_cards.html',
            cards_data=result['cards'],
            pagination=result,
            filter_options=filter_options,
            recent_audit=recent_audit,
        )
    except Exception as e:
        logger.error(f"Actual card tracker view error: {e}")
        return render_template('error.html', error=str(e))

@app.route('/cards/<uuid>')
def card_detail(uuid):
    """Card Detail Page"""
    try:
        card = cards.get_card_by_uuid(uuid)
        
        if not card:
            return render_template('error.html', error='Card not found'), 404
        
        # Parse JSON fields
        card['pitches'] = json.loads(card.get('pitches', '[]'))
        card['quirks'] = json.loads(card.get('quirks', '[]'))
        card['locations'] = json.loads(card.get('locations', '[]'))
        
        return render_template('card_detail.html', card=card)
    except Exception as e:
        logger.error(f"Card detail view error: {e}")
        return render_template('error.html', error=str(e))

@app.route('/cards/search')
def card_search():
    """Card Search Page"""
    return render_template('card_search.html')

@app.route('/api/cards/search', methods=['GET'])
def search_cards_api():
    """API endpoint to search cards by name"""
    try:
        name = request.args.get('name', '')
        
        if not name:
            return jsonify({'success': False, 'error': 'Name parameter required'}), 400
        
        results = api_client.search_cards_by_name(name)
        
        return jsonify({'success': True, 'results': results})
    except Exception as e:
        logger.error(f"Card search API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/cards/search/advanced', methods=['GET'])
def search_cards_advanced_api():
    """API endpoint for advanced card search with filters"""
    try:
        # Get all filter parameters
        name = request.args.get('name', '')
        rarity = request.args.get('rarity', '')
        position = request.args.get('position', '')
        team = request.args.get('team', '')
        series_id = request.args.get('series_id', '')
        min_sell_price = request.args.get('min_sell_price', '')
        max_sell_price = request.args.get('max_sell_price', '')
        min_buy_price = request.args.get('min_buy_price', '')
        max_buy_price = request.args.get('max_buy_price', '')
        sort = request.args.get('sort', 'rank')
        order = request.args.get('order', 'desc')
        page = request.args.get('page', '1')
        
        # Convert empty strings to None for optional parameters
        series_id = int(series_id) if series_id else None
        min_sell_price = int(min_sell_price) if min_sell_price else None
        max_sell_price = int(max_sell_price) if max_sell_price else None
        min_buy_price = int(min_buy_price) if min_buy_price else None
        max_buy_price = int(max_buy_price) if max_buy_price else None
        page = int(page) if page else 1
        
        # Call advanced search
        result = api_client.search_cards_advanced(
            name=name if name else None,
            rarity=rarity if rarity else None,
            position=position if position else None,
            team=team if team else None,
            series_id=series_id,
            min_sell_price=min_sell_price,
            max_sell_price=max_sell_price,
            min_buy_price=min_buy_price,
            max_buy_price=max_buy_price,
            sort=sort,
            order=order,
            page=page
        )
        
        return jsonify({
            'success': True,
            'results': result.get('listings', []),
            'page': result.get('page', 1),
            'total_pages': result.get('total_pages', 0)
        })
    except Exception as e:
        logger.error(f"Advanced card search API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/cards/catalog/status', methods=['GET'])
def card_catalog_status_api():
    """Get local card catalog cache status"""
    try:
        status = cards.get_card_catalog_status()
        progress = api_client.get_sync_progress()
        return jsonify({'success': True, 'status': status, 'sync_progress': progress})
    except Exception as e:
        logger.error(f"Card catalog status API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/cards/catalog/sync', methods=['POST'])
def sync_card_catalog_api():
    """Start a background sync of all cards from Items API into local catalog cache."""
    try:
        started = api_client.sync_card_catalog_background()
        progress = api_client.get_sync_progress()

        if not started:
            return jsonify({'success': False, 'already_running': True, 'sync_progress': progress})

        return jsonify({'success': True, 'started': True, 'sync_progress': progress})
    except Exception as e:
        logger.error(f"Card catalog sync API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/cards/catalog/sync/progress', methods=['GET'])
def sync_catalog_progress_api():
    """Poll the current catalog sync progress."""
    try:
        progress = api_client.get_sync_progress()
        status = cards.get_card_catalog_status()
        return jsonify({'success': True, 'sync_progress': progress, 'status': status})
    except Exception as e:
        logger.error(f"Sync progress API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/cards/actual/sync', methods=['POST'])
def sync_actual_inventory_api():
    """Manual sync endpoint for Beta 3.0 Actual Card Tracker."""
    try:
        result = cards.sync_actual_inventory(sync_source='actual_card_tracker_manual')
        status_code = 200 if result.get('success') else 500
        return jsonify(result), status_code
    except Exception as e:
        logger.error(f"Actual inventory sync API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/cards/actual/audit', methods=['GET'])
def actual_inventory_audit_api():
    """Read recent mission auto-update audit entries generated by actual inventory sync."""
    try:
        limit = int(request.args.get('limit', 50))
        entries = missions.get_mission_auto_update_audit(limit=limit)
        return jsonify({'success': True, 'entries': entries})
    except Exception as e:
        logger.error(f"Actual inventory audit API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/cards/actual/sell', methods=['POST'])
def sell_actual_inventory_card_api():
    """Sell quantity from a card in Actual Card Tracker inventory."""
    try:
        data = request.json or {}
        uuid = (data.get('uuid') or '').strip()
        quantity_sold = data.get('quantity_sold')
        stubs_per_card = data.get('stubs_per_card')

        if not uuid:
            return jsonify({'success': False, 'error': 'uuid is required'}), 400

        result = cards.sell_inventory_card(uuid, quantity_sold, stubs_per_card)
        status_code = 200 if result.get('success') else 400
        return jsonify(result), status_code
    except Exception as e:
        logger.error(f"Sell actual inventory card API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/cards/actual/transactions', methods=['GET'])
def actual_inventory_transactions_api():
    """Read sell transactions history for Actual Card Tracker."""
    try:
        limit = int(request.args.get('limit', 300))
        entries = cards.get_actual_inventory_transactions(limit=limit)
        return jsonify({'success': True, 'entries': entries})
    except Exception as e:
        logger.error(f"Actual inventory transactions API error: {e}")
        return jsonify({'success': False, 'error': str(e), 'entries': []}), 500


@app.route('/api/settings/api-auth', methods=['GET'])
def get_api_auth_settings_api():
    """Get masked API auth settings for local UI management."""
    try:
        settings_payload = app_settings.get_masked_api_auth_settings()
        return jsonify({'success': True, 'settings': settings_payload})
    except Exception as e:
        logger.error(f"Get API auth settings error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/api-auth', methods=['POST'])
def save_api_auth_settings_api():
    """Save API auth settings to a local untracked config file."""
    try:
        data = request.json or {}
        saved = app_settings.save_api_auth_settings(data)
        return jsonify({'success': True, 'settings': saved})
    except Exception as e:
        logger.error(f"Save API auth settings error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/api-auth/test', methods=['POST'])
def test_api_auth_settings_api():
    """Test configured API auth by attempting inventory fetch."""
    try:
        result = api_client.get_owned_inventory_cards()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Test API auth settings error: {e}")
        return jsonify({'success': False, 'error': str(e), 'cards': []}), 500

@app.route('/api/cards/search/unified', methods=['GET'])
def search_cards_unified_api():
    """Search both listings API (sellable) and local catalog / live Items fallback."""
    try:
        name = request.args.get('name', '').strip()
        if not name:
            return jsonify({'success': False, 'error': 'Name parameter required'}), 400

        rarity = request.args.get('rarity', '')
        position = request.args.get('position', '')
        team = request.args.get('team', '')
        series_id = request.args.get('series_id', '')
        min_sell_price = request.args.get('min_sell_price', '')
        max_sell_price = request.args.get('max_sell_price', '')
        min_buy_price = request.args.get('min_buy_price', '')
        max_buy_price = request.args.get('max_buy_price', '')
        sort = request.args.get('sort', 'rank')
        order = request.args.get('order', 'desc')
        page = request.args.get('page', '1')

        series_id = int(series_id) if series_id else None
        min_sell_price = int(min_sell_price) if min_sell_price else None
        max_sell_price = int(max_sell_price) if max_sell_price else None
        min_buy_price = int(min_buy_price) if min_buy_price else None
        max_buy_price = int(max_buy_price) if max_buy_price else None
        page = int(page) if page else 1

        # --- Purchasable cards via Listings API ---
        listings_result = api_client.search_cards_advanced(
            name=name,
            rarity=rarity if rarity else None,
            position=position if position else None,
            team=team if team else None,
            series_id=series_id,
            min_sell_price=min_sell_price,
            max_sell_price=max_sell_price,
            min_buy_price=min_buy_price,
            max_buy_price=max_buy_price,
            sort=sort,
            order=order,
            page=page
        )

        purchasable_cards = []
        purchasable_uuids = set()

        for listing in listings_result.get('listings', []):
            item = listing.get('item', {}) if isinstance(listing, dict) else {}
            if not item:
                continue
            card_uuid = item.get('uuid')
            if card_uuid:
                purchasable_uuids.add(card_uuid)
            purchasable_cards.append({
                'uuid': card_uuid,
                'name': item.get('name'),
                'rarity': item.get('rarity'),
                'ovr': item.get('ovr'),
                'display_position': item.get('display_position'),
                'team': item.get('team'),
                'team_short_name': item.get('team_short_name'),
                'series': item.get('series'),
                'series_year': item.get('series_year'),
                'img': item.get('img'),
                'best_sell_price': listing.get('best_sell_price'),
                'best_buy_price': listing.get('best_buy_price'),
                'is_sellable': True,
                'source': 'listings',
            })

        # --- Earned/Reward cards: local catalog first, live fallback if empty ---
        catalog_status = cards.get_card_catalog_status()
        catalog_source = 'catalog'
        catalog_is_partial = False

        if catalog_status['is_ready']:
            catalog_cards = cards.search_local_catalog(
                name=name,
                rarity=rarity if rarity else None,
                position=position if position else None,
                team=team if team else None,
                series_id=series_id,
                limit=200
            )
        else:
            # Catalog not synced — fall back to a limited live Items API scan
            live_result = api_client.search_items_api_live(name, max_pages=8)
            raw_hits = live_result.get('cards', [])

            # Apply optional filters client-side
            catalog_cards = []
            for card in raw_hits:
                if rarity and (card.get('rarity') or '').lower() != rarity.lower():
                    continue
                if position and card.get('display_position') != position:
                    continue
                if team and card.get('team') != team and card.get('team_short_name') != team:
                    continue
                catalog_cards.append({
                    'uuid': card.get('uuid'),
                    'name': card.get('name'),
                    'rarity': card.get('rarity'),
                    'ovr': card.get('ovr'),
                    'display_position': card.get('display_position'),
                    'team': card.get('team') or card.get('team_short_name'),
                    'series': card.get('series'),
                })

            catalog_source = 'live_fallback'
            catalog_is_partial = live_result.get('is_partial', True)

        reward_cards = []
        for card in catalog_cards:
            card_uuid = card.get('uuid')
            if not card_uuid or card_uuid in purchasable_uuids:
                continue
            reward_cards.append({
                'uuid': card_uuid,
                'name': card.get('name'),
                'rarity': card.get('rarity'),
                'ovr': card.get('ovr'),
                'display_position': card.get('display_position'),
                'team': card.get('team'),
                'team_short_name': card.get('team'),
                'series': card.get('series'),
                'series_year': None,
                'img': None,
                'best_sell_price': None,
                'best_buy_price': None,
                'is_sellable': False,
                'source': catalog_source,
            })

        return jsonify({
            'success': True,
            'purchasable_cards': purchasable_cards,
            'reward_cards': reward_cards,
            'page': listings_result.get('page', 1),
            'total_pages': listings_result.get('total_pages', 0),
            'catalog_ready': catalog_status['is_ready'],
            'catalog_is_partial': catalog_is_partial,
            'catalog_source': catalog_source,
        })
    except Exception as e:
        logger.error(f"Unified card search API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/cards', methods=['POST'])
def create_card_api():
    """API endpoint to create a new card"""
    try:
        data = request.json
        uuid = data.get('uuid')
        user_data = data.get('user_data', {})
        
        if not uuid:
            return jsonify({'success': False, 'error': 'UUID required'}), 400
        
        created_uuid = cards.create_card(uuid, user_data)
        
        if created_uuid:
            return jsonify({'success': True, 'uuid': created_uuid})
        else:
            return jsonify({'success': False, 'error': 'Failed to create card'}), 500
    except Exception as e:
        logger.error(f"Create card API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/cards/manual', methods=['POST'])
def create_card_manual_api():
    """API endpoint to manually create a card (for non-marketplace cards)"""
    try:
        data = request.json
        
        # Validate required fields
        required_fields = ['player_name', 'ovr', 'display_position', 'series', 'rarity']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400
        
        created_uuid = cards.create_card_manual(data)
        
        if created_uuid:
            return jsonify({'success': True, 'uuid': created_uuid})
        else:
            return jsonify({'success': False, 'error': 'Failed to create manual card'}), 500
    except Exception as e:
        logger.error(f"Create manual card API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/cards/<uuid>', methods=['PUT'])
def update_card_api(uuid):
    """API endpoint to update a card"""
    try:
        data = request.json
        success = cards.update_card(uuid, data)
        
        if success:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Failed to update card'}), 500
    except Exception as e:
        logger.error(f"Update card API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/cards/delete', methods=['POST'])
def delete_cards_api():
    """API endpoint to delete cards"""
    try:
        data = request.json
        uuids = data.get('uuids', [])
        
        # Create backup before deletion
        backup_cards('bulk_delete')
        
        deleted_count = cards.delete_cards(uuids)
        
        return jsonify({'success': True, 'deleted_count': deleted_count})
    except Exception as e:
        logger.error(f"Delete cards API error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/home/glance', methods=['GET'])
def home_glance_api():
    """Today at a Glance stats for the home page. Returns partial results on individual source failures."""
    result = {}

    # Live / total MLB games today
    try:
        ticker = scores.get_ticker_payload()
        games = ticker.get('games', [])
        result['live_games'] = sum(1 for g in games if g.get('state') == 'Live')
        result['total_games'] = ticker.get('total_games', len(games))
    except Exception as e:
        logger.error(f"Home glance scores error: {e}")
        result['live_games'] = None
        result['total_games'] = None

    # At-Bat missions
    try:
        conn = sqlite3.connect(str(MISSIONS_DB))
        conn.row_factory = sqlite3.Row
        with conn:
            row = conn.execute("SELECT COUNT(*) as cnt FROM missions WHERE priority = 'At-Bat'").fetchone()
        result['at_bat_missions'] = row['cnt'] if row else 0
    except Exception as e:
        logger.error(f"Home glance missions error: {e}")
        result['at_bat_missions'] = None

    # Total tracked cards
    try:
        conn = sqlite3.connect(str(CARDS_DB))
        conn.row_factory = sqlite3.Row
        with conn:
            row = conn.execute('SELECT COUNT(*) as cnt FROM cards').fetchone()
        result['tracked_cards'] = row['cnt'] if row else 0
    except Exception as e:
        logger.error(f"Home glance tracked cards error: {e}")
        result['tracked_cards'] = None

    # Owned inventory cards
    try:
        conn = sqlite3.connect(str(CARDS_DB))
        conn.row_factory = sqlite3.Row
        with conn:
            row = conn.execute("SELECT COUNT(*) as cnt FROM cards WHERE quantity > 0 AND card_status = 'Active'").fetchone()
        result['owned_cards'] = row['cnt'] if row else 0
    except Exception as e:
        logger.error(f"Home glance owned cards error: {e}")
        result['owned_cards'] = None

    return jsonify({'success': True, **result})


@app.route('/api/scores/ticker', methods=['GET'])
def scores_ticker_api():
    """Ticker payload endpoint for bottom MLB score ticker."""
    try:
        date_str = request.args.get('date', '').strip() or None
        payload = scores.get_ticker_payload(date_str=date_str)
        status_code = 200 if payload.get('success') else 502
        return jsonify(payload), status_code
    except Exception as e:
        logger.error(f"Scores ticker API error: {e}")
        return jsonify({'success': False, 'error': str(e), 'games': []}), 500


@app.route('/api/scores/dashboard', methods=['GET'])
def scores_dashboard_api():
    """Dashboard payload endpoint for expanded MLB score details."""
    try:
        date_str = request.args.get('date', '').strip() or None
        payload = scores.get_dashboard_payload(date_str=date_str)
        status_code = 200 if payload.get('success') else 502
        return jsonify(payload), status_code
    except Exception as e:
        logger.error(f"Scores dashboard API error: {e}")
        return jsonify({'success': False, 'error': str(e), 'games': []}), 500


@app.route('/api/scores/standings', methods=['GET'])
def scores_standings_api():
    """Current MLB standings by league and division."""
    try:
        payload = scores.get_standings_payload()
        status_code = 200 if payload.get('success') else 502
        return jsonify(payload), status_code
    except Exception as e:
        logger.error(f"Scores standings API error: {e}")
        return jsonify({'success': False, 'error': str(e), 'standings': None}), 500


@app.route('/api/scores/lineups/<int:game_pk>', methods=['GET'])
def scores_lineups_api(game_pk):
    """Away/home lineup payload for a specific game PK."""
    try:
        payload = scores.get_game_lineups_payload(game_pk)
        status_code = 200 if payload.get('success') else 502
        return jsonify(payload), status_code
    except Exception as e:
        logger.error(f"Scores lineups API error: {e}")
        return jsonify({'success': False, 'error': str(e), 'game_pk': game_pk}), 500


@app.route('/api/scores/events/home-runs/<int:game_pk>', methods=['GET'])
def scores_home_run_events_api(game_pk):
    """Home run event feed for alerting on a specific game."""
    try:
        payload = scores.get_game_home_run_events_payload(game_pk)
        status_code = 200 if payload.get('success') else 502
        return jsonify(payload), status_code
    except Exception as e:
        logger.error(f"Scores home run events API error: {e}")
        return jsonify({'success': False, 'error': str(e), 'game_pk': game_pk, 'events': []}), 500


@app.route('/api/scores/at-bats', methods=['GET'])
def scores_at_bats_api():
    """Recent at-bat feed aggregated across live games."""
    try:
        date_str = request.args.get('date', '').strip() or None
        limit = int(request.args.get('limit', 80))
        payload = scores.get_at_bat_feed_payload(date_str=date_str, limit=limit)
        status_code = 200 if payload.get('success') else 502
        return jsonify(payload), status_code
    except Exception as e:
        logger.error(f"Scores at-bats API error: {e}")
        return jsonify({'success': False, 'error': str(e), 'entries': []}), 500


@app.route('/api/scores/at-bats/<int:game_pk>', methods=['GET'])
def scores_game_at_bats_api(game_pk):
    """Recent at-bat feed for a specific game."""
    try:
        limit = int(request.args.get('limit', 40))
        payload = scores.get_game_at_bat_feed_payload(game_pk=game_pk, limit=limit)
        status_code = 200 if payload.get('success') else 502
        return jsonify(payload), status_code
    except Exception as e:
        logger.error(f"Scores game at-bats API error: {e}")
        return jsonify({'success': False, 'error': str(e), 'game_pk': game_pk, 'entries': []}), 500

# ========================
# Error Handlers
# ========================

@app.errorhandler(404)
def not_found(error):
    return render_template('error.html', error='Page not found'), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', error='Internal server error'), 500

# ========================
# Application Entry Point
# ========================

if __name__ == '__main__':
    logger.info("Starting MLB The Show 26 DD Tracker")
    app.run(debug=DEBUG, host='127.0.0.1', port=5000)
