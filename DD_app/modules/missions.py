"""
SQLite-backed Mission Tracker module (Beta 2.3).
"""

import sqlite3
import re
from datetime import datetime
from typing import Iterable

from config import MISSIONS_DB, CARDS_DB
from modules.logger import logger

MISSION_TYPE_OPTIONS = {'Single Card', 'Grouped Card', 'Moment', 'Other'}
TRACKING_MODE_OPTIONS = {'Quantity Count', 'Moment Count'}
PRIORITY_OPTIONS = {'At-Bat', 'On-Deck', 'In the Hole'}
PROGRAM_TYPE_OPTIONS = {'TA', 'WBC', 'Player', 'Spotlight', 'not-assigned'}
PRIORITY_SCORE_MAP = {'At-Bat': 3, 'On-Deck': 2, 'In the Hole': 1}
AUTO_GRIND_DIAMOND_POSITIONS = {'P', 'C', '1B', '2B', '3B', 'SS', 'LF', 'CF', 'RF'}
AUTO_GRIND_POSITION_BUCKETS = ('P', 'C', '1B', '2B', '3B', 'SS', 'LF', 'CF', 'RF', 'BENCH', 'UTILITY')
OWNERSHIP_STATE_META = {
    'IN_INVENTORY': {
        'label': 'Owned - In Inventory',
        'color_token': 'green',
        'rank': 1,
    },
    'OWNED_NOT_IN_INVENTORY': {
        'label': 'Owned - Not On Team',
        'color_token': 'yellow',
        'rank': 2,
    },
    'NOT_OWNED': {
        'label': 'Not Owned',
        'color_token': 'red',
        'rank': 3,
    },
}


def _to_int(value, default=0):
    try:
        if value is None or str(value).strip() == '':
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _normalize_mission_type(value):
    mission_type = (value or '').strip()
    if mission_type in MISSION_TYPE_OPTIONS:
        return mission_type
    return 'Other'


def _normalize_tracking_mode(value):
    tracking_mode = (value or '').strip()
    if tracking_mode in TRACKING_MODE_OPTIONS:
        return tracking_mode
    return 'Quantity Count'


def _normalize_priority(value):
    priority = (value or '').strip()
    if priority in PRIORITY_OPTIONS:
        return priority
    return 'At-Bat'


def _normalize_program_type(value):
    program_type = (value or '').strip()
    if program_type:
        return program_type
    return 'not-assigned'


def _priority_label_from_score(score):
    score = float(score or 0)
    if score >= 2.5:
        return 'At-Bat'
    if score >= 1.5:
        return 'On-Deck'
    return 'In the Hole'


def _derive_progress_status(mission_total, current_status):
    mission_total = max(1, _to_int(mission_total, 1))
    current_status = max(0, min(_to_int(current_status, 0), mission_total))
    progress_percent = round((current_status / mission_total) * 100, 2)

    if current_status <= 0:
        status = 'Not Started'
    elif current_status >= mission_total:
        status = 'Completed'
    else:
        status = 'In-Progress'

    return mission_total, current_status, progress_percent, status


def get_missions_connection():
    conn = sqlite3.connect(MISSIONS_DB)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def _get_cards_connection():
    conn = sqlite3.connect(CARDS_DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_missions_db():
    try:
        conn = get_missions_connection()
        cursor = conn.cursor()

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS missions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                program_category TEXT NOT NULL,
                mission_name TEXT NOT NULL,
                mission_type TEXT NOT NULL,
                tracking_mode TEXT NOT NULL DEFAULT 'Quantity Count',
                mission_total INTEGER NOT NULL,
                current_status INTEGER NOT NULL DEFAULT 0,
                priority TEXT NOT NULL,
                acquired TEXT NOT NULL DEFAULT 'Not Acquired',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS programs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                program_category TEXT NOT NULL UNIQUE,
                program_type TEXT NOT NULL DEFAULT 'not-assigned',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS sync_players (
                player_id TEXT PRIMARY KEY,
                player_name TEXT NOT NULL,
                primary_position TEXT,
                secondary_positions TEXT,
                team TEXT,
                source_card_uuid TEXT,
                series TEXT,
                ovr INTEGER,
                last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS mission_players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mission_id INTEGER NOT NULL,
                player_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (mission_id) REFERENCES missions(id) ON DELETE CASCADE,
                FOREIGN KEY (player_id) REFERENCES sync_players(player_id) ON DELETE CASCADE,
                UNIQUE(mission_id, player_id)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS dynamic_selections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                field_name TEXT NOT NULL,
                value TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(field_name, value)
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS mission_auto_update_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mission_id INTEGER NOT NULL,
                player_id TEXT NOT NULL,
                sync_source TEXT NOT NULL DEFAULT 'actual_card_tracker',
                previous_acquired TEXT,
                previous_priority TEXT,
                new_acquired TEXT NOT NULL,
                new_priority TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (mission_id) REFERENCES missions(id) ON DELETE CASCADE
            )
            '''
        )

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_missions_program ON missions(program_category)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_missions_priority ON missions(priority)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_missions_created_at ON missions(created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_programs_category ON programs(program_category)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_programs_type ON programs(program_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_mission_players_mission_id ON mission_players(mission_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_mission_players_player_id ON mission_players(player_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_players_name ON sync_players(player_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_mission_auto_update_mission_id ON mission_auto_update_audit(mission_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_mission_auto_update_created_at ON mission_auto_update_audit(created_at)')

        # Backfill existing program categories as TA when programs table is first introduced.
        cursor.execute(
            '''
            INSERT OR IGNORE INTO programs (program_category, program_type)
            SELECT DISTINCT program_category, 'TA'
            FROM missions
            WHERE TRIM(COALESCE(program_category, '')) <> ''
            '''
        )

        # Migration: Add acquired column to missions if it does not exist.
        try:
            cursor.execute('SELECT acquired FROM missions LIMIT 1')
        except sqlite3.OperationalError:
            logger.info('Adding acquired column to missions table...')
            cursor.execute("ALTER TABLE missions ADD COLUMN acquired TEXT NOT NULL DEFAULT 'Not Acquired'")

        # Migration: Add secondary_positions to sync_players if it does not exist.
        try:
            cursor.execute('SELECT secondary_positions FROM sync_players LIMIT 1')
        except sqlite3.OperationalError:
            logger.info('Adding secondary_positions column to sync_players table...')
            cursor.execute('ALTER TABLE sync_players ADD COLUMN secondary_positions TEXT')

        # Migration: Rebuild sync_players to use name::series as primary key
        # (old schema had source_card_uuid TEXT UNIQUE which prevented multiple OVR versions).
        cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='sync_players'"
        )
        sync_ddl_row = cursor.fetchone()
        if sync_ddl_row and 'source_card_uuid TEXT UNIQUE' in (sync_ddl_row['sql'] or ''):
            logger.info('Rebuilding sync_players table for name::series composite key...')
            cursor.execute('DROP TABLE IF EXISTS mission_players')
            cursor.execute('DROP TABLE IF EXISTS sync_players')
            cursor.execute(
                '''
                CREATE TABLE sync_players (
                    player_id TEXT PRIMARY KEY,
                    player_name TEXT NOT NULL,
                    primary_position TEXT,
                    secondary_positions TEXT,
                    team TEXT,
                    source_card_uuid TEXT,
                    series TEXT,
                    ovr INTEGER,
                    last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                '''
            )
            cursor.execute(
                '''
                CREATE TABLE mission_players (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mission_id INTEGER NOT NULL,
                    player_id TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (mission_id) REFERENCES missions(id) ON DELETE CASCADE,
                    FOREIGN KEY (player_id) REFERENCES sync_players(player_id) ON DELETE CASCADE,
                    UNIQUE(mission_id, player_id)
                )
                '''
            )
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_mission_players_mission_id ON mission_players(mission_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_mission_players_player_id ON mission_players(player_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sync_players_name ON sync_players(player_name)')
            logger.info('sync_players rebuilt with name::series primary key.')

        conn.commit()
        conn.close()
        logger.info('Missions database initialized successfully')
        return True
    except Exception as e:
        logger.error(f'Failed to initialize missions database: {e}')
        return False


def _persist_program_category(program_category):
    if not program_category:
        return
    conn = get_missions_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'INSERT OR IGNORE INTO dynamic_selections (field_name, value) VALUES (?, ?)',
            ('program_category', program_category),
        )
        conn.commit()
    finally:
        conn.close()


def _upsert_program(program_category, program_type='not-assigned', update_existing=False):
    program_category = (program_category or '').strip()
    if not program_category:
        return

    normalized_type = _normalize_program_type(program_type)
    conn = get_missions_connection()
    cursor = conn.cursor()

    try:
        if update_existing:
            cursor.execute(
                '''
                INSERT INTO programs (program_category, program_type)
                VALUES (?, ?)
                ON CONFLICT(program_category) DO UPDATE SET program_type = excluded.program_type
                ''',
                (program_category, normalized_type),
            )
        else:
            cursor.execute(
                'INSERT OR IGNORE INTO programs (program_category, program_type) VALUES (?, ?)',
                (program_category, normalized_type),
            )

        cursor.execute(
            'INSERT OR IGNORE INTO dynamic_selections (field_name, value) VALUES (?, ?)',
            ('program_category', program_category),
        )
        conn.commit()
    finally:
        conn.close()


def _get_program_type_map(program_categories):
    categories = [c for c in (program_categories or []) if c]
    if not categories:
        return {}

    placeholders = ','.join('?' for _ in categories)
    conn = get_missions_connection()
    cursor = conn.cursor()
    cursor.execute(
        f'SELECT program_category, program_type FROM programs WHERE program_category IN ({placeholders})',
        categories,
    )
    result = {r['program_category']: _normalize_program_type(r['program_type']) for r in cursor.fetchall()}
    conn.close()
    return result


def sync_players_from_catalog():
    """Populate sync_players from cards.db card_catalog (full game catalog)."""
    try:
        cards_conn = _get_cards_connection()
        cards_cursor = cards_conn.cursor()
        cards_cursor.execute(
            '''
            SELECT uuid, name, display_position, display_secondary_positions, team, series, ovr
            FROM card_catalog
            WHERE uuid IS NOT NULL AND name IS NOT NULL
            '''
        )
        catalog_rows = cards_cursor.fetchall()
        cards_conn.close()

        if not catalog_rows:
            return {'synced': 0}

        missions_conn = get_missions_connection()
        missions_cursor = missions_conn.cursor()

        for row in catalog_rows:
            player_series = (row['series'] or 'Unknown').strip()
            composite_id = f"{row['name']}::{player_series}"
            missions_cursor.execute(
                '''
                INSERT INTO sync_players (
                    player_id,
                    player_name,
                    primary_position,
                    secondary_positions,
                    team,
                    source_card_uuid,
                    series,
                    ovr,
                    last_synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(player_id) DO UPDATE SET
                    primary_position = excluded.primary_position,
                    secondary_positions = excluded.secondary_positions,
                    team = excluded.team,
                    source_card_uuid = CASE WHEN excluded.ovr >= COALESCE(sync_players.ovr, 0)
                                            THEN excluded.source_card_uuid
                                            ELSE sync_players.source_card_uuid END,
                    ovr = MAX(excluded.ovr, COALESCE(sync_players.ovr, 0)),
                    last_synced_at = CURRENT_TIMESTAMP
                ''',
                (
                    composite_id,
                    row['name'],
                    row['display_position'],
                    row['display_secondary_positions'],
                    row['team'],
                    row['uuid'],
                    player_series,
                    row['ovr'] if row['ovr'] is not None else 0,
                ),
            )

        missions_conn.commit()
        missions_conn.close()
        return {'synced': len(catalog_rows)}
    except Exception as e:
        logger.error(f'Failed to sync players from catalog: {e}')
        return {'synced': 0, 'error': str(e)}


def _owned_player_ids(player_ids: Iterable[str]):
    ids = [pid for pid in (player_ids or []) if pid]
    if not ids:
        return set()

    # Look up source_card_uuid for each player_id (name::series composite key)
    placeholders = ','.join('?' for _ in ids)
    missions_conn = get_missions_connection()
    missions_cursor = missions_conn.cursor()
    missions_cursor.execute(
        f'SELECT player_id, source_card_uuid FROM sync_players WHERE player_id IN ({placeholders})',
        ids,
    )
    pid_to_uuid = {r['player_id']: r['source_card_uuid'] for r in missions_cursor.fetchall()}
    missions_conn.close()

    uuids = [v for v in pid_to_uuid.values() if v]
    if not uuids:
        return set()

    uuid_placeholders = ','.join('?' for _ in uuids)
    cards_conn = _get_cards_connection()
    cards_cursor = cards_conn.cursor()
    cards_cursor.execute(f'SELECT uuid FROM cards WHERE uuid IN ({uuid_placeholders})', uuids)
    owned_uuids = {r['uuid'] for r in cards_cursor.fetchall()}
    cards_conn.close()

    return {pid for pid, uuid in pid_to_uuid.items() if uuid in owned_uuids}


def search_sync_players(query='', limit=120):
    sync_players_from_catalog()

    conn = get_missions_connection()
    cursor = conn.cursor()

    sql = (
        'SELECT player_id, player_name, primary_position, secondary_positions, team, series, ovr '
        'FROM sync_players WHERE 1=1'
    )
    params = []
    if query:
        sql += ' AND player_name LIKE ?'
        params.append(f'%{query.strip()}%')

    sql += ' ORDER BY player_name ASC LIMIT ?'
    params.append(limit)

    cursor.execute(sql, params)
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()

    owned_ids = _owned_player_ids([r['player_id'] for r in rows])
    for row in rows:
        row['ownership_status'] = 'Owned' if row['player_id'] in owned_ids else 'Not Owned'

    return rows


def get_mission_players(mission_id):
    conn = get_missions_connection()
    cursor = conn.cursor()
    cursor.execute(
        '''
        SELECT sp.player_id, sp.player_name, sp.primary_position, sp.secondary_positions, sp.team, sp.series, sp.ovr
        FROM mission_players mp
        JOIN sync_players sp ON sp.player_id = mp.player_id
        WHERE mp.mission_id = ?
        ORDER BY sp.player_name ASC
        ''',
        (mission_id,),
    )
    players = [dict(r) for r in cursor.fetchall()]
    conn.close()

    owned_ids = _owned_player_ids([p['player_id'] for p in players])
    for player in players:
        player['ownership_status'] = 'Owned' if player['player_id'] in owned_ids else 'Not Owned'

    return players


def set_mission_players(mission_id, player_ids):
    conn = get_missions_connection()
    cursor = conn.cursor()

    normalized_ids = list(dict.fromkeys([pid for pid in (player_ids or []) if pid]))

    cursor.execute('DELETE FROM mission_players WHERE mission_id = ?', (mission_id,))

    if normalized_ids:
        placeholders = ','.join('?' for _ in normalized_ids)
        cursor.execute(
            f'SELECT player_id FROM sync_players WHERE player_id IN ({placeholders})',
            normalized_ids,
        )
        valid_ids = [r['player_id'] for r in cursor.fetchall()]

        for player_id in valid_ids:
            cursor.execute(
                'INSERT OR IGNORE INTO mission_players (mission_id, player_id) VALUES (?, ?)',
                (mission_id, player_id),
            )
    else:
        valid_ids = []

    cursor.execute('SELECT acquired FROM missions WHERE id = ?', (mission_id,))
    mission_row = cursor.fetchone()
    mission_acquired = (mission_row['acquired'] if mission_row else 'Not Acquired') == 'Acquired'

    conn.commit()
    conn.close()

    auto_card_result = _auto_add_linked_players_to_card_tracker(valid_ids)
    auto_inventory_result = {'added_to_inventory': 0, 'existing_promoted': 0, 'failed': 0}
    if mission_acquired and valid_ids:
        auto_inventory_result = _auto_add_players_to_inventory(valid_ids)

    return {
        **auto_card_result,
        **auto_inventory_result,
    }


def _auto_add_linked_players_to_card_tracker(player_ids):
    normalized_ids = list(dict.fromkeys([pid for pid in (player_ids or []) if (pid or '').strip()]))
    if not normalized_ids:
        return {'added_to_card_tracker': 0, 'already_in_card_tracker': 0, 'failed_to_add': 0}

    conn = get_missions_connection()
    cursor = conn.cursor()
    placeholders = ','.join('?' for _ in normalized_ids)
    cursor.execute(
        f'SELECT player_id, source_card_uuid FROM sync_players WHERE player_id IN ({placeholders})',
        normalized_ids,
    )
    rows = cursor.fetchall()
    conn.close()

    player_to_uuid = {row['player_id']: (row['source_card_uuid'] or '').strip() for row in rows}
    uuids = list(dict.fromkeys([uuid for uuid in player_to_uuid.values() if uuid]))
    if not uuids:
        return {'added_to_card_tracker': 0, 'already_in_card_tracker': 0, 'failed_to_add': len(normalized_ids)}

    cards_conn = _get_cards_connection()
    cards_cursor = cards_conn.cursor()
    uuid_placeholders = ','.join('?' for _ in uuids)
    cards_cursor.execute(
        f'SELECT uuid FROM cards WHERE uuid IN ({uuid_placeholders})',
        uuids,
    )
    existing_uuids = {row['uuid'] for row in cards_cursor.fetchall()}
    cards_conn.close()

    added = 0
    already = len(existing_uuids)
    failed = 0

    from modules import cards as cards_module

    for uuid in uuids:
        if uuid in existing_uuids:
            continue
        created_uuid = cards_module.create_card(
            uuid,
            {
                'purchased_price': None,
                'quantity': 1,
                'on_team': False,
                'grind_card': False,
                'pxp': 0,
                'comments': 'Auto-added from mission player link',
                'inside_edge': '',
            },
        )
        if created_uuid:
            added += 1
        else:
            failed += 1

    return {
        'added_to_card_tracker': added,
        'already_in_card_tracker': already,
        'failed_to_add': failed,
    }


def _auto_add_players_to_inventory(player_ids):
    normalized_ids = list(dict.fromkeys([pid for pid in (player_ids or []) if (pid or '').strip()]))
    if not normalized_ids:
        return {'added_to_inventory': 0, 'existing_promoted': 0, 'failed': 0}

    # Ensure cards exist first; linking should always seed card tracker.
    seed_result = _auto_add_linked_players_to_card_tracker(normalized_ids)

    conn = get_missions_connection()
    cursor = conn.cursor()
    placeholders = ','.join('?' for _ in normalized_ids)
    cursor.execute(
        f'SELECT player_id, source_card_uuid FROM sync_players WHERE player_id IN ({placeholders})',
        normalized_ids,
    )
    rows = cursor.fetchall()
    conn.close()

    uuids = list(dict.fromkeys([(row['source_card_uuid'] or '').strip() for row in rows if (row['source_card_uuid'] or '').strip()]))
    if not uuids:
        return {'added_to_inventory': 0, 'existing_promoted': 0, 'failed': len(normalized_ids)}

    now_iso = datetime.now().isoformat()
    cards_conn = _get_cards_connection()
    cards_cursor = cards_conn.cursor()
    uuid_placeholders = ','.join('?' for _ in uuids)
    cards_cursor.execute(
        f'SELECT uuid, quantity, on_team, card_status FROM cards WHERE uuid IN ({uuid_placeholders})',
        uuids,
    )
    existing_rows = {row['uuid']: row for row in cards_cursor.fetchall()}

    added_to_inventory = 0
    promoted = 0
    failed = 0

    for uuid in uuids:
        row = existing_rows.get(uuid)
        if not row:
            failed += 1
            continue

        quantity = _to_int(row['quantity'], 0)
        on_team = _to_int(row['on_team'], 0)
        card_status = (row['card_status'] or '').strip().lower()

        if quantity <= 0:
            cards_cursor.execute(
                '''
                UPDATE cards
                SET quantity = 1,
                    on_team = 1,
                    card_status = 'Active',
                    updated_at = ?
                WHERE uuid = ?
                ''',
                (now_iso, uuid),
            )
            added_to_inventory += 1
        elif on_team != 1 or card_status != 'active':
            cards_cursor.execute(
                '''
                UPDATE cards
                SET on_team = 1,
                    card_status = 'Active',
                    updated_at = ?
                WHERE uuid = ?
                ''',
                (now_iso, uuid),
            )
            promoted += 1

    cards_conn.commit()
    cards_conn.close()

    return {
        'added_to_inventory': added_to_inventory,
        'existing_promoted': promoted,
        'failed': failed + seed_result.get('failed_to_add', 0),
    }


def _row_to_mission(row):
    mission_total, current_status, progress_percent, status = _derive_progress_status(
        row['mission_total'], row['current_status']
    )

    completed_at = row['completed_at']
    if status == 'Completed' and not completed_at:
        completed_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return {
        'Mission ID': row['id'],
        'Program Category': row['program_category'],
        'Mission Name': row['mission_name'],
        'Mission Type': row['mission_type'],
        'Tracking Mode': row['tracking_mode'],
        'Mission Total': mission_total,
        'Current Status': current_status,
        'Progress Percent': progress_percent,
        'Status': status,
        'Priority': row['priority'],
        'Acquired': row['acquired'] if 'acquired' in row.keys() else 'Not Acquired',
        'Date Created': row['created_at'],
        'Date Completed': completed_at if status == 'Completed' else None,
    }


def _update_derived_fields(conn, mission_id):
    cursor = conn.cursor()
    cursor.execute(
        'SELECT id, mission_total, current_status FROM missions WHERE id = ?',
        (mission_id,),
    )
    row = cursor.fetchone()
    if not row:
        return

    mission_total, current_status, progress_percent, status = _derive_progress_status(
        row['mission_total'], row['current_status']
    )

    completed_at = None
    if status == 'Completed':
        completed_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    cursor.execute(
        '''
        UPDATE missions
        SET mission_total = ?,
            current_status = ?,
            completed_at = ?
        WHERE id = ?
        ''',
        (mission_total, current_status, completed_at, mission_id),
    )


def read_missions(program_category=None):
    conn = get_missions_connection()
    cursor = conn.cursor()

    sql = 'SELECT * FROM missions'
    params = []
    if program_category:
        sql += ' WHERE program_category = ?'
        params.append(program_category)

    sql += ' ORDER BY created_at DESC, id DESC'
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    conn.close()

    missions = []
    for row in rows:
        mission = _row_to_mission(row)
        missions.append(mission)

    return missions


def read_missions_with_players(program_category=None):
    missions = read_missions(program_category)
    for mission in missions:
        mission['Players'] = get_mission_players(mission['Mission ID'])
    return missions


def get_mission_by_id(mission_id):
    conn = get_missions_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM missions WHERE id = ?', (mission_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None

    mission = _row_to_mission(row)
    mission['Players'] = get_mission_players(mission_id)
    return mission


def add_mission(mission_data):
    sync_players_from_catalog()

    mission_total = _to_int(mission_data.get('mission_total'), 0)
    current_status = _to_int(mission_data.get('current_status'), 0)
    if mission_total <= 0:
        raise ValueError('Mission Total must be greater than 0')
    if current_status < 0 or current_status > mission_total:
        raise ValueError('Current Status must be between 0 and Mission Total')

    program_category = (mission_data.get('program_category') or '').strip()
    program_type = _normalize_program_type(mission_data.get('program_type'))
    mission_name = (mission_data.get('mission_name') or '').strip()
    mission_type = _normalize_mission_type(mission_data.get('mission_type'))
    tracking_mode = _normalize_tracking_mode(mission_data.get('tracking_mode'))
    priority = _normalize_priority(mission_data.get('priority'))
    acquired = mission_data.get('acquired', 'Not Acquired')
    if acquired not in ('Acquired', 'Not Acquired'):
        acquired = 'Not Acquired'

    if not program_category:
        raise ValueError('Program Category is required')
    if not mission_name:
        raise ValueError('Mission Name is required')

    conn = get_missions_connection()
    cursor = conn.cursor()
    cursor.execute(
        '''
        INSERT INTO missions (
            program_category,
            mission_name,
            mission_type,
            tracking_mode,
            mission_total,
            current_status,
            priority,
            acquired
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            program_category,
            mission_name,
            mission_type,
            tracking_mode,
            mission_total,
            current_status,
            priority,
            acquired,
        ),
    )

    mission_id = cursor.lastrowid
    _update_derived_fields(conn, mission_id)
    conn.commit()
    conn.close()

    _upsert_program(program_category, program_type)

    player_ids = mission_data.get('player_ids', [])
    if isinstance(player_ids, str):
        player_ids = [p.strip() for p in player_ids.split(',') if p.strip()]
    set_mission_players(mission_id, player_ids)

    logger.info('Mission created: %s (%s)', mission_name, program_category)
    return get_mission_by_id(mission_id)


def update_mission(mission_id, updates):
    conn = get_missions_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM missions WHERE id = ?', (mission_id,))
    existing = cursor.fetchone()
    if not existing:
        conn.close()
        return False

    fields = []
    values = []

    if 'program_category' in updates:
        program_category = (updates.get('program_category') or '').strip()
        if not program_category:
            raise ValueError('Program Category is required')
        fields.append('program_category = ?')
        values.append(program_category)
        _upsert_program(program_category, updates.get('program_type', 'not-assigned'))

    if 'mission_name' in updates:
        mission_name = (updates.get('mission_name') or '').strip()
        if not mission_name:
            raise ValueError('Mission Name is required')
        fields.append('mission_name = ?')
        values.append(mission_name)

    if 'mission_type' in updates:
        fields.append('mission_type = ?')
        values.append(_normalize_mission_type(updates.get('mission_type')))

    if 'tracking_mode' in updates:
        fields.append('tracking_mode = ?')
        values.append(_normalize_tracking_mode(updates.get('tracking_mode')))

    if 'priority' in updates:
        fields.append('priority = ?')
        values.append(_normalize_priority(updates.get('priority')))

    if 'acquired' in updates:
        acquired_val = updates.get('acquired', 'Not Acquired')
        if acquired_val not in ('Acquired', 'Not Acquired'):
            acquired_val = 'Not Acquired'
        fields.append('acquired = ?')
        values.append(acquired_val)

    status_override = (updates.get('status') or '').strip() if 'status' in updates else ''

    if 'mission_total' in updates:
        fields.append('mission_total = ?')
        values.append(_to_int(updates.get('mission_total'), existing['mission_total']))

    if 'current_status' in updates:
        fields.append('current_status = ?')
        values.append(_to_int(updates.get('current_status'), existing['current_status']))

    if status_override == 'Completed':
        fields.append('current_status = mission_total')
    elif status_override == 'Not Started':
        fields.append('current_status = 0')

    if fields:
        values.append(mission_id)
        cursor.execute(
            f'UPDATE missions SET {", ".join(fields)} WHERE id = ?',
            values,
        )

    _update_derived_fields(conn, mission_id)

    cursor.execute('SELECT mission_total, current_status FROM missions WHERE id = ?', (mission_id,))
    row = cursor.fetchone()
    if row:
        mission_total = _to_int(row['mission_total'], 0)
        current_status = _to_int(row['current_status'], 0)
        if mission_total <= 0:
            conn.rollback()
            conn.close()
            raise ValueError('Mission Total must be greater than 0')
        if current_status < 0 or current_status > mission_total:
            conn.rollback()
            conn.close()
            raise ValueError('Current Status must be between 0 and Mission Total')

    conn.commit()
    conn.close()

    if 'player_ids' in updates:
        player_ids = updates.get('player_ids', [])
        if isinstance(player_ids, str):
            player_ids = [p.strip() for p in player_ids.split(',') if p.strip()]
        set_mission_players(mission_id, player_ids)

    if updates.get('acquired') == 'Acquired':
        linked_players = get_mission_players(mission_id)
        linked_player_ids = [player.get('player_id') for player in linked_players if player.get('player_id')]
        _auto_add_players_to_inventory(linked_player_ids)

    logger.info('Mission %s updated', mission_id)
    return True


def delete_missions(mission_ids):
    ids = [int(m) for m in mission_ids if str(m).strip().isdigit()]
    if not ids:
        return 0

    conn = get_missions_connection()
    cursor = conn.cursor()
    placeholders = ','.join('?' for _ in ids)
    cursor.execute(f'DELETE FROM missions WHERE id IN ({placeholders})', ids)
    deleted = cursor.rowcount
    conn.commit()
    conn.close()

    logger.info('Deleted %s mission(s)', deleted)
    return deleted


def get_program_progress(existing_missions=None, program_type=None):
    missions = existing_missions if existing_missions is not None else read_missions()
    aggregates = {}

    for mission in missions:
        program = mission.get('Program Category') or 'Unassigned'
        if program not in aggregates:
            aggregates[program] = {
                'Program Category': program,
                'Total Missions': 0,
                'Completed Missions': 0,
                'In-Progress Missions': 0,
                'Not Started Missions': 0,
                'Current Sum': 0,
                'Total Sum': 0,
                'Priority Weighted Sum': 0.0,
                'Priority Weight Sum': 0.0,
            }

        agg = aggregates[program]
        mission_total = _to_int(mission.get('Mission Total'), 0)
        weight = max(1, mission_total)
        priority_label = _normalize_priority(mission.get('Priority'))
        agg['Total Missions'] += 1
        agg['Current Sum'] += _to_int(mission.get('Current Status'), 0)
        agg['Total Sum'] += mission_total
        agg['Priority Weighted Sum'] += PRIORITY_SCORE_MAP.get(priority_label, 3) * weight
        agg['Priority Weight Sum'] += weight

        status = mission.get('Status')
        if status == 'Completed':
            agg['Completed Missions'] += 1
        elif status == 'In-Progress':
            agg['In-Progress Missions'] += 1
        else:
            agg['Not Started Missions'] += 1

    program_type_map = _get_program_type_map(list(aggregates.keys()))
    normalized_filter = _normalize_program_type(program_type) if program_type else None

    results = []
    for program, agg in aggregates.items():
        total_sum = agg['Total Sum']
        completion = round((agg['Current Sum'] / total_sum) * 100, 2) if total_sum > 0 else 0.0
        priority_score = (
            round(agg['Priority Weighted Sum'] / agg['Priority Weight Sum'], 2)
            if agg['Priority Weight Sum'] > 0
            else 0.0
        )
        program_priority = _priority_label_from_score(priority_score)
        resolved_program_type = program_type_map.get(program, 'not-assigned')

        if normalized_filter and resolved_program_type != normalized_filter:
            continue

        if completion >= 100:
            program_status = 'Completed'
        elif completion <= 0:
            program_status = 'Not Started'
        else:
            program_status = 'In-Progress'

        results.append(
            {
                'Program Category': program,
                'Total Missions': agg['Total Missions'],
                'Completed Missions': agg['Completed Missions'],
                'In-Progress Missions': agg['In-Progress Missions'],
                'Not Started Missions': agg['Not Started Missions'],
                'Program Completion Percent': completion,
                'Program Status': program_status,
                'Program Type': resolved_program_type,
                'Program Priority': program_priority,
                'Program Priority Score': priority_score,
            }
        )

    results.sort(key=lambda x: x['Program Completion Percent'], reverse=True)
    return results


def get_program_missions(program_category):
    return read_missions_with_players(program_category)


def recalculate_mission_priorities(program_category=None, mission_ids=None):
    """Apply manual priority rules for missions based on status and inventory ownership."""
    conn = get_missions_connection()
    cursor = conn.cursor()

    sql = 'SELECT id, program_category, mission_total, current_status, priority FROM missions WHERE 1=1'
    params = []

    if program_category:
        sql += ' AND program_category = ?'
        params.append((program_category or '').strip())

    clean_ids = []
    for mid in mission_ids or []:
        if str(mid).strip().isdigit():
            clean_ids.append(int(mid))

    if clean_ids:
        placeholders = ','.join('?' for _ in clean_ids)
        sql += f' AND id IN ({placeholders})'
        params.extend(clean_ids)

    cursor.execute(sql, params)
    rows = [dict(r) for r in cursor.fetchall()]
    if not rows:
        conn.close()
        return {'evaluated': 0, 'updated': 0}

    target_ids = [row['id'] for row in rows]
    id_placeholders = ','.join('?' for _ in target_ids)
    cursor.execute(
        f'''
        SELECT mp.mission_id, mp.player_id
        FROM mission_players mp
        WHERE mp.mission_id IN ({id_placeholders})
        ''',
        target_ids,
    )
    mission_player_rows = cursor.fetchall()
    mission_to_players = {mid: [] for mid in target_ids}
    for row in mission_player_rows:
        mission_to_players.setdefault(row['mission_id'], []).append(row['player_id'])

    all_player_ids = list({row['player_id'] for row in mission_player_rows if row['player_id']})
    ownership_states = _resolve_player_ownership_states(all_player_ids)

    updated = 0
    for row in rows:
        mission_total, current_status, _, status = _derive_progress_status(
            row.get('mission_total'), row.get('current_status')
        )

        new_priority = None
        if status == 'In-Progress':
            new_priority = 'At-Bat'
        elif status == 'Not Started':
            linked_ids = mission_to_players.get(row['id'], [])
            has_in_inventory = any(
                ownership_states.get(player_id) == 'IN_INVENTORY'
                for player_id in linked_ids
            )
            new_priority = 'On-Deck' if has_in_inventory else 'In the Hole'

        if not new_priority:
            continue

        if _normalize_priority(row.get('priority')) != new_priority:
            cursor.execute(
                'UPDATE missions SET priority = ? WHERE id = ?',
                (new_priority, row['id']),
            )
            _update_derived_fields(conn, row['id'])
            updated += 1

    conn.commit()
    conn.close()
    return {'evaluated': len(rows), 'updated': updated}


def apply_owned_player_sync(owned_player_ids, sync_source='actual_card_tracker'):
    """Auto-update mission acquisition/priority from owned player_ids (exact match only)."""
    normalized_ids = list(dict.fromkeys([pid for pid in (owned_player_ids or []) if (pid or '').strip()]))
    if not normalized_ids:
        return {
            'players_matched': 0,
            'missions_linked': 0,
            'missions_updated': 0,
            'audit_entries': 0,
        }

    conn = get_missions_connection()
    cursor = conn.cursor()

    placeholders = ','.join('?' for _ in normalized_ids)
    cursor.execute(
        f'''
        SELECT
            m.id AS mission_id,
            m.acquired AS acquired,
            m.priority AS priority,
            mp.player_id AS player_id
        FROM mission_players mp
        JOIN missions m ON m.id = mp.mission_id
        WHERE mp.player_id IN ({placeholders})
        ORDER BY m.id ASC
        ''',
        normalized_ids,
    )
    linked_rows = cursor.fetchall()

    updated_mission_ids = set()
    for row in linked_rows:
        mission_id = row['mission_id']
        if mission_id in updated_mission_ids:
            continue

        previous_acquired = row['acquired'] or 'Not Acquired'
        previous_priority = _normalize_priority(row['priority'])
        if previous_acquired == 'Acquired' and previous_priority == 'At-Bat':
            continue

        cursor.execute(
            "UPDATE missions SET acquired = 'Acquired', priority = 'At-Bat' WHERE id = ?",
            (mission_id,),
        )
        cursor.execute(
            '''
            INSERT INTO mission_auto_update_audit (
                mission_id,
                player_id,
                sync_source,
                previous_acquired,
                previous_priority,
                new_acquired,
                new_priority
            ) VALUES (?, ?, ?, ?, ?, 'Acquired', 'At-Bat')
            ''',
            (
                mission_id,
                row['player_id'],
                (sync_source or 'actual_card_tracker').strip() or 'actual_card_tracker',
                previous_acquired,
                previous_priority,
            ),
        )
        updated_mission_ids.add(mission_id)

    conn.commit()
    conn.close()

    return {
        'players_matched': len(normalized_ids),
        'missions_linked': len({r['mission_id'] for r in linked_rows}),
        'missions_updated': len(updated_mission_ids),
        'audit_entries': len(updated_mission_ids),
    }


def backfill_acquired_missions_to_inventory():
    """One-time/backfill helper: ensure all players on Acquired missions are in inventory."""
    conn = get_missions_connection()
    cursor = conn.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT mp.player_id
        FROM mission_players mp
        JOIN missions m ON m.id = mp.mission_id
        WHERE m.acquired = 'Acquired'
        '''
    )
    player_ids = [row['player_id'] for row in cursor.fetchall() if row['player_id']]
    conn.close()

    if not player_ids:
        return {
            'acquired_players_found': 0,
            'added_to_inventory': 0,
            'existing_promoted': 0,
            'failed': 0,
        }

    inventory_result = _auto_add_players_to_inventory(player_ids)
    return {
        'acquired_players_found': len(set(player_ids)),
        **inventory_result,
    }


def get_mission_auto_update_audit(limit=100):
    """Read most recent mission auto-update audit entries for Beta 3.0."""
    capped_limit = max(1, min(_to_int(limit, 100), 500))
    conn = get_missions_connection()
    cursor = conn.cursor()
    cursor.execute(
        '''
        SELECT
            a.id,
            a.mission_id,
            a.player_id,
            a.sync_source,
            a.previous_acquired,
            a.previous_priority,
            a.new_acquired,
            a.new_priority,
            a.created_at,
            m.program_category,
            m.mission_name
        FROM mission_auto_update_audit a
        LEFT JOIN missions m ON m.id = a.mission_id
        ORDER BY a.created_at DESC, a.id DESC
        LIMIT ?
        ''',
        (capped_limit,),
    )
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def _priority_rank(priority_label):
    label = _normalize_priority(priority_label)
    if label == 'At-Bat':
        return 1
    if label == 'On-Deck':
        return 2
    return 3


def _parse_secondary_positions(value):
    raw = (value or '').strip()
    if not raw:
        return []
    return [token.strip().upper() for token in re.split(r'[,/;|]+', raw) if token.strip()]


def _normalize_position_token(token):
    normalized = (token or '').strip().upper()
    if not normalized:
        return set()
    if normalized in {'SP', 'RP', 'CP', 'P'}:
        return {'P'}
    if normalized == 'OF':
        return {'LF', 'CF', 'RF'}
    if normalized == 'IF':
        return {'1B', '2B', '3B', 'SS'}
    if normalized in AUTO_GRIND_DIAMOND_POSITIONS:
        return {normalized}
    return set()


def _collect_eligible_positions(primary_position, secondary_positions):
    positions = set()
    positions.update(_normalize_position_token(primary_position))
    for token in _parse_secondary_positions(secondary_positions):
        positions.update(_normalize_position_token(token))
    return positions


def _resolve_player_ownership_states(player_ids: Iterable[str]):
    ids = [pid for pid in (player_ids or []) if pid]
    states = {pid: 'NOT_OWNED' for pid in ids}
    if not ids:
        return states

    # Resolve source_card_uuid for composite player_ids (name::series)
    placeholders = ','.join('?' for _ in ids)
    missions_conn = get_missions_connection()
    missions_cursor = missions_conn.cursor()
    missions_cursor.execute(
        f'SELECT player_id, source_card_uuid FROM sync_players WHERE player_id IN ({placeholders})',
        ids,
    )
    pid_to_uuid = {r['player_id']: r['source_card_uuid'] for r in missions_cursor.fetchall()}
    missions_conn.close()

    uuids = [v for v in pid_to_uuid.values() if v]
    if not uuids:
        return states

    uuid_placeholders = ','.join('?' for _ in uuids)
    conn = _get_cards_connection()
    cursor = conn.cursor()
    cursor.execute(
        f'''
        SELECT uuid, on_team, quantity, card_status
        FROM cards
        WHERE uuid IN ({uuid_placeholders})
        ''',
        uuids,
    )
    rows = cursor.fetchall()
    conn.close()

    uuid_to_state = {}
    for row in rows:
        uuid = row['uuid']
        card_status = (row['card_status'] or '').strip().lower()
        quantity = _to_int(row['quantity'], 0)
        on_team = _to_int(row['on_team'], 0)

        if card_status == 'active' and quantity > 0:
            uuid_to_state[uuid] = 'IN_INVENTORY' if on_team == 1 else 'OWNED_NOT_IN_INVENTORY'
        else:
            uuid_to_state[uuid] = 'NOT_OWNED'

    for pid, uuid in pid_to_uuid.items():
        if uuid and uuid in uuid_to_state:
            states[pid] = uuid_to_state[uuid]

    return states


def _bucket_sort_key(player):
    ownership_state = player.get('ownership_state', 'NOT_OWNED')
    ownership_rank = OWNERSHIP_STATE_META.get(ownership_state, OWNERSHIP_STATE_META['NOT_OWNED'])['rank']
    return (
        -float(player.get('completion_closeness') or 0),
        _priority_rank(player.get('top_priority')),
        ownership_rank,
        -_to_int(player.get('mission_count'), 0),
        (player.get('player_name') or '').lower(),
    )


def _get_active_mission_player_rows(include_completed=False):
    conn = get_missions_connection()
    cursor = conn.cursor()

    sql = (
        'SELECT '
        'm.id AS mission_id, m.priority, m.mission_total, m.current_status, '
        'sp.player_id, sp.player_name, sp.primary_position, sp.secondary_positions, sp.team '
        'FROM mission_players mp '
        'JOIN missions m ON m.id = mp.mission_id '
        'JOIN sync_players sp ON sp.player_id = mp.player_id '
        "WHERE m.acquired = 'Acquired' "
    )

    if not include_completed:
        sql += ' AND m.current_status < m.mission_total '

    sql += ' ORDER BY sp.player_name ASC '

    cursor.execute(sql)
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_auto_grind_diamond(limit_per_position=10, include_completed=False):
    limit = max(1, _to_int(limit_per_position, 10))
    rows = _get_active_mission_player_rows(include_completed=include_completed)

    positions = {bucket: [] for bucket in AUTO_GRIND_POSITION_BUCKETS}
    if not rows:
        return {
            'meta': {
                'missions_considered': 0,
                'players_considered': 0,
                'include_completed': bool(include_completed),
                'generated_at': datetime.utcnow().isoformat() + 'Z',
            },
            'positions': positions,
        }

    players = {}
    mission_ids = set()

    for row in rows:
        mission_id = row.get('mission_id')
        if mission_id is not None:
            mission_ids.add(mission_id)

        mission_total = max(1, _to_int(row.get('mission_total'), 1))
        current_status = max(0, min(_to_int(row.get('current_status'), 0), mission_total))
        progress_percent = round((current_status / mission_total) * 100, 2)

        player_id = row.get('player_id')
        if not player_id:
            continue

        profile = players.setdefault(
            player_id,
            {
                'player_id': player_id,
                'player_name': row.get('player_name') or 'Unknown Player',
                'team': row.get('team') or '',
                'primary_position': row.get('primary_position') or '',
                'secondary_positions': row.get('secondary_positions') or '',
                'mission_ids': set(),
                'completion_closeness': 0.0,
                'top_priority': 'In the Hole',
                'top_priority_rank': 3,
            },
        )

        profile['mission_ids'].add(mission_id)
        profile['completion_closeness'] = max(profile['completion_closeness'], progress_percent)

        row_priority = _normalize_priority(row.get('priority'))
        row_priority_rank = _priority_rank(row_priority)
        if row_priority_rank < profile['top_priority_rank']:
            profile['top_priority_rank'] = row_priority_rank
            profile['top_priority'] = row_priority

    ownership_states = _resolve_player_ownership_states(players.keys())

    for profile in players.values():
        eligible_positions = _collect_eligible_positions(
            profile.get('primary_position'),
            profile.get('secondary_positions'),
        )
        state = ownership_states.get(profile['player_id'], 'NOT_OWNED')
        ownership_meta = OWNERSHIP_STATE_META.get(state, OWNERSHIP_STATE_META['NOT_OWNED'])

        payload = {
            'player_id': profile['player_id'],
            'player_name': profile['player_name'],
            'team': profile['team'],
            'primary_position': profile['primary_position'],
            'secondary_positions': _parse_secondary_positions(profile['secondary_positions']),
            'ownership_state': state,
            'ownership_label': ownership_meta['label'],
            'color_token': ownership_meta['color_token'],
            'mission_count': len([mid for mid in profile['mission_ids'] if mid is not None]),
            'completion_closeness': round(profile['completion_closeness'], 2),
            'top_priority': profile['top_priority'],
        }

        for position in sorted(eligible_positions):
            if position in AUTO_GRIND_DIAMOND_POSITIONS:
                bucket_entry = dict(payload)
                bucket_entry['position_bucket'] = position
                positions[position].append(bucket_entry)

        bench_entry = dict(payload)
        bench_entry['position_bucket'] = 'BENCH'
        positions['BENCH'].append(bench_entry)

        non_pitch_positions = [pos for pos in eligible_positions if pos != 'P']
        include_utility = bool(non_pitch_positions) or not eligible_positions
        if include_utility:
            utility_entry = dict(payload)
            utility_entry['position_bucket'] = 'UTILITY'
            positions['UTILITY'].append(utility_entry)

    for bucket in AUTO_GRIND_POSITION_BUCKETS:
        positions[bucket].sort(key=_bucket_sort_key)
        positions[bucket] = positions[bucket][:limit]

    return {
        'meta': {
            'missions_considered': len(mission_ids),
            'players_considered': len(players),
            'include_completed': bool(include_completed),
            'generated_at': datetime.utcnow().isoformat() + 'Z',
        },
        'positions': positions,
    }


def get_grind_player_list(mission_ids=None):
    conn = get_missions_connection()
    cursor = conn.cursor()

    sql = (
        'SELECT sp.player_id, sp.player_name, sp.primary_position, sp.team, '
        'MIN(CASE m.priority '
        "WHEN 'At-Bat' THEN 1 WHEN 'On-Deck' THEN 2 ELSE 3 END) AS top_priority_rank, "
        'GROUP_CONCAT(DISTINCT m.priority) AS priorities, '
        'COUNT(DISTINCT mp.mission_id) AS mission_count '
        'FROM mission_players mp '
        'JOIN sync_players sp ON sp.player_id = mp.player_id '
        'JOIN missions m ON m.id = mp.mission_id '
        'WHERE 1=1 '
    )
    params = []

    if mission_ids:
        clean_ids = [int(mid) for mid in mission_ids if str(mid).strip().isdigit()]
        if clean_ids:
            placeholders = ','.join('?' for _ in clean_ids)
            sql += f' AND mp.mission_id IN ({placeholders}) '
            params.extend(clean_ids)

    sql += ' GROUP BY sp.player_id, sp.player_name, sp.primary_position, sp.team '
    sql += ' ORDER BY top_priority_rank ASC, mission_count DESC, sp.player_name ASC '

    cursor.execute(sql, params)
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()

    owned_ids = _owned_player_ids([r['player_id'] for r in rows])
    rank_to_label = {1: 'At-Bat', 2: 'On-Deck', 3: 'In the Hole'}

    for row in rows:
        row['ownership_status'] = 'Owned' if row['player_id'] in owned_ids else 'Not Owned'
        row['top_priority'] = rank_to_label.get(row.get('top_priority_rank', 3), 'In the Hole')

    return rows


def get_lineup_suggestion(mission_ids=None):
    players = get_grind_player_list(mission_ids)
    lineup = []
    used_positions = set()

    for player in players:
        position = (player.get('primary_position') or 'UTIL').strip() or 'UTIL'

        # Prefer unique positions; UTIL is fallback bucket.
        if position != 'UTIL' and position in used_positions:
            continue

        lineup.append(
            {
                'slot': len(lineup) + 1,
                'player_name': player.get('player_name'),
                'position': position,
                'ownership_status': player.get('ownership_status'),
                'top_priority': player.get('top_priority'),
                'mission_count': player.get('mission_count'),
            }
        )

        if position != 'UTIL':
            used_positions.add(position)

        if len(lineup) >= 9:
            break

    return lineup


def get_program_categories():
    conn = get_missions_connection()
    cursor = conn.cursor()
    cursor.execute(
        'SELECT program_category FROM programs ORDER BY program_category ASC'
    )
    categories = [r['program_category'] for r in cursor.fetchall()]

    if not categories:
        cursor.execute(
            "SELECT DISTINCT value FROM dynamic_selections WHERE field_name = 'program_category' ORDER BY value ASC"
        )
        categories = [r['value'] for r in cursor.fetchall()]

    conn.close()
    return categories


def get_program_type_options():
    predefined = ['not-assigned', 'TA', 'WBC', 'Player', 'Spotlight']
    conn = get_missions_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT DISTINCT program_type FROM programs WHERE program_type IS NOT NULL AND program_type != '' ORDER BY program_type ASC"
        )
        stored = [r['program_type'] for r in cursor.fetchall()]
    finally:
        conn.close()
    combined = ['not-assigned'] + sorted(set(predefined + stored) - {'not-assigned'})
    return combined


def update_program_type(program_category, new_type):
    program_category = (program_category or '').strip()
    if not program_category:
        raise ValueError('Program Category is required')
    normalized = _normalize_program_type(new_type)
    conn = get_missions_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'UPDATE programs SET program_type = ? WHERE program_category = ?',
            (normalized, program_category),
        )
        if cursor.rowcount == 0:
            cursor.execute(
                'INSERT OR IGNORE INTO programs (program_category, program_type) VALUES (?, ?)',
                (program_category, normalized),
            )
        conn.commit()
    finally:
        conn.close()


# Initialize database on module import.
init_missions_db()
