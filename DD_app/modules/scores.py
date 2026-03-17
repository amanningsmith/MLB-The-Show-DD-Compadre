"""
MLB scores service for Beta 4.0.
Provides normalized daily game data for ticker and dashboard views.
"""

from datetime import datetime, timedelta, tzinfo
from threading import Lock
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests

from modules.logger import logger

MLB_STATS_API_BASE_URL = 'https://statsapi.mlb.com'
DEFAULT_REFRESH_SECONDS = 30

ZERO_DELTA = timedelta(0)
ONE_HOUR = timedelta(hours=1)


def _first_sunday_on_or_after(dt):
    days_to_go = 6 - dt.weekday()
    if days_to_go:
        dt += timedelta(days=days_to_go)
    return dt


def _us_dst_range(year):
    # US DST rules in effect since 2007.
    dst_start = _first_sunday_on_or_after(datetime(year, 3, 8, 2))
    dst_end = _first_sunday_on_or_after(datetime(year, 11, 1, 2))
    return dst_start, dst_end


class _EasternFallbackTimezone(tzinfo):
    """US Eastern tzinfo fallback when IANA tzdata is unavailable."""

    def tzname(self, dt):
        return 'EDT' if self.dst(dt) else 'EST'

    def utcoffset(self, dt):
        return timedelta(hours=-5) + self.dst(dt)

    def dst(self, dt):
        if dt is None:
            return ZERO_DELTA

        start, end = _us_dst_range(dt.year)
        naive = dt.replace(tzinfo=None)

        if start + ONE_HOUR <= naive < end - ONE_HOUR:
            return ONE_HOUR
        if end - ONE_HOUR <= naive < end:
            return ZERO_DELTA if dt.fold else ONE_HOUR
        if start <= naive < start + ONE_HOUR:
            return ONE_HOUR if dt.fold else ZERO_DELTA
        return ZERO_DELTA

    def fromutc(self, dt):
        if dt.tzinfo is not self:
            raise ValueError('fromutc: dt.tzinfo is not self')

        start, end = _us_dst_range(dt.year)
        start = start.replace(tzinfo=self)
        end = end.replace(tzinfo=self)

        std_time = dt + timedelta(hours=-5)
        dst_time = std_time + ONE_HOUR

        if end <= dst_time < end + ONE_HOUR:
            return std_time.replace(fold=1)
        if std_time < start or dst_time >= end:
            return std_time
        if start <= std_time < end - ONE_HOUR:
            return dst_time
        return std_time


def _resolve_et_timezone():
    try:
        return ZoneInfo('America/New_York')
    except ZoneInfoNotFoundError:
        logger.warning(
            'IANA timezone data missing for America/New_York. '
            'Using built-in US Eastern DST fallback timezone.'
        )
        return _EasternFallbackTimezone()


ET_TIMEZONE = _resolve_et_timezone()

# Team ID -> standard abbreviation fallback.
TEAM_ABBR_MAP = {
    108: 'LAA',
    109: 'AZ',
    110: 'BAL',
    111: 'BOS',
    112: 'CHC',
    113: 'CIN',
    114: 'CLE',
    115: 'COL',
    116: 'DET',
    117: 'HOU',
    118: 'KC',
    119: 'LAD',
    120: 'WSH',
    121: 'NYM',
    133: 'ATH',
    134: 'PIT',
    135: 'SD',
    136: 'SEA',
    137: 'SF',
    138: 'STL',
    139: 'TB',
    140: 'TEX',
    141: 'TOR',
    142: 'MIN',
    143: 'PHI',
    144: 'ATL',
    145: 'CWS',
    146: 'MIA',
    147: 'NYY',
    158: 'MIL',
}

_cache_lock = Lock()
_last_success_cache = {
    'ticker': {},
    'dashboard': {},
}


def get_et_today_date_str():
    """Return today's date in Eastern Time as YYYY-MM-DD."""
    return datetime.now(ET_TIMEZONE).strftime('%Y-%m-%d')


def _normalize_date(date_value):
    date_value = (date_value or '').strip()
    if not date_value:
        return get_et_today_date_str()
    try:
        datetime.strptime(date_value, '%Y-%m-%d')
    except ValueError:
        return get_et_today_date_str()
    return date_value


def _fetch_schedule(date_str):
    params = {
        'sportId': 1,
        'date': date_str,
        'hydrate': 'linescore,team,probablePitcher',
    }
    response = requests.get(
        f'{MLB_STATS_API_BASE_URL}/api/v1/schedule',
        params=params,
        timeout=12,
    )
    response.raise_for_status()
    return response.json()


def _et_timestamp():
    return datetime.now(ET_TIMEZONE).strftime('%Y-%m-%d %I:%M:%S %p ET').lstrip('0')


def _to_et_time_label(utc_iso_value):
    if not utc_iso_value:
        return None
    try:
        dt_utc = datetime.fromisoformat(utc_iso_value.replace('Z', '+00:00'))
        dt_et = dt_utc.astimezone(ET_TIMEZONE)
        return dt_et.strftime('%I:%M %p ET').lstrip('0')
    except Exception:
        return None


def _resolve_team_abbr(team_obj):
    if not isinstance(team_obj, dict):
        return 'TBD'

    team_id = team_obj.get('id')
    if team_id in TEAM_ABBR_MAP:
        return TEAM_ABBR_MAP[team_id]

    for key in ('abbreviation', 'teamCode', 'fileCode'):
        value = (team_obj.get(key) or '').strip()
        if value:
            return value.upper()

    name = (team_obj.get('name') or '').strip()
    if not name:
        return 'TBD'

    pieces = name.split()
    if len(pieces) == 1:
        return pieces[0][:3].upper()
    return ''.join(piece[0] for piece in pieces[-2:]).upper()


def _team_logo_url(team_id):
    """Build best-effort MLB static logo URL for a numeric team id."""
    team_id = _to_int(team_id, None)
    if not team_id:
        return None
    return f'https://www.mlbstatic.com/team-logos/{team_id}.svg'


def _normalize_state(status_obj):
    abstract_state = (status_obj.get('abstractGameState') or '').strip()
    detailed_state = (status_obj.get('detailedState') or '').strip()
    state_code = (status_obj.get('statusCode') or '').strip()

    state_lower = abstract_state.lower()
    if state_lower == 'live':
        state = 'Live'
    elif state_lower == 'final':
        state = 'Final'
    else:
        state = 'Pre-Game'

    return {
        'state': state,
        'abstract_state': abstract_state,
        'detailed_state': detailed_state,
        'status_code': state_code,
    }


def _to_int(value, fallback=None):
    try:
        if value is None:
            return fallback
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _build_line_score(game):
    linescore = game.get('linescore') or {}
    line_teams = linescore.get('teams') or {}

    def _side(side_name):
        team_line = line_teams.get(side_name) or {}
        return {
            'runs': _to_int(team_line.get('runs'), _to_int((game.get('teams') or {}).get(side_name, {}).get('score'), 0)),
            'hits': _to_int(team_line.get('hits'), 0),
            'errors': _to_int(team_line.get('errors'), 0),
        }

    inning_ordinal = (linescore.get('currentInningOrdinal') or '').strip()
    inning_half = (linescore.get('inningHalf') or '').strip()
    outs = _to_int(linescore.get('outs'), None)

    live_context = ''
    if inning_ordinal and inning_half:
        live_context = f'{inning_half} {inning_ordinal}'
        if outs is not None:
            out_label = 'Out' if outs == 1 else 'Outs'
            live_context = f'{live_context} | {outs} {out_label}'

    return {
        'away': _side('away'),
        'home': _side('home'),
        'live_context': live_context,
    }


def _build_probable_pitchers(game):
    game_teams = game.get('teams') or {}
    away_pitcher = ((game_teams.get('away') or {}).get('probablePitcher') or {}).get('fullName')
    home_pitcher = ((game_teams.get('home') or {}).get('probablePitcher') or {}).get('fullName')

    return {
        'away': away_pitcher or 'TBD',
        'home': home_pitcher or 'TBD',
    }


def _build_game_link(game_pk, state):
    base = f'https://www.mlb.com/gameday/{game_pk}'
    if state == 'Pre-Game':
        return f'{base}/preview'
    return base


def _normalize_games(schedule_payload):
    dates = schedule_payload.get('dates') or []
    if not dates:
        return []

    games = (dates[0] or {}).get('games') or []
    normalized = []

    for game in games:
        teams = game.get('teams') or {}
        away_team = (teams.get('away') or {}).get('team') or {}
        home_team = (teams.get('home') or {}).get('team') or {}
        away_team_id = _to_int(away_team.get('id'), None)
        home_team_id = _to_int(home_team.get('id'), None)

        state_data = _normalize_state(game.get('status') or {})
        line_data = _build_line_score(game)
        game_pk = game.get('gamePk')

        start_time_et = _to_et_time_label(game.get('gameDate')) or 'TBD'
        away_name = (away_team.get('name') or 'Away').strip()
        home_name = (home_team.get('name') or 'Home').strip()

        item = {
            'game_pk': game_pk,
            'official_date': game.get('officialDate'),
            'state': state_data['state'],
            'detailed_state': state_data['detailed_state'],
            'status_code': state_data['status_code'],
            'start_time_et': start_time_et,
            'away': {
                'id': away_team_id,
                'name': away_name,
                'abbr': _resolve_team_abbr(away_team),
                'runs': line_data['away']['runs'],
                'hits': line_data['away']['hits'],
                'errors': line_data['away']['errors'],
                'logo_url': _team_logo_url(away_team_id),
            },
            'home': {
                'id': home_team_id,
                'name': home_name,
                'abbr': _resolve_team_abbr(home_team),
                'runs': line_data['home']['runs'],
                'hits': line_data['home']['hits'],
                'errors': line_data['home']['errors'],
                'logo_url': _team_logo_url(home_team_id),
            },
            'live_context': line_data['live_context'],
            'probable_pitchers': _build_probable_pitchers(game),
            'mlb_url': _build_game_link(game_pk, state_data['state']),
        }

        if state_data['state'] == 'Pre-Game':
            item['ticker_display'] = f"{item['away']['abbr']} at {item['home']['abbr']} | {start_time_et} | {state_data['state']}"
        else:
            item['ticker_display'] = (
                f"{item['away']['abbr']} {item['away']['runs']} - {item['home']['runs']} {item['home']['abbr']}"
                f" | {state_data['state']}"
            )
            if item['live_context']:
                item['ticker_display'] = f"{item['ticker_display']} | {item['live_context']}"

        normalized.append(item)

    normalized.sort(key=lambda game: (game.get('start_time_et') or '', str(game.get('game_pk') or '')))
    return normalized


def _cache_success(kind, date_str, payload):
    with _cache_lock:
        _last_success_cache.setdefault(kind, {})[date_str] = payload


def _read_cached(kind, date_str):
    with _cache_lock:
        return (_last_success_cache.get(kind) or {}).get(date_str)


def _payload_base(date_str):
    return {
        'date': date_str,
        'timezone': 'America/New_York',
        'refresh_seconds': DEFAULT_REFRESH_SECONDS,
        'generated_at_et': _et_timestamp(),
    }


def get_ticker_payload(date_str=None):
    """Get ticker payload for the requested ET date."""
    date_str = _normalize_date(date_str)
    base = _payload_base(date_str)

    try:
        schedule = _fetch_schedule(date_str)
        games = _normalize_games(schedule)

        payload = {
            **base,
            'success': True,
            'warning': None,
            'games': games,
            'total_games': len(games),
        }
        _cache_success('ticker', date_str, payload)
        return payload
    except Exception as exc:
        logger.error(f'Failed to fetch ticker scores for {date_str}: {exc}')
        cached = _read_cached('ticker', date_str)
        if cached:
            fallback = dict(cached)
            fallback['warning'] = 'Live scores temporarily unavailable. Showing last successful update.'
            fallback['generated_at_et'] = _et_timestamp()
            return fallback

        return {
            **base,
            'success': False,
            'warning': 'Live scores unavailable right now. Please try again.',
            'games': [],
            'total_games': 0,
        }


def get_dashboard_payload(date_str=None):
    """Get dashboard payload for the requested ET date."""
    date_str = _normalize_date(date_str)
    base = _payload_base(date_str)

    try:
        schedule = _fetch_schedule(date_str)
        games = _normalize_games(schedule)

        payload = {
            **base,
            'success': True,
            'warning': None,
            'games': games,
            'total_games': len(games),
            'counts': {
                'live': sum(1 for game in games if game.get('state') == 'Live'),
                'pregame': sum(1 for game in games if game.get('state') == 'Pre-Game'),
                'final': sum(1 for game in games if game.get('state') == 'Final'),
            },
        }
        _cache_success('dashboard', date_str, payload)
        return payload
    except Exception as exc:
        logger.error(f'Failed to fetch dashboard scores for {date_str}: {exc}')
        cached = _read_cached('dashboard', date_str)
        if cached:
            fallback = dict(cached)
            fallback['warning'] = 'Live scores temporarily unavailable. Showing last successful update.'
            fallback['generated_at_et'] = _et_timestamp()
            return fallback

        return {
            **base,
            'success': False,
            'warning': 'Live scores unavailable right now. Please try again.',
            'games': [],
            'total_games': 0,
            'counts': {'live': 0, 'pregame': 0, 'final': 0},
        }
