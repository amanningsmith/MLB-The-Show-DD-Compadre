"""
MLB scores service for Beta 4.0.
Provides normalized daily game data for ticker and dashboard views.
"""

from datetime import datetime, timedelta, tzinfo
from threading import Lock
from urllib.parse import quote_plus
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

# Team ID -> MLB website slug for roster URLs.
TEAM_ROSTER_SLUG_MAP = {
    108: 'angels',
    109: 'dbacks',
    110: 'orioles',
    111: 'redsox',
    112: 'cubs',
    113: 'reds',
    114: 'guardians',
    115: 'rockies',
    116: 'tigers',
    117: 'astros',
    118: 'royals',
    119: 'dodgers',
    120: 'nationals',
    121: 'mets',
    133: 'athletics',
    134: 'pirates',
    135: 'padres',
    136: 'mariners',
    137: 'giants',
    138: 'cardinals',
    139: 'rays',
    140: 'rangers',
    141: 'bluejays',
    142: 'twins',
    143: 'phillies',
    144: 'braves',
    145: 'whitesox',
    146: 'marlins',
    147: 'yankees',
    158: 'brewers',
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


def _parse_game_datetime_et(utc_iso_value):
    """Parse MLB UTC ISO datetime and return timezone-aware ET datetime."""
    if not utc_iso_value:
        return None
    try:
        dt_utc = datetime.fromisoformat(utc_iso_value.replace('Z', '+00:00'))
        return dt_utc.astimezone(ET_TIMEZONE)
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


def _slugify_team_value(value):
    raw = (value or '').strip().lower()
    if not raw:
        return None

    chars = []
    prev_dash = False
    for ch in raw:
        if ch.isalnum():
            chars.append(ch)
            prev_dash = False
        elif not prev_dash:
            chars.append('-')
            prev_dash = True

    slug = ''.join(chars).strip('-')
    return slug or None


def _build_team_roster_url(team_obj):
    """Build best-effort MLB team roster URL like /nationals/roster."""
    if not isinstance(team_obj, dict):
        return None

    team_id = _to_int(team_obj.get('id'), None)
    if team_id in TEAM_ROSTER_SLUG_MAP:
        return f"https://www.mlb.com/{TEAM_ROSTER_SLUG_MAP[team_id]}/roster"

    for key in ('clubName', 'teamName', 'fileCode'):
        slug = _slugify_team_value(team_obj.get(key))
        if slug:
            return f'https://www.mlb.com/{slug}/roster'

    team_name = (team_obj.get('name') or '').strip()
    if team_name:
        tail = team_name.split()[-1]
        slug = _slugify_team_value(tail)
        if slug:
            return f'https://www.mlb.com/{slug}/roster'

    return None


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
    innings_raw = linescore.get('innings') or []
    offense = linescore.get('offense') or {}

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
        'innings': [
            {
                'inning': _to_int(inning_obj.get('num'), None),
                'away_runs': _to_int(((inning_obj.get('away') or {}).get('runs')), None),
                'home_runs': _to_int(((inning_obj.get('home') or {}).get('runs')), None),
            }
            for inning_obj in innings_raw
            if isinstance(inning_obj, dict)
        ],
        'live_context': live_context,
        'current_inning': _to_int(linescore.get('currentInning'), None),
        'current_inning_ordinal': inning_ordinal or None,
        'inning_half': inning_half.lower() if inning_half else None,
        'outs': outs,
        'runners_on': sum(1 for key in ('first', 'second', 'third') if offense.get(key)),
        'runners_in_scoring_position': bool(offense.get('second') or offense.get('third')),
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
        linescore = game.get('linescore') or {}
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
                'roster_url': _build_team_roster_url(away_team),
            },
            'home': {
                'id': home_team_id,
                'name': home_name,
                'abbr': _resolve_team_abbr(home_team),
                'runs': line_data['home']['runs'],
                'hits': line_data['home']['hits'],
                'errors': line_data['home']['errors'],
                'logo_url': _team_logo_url(home_team_id),
                'roster_url': _build_team_roster_url(home_team),
            },
            'live_context': line_data['live_context'],
            'linescore_innings': line_data['innings'],
            'current_inning': line_data['current_inning'],
            'current_inning_ordinal': line_data['current_inning_ordinal'],
            'inning_half': line_data['inning_half'],
            'outs': line_data['outs'],
            'runners_on': line_data['runners_on'],
            'runners_in_scoring_position': line_data['runners_in_scoring_position'],
            'probable_pitchers': _build_probable_pitchers(game),
            'mlb_url': _build_game_link(game_pk, state_data['state']),
            # Internal-only sort helpers removed after ordering.
            '_sort_start_dt': _parse_game_datetime_et(game.get('gameDate')),
            '_sort_inning': _to_int(linescore.get('currentInning'), 1),
            '_sort_inning_half': (linescore.get('inningHalf') or '').strip().lower(),
            '_sort_outs': _to_int(linescore.get('outs'), 0),
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

    def _game_sort_key(game):
        state = game.get('state')
        game_pk = str(game.get('game_pk') or '')
        start_dt = game.get('_sort_start_dt') or datetime.max.replace(tzinfo=ET_TIMEZONE)

        if state == 'Live':
            inning = _to_int(game.get('_sort_inning'), 1)
            inning_half = game.get('_sort_inning_half') or ''
            outs = _to_int(game.get('_sort_outs'), 0)
            # Later game state (fewer innings left) should appear first.
            half_rank = 1 if inning_half == 'bottom' else 0
            return (0, -inning, -half_rank, -outs, start_dt, game_pk)

        if state == 'Pre-Game':
            return (1, start_dt, game_pk)

        return (2, start_dt, game_pk)

    normalized.sort(key=_game_sort_key)

    for game in normalized:
        game.pop('_sort_start_dt', None)
        game.pop('_sort_inning', None)
        game.pop('_sort_inning_half', None)
        game.pop('_sort_outs', None)

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


# ---------------------------------------------------------------------------
# Standings (Beta 4.1)
# ---------------------------------------------------------------------------

_DIVISION_NAMES = {
    200: 'West',
    201: 'East',
    202: 'Central',
    203: 'West',
    204: 'East',
    205: 'Central',
}

_LEAGUE_NAMES = {
    103: 'AL',
    104: 'NL',
}


def _normalize_standings(raw_payload):
    """
    Normalize the MLB Stats API standings response into a structure keyed by
    league then division: { 'AL': { 'East': [...], 'Central': [...], 'West': [...] }, 'NL': {...} }
    """
    result = {
        'AL': {'East': [], 'Central': [], 'West': []},
        'NL': {'East': [], 'Central': [], 'West': []},
    }

    for record in (raw_payload.get('records') or []):
        division = record.get('division') or {}
        league = record.get('league') or {}
        division_id = division.get('id')
        league_id = league.get('id')

        league_key = _LEAGUE_NAMES.get(league_id)
        division_key = _DIVISION_NAMES.get(division_id)

        if not league_key or not division_key:
            continue

        for entry in (record.get('teamRecords') or []):
            team = entry.get('team') or {}
            team_id = _to_int(team.get('id'), None)

            streak_info = entry.get('streak') or {}
            streak_code = streak_info.get('streakCode') or '-'

            records = entry.get('records') or {}
            last_ten = None
            for split in (records.get('splitRecords') or []):
                if split.get('type') == 'lastTen':
                    last_ten = f"{split.get('wins', 0)}-{split.get('losses', 0)}"
                    break

            games_back = entry.get('gamesBack') or '-'

            result[league_key][division_key].append({
                'team_id': team_id,
                'team_name': (team.get('name') or 'Unknown').strip(),
                'team_abbr': _resolve_team_abbr(team),
                'logo_url': _team_logo_url(team_id),
                'wins': entry.get('wins', 0),
                'losses': entry.get('losses', 0),
                'pct': entry.get('winningPercentage') or '.000',
                'gb': games_back,
                'streak': streak_code,
                'last_ten': last_ten or '-',
                'division_rank': _to_int(entry.get('divisionRank'), 99),
            })

        # Sort each division by division rank
        for div in result[league_key].values():
            div.sort(key=lambda t: t['division_rank'])

    return result


def get_standings_payload():
    """Fetch and normalize current MLB standings."""
    season = datetime.now(ET_TIMEZONE).year
    try:
        response = requests.get(
            f'{MLB_STATS_API_BASE_URL}/api/v1/standings',
            params={
                'leagueId': '103,104',
                'season': season,
                'standingsTypes': 'regularSeason',
                'hydrate': 'team,league,division,record(splitRecords)',
            },
            timeout=12,
        )
        response.raise_for_status()
        standings = _normalize_standings(response.json())
        return {'success': True, 'standings': standings, 'season': season}
    except Exception as exc:
        logger.error(f'Failed to fetch standings for {season}: {exc}')
        return {
            'success': False,
            'error': 'Standings unavailable right now. Please try again later.',
            'standings': None,
            'season': season,
        }


def _baseball_reference_player_url(player_name):
    """Best-effort Baseball-Reference player search URL for a full name."""
    clean_name = (player_name or '').strip()
    if not clean_name:
        return None
    return f'https://www.baseball-reference.com/search/search.fcgi?search={quote_plus(clean_name)}'


def _normalize_team_lineup(team_box):
    batting_order = team_box.get('battingOrder') or []
    players_map = team_box.get('players') or {}
    lineup = []

    def _resolve_batting_avg(player_obj):
        stats_obj = player_obj.get('stats') or {}
        batting_stats = stats_obj.get('batting') or {}
        avg_value = (batting_stats.get('avg') or '').strip()
        if avg_value:
            return avg_value

        season_stats = player_obj.get('seasonStats') or {}
        season_batting = season_stats.get('batting') or {}
        avg_value = (season_batting.get('avg') or '').strip()
        if avg_value:
            return avg_value

        return 'NA'

    for raw_player_id in batting_order:
        player_id = _to_int(raw_player_id, None)
        player_key = f'ID{player_id}' if player_id is not None else str(raw_player_id)
        player_entry = players_map.get(player_key) or {}
        person = player_entry.get('person') or {}
        position = player_entry.get('position') or {}

        name = (person.get('fullName') or '').strip() or 'NA'
        position_abbr = (position.get('abbreviation') or '').strip() or 'NA'

        lineup.append({
            'player_id': player_id,
            'name': name,
            'position': position_abbr,
            'batting_avg': _resolve_batting_avg(player_entry),
            'baseball_reference_url': _baseball_reference_player_url(name) if name != 'NA' else None,
        })

    if lineup:
        return lineup

    return [
        {
            'player_id': None,
            'name': 'NA',
            'position': 'NA',
            'batting_avg': 'NA',
            'baseball_reference_url': None,
        }
    ]


def _extract_probable_starter(team_box):
    """Best-effort starter extraction from MLB boxscore team payload."""
    players_map = team_box.get('players') or {}

    probable = team_box.get('probablePitcher')
    if isinstance(probable, dict):
        person = probable.get('person') or probable
        full_name = (person.get('fullName') or '').strip()
        if full_name:
            return full_name
        probable_id = _to_int(person.get('id'), None)
        if probable_id is not None:
            entry = players_map.get(f'ID{probable_id}') or {}
            full_name = ((entry.get('person') or {}).get('fullName') or '').strip()
            if full_name:
                return full_name

    probable_id = _to_int(probable, None)
    if probable_id is not None:
        entry = players_map.get(f'ID{probable_id}') or {}
        full_name = ((entry.get('person') or {}).get('fullName') or '').strip()
        if full_name:
            return full_name

    pitchers = team_box.get('pitchers') or []
    if pitchers:
        starter_id = _to_int(pitchers[0], None)
        if starter_id is not None:
            entry = players_map.get(f'ID{starter_id}') or {}
            full_name = ((entry.get('person') or {}).get('fullName') or '').strip()
            if full_name:
                return full_name

    return 'NA'


def get_game_lineups_payload(game_pk):
    """Fetch away/home lineups for a game using MLB Stats API boxscore."""
    game_pk_value = _to_int(game_pk, None)
    if game_pk_value is None or game_pk_value <= 0:
        return {'success': False, 'error': 'Invalid game_pk'}

    try:
        response = requests.get(
            f'{MLB_STATS_API_BASE_URL}/api/v1/game/{game_pk_value}/boxscore',
            timeout=12,
        )
        response.raise_for_status()
        payload = response.json()

        teams = payload.get('teams') or {}
        away_box = teams.get('away') or {}
        home_box = teams.get('home') or {}

        away_team = away_box.get('team') or {}
        home_team = home_box.get('team') or {}

        return {
            'success': True,
            'game_pk': game_pk_value,
            'away': {
                'team_id': _to_int(away_team.get('id'), None),
                'team_name': (away_team.get('name') or 'Away').strip(),
                'team_abbr': _resolve_team_abbr(away_team),
                'probable_starter': _extract_probable_starter(away_box),
                'lineup': _normalize_team_lineup(away_box),
            },
            'home': {
                'team_id': _to_int(home_team.get('id'), None),
                'team_name': (home_team.get('name') or 'Home').strip(),
                'team_abbr': _resolve_team_abbr(home_team),
                'probable_starter': _extract_probable_starter(home_box),
                'lineup': _normalize_team_lineup(home_box),
            },
        }
    except Exception as exc:
        logger.error(f'Failed to fetch game lineups for {game_pk_value}: {exc}')
        return {
            'success': False,
            'error': 'Lineups unavailable right now. Please try again.',
            'game_pk': game_pk_value,
            'away': {
                'probable_starter': 'NA',
                'lineup': [{'player_id': None, 'name': 'NA', 'position': 'NA', 'batting_avg': 'NA', 'baseball_reference_url': None}],
            },
            'home': {
                'probable_starter': 'NA',
                'lineup': [{'player_id': None, 'name': 'NA', 'position': 'NA', 'batting_avg': 'NA', 'baseball_reference_url': None}],
            },
        }


def get_game_home_run_events_payload(game_pk):
    """Fetch home run play events for a game from MLB live feed."""
    game_pk_value = _to_int(game_pk, None)
    if game_pk_value is None or game_pk_value <= 0:
        return {'success': False, 'error': 'Invalid game_pk', 'events': []}

    try:
        response = requests.get(
            f'{MLB_STATS_API_BASE_URL}/api/v1.1/game/{game_pk_value}/feed/live',
            timeout=12,
        )
        response.raise_for_status()
        payload = response.json()

        game_data = payload.get('gameData') or {}
        teams = game_data.get('teams') or {}
        away_team = teams.get('away') or {}
        home_team = teams.get('home') or {}

        away_name = (away_team.get('name') or 'Away').strip()
        home_name = (home_team.get('name') or 'Home').strip()

        all_plays = (((payload.get('liveData') or {}).get('plays') or {}).get('allPlays') or [])
        events = []

        for play in all_plays:
            result = play.get('result') or {}
            about = play.get('about') or {}
            matchup = play.get('matchup') or {}

            event_type = (result.get('eventType') or '').strip().lower()
            if event_type != 'home_run':
                continue

            at_bat_index = _to_int(about.get('atBatIndex'), None)
            if at_bat_index is None:
                at_bat_index = len(events)

            inning = _to_int(about.get('inning'), None)
            half_inning = (about.get('halfInning') or '').strip().lower()
            batter = (matchup.get('batter') or {}).get('fullName') or 'Unknown Batter'
            description = (result.get('description') or '').strip() or 'Home run'

            batting_team = away_name if half_inning == 'top' else home_name

            events.append({
                'event_id': f'{game_pk_value}:{at_bat_index}:home_run',
                'game_pk': game_pk_value,
                'event_type': 'home_run',
                'batter': batter,
                'team_name': batting_team,
                'inning': inning,
                'inning_half': half_inning or None,
                'description': description,
            })

        return {
            'success': True,
            'game_pk': game_pk_value,
            'events': events,
        }
    except Exception as exc:
        logger.error(f'Failed to fetch home run events for {game_pk_value}: {exc}')
        return {
            'success': False,
            'error': 'Home run events unavailable right now. Please try again.',
            'game_pk': game_pk_value,
            'events': [],
        }


def _extract_at_bat_entries_for_game(game):
    """Extract recent at-bat events for a single live game feed."""
    game_pk = _to_int(game.get('game_pk'), None)
    if game_pk is None:
        return []

    try:
        response = requests.get(
            f'{MLB_STATS_API_BASE_URL}/api/v1.1/game/{game_pk}/feed/live',
            timeout=12,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        logger.warning(f'At-bat feed skipped for game {game_pk}: {exc}')
        return []

    plays = (((payload.get('liveData') or {}).get('plays') or {}).get('allPlays') or [])
    if not plays:
        return []

    away = game.get('away') or {}
    home = game.get('home') or {}
    entries = []

    for play in plays[-8:]:
        result = play.get('result') or {}
        about = play.get('about') or {}
        matchup = play.get('matchup') or {}
        count = play.get('count') or {}

        at_bat_index = _to_int(about.get('atBatIndex'), None)
        if at_bat_index is None:
            continue

        half = (about.get('halfInning') or '').strip().lower()
        batting_side = away if half == 'top' else home
        batter_name = ((matchup.get('batter') or {}).get('fullName') or '').strip()

        raw_description = (result.get('description') or '').strip()
        if not raw_description:
            event_type = (result.get('eventType') or '').strip().lower()
            if event_type:
                event_label = event_type.replace('_', ' ')
                if batter_name:
                    raw_description = f'{batter_name}: {event_label}'
                else:
                    raw_description = event_label

        entries.append({
            'play_id': f'{game_pk}:{at_bat_index}',
            'game_pk': game_pk,
            'game_label': f"{away.get('abbr', 'AWY')} @ {home.get('abbr', 'HME')}",
            'team_name': batting_side.get('name') or 'Team',
            'team_abbr': batting_side.get('abbr') or 'TEAM',
            'inning': _to_int(about.get('inning'), None),
            'inning_half': half or None,
            'event_type': (result.get('eventType') or '').strip().lower() or 'event',
            'description': raw_description,
            'is_scoring_play': bool(about.get('isScoringPlay')),
            'outs': _to_int(count.get('outs'), None),
            'batter': batter_name or None,
            'pitcher': ((matchup.get('pitcher') or {}).get('fullName') or '').strip() or None,
            'event_time': (about.get('endTime') or about.get('startTime') or '').strip() or None,
            'mlb_url': _build_game_link(game_pk, game.get('state') or 'Live'),
        })

    return entries


def get_at_bat_feed_payload(date_str=None, limit=80):
    """Aggregate recent at-bat events across current live games for a date."""
    date_str = _normalize_date(date_str)
    limit = max(10, min(_to_int(limit, 80) or 80, 200))

    try:
        schedule = _fetch_schedule(date_str)
        games = _normalize_games(schedule)
        live_games = [g for g in games if g.get('state') == 'Live']

        entries = []
        for game in live_games:
            entries.extend(_extract_at_bat_entries_for_game(game))

        def _sort_key(entry):
            event_time = entry.get('event_time') or ''
            return (event_time, str(entry.get('play_id') or ''))

        entries.sort(key=_sort_key, reverse=True)

        return {
            'success': True,
            'date': date_str,
            'generated_at_et': _et_timestamp(),
            'live_games': len(live_games),
            'entries': entries[:limit],
        }
    except Exception as exc:
        logger.error(f'Failed to fetch at-bat feed for {date_str}: {exc}')
        return {
            'success': False,
            'date': date_str,
            'generated_at_et': _et_timestamp(),
            'error': 'At-bat feed unavailable right now. Please try again.',
            'live_games': 0,
            'entries': [],
        }


def get_game_at_bat_feed_payload(game_pk, limit=40):
    """Fetch recent at-bat events for one game."""
    game_pk_value = _to_int(game_pk, None)
    limit = max(10, min(_to_int(limit, 40) or 40, 120))
    if game_pk_value is None or game_pk_value <= 0:
        return {'success': False, 'error': 'Invalid game_pk', 'entries': []}

    try:
        response = requests.get(
            f'{MLB_STATS_API_BASE_URL}/api/v1.1/game/{game_pk_value}/feed/live',
            timeout=12,
        )
        response.raise_for_status()
        payload = response.json()

        game_data = payload.get('gameData') or {}
        team_data = game_data.get('teams') or {}
        away_team = team_data.get('away') or {}
        home_team = team_data.get('home') or {}

        away_abbr = _resolve_team_abbr(away_team)
        home_abbr = _resolve_team_abbr(home_team)
        away_name = (away_team.get('name') or away_abbr or 'Away').strip()
        home_name = (home_team.get('name') or home_abbr or 'Home').strip()

        plays = (((payload.get('liveData') or {}).get('plays') or {}).get('allPlays') or [])
        entries = []

        for play in plays[-limit:]:
            result = play.get('result') or {}
            about = play.get('about') or {}
            matchup = play.get('matchup') or {}
            count = play.get('count') or {}

            at_bat_index = _to_int(about.get('atBatIndex'), None)
            if at_bat_index is None:
                continue

            half = (about.get('halfInning') or '').strip().lower()
            team_name = away_name if half == 'top' else home_name
            team_abbr = away_abbr if half == 'top' else home_abbr
            batter_name = ((matchup.get('batter') or {}).get('fullName') or '').strip()

            raw_description = (result.get('description') or '').strip()
            if not raw_description:
                event_type = (result.get('eventType') or '').strip().lower()
                if event_type:
                    event_label = event_type.replace('_', ' ')
                    raw_description = f'{batter_name}: {event_label}' if batter_name else event_label

            entries.append({
                'play_id': f'{game_pk_value}:{at_bat_index}',
                'game_pk': game_pk_value,
                'game_label': f'{away_abbr} @ {home_abbr}',
                'team_name': team_name,
                'team_abbr': team_abbr,
                'inning': _to_int(about.get('inning'), None),
                'inning_half': half or None,
                'event_type': (result.get('eventType') or '').strip().lower() or 'event',
                'description': raw_description,
                'is_scoring_play': bool(about.get('isScoringPlay')),
                'outs': _to_int(count.get('outs'), None),
                'batter': batter_name or None,
                'pitcher': ((matchup.get('pitcher') or {}).get('fullName') or '').strip() or None,
                'event_time': (about.get('endTime') or about.get('startTime') or '').strip() or None,
                'mlb_url': _build_game_link(game_pk_value, 'Live'),
            })

        entries.sort(key=lambda e: ((e.get('event_time') or ''), str(e.get('play_id') or '')), reverse=True)
        entries = [entry for entry in entries if (entry.get('description') or '').strip()]

        return {
            'success': True,
            'game_pk': game_pk_value,
            'generated_at_et': _et_timestamp(),
            'entries': entries[:limit],
        }
    except Exception as exc:
        logger.error(f'Failed to fetch at-bat feed for game {game_pk_value}: {exc}')
        return {
            'success': False,
            'game_pk': game_pk_value,
            'generated_at_et': _et_timestamp(),
            'error': 'At-bat feed unavailable right now. Please try again.',
            'entries': [],
        }
