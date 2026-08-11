"""Streaming site blueprint extracted from streaming-website repo.

Provides the main streaming homepage at "/", game detail pages at
"/game/<int:game_id>", slug redirect at "/g/<slug>", and a heartbeat endpoint
for tracking active viewers.
"""

from __future__ import annotations

import atexit
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlencode, urlparse

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from flask import (
    Blueprint,
    Response,
    abort,
    g,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    session,
    stream_with_context,
    url_for,
)
import requests
from curl_cffi import requests as curl_requests

from harvest import CdpHarvester, ensure_cdp_chrome, rewrite_playlist, build_master

streaming_bp = Blueprint("streaming", __name__)


@streaming_bp.app_context_processor
def _inject_template_helpers():
    return {
        "build_cdp_player_url": build_cdp_player_url,
        "build_embed_player_fallback_url": build_embed_player_fallback_url,
    }


GAMES_DB_PATH = Path(
    os.environ.get(
        "GAMES_DB_PATH",
        Path(__file__).parent / "data" / "games.db",
    )
).expanduser()


# ====================== PERFORMANCE CONTROLS ======================
# Cache games in memory to avoid sqlite reads on every request
GAMES_CACHE: dict[str, Any] = {
    "games": [],
    "ts": 0.0,
    "mtime": 0.0,
}
GAMES_CACHE_LOCK = threading.Lock()
GAMES_DB_LOCK = threading.Lock()

# Refresh at most every N seconds OR when file mtime changes
GAMES_CACHE_TTL_SECONDS = int(os.environ.get("GAMES_CACHE_TTL_SECONDS", "1800"))

# CDP headless-harvest live relay (see harvest.py)
CDP_CHROME_PORT = int(os.environ.get("CDP_CHROME_PORT", "9223"))
CDP_HARVEST_TIMEOUT_MS = int(os.environ.get("CDP_HARVEST_TIMEOUT_MS", "50000"))
CDP_MEDIA_WAIT_MS = int(os.environ.get("CDP_MEDIA_WAIT_MS", "20000"))
CDP_FOLLOW_INTERVAL_MS = int(os.environ.get("CDP_FOLLOW_INTERVAL_MS", "6000"))
CDP_REFRESH_TIMEOUT_MS = int(os.environ.get("CDP_REFRESH_TIMEOUT_MS", "10000"))
CDP_SESSION_TTL_S = int(os.environ.get("CDP_SESSION_TTL_S", str(12 * 3600)))
# embedUrl -> {"id": sid, "harvester": CdpHarvester, "created": ts}
CDP_SESSIONS: dict[str, dict] = {}
CDP_SESSIONS_LOCK = threading.Lock()

# Cloudflare / browser caching for HTML (keep short to avoid stale)
HTML_CACHE_SECONDS = int(os.environ.get("HTML_CACHE_SECONDS", "30"))

# Viewer tracking: keep it, but reduce work
ENABLE_VIEWER_TRACKING = os.environ.get("ENABLE_VIEWER_TRACKING", "1") == "1"

# IMPORTANT: do not run scraper in the web process unless explicitly enabled
ENABLE_SCRAPER_IN_WEB = os.environ.get("ENABLE_SCRAPER_IN_WEB", "1") == "1"
SCRAPER_SUBPROCESS = os.environ.get("SCRAPER_SUBPROCESS", "1") == "1"
SCRAPE_INTERVAL_MINUTES = int(os.environ.get("SCRAPE_INTERVAL_MINUTES", "30"))
STARTUP_SCRAPE_ON_BOOT = os.environ.get("STARTUP_SCRAPE_ON_BOOT", "1") == "1"


# ====================== ACTIVE VIEWER TRACKER ======================
ACTIVE_VIEWERS: dict[str, datetime] = {}  # session_id → last_seen timestamp
ACTIVE_PAGE_VIEWS: dict[tuple[str, str], datetime] = {}  # (session_id, path) → last_seen timestamp
LAST_VIEWER_PRINT: datetime | None = None  # throttle printing





def get_session_id() -> str:
    if "sid" not in session:
        session["sid"] = str(uuid.UUID(bytes=os.urandom(16)))
    return session["sid"]



def mark_active() -> None:
    """Track active viewers when enabled."""

    if not ENABLE_VIEWER_TRACKING:
        return
    sid = get_session_id()
    now = datetime.now(timezone.utc)
    ACTIVE_VIEWERS[sid] = now

    cutoff = now - timedelta(seconds=45)
    # keep cleanup cheap
    for s, ts in list(ACTIVE_VIEWERS.items()):
        if ts < cutoff:
            del ACTIVE_VIEWERS[s]





@streaming_bp.route("/heartbeat", methods=["POST"])
def heartbeat():
    global LAST_VIEWER_PRINT

    if not ENABLE_VIEWER_TRACKING:
        return jsonify({"ok": True, "disabled": True})

    sid = get_session_id()
    now = datetime.now(timezone.utc)

    data = request.get_json(silent=True) or {}
    path = data.get("path") or request.path

    ACTIVE_VIEWERS[sid] = now
    ACTIVE_PAGE_VIEWS[(sid, path)] = now

    cutoff = now - timedelta(seconds=45)

    for key, ts in list(ACTIVE_PAGE_VIEWS.items()):
        if ts < cutoff:
            del ACTIVE_PAGE_VIEWS[key]

    for s, ts in list(ACTIVE_VIEWERS.items()):
        if ts < cutoff:
            del ACTIVE_VIEWERS[s]

    # print at most once per minute
    if LAST_VIEWER_PRINT is None or (now - LAST_VIEWER_PRINT) > timedelta(seconds=60):
        total_active = len(ACTIVE_VIEWERS)

        home_sids = {sid for (sid, p) in ACTIVE_PAGE_VIEWS.keys() if p == "/"}
        home_count = len(home_sids)

        game_sids = {sid for (sid, p) in ACTIVE_PAGE_VIEWS.keys() if p.startswith("/game/") or p.startswith("/g/")}
        game_count = len(game_sids)

        print(f"[VIEWERS] Total active sessions (≈people): {total_active}")
        print(f"[VIEWERS] Active on '/': {home_count}")
        print(f"[VIEWERS] Active on game pages: {game_count}")

        LAST_VIEWER_PRINT = now

    return jsonify({"ok": True})


# ====================== UTILITIES ======================
TEAM_SEP_REGEX = re.compile(r"\bvs\b|\bvs.\b|\bv\b|\bv.\b| - | – | — | @ ", re.IGNORECASE)
SLUG_CLEAN_QUOTES = re.compile(r"['\"`]")
SLUG_NON_ALNUM = re.compile(r"[^a-z0-9]+")
SLUG_MULTI_DASH = re.compile(r"-{2,}")
M3U8_SUFFIX = ".m3u8"
M3U8_PROXY_TIMEOUT = int(os.environ.get("M3U8_PROXY_TIMEOUT", "12"))
M3U8_PROXY_PLAYLIST_CACHE_SECONDS = int(
    os.environ.get("M3U8_PROXY_PLAYLIST_CACHE_SECONDS", "3")
)
M3U8_PROXY_SEGMENT_CACHE_SECONDS = int(
    os.environ.get("M3U8_PROXY_SEGMENT_CACHE_SECONDS", "60")
)


def safe_lower(value: Any) -> str:
    return value.lower() if isinstance(value, str) else ""


def normalize_sport_name(value: Any) -> str:
    """Normalize sport values so grouping/sorting never mixes types."""
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return "Other"
        compact = re.sub(r"[\s_-]+", " ", value).strip().lower()
        if compact == "american football":
            return "American Football"
        return value
    try:
        text = str(value).strip()
        return text or "Other"
    except Exception:
        return "Other"


def is_m3u8_url(value: str) -> bool:
    return M3U8_SUFFIX in (value or "").lower()


def build_m3u8_player_url(src: str) -> str:
    if not src:
        return ""
    if not is_m3u8_url(src):
        return src
    return f"/m3u8_player?{urlencode({'src': src})}"


def build_m3u8_proxy_url(src: str) -> str:
    if not src:
        return ""
    return f"/m3u8_proxy?{urlencode({'src': src})}"


CDP_EMBED_HOSTS = ("embed.st", "embedsports.top", "strmd.st")

# Endpoint the lock.wasm /fetch flow proxies to (browser-side mint lives in the
# embed_player template; the wasm POSTs its protobuf to this same-origin route).
EMBED_FETCH_UPSTREAM = "https://embed.st/fetch"
# lock.js + lock.wasm are served by the stream provider's CDN.
LOCK_JS_URL = "https://strmd.b-cdn.net/js/wasm/lock.js"


def is_cdp_embed_url(value: str) -> bool:
    """Do these embeds need the headless-harvest relay? (embed.st / embedsports.top family.)"""
    if not value:
        return False
    netloc = (urlparse(value).netloc or "").lower()
    return any(h in netloc for h in CDP_EMBED_HOSTS) or "/embed/" in value


def build_cdp_player_url(src: str) -> str:
    if not src:
        return ""
    if is_cdp_embed_url(src):
        # Prefer the browser-side mint player (no headless Chrome needed).
        # Falls back to /cdp_player for URLs the mint path can't parse.
        return build_embed_player_url(src)
    # plain m3u8 stays on the existing proxy path
    if is_m3u8_url(src):
        return build_m3u8_player_url(src)
    return src


def parse_embed_parts(src: str) -> list[str]:
    """Split an embed.st URL like https://embed.st/embed/<channel>/<id>/<part>
    into [channel, id, part]. Returns [] when it doesn't look like one."""
    if not src:
        return []
    try:
        path = urlparse(src).path or ""
    except Exception:
        return []
    m = re.match(r"^/embed/([^/]+)/([^/]+)/([0-9]+)$", path)
    if not m:
        return []
    return [m.group(1), m.group(2), m.group(3)]


def build_embed_player_url(src: str) -> str:
    """Route embed.st family URLs to the browser-side mint player."""
    if not src:
        return ""
    if parse_embed_parts(src):
        return f"/embed_player?{urlencode({'src': src})}"
    return build_cdp_player_url(src)


def normalize_http_url(value: str) -> str:
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https"):
        return ""
    return value


def normalize_m3u8_src(value: str) -> str:
    if not value or not is_m3u8_url(value):
        return ""
    return normalize_http_url(value)


INVALID_SPORT_MARKERS = {"other", "unknown", "nan", "n/a", "none", "null", ""}


def sport_is_invalid(value: Any) -> bool:
    """Return True when the sport should be treated as unclassified."""

    normalized = normalize_sport_name(value)
    return normalized.lower() in INVALID_SPORT_MARKERS


def coerce_start_datetime(rowd: dict[str, Any]) -> datetime | None:
    """Try to produce a timezone-aware UTC datetime from available fields."""

    ts = rowd.get("time_unix")
    if ts not in (None, ""):
        try:
            ts_float = float(ts)
            if ts_float == ts_float:  # not NaN
                if ts_float > 1e11:  # likely ms
                    ts_float = ts_float / 1000.0
                return datetime.fromtimestamp(ts_float, tz=timezone.utc)
        except Exception:
            pass

    raw_time = rowd.get("time")
    if isinstance(raw_time, str) and raw_time.strip():
        dt = _parse_iso_to_utc(raw_time)
        if dt is not None:
            return dt

    date_header = rowd.get("date_header")
    if isinstance(date_header, str) and date_header.strip():
        try:
            naive = datetime.strptime(date_header, "%A, %B %d, %Y")
            return naive.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    return None


def _parse_iso_to_utc(value: str) -> datetime | None:
    """Parse an ISO-8601 timestamp (with optional offset) to aware UTC using stdlib."""
    try:
        dt = datetime.fromisoformat(value.strip())
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _game_not_started(game: dict[str, Any]) -> bool:
    """True when the game has a future start time but isn't live yet.

    Matches the is_live window used in _build_games_from_rows (live from
    15 minutes before start through 5 hours after). If we can't determine a
    start time, assume the stream is available so the player still renders.
    """
    if game.get("is_live"):
        return False
    start_dt = coerce_start_datetime(game)
    if start_dt is None:
        return False
    now_utc = datetime.now(timezone.utc)
    return start_dt > now_utc + timedelta(minutes=15)



def make_stable_id(row: dict[str, Any]) -> int:
    key = f"{row.get('date_header', '')}|{row.get('sport', '')}|{row.get('tournament', '')}|{row.get('matchup', '')}"
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def slugify(text: str) -> str:
    if not isinstance(text, str):
        return ""
    s = text.strip().lower()
    s = SLUG_CLEAN_QUOTES.sub("", s)
    s = SLUG_NON_ALNUM.sub("-", s)
    s = SLUG_MULTI_DASH.sub("-", s).strip("-")
    return s


def game_slug(game: dict[str, Any]) -> str:
    date_part = slugify(str(game.get("date_header") or "today"))
    matchup_part = slugify(str(game.get("matchup") or "game"))
    sport_part = slugify(str(game.get("sport") or "sport"))
    base = f"{date_part}-{matchup_part}-{sport_part}"
    base = SLUG_MULTI_DASH.sub("-", base).strip("-")

    gid = str(game.get("id") or "")
    suffix = gid[-4:] if gid else "0000"
    return f"{base}-{suffix}"


def normalize_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "live", "t")


def parse_streams_json(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return value

    raw = str(value).strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []

    out = []
    for item in parsed:
        if isinstance(item, dict) and item.get("embed_url"):
            fixed = dict(item)
            fixed["label"] = fixed.get("label") or "Stream"
            fixed["embed_url"] = fixed.get("embed_url")
            fixed["watch_url"] = fixed.get("watch_url")
            out.append(fixed)
    return out


def _serialize_streams(streams_list: list[dict[str, Any]]) -> str:
    cleaned = []
    for s in (streams_list or []):
        if not isinstance(s, dict):
            continue
        embed = s.get("embed_url")
        if not embed:
            continue
        fixed = dict(s)
        fixed["label"] = fixed.get("label") or "Stream"
        fixed["embed_url"] = embed
        cleaned.append(fixed)
    return json.dumps(cleaned, ensure_ascii=False)


def _ensure_games_db() -> None:
    GAMES_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(GAMES_DB_PATH, check_same_thread=False) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY,
                source TEXT,
                date_header TEXT,
                sport TEXT,
                time_unix REAL,
                time TEXT,
                tournament TEXT,
                tournament_url TEXT,
                matchup TEXT,
                watch_url TEXT,
                is_live INTEGER DEFAULT 0,
                streams_json TEXT,
                embed_url TEXT,
                updated_at REAL,
                home_team TEXT,
                away_team TEXT,
                home_score INTEGER,
                away_score INTEGER,
                game_status TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS games_meta (
                id INTEGER PRIMARY KEY,
                updated_at REAL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO games_meta (id, updated_at)
            VALUES (1, 0)
            ON CONFLICT(id) DO NOTHING
            """
        )
        _migrate_score_columns(conn)


def _get_games_db_connection() -> sqlite3.Connection:
    _ensure_games_db()
    conn = sqlite3.connect(GAMES_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _migrate_score_columns(conn: sqlite3.Connection) -> None:
    """Add score columns to an existing games table if missing."""
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(games)").fetchall()}
    except Exception:
        return
    for name, ddl in (
        ("home_team", "TEXT"),
        ("away_team", "TEXT"),
        ("home_score", "INTEGER"),
        ("away_score", "INTEGER"),
        ("game_status", "TEXT"),
    ):
        if name not in cols:
            try:
                conn.execute(f"ALTER TABLE games ADD COLUMN {name} {ddl}")
            except Exception:
                pass


def _get_games_db_last_updated() -> float:
    _ensure_games_db()
    with sqlite3.connect(GAMES_DB_PATH, check_same_thread=False) as conn:
        row = conn.execute("SELECT updated_at FROM games_meta WHERE id = 1").fetchone()
    if row and row[0]:
        return float(row[0])
    return 0.0


def _touch_games_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT INTO games_meta (id, updated_at)
        VALUES (1, ?)
        ON CONFLICT(id) DO UPDATE SET updated_at = excluded.updated_at
        """,
        (time.time(),),
    )


def _games_db_has_rows() -> bool:
    _ensure_games_db()
    with sqlite3.connect(GAMES_DB_PATH, check_same_thread=False) as conn:
        row = conn.execute("SELECT COUNT(*) FROM games").fetchone()
    return bool(row and row[0])


def require_admin() -> bool:
    required = os.environ.get("ADMIN_API_KEY", "").strip()
    if not required:
        return True
    got = request.headers.get("X-API-Key", "").strip()
    return got == required


def _absolute_url(path: str) -> str:
    return urljoin(request.url_root, path.lstrip("/"))


@streaming_bp.after_request
def add_cache_headers(resp):
    """Helps Cloudflare + browser caching."""

    try:
        if request.method == "GET" and resp.mimetype in ("text/html", "text/plain"):
            resp.headers["Cache-Control"] = f"public, max-age={HTML_CACHE_SECONDS}"
    except Exception:
        pass
    return resp


def _dedup_stream_slug(slug: str, seen: set[str]) -> str:
    if not slug:
        slug = "stream"
    base = slug
    i = 2
    while slug in seen:
        slug = f"{base}-{i}"
        i += 1
    seen.add(slug)
    return slug


SPORT_MAP = {
    "Football": "Soccer",
    "Soccer": "Soccer",
    "American Football": "American Football",
    "NFL": "American Football",
    "Basketball": "Basketball",
    "NBA": "Basketball",
    "Tennis": "Tennis",
    "Ice Hockey": "Ice Hockey",
    "Hockey": "Ice Hockey",
    "Rugby Union": "Rugby",
    "Rugby": "Rugby",
    "Handball": "Handball",
    "Darts": "Darts",
    "Boxing": "Boxing",
    "Cricket": "Cricket",
    "Volleyball": "Volleyball",
    "Equestrian": "Equestrian",
}

SPORT_KEYWORD_MAP = [
    ("nba", "Basketball"),
    ("basketball", "Basketball"),
    ("wnba", "Basketball"),
    ("ncaa basketball", "Basketball"),
    ("college basketball", "Basketball"),
    ("nba g-league", "Basketball"),
    ("nfl", "American Football"),
    ("american football", "American Football"),
    ("ncaa football", "College Football"),
    ("college football", "College Football"),
    ("mlb", "MLB"),
    ("baseball", "MLB"),
    ("nhl", "Ice Hockey"),
    ("hockey", "Ice Hockey"),
    ("ice hockey", "Ice Hockey"),
    ("pwhl", "Ice Hockey"),
    ("soccer", "Soccer"),
    ("football", "Soccer"),
    ("mls", "Soccer"),
    ("premier league", "Soccer"),
    ("la liga", "Soccer"),
    ("bundesliga", "Soccer"),
    ("serie a", "Soccer"),
    ("ligue 1", "Soccer"),
    ("champions league", "Soccer"),
    ("uefa", "Soccer"),
    ("ucl", "Soccer"),
    ("africa cup of nations", "Soccer"),
    ("copa", "Soccer"),
    ("eredivisie", "Soccer"),
    ("laliga", "Soccer"),
    ("ligue 2", "Soccer"),
    ("ufc", "MMA"),
    ("mma", "MMA"),
    ("bellator", "MMA"),
    ("boxing", "Boxing"),
    ("formula 1", "Motorsport"),
    ("formula1", "Motorsport"),
    ("f1", "Motorsport"),
    ("f2", "Motorsport"),
    ("nascar", "Motorsport"),
    ("motogp", "Motorsport"),
    ("tennis", "Tennis"),
    ("atp", "Tennis"),
    ("wta", "Tennis"),
    ("golf", "Golf"),
    ("pga", "Golf"),
    ("lpga", "Golf"),
    ("cricket", "Cricket"),
    ("ashes", "Cricket"),
    ("t20", "Cricket"),
    ("bbl", "Cricket"),
    ("big bash", "Cricket"),
    ("international league t20", "Cricket"),
    ("ilt20", "Cricket"),
    ("test series", "Cricket"),
    ("one day", "Cricket"),
    ("odi", "Cricket"),
    ("rugby", "Rugby"),
    ("rugby union", "Rugby"),
    ("top 14", "Rugby"),
    ("premiership", "Rugby"),
    ("handball", "Handball"),
    ("volleyball", "Volleyball"),
    ("darts", "Darts"),
    ("equestrian", "Equestrian"),
    ("curling", "Curling"),
    ("horse racing", "Horse Racing"),
]


def merge_streams(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]):
    def norm(s):
        return (
            (s.get("embed_url") or "").strip(),
            (s.get("watch_url") or "").strip(),
            (s.get("label") or "").strip().lower(),
        )

    seen = set()
    out = []

    for s in (existing or []):
        if not isinstance(s, dict) or not s.get("embed_url"):
            continue
        k = norm(s)
        if k in seen:
            continue
        seen.add(k)
        out.append(dict(s))

    for s in (incoming or []):
        if not isinstance(s, dict) or not s.get("embed_url"):
            continue
        k = norm(s)
        if k in seen:
            continue
        seen.add(k)
        out.append(dict(s))

    for s in out:
        s["label"] = s.get("label") or "Stream"
    return out


def _load_games_from_db() -> list[dict[str, Any]]:
    with _get_games_db_connection() as conn:
        rows = conn.execute("SELECT * FROM games").fetchall()
    row_dicts = []
    for row in rows:
        rowd = dict(row)
        rowd["streams"] = parse_streams_json(rowd.get("streams_json"))
        row_dicts.append(rowd)
    return _build_games_from_rows(row_dicts)


def load_games_cached() -> list[dict[str, Any]]:
    """Cached loader for games from sqlite."""

    now = time.time()
    previous_games: list[dict[str, Any]] = []
    db_updated = _get_games_db_last_updated()

    with GAMES_CACHE_LOCK:
        previous_games = list(GAMES_CACHE.get("games") or [])
        cache_ok = (
            GAMES_CACHE["games"]
            and (now - GAMES_CACHE["ts"] < GAMES_CACHE_TTL_SECONDS)
            and (db_updated == GAMES_CACHE["mtime"])
        )
        if cache_ok:
            cached_games = GAMES_CACHE["games"]
            if any(sport_is_invalid(g.get("sport")) for g in cached_games):
                cache_ok = False
            else:
                return cached_games

    games = _load_games_from_db()

    if not games and previous_games:
        print(
            f"[loader][WARN] Parsed 0 games from {GAMES_DB_PATH}; "
            f"serving {len(previous_games)} cached games instead."
        )
        return previous_games

    with GAMES_CACHE_LOCK:
        GAMES_CACHE["games"] = games
        GAMES_CACHE["ts"] = now
        GAMES_CACHE["mtime"] = db_updated

    return games


def get_game_view_counts(cutoff_seconds: int = 45) -> dict[int, int]:
    if not ENABLE_VIEWER_TRACKING:
        return {}

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=cutoff_seconds)
    counts: dict[int, int] = {}

    for (sid, path), ts in list(ACTIVE_PAGE_VIEWS.items()):
        if ts < cutoff:
            continue
        if not path.startswith("/game/"):
            continue
        try:
            game_id_str = path.rstrip("/").split("/")[-1]
            game_id = int(game_id_str)
        except ValueError:
            continue
        counts[game_id] = counts.get(game_id, 0) + 1

    return counts


def get_most_viewed_games(all_games: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    counts = get_game_view_counts()
    if not counts:
        return []

    games_by_id = {g["id"]: g for g in all_games}
    sorted_ids = sorted(counts.keys(), key=lambda gid: counts[gid], reverse=True)

    result = []
    for gid in sorted_ids:
        game = games_by_id.get(gid)
        if not game:
            continue
        g_copy = dict(game)
        g_copy["active_viewers"] = counts[gid]
        result.append(g_copy)
        if len(result) >= limit:
            break

    return result


@streaming_bp.route("/")
def index():
    mark_active()

    all_games = load_games_cached()
    games = list(all_games)

    q = request.args.get("q", "").strip().lower()
    if q:
        games = [
            game
            for game in games
            if q in safe_lower(game.get("matchup"))
            or q in safe_lower(game.get("sport"))
            or q in safe_lower(game.get("tournament"))
        ]

    live_only = request.args.get("live_only", "").lower() in ("1", "true", "yes", "on")
    if live_only:
        games = [game for game in games if game.get("is_live")]

    sections_by_sport: dict[str, list[dict[str, Any]]] = {}
    for game in games:
        sport = normalize_sport_name(game.get("sport"))
        if sport_is_invalid(sport):
            continue
        sections_by_sport.setdefault(sport, []).append(game)

    sections = [{"sport": s, "games": lst} for s, lst in sections_by_sport.items()]
    sections.sort(key=lambda s: normalize_sport_name(s["sport"]).lower())

    most_viewed_games = get_most_viewed_games(all_games, limit=5)

    return render_template(
        "streaming_index.html",
        sections=sections,
        search_query=q,
        live_only=live_only,
        most_viewed_games=most_viewed_games,
        current_session=get_session_id(),
    )



@streaming_bp.route("/m3u8_player")
def m3u8_player():
    src = normalize_m3u8_src((request.args.get("src") or "").strip())
    return render_template("m3u8_player.html", src=src)


@streaming_bp.route("/cdp_player")
def cdp_player():
    src = normalize_http_url((request.args.get("src") or "").strip())
    if not src:
        return render_template("cdp_player.html", src="")
    return render_template("cdp_player.html", src=src)


@streaming_bp.route("/embed_player")
def embed_player():
    src = normalize_http_url((request.args.get("src") or "").strip())
    candidates_raw = request.args.get("srcs") or ""
    candidates: list[str] = []
    if candidates_raw:
        try:
            parsed = json.loads(candidates_raw)
        except Exception:
            parsed = []
        for u in parsed:
            nu = normalize_http_url(str(u or ""))
            if nu and parse_embed_parts(nu) and nu not in candidates:
                candidates.append(nu)
    if src and src not in candidates and parse_embed_parts(src):
        candidates.append(src)
    if not candidates:
        return render_template("embed_player.html", src="", parts=[], candidates=[], idx=0, lock_url=LOCK_JS_URL)

    try:
        idx = max(0, int(request.args.get("idx", "0")))
    except Exception:
        idx = 0
    if idx >= len(candidates):
        idx = 0
    active_src = candidates[idx]
    parts = parse_embed_parts(active_src)
    candidate_parts = [{"src": c, "parts": parse_embed_parts(c)} for c in candidates]
    return render_template(
        "embed_player.html",
        src=active_src,
        parts=parts,
        candidates=candidate_parts,
        idx=idx,
        lock_url=LOCK_JS_URL,
    )


def build_embed_player_fallback_url(active_embed_url: str, all_embeds) -> str:
    """Build an /embed_player URL that starts on active_embed_url but falls back
    to the other candidates in all_embeds when the active stream is dead."""
    candidates: list[str] = []
    for u in (all_embeds or []):
        nu = normalize_http_url(str(u or ""))
        if nu and parse_embed_parts(nu) and nu not in candidates:
            candidates.append(nu)
    if active_embed_url in candidates:
        idx = candidates.index(active_embed_url)
    else:
        idx = 0
    if not candidates:
        return ""
    return "/embed_player?" + urlencode({
        "src": candidates[idx],
        "srcs": json.dumps(candidates),
        "idx": str(idx),
    })


@streaming_bp.route("/fetch", methods=["POST", "OPTIONS"])
@streaming_bp.route("/embed_fetch", methods=["POST", "OPTIONS"])
def embed_fetch():
    """Same-origin relay for lock.wasm's /fetch.

    The viewer browser runs lock.wasm, which POSTs its protobuf to the SAME ORIGIN
    path /fetch (the wasm hardcodes {origin}/fetch). We proxy the body to the real
    embed.st /fetch and forward ALL response headers verbatim - most importantly the
    `Goat` header (and its Access-Control-Expose-Headers) that the wasm decodes the
    blob with. No secrets are touched server-side.
    """
    if request.method == "OPTIONS":
        resp = make_response("", 200)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "*"
        resp.headers["Access-Control-Max-Age"] = "600"
        return resp

    body = request.get_data()
    if not body:
        return jsonify({"ok": False, "error": "empty body"}), 400
    try:
        referer = "https://embed.st/embed/admin/2501/1"
        browser_referer = request.headers.get("Referer", "") or ""
        if "src=" in browser_referer:
            try:
                from urllib.parse import urlparse, parse_qs
                src_candidate = parse_qs(urlparse(browser_referer).query).get("src", [""])[0]
                if src_candidate.startswith("http"):
                    referer = src_candidate
            except Exception:
                pass
        upstream_headers = {
            "Content-Type": "application/octet-stream",
            "Referer": referer,
            "User-Agent": request.headers.get("User-Agent", "") or "Mozilla/5.0",
        }
        for header_name in ("Accept", "Accept-Language"):
            header_value = request.headers.get(header_name)
            if header_value:
                upstream_headers[header_name] = header_value
        resp = requests.post(
            EMBED_FETCH_UPSTREAM,
            data=body,
            headers=upstream_headers,
            timeout=25,
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 502

    proxy_resp = Response(resp.content, status=resp.status_code)
    proxy_resp.headers["Content-Type"] = resp.headers.get(
        "content-type", "application/octet-stream"
    )
    # Forward the custom Goat header + the CORS expose directive verbatim.
    for header_name in ("Goat", "Access-Control-Expose-Headers", "Cache-Control"):
        header_value = resp.headers.get(header_name)
        if header_value:
            proxy_resp.headers[header_name] = header_value
    proxy_resp.headers["Access-Control-Allow-Origin"] = "*"
    return proxy_resp


@streaming_bp.route("/m3u8_proxy")
def m3u8_proxy():
    src = normalize_http_url((request.args.get("src") or "").strip())
    if not src:
        return abort(400)

    parsed_src = urlparse(src)
    upstream_origin = f"{parsed_src.scheme}://{parsed_src.netloc}" if parsed_src.scheme else ""
    upstream_headers = {
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    for header_name in ("User-Agent", "Accept", "Accept-Language", "Cookie"):
        header_value = request.headers.get(header_name)
        if header_value:
            upstream_headers[header_name] = header_value
    if upstream_origin:
        upstream_headers["Referer"] = upstream_origin
        upstream_headers["Origin"] = upstream_origin

    try:
        resp = requests.get(
            src,
            timeout=M3U8_PROXY_TIMEOUT,
            stream=True,
            headers=upstream_headers,
        )
    except Exception:
        return abort(502)

    content_type = resp.headers.get("content-type", "application/octet-stream")
    status = resp.status_code
    base_url = resp.url

    if is_m3u8_url(base_url) or "mpegurl" in content_type.lower():
        try:
            text = resp.text
        except Exception:
            return abort(502)

        lines = text.splitlines()
        rewritten = []
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                rewritten.append(line)
                continue
            absolute = urljoin(base_url, stripped)
            rewritten.append(build_m3u8_proxy_url(absolute))

        body = "\n".join(rewritten)
        proxy_resp = make_response(body, status)
        proxy_resp.headers["Content-Type"] = "application/vnd.apple.mpegurl"
        proxy_resp.headers["Access-Control-Allow-Origin"] = "*"
        proxy_resp.headers["Cache-Control"] = (
            "public, max-age={max_age}, s-maxage={max_age}, "
            "stale-while-revalidate=10"
        ).format(max_age=M3U8_PROXY_PLAYLIST_CACHE_SECONDS)
        return proxy_resp

    def generate():
        for chunk in resp.iter_content(chunk_size=256 * 1024):
            if chunk:
                yield chunk

    proxy_resp = Response(stream_with_context(generate()), status=status)
    proxy_resp.headers["Content-Type"] = content_type
    proxy_resp.headers["Access-Control-Allow-Origin"] = "*"
    proxy_resp.headers["Cache-Control"] = (
        "public, max-age={max_age}, s-maxage={max_age}, stale-while-revalidate=60"
    ).format(max_age=M3U8_PROXY_SEGMENT_CACHE_SECONDS)
    return proxy_resp


@streaming_bp.route("/strmd/<path:media_path>")
def strmd_media_proxy(media_path):
    """Same-origin relay for lb*.strmd.st media.

    lock.wasm mints a `/secure/<tok>/rtmp/stream/<id>` URL that the CDN only serves
    to real-browser sessions (the CDN TLS-fingerprints the client: plain python-requests
    gets 403 nginx, a Chrome-impersonating client gets 200). The template rewrites any
    https://lb*.strmd.st/* source into /strmd/lb*.strmd.st/* so the browser's
    hls.js/XHR traffic is same-origin here, and we re-request upstream with curl_cffi
    impersonating Chrome. We rewrite the m3u8 bodies so nested playlists/segments keep
    flowing through this proxy.
    """
    try:
        return _strmd_media_proxy_impl(media_path)
    except Exception as e:
        return abort(500)


_STRMD_CURL_CLIENT = None
_STRMD_CURL_LOCK = threading.Lock()


def _get_curl_client():
    global _STRMD_CURL_CLIENT
    if _STRMD_CURL_CLIENT is None:
        with _STRMD_CURL_LOCK:
            if _STRMD_CURL_CLIENT is None:
                _STRMD_CURL_CLIENT = curl_requests.Session(impersonate="chrome")
    return _STRMD_CURL_CLIENT


def _curl_get(url, headers=None, stream=False, timeout=None):
    client = _get_curl_client()
    return client.get(
        url,
        headers=headers or {},
        stream=stream,
        timeout=timeout or M3U8_PROXY_TIMEOUT,
    )


def _strmd_media_proxy_impl(media_path):
    if not media_path or "..." in media_path:
        return abort(400)

    # media_path = "lb8.strmd.st/secure/<tok>/rtmp/stream/<id>[/...].m3u8"
    host, _, rest = media_path.partition("/")
    if "." not in host:
        return abort(400)
    upstream = "https://{}/{}".format(host, rest)

    referer = "https://embed.st/"
    try:
        parts = parse_embed_parts(request.args.get("src") or "")
        if parts:
            ctype, cid, cpart = parts
            referer = "https://embed.st/embed/{}/{}/{}".format(ctype, cid, cpart)
    except Exception:
        pass

    upstream_headers = {
        "User-Agent": request.headers.get("User-Agent", "") or "Mozilla/5.0",
        "Referer": referer,
        "Accept": request.headers.get("Accept", "*/*"),
    }
    if request.headers.get("Range"):
        upstream_headers["Range"] = request.headers.get("Range")

    try:
        resp = _curl_get(upstream, headers=upstream_headers, stream=False)
    except Exception as e:
        return abort(502)

    content_type = resp.headers.get("content-type", "application/octet-stream")
    status = resp.status_code
    if status >= 400:
        try:
            resp.close()
        except Exception:
            pass
        return ("proxy error: {}".format(status), status)

    if "mpegurl" in content_type.lower() or upstream.rstrip("/").endswith(".m3u8"):
        try:
            text = resp.text
            resp.close()
        except Exception:
            return abort(502)
        # Only rewrite absolute lb*.strmd.st refs in the body into proxy URLs;
        # bare relative refs already resolve under this same /strmd/ proxy path.
        proxied_base = "https://{}/strmd/".format(request.host)
        rewritten = re.sub(
            r"https://(lb[\w.-]+\.strmd\.st/)([^\s\r\n]*)",
            lambda m: proxied_base + m.group(1) + m.group(2),
            text,
        )
        proxy_resp = make_response(rewritten, status)
        proxy_resp.headers["Content-Type"] = "application/vnd.apple.mpegurl"
        proxy_resp.headers["Access-Control-Allow-Origin"] = "*"
        proxy_resp.headers["Cache-Control"] = "no-store"
        return proxy_resp

    body = resp.content
    proxy_resp = Response(body, status=status)
    proxy_resp.headers["Content-Type"] = content_type
    proxy_resp.headers["Access-Control-Allow-Origin"] = "*"
    proxy_resp.headers["Cache-Control"] = "no-store"
    return proxy_resp
# Some embeds (embed.st / strmd.st) serve their HLS master/media manifests only to a
# real browser session (plain requests get 403 nginx). We drive a headless Chrome via
# CDP to capture those manifests, then serve just the tiny manifests to the viewer;
# segments play directly from the CDN (CORS-open). A background "follower" reloads
# the embed page periodically so the served window tracks the live broadcast.


def _cdp_alive(harvester) -> bool:
    try:
        return (
            not harvester.stopped
            and harvester.ws is not None
            and harvester.ws.connected
        )
    except Exception:
        return False


def _cdp_chrome_reachable(port: int) -> bool:
    import socket

    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            return True
    except Exception:
        return False


def cdp_get_or_harvest(embed_url: str):
    now = time.time()
    # prune dead/expired sessions once in a while
    if len(CDP_SESSIONS) > 0 and int(now) % 60 == 0:
        with CDP_SESSIONS_LOCK:
            dead = [
                k
                for k, s in CDP_SESSIONS.items()
                if now - s["created"] > CDP_SESSION_TTL_S or not _cdp_alive(s["harvester"])
            ]
            for k in dead:
                try:
                    CDP_SESSIONS[k]["harvester"].stop()
                except Exception:
                    pass
                CDP_SESSIONS.pop(k, None)

    with CDP_SESSIONS_LOCK:
        s = CDP_SESSIONS.get(embed_url)
        if s and _cdp_alive(s["harvester"]):
            return s["id"], s["harvester"]
        if s:
            CDP_SESSIONS.pop(embed_url, None)

    ensure_cdp_chrome(CDP_CHROME_PORT)
    # If no Chrome could be launched (e.g. Python buildpack w/o Chromium), fail
    # fast so the client falls back to a direct iframe instead of hanging.
    if not _cdp_chrome_reachable(CDP_CHROME_PORT):
        raise RuntimeError("CDP unavailable (no Chrome)")
    # release the lock while the slow CDP capture runs so other requests stay responsive
    h = CdpHarvester(
        embed_url,
        chrome_port=CDP_CHROME_PORT,
        harvest_timeout_ms=CDP_HARVEST_TIMEOUT_MS,
        media_wait_ms=CDP_MEDIA_WAIT_MS,
        follow_interval_ms=CDP_FOLLOW_INTERVAL_MS,
        refresh_timeout_ms=CDP_REFRESH_TIMEOUT_MS,
    ).run()
    sid = os.urandom(6).hex()
    with CDP_SESSIONS_LOCK:
        CDP_SESSIONS[embed_url] = {"id": sid, "harvester": h, "created": time.time()}
    return sid, h


def cdp_find(session_id: str):
    with CDP_SESSIONS_LOCK:
        for s in CDP_SESSIONS.values():
            if s["id"] == session_id:
                return s["harvester"]
    return None


@streaming_bp.route("/api/harvest", methods=["POST"])
def cdp_api_harvest():
    payload = request.get_json(silent=True) or {}
    embed_url = (payload.get("embedUrl") or payload.get("embed_url") or "").strip()
    if not embed_url or not str(embed_url).startswith(("http://", "https://")):
        return jsonify({"ok": False, "error": "embedUrl required"}), 400
    try:
        sid, h = cdp_get_or_harvest(str(embed_url))
        return jsonify({
            "ok": True,
            "id": sid,
            "embedUrl": embed_url,
            "secureUrl": h.secure_url,
            "token": h.token,
            "harvestedInMs": h.prep_time_ms,
            "variants": list(h.media.keys()),
            "playbackUrl": f"/api/playback/{sid}/master.m3u8",
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 502


@streaming_bp.route("/api/playback/<session_id>/master.m3u8")
def cdp_playback_master(session_id: str):
    h = cdp_find(session_id)
    if not h:
        return Response("session expired - re-harvest", status=404, mimetype="text/plain")
    snap = h.snapshot()
    body = snap["master"] or "#EXTM3U\n"
    return Response(
        build_master(session_id, body),
        mimetype="application/vnd.apple.mpegurl",
        headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "no-store"},
    )


@streaming_bp.route("/api/playback/<session_id>/stream_<kind>.m3u8")
def cdp_playback_variant(session_id: str, kind: str):
    h = cdp_find(session_id)
    if not h:
        return Response("session expired - re-harvest", status=404, mimetype="text/plain")
    snap = h.snapshot()
    media = snap["media"].get(kind)
    if not media or not media.get("body"):
        return Response(f"no {kind} variant captured", status=404, mimetype="text/plain")
    body = rewrite_playlist(media["body"], media["url"], direct=True)
    return Response(
        body,
        mimetype="application/vnd.apple.mpegurl",
        headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "no-store"},
    )


@streaming_bp.route("/api/refresh/<session_id>", methods=["POST"])
def cdp_playback_refresh(session_id: str):
    h = cdp_find(session_id)
    if not h or not h.refresh_now():
        return jsonify({"ok": False, "error": "harvest not ready"}), 409
    return jsonify({"ok": True})


def _build_games_from_rows(rows: list[dict[str, Any]]):
    if not rows:
        return []

    games = []
    dedup_map: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    now_utc = datetime.now(timezone.utc)
    stale_cutoff = now_utc - timedelta(hours=6)
    live_window_after_start = timedelta(hours=5)

    for rowd in rows:
        streams = rowd.get("streams") or []
        raw_embed_url = rowd.get("embed_url")
        embed_url = raw_embed_url.strip() if isinstance(raw_embed_url, str) else ""

        # Fallback: if the scraper populated embed_url but streams is empty,
        # expose a single stream so the UI renders an iframe instead of "no stream".
        if not streams and embed_url:
            streams = [
                {
                    "label": "Stream",
                    "embed_url": embed_url,
                    "watch_url": rowd.get("watch_url") or embed_url,
                }
            ]
        if streams:
            fixed_streams = []
            for stream in streams:
                if not isinstance(stream, dict):
                    continue
                fixed = dict(stream)
                embed = fixed.get("embed_url")
                if isinstance(embed, str) and embed:
                    fixed["embed_url"] = build_m3u8_player_url(embed)
                fixed_streams.append(fixed)
            streams = fixed_streams
        if not streams:
            continue
        game_id = make_stable_id(rowd)

        raw_sport = rowd.get("sport")
        raw_sport = raw_sport.strip() if isinstance(raw_sport, str) else raw_sport
        sport = SPORT_MAP.get(raw_sport, raw_sport)
        if not sport:
            haystack_parts = [
                rowd.get("tournament", ""),
                rowd.get("matchup", ""),
                rowd.get("watch_url", ""),
                rowd.get("source", ""),
            ]
            haystack = " ".join([str(p or "") for p in haystack_parts]).lower()
            for keyword, mapped in SPORT_KEYWORD_MAP:
                if keyword in haystack:
                    sport = mapped
                    break
        sport = sport or "Other"

        normalized_sport = sport.lower() if isinstance(sport, str) else ""
        needs_infer = normalized_sport in ("", "other", "unknown", "nan", "n/a", "none")

        if needs_infer:
            haystack_parts = [
                rowd.get("sport", ""),
                rowd.get("tournament", ""),
                rowd.get("matchup", ""),
                rowd.get("watch_url", ""),
                rowd.get("source", ""),
            ]
            haystack = " ".join([str(p or "") for p in haystack_parts]).lower()
            for keyword, mapped in SPORT_KEYWORD_MAP:
                if keyword in haystack:
                    sport = mapped
                    break

        sport = normalize_sport_name(sport or "")
        if sport.lower() in ("other", "unknown", "nan", "n/a", "none", "null", ""):
            continue

        start_dt = coerce_start_datetime(rowd)
        if start_dt and start_dt < stale_cutoff:
            continue

        normalized_sport = sport.lower() if isinstance(sport, str) else ""
        needs_infer = normalized_sport in ("", "other", "unknown", "nan", "n/a", "none")

        if needs_infer:
            haystack_parts = [
                rowd.get("sport", ""),
                rowd.get("tournament", ""),
                rowd.get("matchup", ""),
                rowd.get("watch_url", ""),
                rowd.get("source", ""),
            ]
            haystack = " ".join([str(p or "") for p in haystack_parts]).lower()
            for keyword, mapped in SPORT_KEYWORD_MAP:
                if keyword in haystack:
                    sport = mapped
                    break
            if not sport and "/" in haystack:
                parts = [p for p in haystack.replace("-", " ").split("/") if p]
                for keyword, mapped in SPORT_KEYWORD_MAP:
                    if any(keyword in p for p in parts):
                        sport = mapped
                        break

        sport = normalize_sport_name(sport or "")
        if sport.lower() in ("other", "unknown", "nan", "n/a", "none", "null", ""):
            continue

        start_dt = coerce_start_datetime(rowd)
        if start_dt and start_dt < stale_cutoff:
            continue

        is_live = normalize_bool(rowd.get("is_live"))
        if not is_live and start_dt:
            if (start_dt - timedelta(minutes=15)) <= now_utc <= (start_dt + live_window_after_start):
                is_live = True

        time_display = None
        raw_time = rowd.get("time")
        if isinstance(raw_time, str) and raw_time.strip():
            try:
                dt = datetime.fromisoformat(raw_time.strip())
                dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
                time_display = dt.strftime("%I:%M %p ET").lstrip("0")
            except Exception:
                time_display = None

        matchup = rowd.get("matchup")

        game_obj = {
            "id": game_id,
            "date_header": rowd.get("date_header"),
            "sport": sport,
            "time_unix": rowd.get("time_unix"),
            "time": time_display,
            "tournament": rowd.get("tournament"),
            "tournament_url": rowd.get("tournament_url"),
            "matchup": matchup,
            "watch_url": rowd.get("watch_url"),
            "streams": streams,
            "is_live": is_live,
            "home_team": rowd.get("home_team"),
            "away_team": rowd.get("away_team"),
            "home_score": rowd.get("home_score"),
            "away_score": rowd.get("away_score"),
            "game_status": rowd.get("game_status"),
        }

        dedup_key = (
            normalize_sport_name(sport).lower(),
            slugify(game_obj.get("matchup") or ""),
            str(rowd.get("date_header") or "").strip().lower(),
            str(int(start_dt.timestamp())) if start_dt else str(rowd.get("time_unix") or "").strip(),
        )

        existing = dedup_map.get(dedup_key)
        if existing:
            existing["streams"] = merge_streams(existing.get("streams"), game_obj.get("streams"))
            existing["is_live"] = existing.get("is_live") or game_obj.get("is_live")
            if not existing.get("watch_url") and game_obj.get("watch_url"):
                existing["watch_url"] = game_obj["watch_url"]
            if not existing.get("tournament_url") and game_obj.get("tournament_url"):
                existing["tournament_url"] = game_obj["tournament_url"]
            if not existing.get("time") and game_obj.get("time"):
                existing["time"] = game_obj["time"]
            if not existing.get("time_unix") and game_obj.get("time_unix"):
                existing["time_unix"] = game_obj["time_unix"]
            existing["id"] = existing.get("id") or game_obj.get("id")
            continue

        dedup_map[dedup_key] = game_obj

    for game_obj in dedup_map.values():
        game_obj["slug"] = game_slug(game_obj)

        seen: set[str] = set()
        for s in game_obj["streams"]:
            label = s.get("label") or "Stream"
            s_slug = slugify(label)
            s["slug"] = _dedup_stream_slug(s_slug, seen)

        games.append(game_obj)

    return games


@streaming_bp.route("/api/streams/add", methods=["POST"])
def api_add_streams():
    if not require_admin():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    if "game_id" not in payload:
        return jsonify({"ok": False, "error": "missing game_id"}), 400

    try:
        game_id = int(payload["game_id"])
    except Exception:
        return jsonify({"ok": False, "error": "game_id must be an int"}), 400

    incoming_streams = payload.get("streams")
    if incoming_streams is None:
        single = payload.get("stream")
        incoming_streams = [single] if single else []

    if not isinstance(incoming_streams, list):
        return jsonify({"ok": False, "error": "streams must be a list"}), 400

    with GAMES_DB_LOCK:
        with _get_games_db_connection() as conn:
            row = conn.execute(
                "SELECT streams_json, embed_url, is_live FROM games WHERE id = ?",
                (game_id,),
            ).fetchone()
            if row is None:
                return jsonify({"ok": False, "error": "game not found", "game_id": game_id}), 404

            existing_streams = parse_streams_json(row["streams_json"])
            merged = merge_streams(existing_streams, incoming_streams)

            set_embed_url = payload.get("set_embed_url")
            if isinstance(set_embed_url, str) and set_embed_url.strip():
                embed_url = set_embed_url.strip()
            else:
                embed_url = row["embed_url"] or ""
                if not embed_url and merged:
                    embed_url = merged[0].get("embed_url") or ""

            is_live = int(row["is_live"] or 0)
            if "set_is_live" in payload:
                is_live = 1 if payload.get("set_is_live") else 0

            conn.execute(
                """
                UPDATE games
                SET streams_json = ?, embed_url = ?, is_live = ?, updated_at = ?
                WHERE id = ?
                """,
                (_serialize_streams(merged), embed_url, is_live, time.time(), game_id),
            )
            _touch_games_db(conn)

    with GAMES_CACHE_LOCK:
        GAMES_CACHE["ts"] = 0
        GAMES_CACHE["mtime"] = 0

    return jsonify({"ok": True, "game_id": game_id, "streams_count": len(merged)})


@streaming_bp.route("/api/games/remove", methods=["POST"])
def api_games_remove():
    if not require_admin():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}

    if "game_id" not in payload:
        return jsonify({"ok": False, "error": "missing game_id"}), 400

    try:
        game_id = int(payload["game_id"])
    except Exception:
        return jsonify({"ok": False, "error": "game_id must be an int"}), 400

    with GAMES_DB_LOCK:
        with _get_games_db_connection() as conn:
            cur = conn.execute("DELETE FROM games WHERE id = ?", (game_id,))
            if cur.rowcount == 0:
                return jsonify({"ok": False, "error": "game not found", "game_id": game_id}), 404
            _touch_games_db(conn)

    with GAMES_CACHE_LOCK:
        GAMES_CACHE["ts"] = 0
        GAMES_CACHE["mtime"] = 0

    return jsonify({"ok": True, "removed": True, "game_id": game_id})


@streaming_bp.route("/api/games/upsert", methods=["POST"])
def api_games_upsert():
    if not require_admin():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}

    games = []
    if isinstance(payload.get("game"), dict):
        games = [payload["game"]]
    elif isinstance(payload.get("games"), list):
        games = [g for g in payload["games"] if isinstance(g, dict)]
    else:
        return jsonify({"ok": False, "error": "expected 'game' object or 'games' list"}), 400

    upserted = []
    with GAMES_DB_LOCK:
        with _get_games_db_connection() as conn:
            for g in games:
                row_like = {
                    "date_header": g.get("date_header", ""),
                    "sport": g.get("sport", ""),
                    "tournament": g.get("tournament", ""),
                    "matchup": g.get("matchup", ""),
                }
                game_id = make_stable_id(row_like)

                streams_list = g.get("streams")
                if isinstance(streams_list, str):
                    streams_list = parse_streams_json(streams_list)
                elif not isinstance(streams_list, list):
                    streams_list = []

                existing_row = conn.execute(
                    "SELECT streams_json FROM games WHERE id = ?",
                    (game_id,),
                ).fetchone()
                if existing_row:
                    existing_streams = parse_streams_json(existing_row["streams_json"])
                    merged = merge_streams(existing_streams, streams_list)
                else:
                    merged = merge_streams([], streams_list)

                embed_url = g.get("embed_url")
                if not (isinstance(embed_url, str) and embed_url.strip()):
                    embed_url = merged[0].get("embed_url") if merged else ""

                payload_row = {
                    "id": game_id,
                    "source": g.get("source", ""),
                    "date_header": g.get("date_header", ""),
                    "sport": g.get("sport", ""),
                    "time_unix": g.get("time_unix", ""),
                    "time": g.get("time", ""),
                    "tournament": g.get("tournament", ""),
                    "tournament_url": g.get("tournament_url", ""),
                    "matchup": g.get("matchup", ""),
                    "watch_url": g.get("watch_url", ""),
                    "is_live": 1 if normalize_bool(g.get("is_live")) else 0,
                    "streams_json": _serialize_streams(merged),
                    "embed_url": embed_url or "",
                    "updated_at": time.time(),
                }

                conn.execute(
                    """
                    INSERT INTO games (
                        id, source, date_header, sport, time_unix, time,
                        tournament, tournament_url, matchup, watch_url, is_live,
                        streams_json, embed_url, updated_at
                    )
                    VALUES (
                        :id, :source, :date_header, :sport, :time_unix, :time,
                        :tournament, :tournament_url, :matchup, :watch_url, :is_live,
                        :streams_json, :embed_url, :updated_at
                    )
                    ON CONFLICT(id) DO UPDATE SET
                        source = excluded.source,
                        date_header = excluded.date_header,
                        sport = excluded.sport,
                        time_unix = excluded.time_unix,
                        time = excluded.time,
                        tournament = excluded.tournament,
                        tournament_url = excluded.tournament_url,
                        matchup = excluded.matchup,
                        watch_url = excluded.watch_url,
                        is_live = excluded.is_live,
                        streams_json = excluded.streams_json,
                        embed_url = excluded.embed_url,
                        updated_at = excluded.updated_at
                    """,
                    payload_row,
                )
                action = "updated" if existing_row else "inserted"
                upserted.append({"game_id": game_id, "action": action})

            _touch_games_db(conn)

    with GAMES_CACHE_LOCK:
        GAMES_CACHE["ts"] = 0
        GAMES_CACHE["mtime"] = 0

    return jsonify({"ok": True, "results": upserted})


@streaming_bp.route("/api/games/clear_streams", methods=["POST"])
def api_games_clear_streams():
    """Clear all stream/embed data to force a full reload via the scraper."""
    if not require_admin():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    with GAMES_DB_LOCK:
        with _get_games_db_connection() as conn:
            conn.execute(
                "UPDATE games SET streams_json = ?, embed_url = ?, updated_at = ?",
                ("[]", "", time.time()),
            )
            _touch_games_db(conn)
            row = conn.execute("SELECT COUNT(*) FROM games").fetchone()
            row_count = int(row[0]) if row else 0

    with GAMES_CACHE_LOCK:
        GAMES_CACHE["ts"] = 0
        GAMES_CACHE["mtime"] = 0

    return jsonify({"ok": True, "rows": row_count})


def _game_exists(game_id: int) -> bool:
    games = load_games_cached()
    return any(g["id"] == game_id for g in games)


@streaming_bp.route("/game/<int:game_id>")
def game_detail(game_id: int):
    mark_active()

    games = load_games_cached()
    game = next((g for g in games if g["id"] == game_id), None)
    if not game:
        abort(404)

    not_started = _game_not_started(game)

    other_games = [g for g in games if g["id"] != game_id and g.get("streams")]

    slug = game.get("slug") or game_slug(game)
    share_id_url = _absolute_url(url_for("streaming.game_detail", game_id=game_id))
    share_slug_url = _absolute_url(url_for("streaming.game_by_slug", slug=slug))
    og_image_url = _absolute_url(url_for("static", filename="preview.svg"))

    current_user = getattr(g, "current_user", None)

    return render_template(
        "streaming_game.html",
        game=game,
        other_games=other_games,
        not_started=not_started,
        share_id_url=share_id_url,
        share_slug_url=share_slug_url,
        og_image_url=og_image_url,
        current_user=current_user,
    )


@streaming_bp.route("/api/game/<int:game_id>/status")
def api_game_status(game_id: int):
    """Lightweight live-status for the game page auto-refresh."""
    games = load_games_cached()
    game = next((g for g in games if g["id"] == game_id), None)
    if not game:
        return jsonify({"ok": False, "error": "not found"}), 404
    return jsonify({
        "ok": True,
        "is_live": bool(game.get("is_live")),
        "not_started": _game_not_started(game),
        "home_score": game.get("home_score"),
        "away_score": game.get("away_score"),
        "game_status": game.get("game_status"),
    })


@streaming_bp.route("/g/<slug>")
def game_by_slug(slug: str):
    mark_active()

    slug = (slug or "").strip().lower()
    if not slug:
        abort(404)

    games = load_games_cached()
    game = next((g for g in games if (g.get("slug") or "").lower() == slug), None)
    if not game:
        game = next((g for g in games if (g.get("slug") or "").lower().startswith(slug)), None)
    if not game:
        abort(404)

    qs = request.query_string.decode("utf-8", errors="ignore").strip()
    target = url_for("streaming.game_detail", game_id=game["id"])
    if qs:
        target = f"{target}?{qs}"

    return make_response(redirect(target, code=302))


def _build_stream_label(stream: dict[str, Any]) -> str:
    base = f"Stream {stream.get('streamNo')}" if stream.get("streamNo") else "Stream"
    extras = []
    lang = (stream.get("language") or "").strip()
    if lang:
        extras.append(lang)
    if stream.get("hd"):
        extras.append("HD")
    if extras:
        base = f"{base} ({' - '.join(extras)})"
    source = (stream.get("source") or "").strip()
    if source:
        return f"{source} · {base}"
    return base


def _fetch_streams_for_source(session, source: str, source_id: str) -> list[dict[str, Any]]:
    if not source or not source_id:
        return []
    api_url = urljoin(os.environ.get("STREAMED_API_BASE", "https://streamed.pk"), f"/api/stream/{source}/{source_id}")
    try:
        resp = session.get(api_url, timeout=int(os.environ.get("REQUEST_TIMEOUT", "8")))
        if resp.status_code != 200:
            return []
        payload = resp.json() or []
    except Exception:
        return []

    streams: list[dict[str, Any]] = []
    for st in payload:
        if not isinstance(st, dict):
            continue
        embed = (st.get("embedUrl") or "").strip()
        if not embed:
            continue
        streams.append(
            {
                "label": _build_stream_label(st),
                "embed_url": embed,
                "watch_url": embed,
                "origin": "scraped",
                "language": st.get("language"),
                "hd": bool(st.get("hd")),
                "source": st.get("source"),
            }
        )
    return streams


# ====================== SCHEDULER (OFF BY DEFAULT) ======================
ESPN_SCOREBOARDS = {
    "MLB": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
    "American Football": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    "Basketball": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
}

_ESPN_TEAM_NAME_CACHE: dict[str, list[dict[str, Any]]] = {}


def _espn_normalize(name: str) -> str:
    s = re.sub(r"[^a-z0-9 ]", "", (name or "").lower())
    s = re.sub(r"\b(the|fc|sc|cf|afc|nfc|as|club)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _fetch_espn_scoreboard(sport: str) -> list[dict[str, Any]]:
    url = ESPN_SCOREBOARDS.get(sport)
    if not url:
        return []
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return []
        data = resp.json()
    except Exception:
        return []
    events = []
    for ev in data.get("events") or []:
        comps = ev.get("competitions") or []
        if not comps:
            continue
        comp = comps[0]
        teams = {}
        for c in comp.get("competitors") or []:
            side = c.get("homeAway")
            name = (c.get("team") or {}).get("displayName", "")
            teams[side] = {
                "name": name,
                "normalized": _espn_normalize(name),
                "abbrev": (c.get("team") or {}).get("abbreviation", "").lower(),
            }
        status = comp.get("status") or {}
        stype = status.get("type") or {}
        events.append({
            "home": teams.get("home"),
            "away": teams.get("away"),
            "home_score": _safe_int((comp.get("competitors") or [{}])[0].get("score")) if comp.get("competitors") else None,
            "away_score": None,
            "state": stype.get("state"),  # pre | in | post
            "detail": stype.get("detail"),  # e.g. "Final", "Top 7th", "Q3 02:31"
        })
        # home_score/away_score per side
        for c in comp.get("competitors") or []:
            side = c.get("homeAway")
            score = _safe_int(c.get("score"))
            if side == "home":
                events[-1]["home_score"] = score
            elif side == "away":
                events[-1]["away_score"] = score
    return events


def _safe_int(v) -> int | None:
    try:
        return int(float(v))
    except Exception:
        return None


def _match_espn_game(events: list[dict[str, Any]], home_team: str, away_team: str):
    """Match a game to an ESPN event by normalized team names."""
    h = _espn_normalize(home_team)
    a = _espn_normalize(away_team)
    if not h or not a:
        return None
    for ev in events:
        eh = (ev.get("home") or {}).get("normalized")
        ea = (ev.get("away") or {}).get("normalized")
        eh_ab = (ev.get("home") or {}).get("abbrev")
        ea_ab = (ev.get("away") or {}).get("abbrev")
        if (eh and (eh == h or eh_ab == h)) and (ea and (ea == a or ea_ab == a)):
            return ev
        if (eh and (eh == a or eh_ab == a)) and (ea and (ea == h or ea_ab == h)):
            return ev
    return None


def enrich_scores() -> int:
    """Fetch live scores from ESPN for MLB/NFL/NBA and store them in the DB.

    Runs after each scrape. Returns how many games were updated.
    """
    updated = 0
    try:
        with _get_games_db_connection() as conn:
            rows = conn.execute(
                "SELECT id, sport, home_team, away_team FROM games WHERE home_team != '' AND away_team != ''"
            ).fetchall()
    except Exception as exc:
        print(f"[scores][ERROR] DB read failed: {exc}")
        return 0

    by_sport: dict[str, list[dict]] = {}
    for r in rows:
        sport = str(r["sport"] or "").strip()
        if sport in ESPN_SCOREBOARDS:
            by_sport.setdefault(sport, []).append(dict(r))

    if not by_sport:
        return 0

    for sport, game_rows in by_sport.items():
        events = _fetch_espn_scoreboard(sport)
        if not events:
            continue
        with _get_games_db_connection() as conn:
            for gr in game_rows:
                ev = _match_espn_game(events, gr["home_team"], gr["away_team"])
                if not ev:
                    continue
                state = ev.get("state")
                detail = ev.get("detail") or ""
                is_final = state == "post" or detail.strip().lower().startswith("final")
                status = "Final" if is_final else detail
                conn.execute(
                    "UPDATE games SET home_score = ?, away_score = ?, game_status = ?, is_live = ? WHERE id = ?",
                    (ev.get("home_score"), ev.get("away_score"), status,
                     0 if is_final else 1, gr["id"]),
                )
                updated += 1
    if updated:
        print(f"[scores] Updated {updated} games with ESPN scores")
        with GAMES_CACHE_LOCK:
            GAMES_CACHE["ts"] = 0
    return updated


def run_scraper_job():
    try:
        if SCRAPER_SUBPROCESS:
            scraper_path = Path(__file__).parent / "scrape_games.py"
            subprocess.run([sys.executable, str(scraper_path)], check=True)
        else:
            import scrape_games

            scrape_games.main()
        try:
            games = _load_games_from_db()
            with GAMES_CACHE_LOCK:
                GAMES_CACHE["games"] = games
                GAMES_CACHE["ts"] = time.time()
                GAMES_CACHE["mtime"] = _get_games_db_last_updated()
            print(f"[scheduler] Cached {len(games)} games from {GAMES_DB_PATH}")
        except Exception as parse_exc:
            print(f"[scheduler][WARN] Scrape wrote file but parsing failed: {parse_exc}")
            with GAMES_CACHE_LOCK:
                GAMES_CACHE["ts"] = 0
                GAMES_CACHE["mtime"] = 0
    except Exception as exc:  # pragma: no cover - logging only
        print(f"[scheduler][ERROR] Scraper error: {exc}")

    try:
        enrich_scores()
    except Exception as exc:
        print(f"[scores][ERROR] Enrichment failed: {exc}")


def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scraper_job,
        "interval",
        minutes=SCRAPE_INTERVAL_MINUTES,
        id="scrape_job",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    print("[scheduler] Background scheduler started.")
    atexit.register(lambda: scheduler.shutdown(wait=False))


def trigger_startup_scrape():
    def _run():
        print("[scheduler] Running initial scrape on startup...")
        run_scraper_job()

    t = threading.Thread(target=_run, daemon=True)
    t.start()


_SCRAPER_STARTED = False

# Cross-process lock file: with gunicorn -w 2 every worker imports this module,
# so without coordination each process starts its own scheduler (double scrapes,
# double DB writes). Only the first worker to grab the lock runs the scheduler.
_SCHEDULER_LOCK_FD: int | None = None


def _acquire_scheduler_lock() -> bool:
    """Try to become the single scheduler owner across all workers."""
    global _SCHEDULER_LOCK_FD
    import fcntl

    lock_path = GAMES_DB_PATH.parent / "scheduler.lock"
    try:
        GAMES_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            return False
        os.write(fd, str(os.getpid()).encode())
        os.truncate(fd, os.lseek(fd, 0, os.SEEK_CUR))
        _SCHEDULER_LOCK_FD = fd
        return True
    except Exception:
        # Locking is best-effort; if it fails (e.g. weird FS) run anyway.
        return True


def _release_scheduler_lock() -> None:
    global _SCHEDULER_LOCK_FD
    if _SCHEDULER_LOCK_FD is not None:
        try:
            os.close(_SCHEDULER_LOCK_FD)
        except OSError:
            pass
        _SCHEDULER_LOCK_FD = None


def _maybe_start_scraper():
    global _SCRAPER_STARTED
    if _SCRAPER_STARTED:
        return
    should_start = True
    # Avoid double-starting under Flask reloader: only start on the main process when the flag exists.
    reload_flag = os.environ.get("WERKZEUG_RUN_MAIN")
    if reload_flag is not None and reload_flag != "true":
        should_start = False

    if should_start and _acquire_scheduler_lock():
        if STARTUP_SCRAPE_ON_BOOT or not _games_db_has_rows():
            trigger_startup_scrape()
        start_scheduler()
        _SCRAPER_STARTED = True
        atexit.register(_release_scheduler_lock)


if ENABLE_SCRAPER_IN_WEB:
    _maybe_start_scraper()
