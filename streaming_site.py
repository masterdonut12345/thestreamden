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
import random
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

import pandas as pd
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from bs4 import BeautifulSoup
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

import scrape_games

streaming_bp = Blueprint("streaming", __name__)


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

# Cloudflare / browser caching for HTML (keep short to avoid stale)
HTML_CACHE_SECONDS = int(os.environ.get("HTML_CACHE_SECONDS", "30"))

# Viewer tracking: keep it, but reduce work
ENABLE_VIEWER_TRACKING = os.environ.get("ENABLE_VIEWER_TRACKING", "1") == "1"

# IMPORTANT: do not run scraper in the web process unless explicitly enabled
ENABLE_SCRAPER_IN_WEB = os.environ.get("ENABLE_SCRAPER_IN_WEB", "0") == "1"
SCRAPER_SUBPROCESS = os.environ.get("SCRAPER_SUBPROCESS", "1") == "1"
SCRAPE_INTERVAL_MINUTES = int(os.environ.get("SCRAPE_INTERVAL_MINUTES", "180"))
STARTUP_SCRAPE_ON_BOOT = os.environ.get("STARTUP_SCRAPE_ON_BOOT", "1") == "1"


# ====================== ACTIVE VIEWER TRACKER ======================
ACTIVE_VIEWERS: dict[str, datetime] = {}  # session_id → last_seen timestamp
ACTIVE_PAGE_VIEWS: dict[tuple[str, str], datetime] = {}  # (session_id, path) → last_seen timestamp
LAST_VIEWER_PRINT: datetime | None = None  # throttle printing

CHAT_LOCK = threading.Lock()
GAME_CHAT: dict[int, list[dict[str, Any]]] = {}
GAME_CHAT_COUNTER: dict[int, int] = {}
CHAT_FETCH_LIMIT = int(os.environ.get("CHAT_FETCH_LIMIT", "50"))
CHAT_MAX_MESSAGES_PER_GAME = int(os.environ.get("CHAT_MAX_MESSAGES_PER_GAME", "200"))
CHAT_MESSAGE_MAX_LEN = int(os.environ.get("CHAT_MESSAGE_MAX_LEN", "280"))



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


def _get_chat_display_name() -> str:
    user = getattr(g, "current_user", None)
    if user and getattr(user, "username", None):
        return user.username
    username = session.get("username")
    if username:
        return username
    sid = session.get("sid") or get_session_id()
    return f"Guest-{sid[:4]}"


def _append_chat_message(game_id: int, username: str, body: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    message = {
        "id": 0,
        "user": username,
        "body": body,
        "created_at": now.isoformat(),
    }
    with CHAT_LOCK:
        next_id = GAME_CHAT_COUNTER.get(game_id, 0) + 1
        GAME_CHAT_COUNTER[game_id] = next_id
        message["id"] = next_id
        messages = GAME_CHAT.setdefault(game_id, [])
        messages.append(message)
        if len(messages) > CHAT_MAX_MESSAGES_PER_GAME:
            del messages[: len(messages) - CHAT_MAX_MESSAGES_PER_GAME]
    return message


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
            if not pd.isna(ts_float):
                if ts_float > 1e11:  # likely ms
                    ts_float = ts_float / 1000.0
                return datetime.fromtimestamp(ts_float, tz=timezone.utc)
        except Exception:
            pass

    raw_time = rowd.get("time")
    if isinstance(raw_time, str) and raw_time.strip():
        try:
            dt = pd.to_datetime(raw_time, utc=True, errors="coerce")
            if isinstance(dt, pd.Timestamp) and not pd.isna(dt):
                return dt.to_pydatetime()
        except Exception:
            return None

    date_header = rowd.get("date_header")
    if isinstance(date_header, str) and date_header.strip():
        try:
            dt = pd.to_datetime(date_header, utc=True, errors="coerce")
            if isinstance(dt, pd.Timestamp) and not pd.isna(dt):
                return dt.to_pydatetime()
        except Exception:
            return None

    return None


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
                updated_at REAL
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


def _get_games_db_connection() -> sqlite3.Connection:
    _ensure_games_db()
    conn = sqlite3.connect(GAMES_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


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


@streaming_bp.route("/make-money")
def make_money():
    mark_active()
    return render_template("make_money.html")


@streaming_bp.route("/m3u8_player")
def m3u8_player():
    src = normalize_m3u8_src((request.args.get("src") or "").strip())
    proxy_src = build_m3u8_proxy_url(src) if src else ""
    return render_template("m3u8_player.html", src=src, proxy_src=proxy_src)


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
        shark_stream_source = rowd.get("source") == "sharkstreams"
        has_shark_m3u8 = False
        if shark_stream_source:
            if isinstance(embed_url, str) and is_m3u8_url(embed_url):
                has_shark_m3u8 = True
            else:
                for stream in streams:
                    if not isinstance(stream, dict):
                        continue
                    embed = stream.get("embed_url")
                    if isinstance(embed, str) and is_m3u8_url(embed):
                        has_shark_m3u8 = True
                        break

        if streams:
            fixed_streams = []
            for stream in streams:
                if not isinstance(stream, dict):
                    continue
                fixed = dict(stream)
                embed = fixed.get("embed_url")
                if shark_stream_source:
                    if not (isinstance(embed, str) and (is_m3u8_url(embed) or "/m3u8_player" in embed)):
                        continue
                if isinstance(embed, str) and embed:
                    fixed["embed_url"] = build_m3u8_player_url(embed)
                fixed_streams.append(fixed)
            streams = fixed_streams
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
                dt = pd.to_datetime(raw_time, errors="coerce")
                if not pd.isna(dt):
                    time_display = dt.strftime("%I:%M %p ET").lstrip("0")
            except Exception:
                time_display = None

        matchup = rowd.get("matchup")
        if shark_stream_source and has_shark_m3u8 and isinstance(matchup, str):
            suffix = " - Ad free"
            if not matchup.endswith(suffix):
                matchup = f"{matchup}{suffix}"

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


@streaming_bp.route("/api/games/<int:game_id>/chat", methods=["GET", "POST"])
def game_chat_messages(game_id: int):
    if not _game_exists(game_id):
        return jsonify({"ok": False, "error": "game not found"}), 404

    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        body = (payload.get("body") or "").strip()

        if not body:
            return jsonify({"ok": False, "error": "empty"}), 400
        if len(body) > CHAT_MESSAGE_MAX_LEN:
            return jsonify({"ok": False, "error": "too_long"}), 400

        username = _get_chat_display_name()
        message = _append_chat_message(game_id, username, body)
        return jsonify({"ok": True, "message": message, "latest_id": message["id"]})

    after_id = request.args.get("after_id", type=int)
    limit = request.args.get("limit", type=int) or CHAT_FETCH_LIMIT
    limit = max(1, min(limit, CHAT_FETCH_LIMIT))

    with CHAT_LOCK:
        messages = list(GAME_CHAT.get(game_id, []))

    if after_id:
        messages = [msg for msg in messages if msg["id"] > after_id]

    if len(messages) > limit:
        messages = messages[-limit:]

    latest_id = messages[-1]["id"] if messages else (after_id or 0)
    return jsonify({"ok": True, "messages": messages, "latest_id": latest_id})


@streaming_bp.route("/game/<int:game_id>")
def game_detail(game_id: int):
    mark_active()

    requested_stream_slug = request.args.get("stream", "").strip()

    games = load_games_cached()
    game = next((g for g in games if g["id"] == game_id), None)
    if not game:
        abort(404)

    other_games = [g for g in games if g["id"] != game_id and g.get("streams")]

    open_ad = random.random() < 0.25

    slug = game.get("slug") or game_slug(game)
    share_id_url = _absolute_url(url_for("streaming.game_detail", game_id=game_id))
    share_slug_url = _absolute_url(url_for("streaming.game_by_slug", slug=slug))
    og_image_url = _absolute_url(url_for("static", filename="preview.svg"))

    current_user = getattr(g, "current_user", None)

    return render_template(
        "streaming_game.html",
        game=game,
        other_games=other_games,
        open_ad=open_ad,
        share_id_url=share_id_url,
        share_slug_url=share_slug_url,
        og_image_url=og_image_url,
        requested_stream_slug=requested_stream_slug,
        current_user=current_user,
    )


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
        return f"{base} ({' - '.join(extras)})"
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
                "origin": "api",
                "language": st.get("language"),
                "hd": bool(st.get("hd")),
                "source": st.get("source"),
            }
        )
    return streams


# ====================== SCHEDULER (OFF BY DEFAULT) ======================
def run_scraper_job():
    try:
        if SCRAPER_SUBPROCESS:
            scraper_path = Path(__file__).parent / "scrape_games.py"
            subprocess.run([sys.executable, str(scraper_path)], check=True)
        else:
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


def _maybe_start_scraper():
    global _SCRAPER_STARTED
    if _SCRAPER_STARTED:
        return
    should_start = True
    # Avoid double-starting under Flask reloader: only start on the main process when the flag exists.
    reload_flag = os.environ.get("WERKZEUG_RUN_MAIN")
    if reload_flag is not None and reload_flag != "true":
        should_start = False

    if should_start:
        if STARTUP_SCRAPE_ON_BOOT or not _games_db_has_rows():
            trigger_startup_scrape()
        start_scheduler()
        _SCRAPER_STARTED = True


if ENABLE_SCRAPER_IN_WEB:
    _maybe_start_scraper()
