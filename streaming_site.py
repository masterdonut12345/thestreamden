"""Streaming site blueprint extracted from streaming-website repo.

Provides the main streaming homepage at "/", game detail pages at
"/game/<int:game_id>", slug redirect at "/g/<slug>", and a heartbeat endpoint
for tracking active viewers.
"""

from __future__ import annotations

import ast
import atexit
import fcntl
import hashlib
import os
import random
import re
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from bs4 import BeautifulSoup
from flask import Blueprint, abort, jsonify, make_response, redirect, render_template, request, session, url_for
import requests

import scrape_games

streaming_bp = Blueprint("streaming", __name__)


DATA_PATH = Path(
    os.environ.get(
        "GAMES_CSV_PATH",
        Path(__file__).parent / "data" / "today_games_with_all_streams.csv",
    )
).expanduser()


# ====================== PERFORMANCE CONTROLS ======================
# Cache games in memory to avoid pd.read_csv + ast parsing on every request
GAMES_CACHE: dict[str, Any] = {
    "games": [],
    "ts": 0.0,
    "mtime": 0.0,
}
GAMES_CACHE_LOCK = threading.Lock()

# Refresh at most every N seconds OR when file mtime changes
GAMES_CACHE_TTL_SECONDS = int(os.environ.get("GAMES_CACHE_TTL_SECONDS", "10"))

# Cloudflare / browser caching for HTML (keep short to avoid stale)
HTML_CACHE_SECONDS = int(os.environ.get("HTML_CACHE_SECONDS", "30"))

# Viewer tracking: keep it, but reduce work
ENABLE_VIEWER_TRACKING = os.environ.get("ENABLE_VIEWER_TRACKING", "1") == "1"

# IMPORTANT: do not run scraper in the web process unless explicitly enabled
ENABLE_SCRAPER_IN_WEB = os.environ.get("ENABLE_SCRAPER_IN_WEB", "1") == "1"
SCRAPE_INTERVAL_MINUTES = int(os.environ.get("SCRAPE_INTERVAL_MINUTES", "10"))


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


def safe_lower(value: Any) -> str:
    return value.lower() if isinstance(value, str) else ""


def normalize_sport_name(value: Any) -> str:
    """Normalize sport values so grouping/sorting never mixes types."""
    if isinstance(value, str):
        value = value.strip()
        return value or "Other"
    try:
        text = str(value).strip()
        return text or "Other"
    except Exception:
        return "Other"


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


def find_row_index_by_game_id(df: pd.DataFrame, game_id: int):
    for i, row in df.iterrows():
        try:
            rid = make_stable_id(row)
            if rid == game_id:
                return int(i)
        except Exception:
            continue
    return None


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


def parse_streams_cell(cell_value: Any) -> list[dict[str, Any]]:
    """CSV stores streams as python-literal list of dicts."""

    if cell_value is None or (isinstance(cell_value, float) and pd.isna(cell_value)):
        return []
    if isinstance(cell_value, list):
        return cell_value

    s = str(cell_value).strip()
    if not s:
        return []
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            out = []
            for item in parsed:
                if isinstance(item, dict) and item.get("embed_url"):
                    fixed = dict(item)
                    fixed["label"] = fixed.get("label") or "Stream"
                    fixed["embed_url"] = fixed.get("embed_url")
                    fixed["watch_url"] = fixed.get("watch_url")
                    out.append(fixed)
            return out
    except Exception:
        return []
    return []


def streams_to_cell(streams_list: list[dict[str, Any]]) -> str:
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
    return repr(cleaned)


def ensure_csv_exists_with_header() -> None:
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DATA_PATH.exists():
        return
    cols = [
        "source",
        "date_header",
        "sport",
        "time_unix",
        "time",
        "tournament",
        "tournament_url",
        "matchup",
        "watch_url",
        "is_live",
        "streams",
        "embed_url",
    ]
    df = pd.DataFrame(columns=cols)
    df.to_csv(DATA_PATH, index=False)
    print(f"[csv] Created empty CSV at {DATA_PATH}")


def _read_csv_shared_locked(path: Path) -> pd.DataFrame:
    """Fast read path with shared lock."""

    ensure_csv_exists_with_header()
    with path.open("r", encoding="utf-8") as fh:
        try:
            import fcntl

            fcntl.flock(fh.fileno(), fcntl.LOCK_SH)
        except Exception:
            pass
        try:
            df = pd.read_csv(fh)
        except Exception:
            df = pd.DataFrame(
                columns=[
                    "source",
                    "date_header",
                    "sport",
                    "time_unix",
                    "time",
                    "tournament",
                    "tournament_url",
                    "matchup",
                    "watch_url",
                    "is_live",
                    "streams",
                    "embed_url",
                ]
            )
        try:
            import fcntl

            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
    return df


def read_csv_locked_for_write():
    """Exclusive lock to safely modify CSV."""

    ensure_csv_exists_with_header()
    fh = DATA_PATH.open("r+", encoding="utf-8")
    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)

    fh.seek(0)
    try:
        df = pd.read_csv(fh)
    except Exception:
        df = pd.DataFrame(
            columns=[
                "source",
                "date_header",
                "sport",
                "time_unix",
                "time",
                "tournament",
                "tournament_url",
                "matchup",
                "watch_url",
                "is_live",
                "streams",
                "embed_url",
            ]
        )
    return df, fh


def write_csv_locked(df, fh):
    fh.seek(0)
    fh.truncate(0)
    df.to_csv(fh, index=False)
    fh.flush()
    os.fsync(fh.fileno())
    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    fh.close()


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


CSV_COLS = [
    "source",
    "date_header",
    "sport",
    "time_unix",
    "time",
    "tournament",
    "tournament_url",
    "matchup",
    "watch_url",
    "is_live",
    "streams",
    "embed_url",
]


def load_games_cached() -> list[dict[str, Any]]:
    """Cached loader for games from CSV."""

    now = time.time()
    previous_games: list[dict[str, Any]] = []
    try:
        mtime = os.path.getmtime(DATA_PATH) if os.path.exists(DATA_PATH) else 0.0
    except Exception:
        mtime = 0.0

    with GAMES_CACHE_LOCK:
        previous_games = list(GAMES_CACHE.get("games") or [])
        cache_ok = (
            GAMES_CACHE["games"]
            and (now - GAMES_CACHE["ts"] < GAMES_CACHE_TTL_SECONDS)
            and (mtime == GAMES_CACHE["mtime"])
        )
        if cache_ok:
            cached_games = GAMES_CACHE["games"]
            if any(sport_is_invalid(g.get("sport")) for g in cached_games):
                cache_ok = False
            else:
                return cached_games

    df = _read_csv_shared_locked(DATA_PATH)
    games = _build_games_from_df(df)

    if not games and (len(df.index) > 0) and previous_games:
        print(
            f"[loader][WARN] Parsed 0 games from {DATA_PATH} with {len(df.index)} rows; "
            f"serving {len(previous_games)} cached games instead."
        )
        return previous_games

    with GAMES_CACHE_LOCK:
        GAMES_CACHE["games"] = games
        GAMES_CACHE["ts"] = now
        GAMES_CACHE["mtime"] = mtime

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
            g
            for g in games
            if q in safe_lower(g.get("matchup"))
            or q in safe_lower(g.get("sport"))
            or q in safe_lower(g.get("tournament"))
        ]

    live_only = request.args.get("live_only", "").lower() in ("1", "true", "yes", "on")
    if live_only:
        games = [g for g in games if g.get("is_live")]

    sections_by_sport: dict[str, list[dict[str, Any]]] = {}
    for g in games:
        sport = normalize_sport_name(g.get("sport"))
        if sport_is_invalid(sport):
            continue
        sections_by_sport.setdefault(sport, []).append(g)

    sections = [{"sport": s, "games": lst} for s, lst in sections_by_sport.items()]
    sections.sort(key=lambda s: normalize_sport_name(s["sport"]).lower())

    most_viewed_games = get_most_viewed_games(all_games, limit=5)

    return render_template(
        "streaming_index.html",
        sections=sections,
        search_query=q,
        live_only=live_only,
        most_viewed_games=most_viewed_games,
    )


def _build_games_from_df(df: pd.DataFrame):
    if df is None or df.empty:
        return []

    for col in [
        "streams",
        "is_live",
        "sport",
        "time",
        "date_header",
        "tournament",
        "tournament_url",
        "matchup",
        "watch_url",
        "time_unix",
    ]:
        if col not in df.columns:
            df[col] = ""

    games = []
    dedup_map: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    now_utc = datetime.now(timezone.utc)
    stale_cutoff = now_utc - timedelta(hours=6)
    live_window_after_start = timedelta(hours=5)

    for _, row in df.iterrows():
        rowd = row.to_dict()
        streams = parse_streams_cell(rowd.get("streams"))
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

        game_obj = {
            "id": game_id,
            "date_header": rowd.get("date_header"),
            "sport": sport,
            "time_unix": rowd.get("time_unix"),
            "time": time_display,
            "tournament": rowd.get("tournament"),
            "tournament_url": rowd.get("tournament_url"),
            "matchup": rowd.get("matchup"),
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

    df, fh = read_csv_locked_for_write()
    for c in CSV_COLS:
        if c not in df.columns:
            df[c] = ""

    idx = find_row_index_by_game_id(df, game_id)
    if idx is None:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()
        return jsonify({"ok": False, "error": "game not found", "game_id": game_id}), 404

    existing_streams = parse_streams_cell(df.at[idx, "streams"])
    merged = merge_streams(existing_streams, incoming_streams)

    df.at[idx, "streams"] = streams_to_cell(merged)

    set_embed_url = payload.get("set_embed_url")
    if isinstance(set_embed_url, str) and set_embed_url.strip():
        df.at[idx, "embed_url"] = set_embed_url.strip()
    else:
        cur_embed = df.at[idx, "embed_url"] if "embed_url" in df.columns else ""
        if (not isinstance(cur_embed, str) or not cur_embed.strip()) and merged:
            df.at[idx, "embed_url"] = merged[0].get("embed_url")

    if "set_is_live" in payload:
        df.at[idx, "is_live"] = bool(payload.get("set_is_live"))

    write_csv_locked(df[CSV_COLS], fh)

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

    df, fh = read_csv_locked_for_write()
    for c in CSV_COLS:
        if c not in df.columns:
            df[c] = ""

    idx = find_row_index_by_game_id(df, game_id)
    if idx is None:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()
        return jsonify({"ok": False, "error": "game not found", "game_id": game_id}), 404

    df = df.drop(index=idx).reset_index(drop=True)
    write_csv_locked(df[CSV_COLS], fh)

    with GAMES_CACHE_LOCK:
        GAMES_CACHE["ts"] = 0
        GAMES_CACHE["mtime"] = 0

    return jsonify({"ok": True, "removed": True, "game_id": game_id, "rows_now": int(len(df))})


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

    df, fh = read_csv_locked_for_write()
    for c in CSV_COLS:
        if c not in df.columns:
            df[c] = ""

    upserted = []
    for g in games:
        row_like = {
            "date_header": g.get("date_header", ""),
            "sport": g.get("sport", ""),
            "tournament": g.get("tournament", ""),
            "matchup": g.get("matchup", ""),
        }
        game_id = make_stable_id(row_like)

        idx = find_row_index_by_game_id(df, game_id)

        streams_list = g.get("streams")
        if isinstance(streams_list, str):
            streams_list = parse_streams_cell(streams_list)
        elif not isinstance(streams_list, list):
            streams_list = []

        embed_url = g.get("embed_url")
        if not (isinstance(embed_url, str) and embed_url.strip()):
            embed_url = streams_list[0].get("embed_url") if streams_list else ""

        new_row = {
            "source": g.get("source", ""),
            "date_header": g.get("date_header", ""),
            "sport": g.get("sport", ""),
            "time_unix": g.get("time_unix", ""),
            "time": g.get("time", ""),
            "tournament": g.get("tournament", ""),
            "tournament_url": g.get("tournament_url", ""),
            "matchup": g.get("matchup", ""),
            "watch_url": g.get("watch_url", ""),
            "is_live": normalize_bool(g.get("is_live")),
            "streams": streams_to_cell(streams_list),
            "embed_url": embed_url or "",
        }

        if idx is None:
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            action = "inserted"
        else:
            existing_streams = parse_streams_cell(df.at[idx, "streams"])
            merged = merge_streams(existing_streams, streams_list)
            new_row["streams"] = streams_to_cell(merged)
            if not new_row["embed_url"] and merged:
                new_row["embed_url"] = merged[0].get("embed_url") or ""

            for k, v in new_row.items():
                df.at[idx, k] = v
            action = "updated"

        upserted.append({"game_id": game_id, "action": action})

    write_csv_locked(df[CSV_COLS], fh)

    with GAMES_CACHE_LOCK:
        GAMES_CACHE["ts"] = 0
        GAMES_CACHE["mtime"] = 0

    return jsonify({"ok": True, "results": upserted})


@streaming_bp.route("/api/games/clear_streams", methods=["POST"])
def api_games_clear_streams():
    """Clear all stream/embed data to force a full reload via the scraper."""
    if not require_admin():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    df, fh = read_csv_locked_for_write()
    for c in CSV_COLS:
        if c not in df.columns:
            df[c] = ""

    if not df.empty:
        df["streams"] = ""
        df["embed_url"] = ""

    write_csv_locked(df[CSV_COLS], fh)

    with GAMES_CACHE_LOCK:
        GAMES_CACHE["ts"] = 0
        GAMES_CACHE["mtime"] = 0

    return jsonify({"ok": True, "rows": int(len(df))})


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

    return render_template(
        "streaming_game.html",
        game=game,
        other_games=other_games,
        open_ad=open_ad,
        share_id_url=share_id_url,
        share_slug_url=share_slug_url,
        og_image_url=og_image_url,
        requested_stream_slug=requested_stream_slug,
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
        scrape_games.main()
        try:
            df = _read_csv_shared_locked(DATA_PATH)
            games = _build_games_from_df(df)
            with GAMES_CACHE_LOCK:
                GAMES_CACHE["games"] = games
                GAMES_CACHE["ts"] = time.time()
                try:
                    GAMES_CACHE["mtime"] = os.path.getmtime(DATA_PATH)
                except Exception:
                    GAMES_CACHE["mtime"] = 0
            print(f"[scheduler] Cached {len(games)} games from {DATA_PATH}")
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
        trigger_startup_scrape()
        start_scheduler()
        _SCRAPER_STARTED = True


if ENABLE_SCRAPER_IN_WEB:
    _maybe_start_scraper()
