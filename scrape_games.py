#!/usr/bin/env python3
from __future__ import annotations

"""
scrape_games.py

FAST + SAFE scraper for:
  - Streamed.pk Matches + Streams APIs (preferred, no HTML parsing)
  - sharkstreams.net: today + next 7 days, 1 embed per game (legacy)

Guarantees:
  - Manual streams preserved
  - Scraped streams replaced each run
  - No stream explosion
  - SQLite writes
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import pytz
import re
from urllib.parse import urljoin
import os
import hashlib
import json
import sqlite3
import time

# ---------------- CONFIG ----------------

BASE_URL_SHARK   = "https://sharkstreams.net/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; StreamScraper/1.0)"}
REQUEST_TIMEOUT = 8  # seconds
STREAMED_API_BASE = os.environ.get("STREAMED_API_BASE", "https://streamed.st")
STREAMED_MATCHES_PATH = os.environ.get("STREAMED_MATCHES_PATH", "/api/matches/all-today")
STREAMED_SPORTS_PATH = os.environ.get("STREAMED_SPORTS_PATH", "/api/sports")
SQLITE_DB = os.environ.get("STREAM_DEN_DB", str(Path(__file__).parent / "data" / "games.db"))
# sharkstreams.net is currently dead (DNS does not resolve); keep the parser but
# off by default so the scheduler doesn't spam timeouts. Set to 1 if it returns.
ENABLE_SHARK = os.environ.get("ENABLE_SHARK_SCRAPER", "0") == "1"
_SESSION = None


def _get_session():
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update(HEADERS)
    return _SESSION

EST = pytz.timezone("US/Eastern")
UTC = pytz.UTC

GAMES_DB_PATH = Path(
    os.environ.get(
        "GAMES_DB_PATH",
        Path(__file__).parent / "data" / "games.db",
    )
)

MAX_STREAMS_PER_GAME = 8

DB_COLUMNS = [
    "id",
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
    "streams_json",
    "embed_url",
    "updated_at",
]

# ---------------- HELPERS ----------------

def _parse_streams_json(val):
    if isinstance(val, list):
        return val
    if not isinstance(val, str) or not val.strip():
        return []
    try:
        parsed = json.loads(val)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []

def _dedup_streams(streams):
    seen = set()
    out = []
    for s in streams:
        url = (s.get("embed_url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(s)
    return out[:MAX_STREAMS_PER_GAME]

def _streams_to_json(streams):
    return json.dumps(_dedup_streams(streams), ensure_ascii=False)

def _serialize_time(value):
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    return str(value)


def _serialize_time_unix(value):
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, (datetime, pd.Timestamp)):
        return float(value.timestamp() * 1000)
    try:
        return float(value)
    except Exception:
        return None

def _is_manual(st):
    origin = st.get("origin")
    # Treat missing/unknown origin as manual so scraper refreshes don't wipe user-added streams
    return origin != "scraped"

def _within_days(dt, *, days_ahead: int = 1, days_behind: int = 0) -> bool:
    """
    Return True when dt is within [today - days_behind, today + days_ahead] in EST.
    """
    try:
        today = datetime.now(EST).date()
        delta_days = (dt.date() - today).days
        return -days_behind <= delta_days <= days_ahead
    except Exception:
        return False

def _normalize_bool(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "live", "t")

def _stable_game_id(row: dict) -> int:
    """
    Mirror the web app's stable ID generation so scraper merges align with cache keys.
    """
    key = f"{row.get('date_header', '')}|{row.get('sport', '')}|{row.get('tournament', '')}|{row.get('matchup', '')}"
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)

def _ensure_games_db() -> None:
    GAMES_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(GAMES_DB_PATH) as conn:
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


def _connect_games_db() -> sqlite3.Connection:
    _ensure_games_db()
    conn = sqlite3.connect(GAMES_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def _should_keep_existing(row: dict) -> bool:
    """
    Keep manual/older rows unless they are clearly stale (older than ~1 day in UTC).
    """
    now = datetime.now(UTC)
    ts = row.get("time_unix")
    try:
        ts_val = float(ts)
        dt = datetime.fromtimestamp(ts_val / 1000, tz=UTC)
        return dt >= now - timedelta(days=1)
    except Exception:
        pass

    try:
        dt = pd.to_datetime(row.get("time"), errors="coerce")
        if not pd.isna(dt):
            if dt.tzinfo is None:
                dt = UTC.localize(dt)
            return dt >= now - timedelta(days=1)
    except Exception:
        pass

    # If we cannot parse a time, err on the side of keeping it so we don't drop valid manual entries
    return True

def _force_https(url: str) -> str:
    if not isinstance(url, str):
        return url
    if url.startswith("http://"):
        return "https://" + url[len("http://"):]
    return url

def _guess_sport(text: str) -> str:
    haystack = (text or "").lower()
    for keyword, sport in _SPORT_KEYWORDS:
        if keyword in haystack:
            return sport
    return "Misc"

# ---------------- SPORT71 ----------------

_SPORT_KEYWORDS = [
    ("nba", "Basketball"),
    ("basketball", "Basketball"),
    ("wnba", "Basketball"),
    ("ncaa basketball", "Basketball"),
    ("college basketball", "Basketball"),
    ("nfl", "American Football"),
    ("american football", "American Football"),
    ("ncaa football", "College Football"),
    ("college football", "College Football"),
    ("mlb", "Baseball"),
    ("baseball", "Baseball"),
    ("nhl", "Hockey"),
    ("hockey", "Hockey"),
    ("ice hockey", "Hockey"),
    ("soccer", "Soccer"),
    ("football", "Soccer"),
    ("mls", "Soccer"),
    ("premier league", "Soccer"),
    ("la liga", "Soccer"),
    ("serie a", "Soccer"),
    ("bundesliga", "Soccer"),
    ("uefa", "Soccer"),
    ("champions league", "Soccer"),
    ("fifa", "Soccer"),
    ("ufc", "Fight (UFC, Boxing)"),
    ("boxing", "Fight (UFC, Boxing)"),
    ("mma", "Fight (UFC, Boxing)"),
    ("bellator", "Fight (UFC, Boxing)"),
    ("pga", "Golf"),
    ("golf", "Golf"),
    ("rugby", "Rugby"),
    ("tennis", "Tennis"),
    ("darts", "Darts"),
    ("cricket", "Cricket"),
    ("handball", "Handball"),
    ("volleyball", "Volleyball"),
    ("horse racing", "Horse Racing"),
]

_CHAT_MARKERS = ("text-chat", "/chat", "chat_room", "chatroom", "chat.")


def _looks_like_chat(url: str) -> bool:
    if not url:
        return False
    low = url.lower()
    return "chat" in low or any(marker in low for marker in _CHAT_MARKERS)


def _is_probable_stream_url(url: str) -> bool:
    if not url:
        return False
    u = url.lower()
    keywords = ("/channel/", "embed", "player", "m3u8", "hls", "manifest", ".m3u8")
    return any(k in u for k in keywords)




def _fetch_json(session, url: str):
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


def _load_sports_map(session) -> dict:
    sports = _fetch_json(session, urljoin(STREAMED_API_BASE, STREAMED_SPORTS_PATH)) or []
    return {
        (s.get("id") or "").strip(): (s.get("name") or s.get("id") or "").strip()
        for s in sports
        if isinstance(s, dict)
    }


def _build_stream_label(stream: dict) -> str:
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


def _fetch_streams_for_source(session, source: str, source_id: str) -> list[dict]:
    if not source or not source_id:
        return []
    api_url = urljoin(STREAMED_API_BASE, f"/api/stream/{source}/{source_id}")
    payload = _fetch_json(session, api_url) or []
    streams: list[dict] = []
    for st in payload:
        if not isinstance(st, dict):
            continue
        embed = (st.get("embedUrl") or "").strip()
        if not embed:
            continue
        streams.append({
            "label": _build_stream_label(st),
            "embed_url": _force_https(embed),
            "watch_url": _force_https(embed),
            "origin": "api",
            "language": st.get("language"),
            "hd": bool(st.get("hd")),
            "source": st.get("source"),
        })
    return streams


def scrape_streamed_api() -> pd.DataFrame:
    """
    Fetch matches + streams directly from the Streamed.pk API instead of parsing HTML.
    """
    session = _get_session()
    sports_map = _load_sports_map(session)
    matches_url = urljoin(STREAMED_API_BASE, STREAMED_MATCHES_PATH)
    matches = _fetch_json(session, matches_url)
    if not matches:
        return pd.DataFrame()

    rows = []
    now_utc = datetime.now(UTC)

    for match in matches:
        if not isinstance(match, dict):
            continue
        try:
            ts_ms = int(match.get("date"))
        except Exception:
            continue

        event_dt = datetime.fromtimestamp(ts_ms / 1000, tz=UTC).astimezone(EST)
        # keep today + a small lookahead for upcoming matches
        if not _within_days(event_dt, days_ahead=1, days_behind=0):
            continue

        sources = match.get("sources") or []
        streams: list[dict] = []
        for src in sources:
            if not isinstance(src, dict):
                continue
            streams.extend(_fetch_streams_for_source(session, src.get("source"), src.get("id")))
        streams = _dedup_streams(streams)

        sport_id = match.get("category") or ""
        sport = sports_map.get(sport_id, sport_id.title() if isinstance(sport_id, str) else "Other")

        rows.append({
            "source": "streamed.st",
            "date_header": event_dt.strftime("%A, %B %d, %Y"),
            "sport": sport,
            "time_unix": ts_ms,
            "time": event_dt,
            "tournament": None,
            "tournament_url": None,
            "matchup": match.get("title") or "Unknown match",
            "watch_url": streams[0]["watch_url"] if streams else None,
            "streams": streams,
            "embed_url": streams[0]["embed_url"] if streams else None,
            "is_live": (event_dt - timedelta(minutes=15)) <= now_utc <= (event_dt + timedelta(hours=5)),
        })

    return pd.DataFrame(rows)


# ---------------- SHARKSTREAMS ----------------

_OPENEMBED_RE = re.compile(r"openEmbed\(\s*[\"']([^\"']+)[\"']\s*\)", re.I)
_WINDOWOPEN_RE = re.compile(r"window\.open\(\s*[\"']([^\"']+)[\"']\s*,", re.I)


def scrape_shark() -> pd.DataFrame:
    session = _get_session()
    try:
        r = session.get(BASE_URL_SHARK, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            print(f"[scraper] SharkStreams returned HTTP {r.status_code}")
            return pd.DataFrame()
    except Exception as exc:
        print(f"[scraper] SharkStreams unavailable: {exc}")
        return pd.DataFrame()

    soup = BeautifulSoup(r.text, "html.parser")
    rows = []
    for div in soup.find_all("div", class_="row"):
        date_span = div.find("span", class_="ch-date")
        name_span = div.find("span", class_="ch-name")
        cat_span = div.find("span", class_="ch-category")
        if not (date_span and name_span):
            continue

        try:
            dt = EST.localize(datetime.strptime(date_span.text.strip(), "%Y-%m-%d %H:%M:%S"))
        except Exception:
            continue

        if not _within_days(dt, days_ahead=7):
            continue

        # Use the URL from the Embed button's openEmbed() call as the embed URL.
        # Fall back to the Watch button's window.open() URL if no embed URL is found.
        embed_url = None
        watch_url = None
        for a in div.find_all("a"):
            onclick = a.get("onclick", "")
            if embed_url is None:
                m = _OPENEMBED_RE.search(onclick)
                if m:
                    embed_url = urljoin(BASE_URL_SHARK, m.group(1))
            if watch_url is None:
                m = _WINDOWOPEN_RE.search(onclick)
                if m:
                    watch_url = urljoin(BASE_URL_SHARK, m.group(1))

        if not embed_url:
            continue

        streams = [{
            "label": "SharkStreams",
            "embed_url": embed_url,
            "watch_url": watch_url or embed_url,
            "origin": "scraped",
        }]

        rows.append({
            "source": "sharkstreams",
            "date_header": dt.strftime("%A, %B %d, %Y"),
            "sport": cat_span.get_text(strip=True) if cat_span else "Other",
            "time_unix": int(dt.timestamp() * 1000),
            "time": dt,
            "tournament": None,
            "tournament_url": None,
            "matchup": name_span.text.strip(),
            "watch_url": watch_url or embed_url,
            "streams": streams,
            "embed_url": embed_url,
            "is_live": False,
        })

    print(f"[scraper] SharkStreams scraped {len(rows)} games")
    return pd.DataFrame(rows)

# ---------------- MERGE + WRITE ----------------

def merge_streams(new, old):
    manual = [s for s in old if _is_manual(s)]
    scraped = [s for s in new if not _is_manual(s)]
    return _dedup_streams(manual + scraped)

def main():
    df_streamed = pd.DataFrame()
    df_shark = pd.DataFrame()
    try:
        df_streamed = scrape_streamed_api()
    except Exception as exc:
        print(f"[scraper][ERROR] streamed.st scrape failed: {exc}")
    try:
        if ENABLE_SHARK:
            df_shark = scrape_shark()
        else:
            print("[scraper] SharkStreams disabled (ENABLE_SHARK_SCRAPER=0), skipping")
    except Exception as exc:
        print(f"[scraper][ERROR] shark scrape failed: {exc}")

    source_counts = {
        "streamed.st": len(df_streamed),
        "sharkstreams": len(df_shark),
    }

    scraped_frames = [df for df in (df_streamed, df_shark) if not df.empty]
    if not scraped_frames:
        print("[scraper] No games found.")
        return

    df_new = pd.concat(scraped_frames, ignore_index=True)
    with _connect_games_db() as conn:
        # Build a lookup of existing rows by stable ID so we don't drop manual edits
        old_map: dict[int, dict] = {}
        for row in conn.execute("SELECT * FROM games").fetchall():
            rd = dict(row)
            rd["streams"] = _parse_streams_json(rd.get("streams_json"))
            old_map[int(rd["id"])] = rd

        # Deduplicate new scrape across sources before merging into existing rows
        new_map: dict[int, dict] = {}
        for _, row in df_new.iterrows():
            rd = row.to_dict()
            gid = _stable_game_id(rd)
            existing = new_map.get(gid)
            if existing:
                merged_streams = merge_streams(rd.get("streams") or [], existing.get("streams") or [])
                rd["streams"] = merged_streams
                rd["embed_url"] = rd.get("embed_url") or (merged_streams[0]["embed_url"] if merged_streams else None)
                rd["is_live"] = existing.get("is_live") or _normalize_bool(rd.get("is_live"))
            new_map[gid] = rd

        out_rows = []

        # Merge scraped rows with existing where possible
        for rd in new_map.values():
            gid = _stable_game_id(rd)
            old = old_map.pop(gid, None)

            old_streams = old.get("streams") if old else []
            merged = merge_streams(rd.get("streams") or [], old_streams)

            rd["streams"] = merged
            rd["embed_url"] = rd.get("embed_url") or (merged[0]["embed_url"] if merged else None)
            rd["is_live"] = _normalize_bool(old.get("is_live") if old else rd.get("is_live"))
            rd["id"] = gid

            out_rows.append(rd)

        # Carry forward unmatched (typically manual) rows so they are not wiped out
        for rd in old_map.values():
            if not _should_keep_existing(rd):
                continue
            streams = rd.get("streams") or []
            rd["streams"] = streams
            rd["embed_url"] = rd.get("embed_url") or (streams[0]["embed_url"] if streams else None)
            rd["is_live"] = _normalize_bool(rd.get("is_live"))
            out_rows.append(rd)

        now_ts = time.time()
        keep_ids = [int(row.get("id")) for row in out_rows if row.get("id") is not None]

        with conn:
            for row in out_rows:
                payload = {
                    "id": int(row.get("id")),
                    "source": row.get("source"),
                    "date_header": row.get("date_header"),
                    "sport": row.get("sport"),
                    "time_unix": _serialize_time_unix(row.get("time_unix")),
                    "time": _serialize_time(row.get("time")),
                    "tournament": row.get("tournament"),
                    "tournament_url": row.get("tournament_url"),
                    "matchup": row.get("matchup"),
                    "watch_url": row.get("watch_url"),
                    "is_live": 1 if _normalize_bool(row.get("is_live")) else 0,
                    "streams_json": _streams_to_json(row.get("streams") or []),
                    "embed_url": row.get("embed_url") or "",
                    "updated_at": now_ts,
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
                    payload,
                )

            if keep_ids:
                placeholders = ",".join("?" for _ in keep_ids)
                conn.execute(f"DELETE FROM games WHERE id NOT IN ({placeholders})", keep_ids)
            else:
                conn.execute("DELETE FROM games")
            conn.execute(
                """
                INSERT INTO games_meta (id, updated_at)
                VALUES (1, ?)
                ON CONFLICT(id) DO UPDATE SET updated_at = excluded.updated_at
                """,
                (now_ts,),
            )

    print(f"[scraper] Wrote {len(out_rows)} games (streamed.st={source_counts['streamed.st']}, sharkstreams={source_counts['sharkstreams']})")

if __name__ == "__main__":
    main()
