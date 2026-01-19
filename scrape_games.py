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
import subprocess
import json
import sqlite3
import time
from playwright.sync_api import sync_playwright

# ---------------- CONFIG ----------------

BASE_URL_SHARK   = "https://sharkstreams.net/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; StreamScraper/1.0)"}
REQUEST_TIMEOUT = 8  # seconds
STREAMED_API_BASE = os.environ.get("STREAMED_API_BASE", "https://streamed.pk")
STREAMED_MATCHES_PATH = os.environ.get("STREAMED_MATCHES_PATH", "/api/matches/all-today")
STREAMED_SPORTS_PATH = os.environ.get("STREAMED_SPORTS_PATH", "/api/sports")
M3U8_WAIT_SECONDS = float(os.environ.get("M3U8_WAIT_SECONDS", "3.5"))
M3U8_TIMEOUT_SECONDS = float(os.environ.get("M3U8_TIMEOUT_SECONDS", "15"))
SHARK_M3U8_MAX_LOOKUPS = int(os.environ.get("SHARK_M3U8_MAX_LOOKUPS", "10"))

_SESSION = None
_PLAYWRIGHT_INSTALL_ATTEMPTED = False


def _get_session():
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update(HEADERS)
    return _SESSION


def _ensure_playwright_chromium() -> bool:
    global _PLAYWRIGHT_INSTALL_ATTEMPTED
    if _PLAYWRIGHT_INSTALL_ATTEMPTED:
        return False
    _PLAYWRIGHT_INSTALL_ATTEMPTED = True
    try:
        result = subprocess.run(
            ["playwright", "install", "chromium"],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        print("[scraper] Playwright CLI not found; cannot install Chromium.")
        return False
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        msg = f" Details: {err}" if err else ""
        print(f"[scraper] Failed to install Playwright Chromium.{msg}")
        return False
    return True

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
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    return str(value)

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
            "source": "streamed.pk",
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
_HREF_URL_RE = re.compile(r"https?://[^\\s\"']+", re.I)
_M3U8_RE = re.compile(r"""https?://[^\s"'<>]+?\.m3u8[^\s"'<>]*""", re.IGNORECASE)


def _is_m3u8_url(url: str) -> bool:
    return ".m3u8" in (url or "").lower()


def _find_m3u8_from_page(
    url: str,
    *,
    browser,
    wait_seconds: float,
    timeout_seconds: float,
) -> str | None:
    found: set[str] = set()

    def add(maybe_url: str | None) -> None:
        if not maybe_url:
            return
        if _is_m3u8_url(maybe_url):
            found.add(maybe_url)

    try:
        ctx = browser.new_context()
        page = ctx.new_page()

        def on_request(req):
            add(req.url)

        page.on("request", on_request)

        def on_response(res):
            add(res.url)
            try:
                ct = (res.headers.get("content-type") or "").lower()
                if any(x in ct for x in ("text", "json", "javascript", "xml", "mpegurl")):
                    body = res.text()
                    if body and len(body) <= 2_000_000:
                        for m in _M3U8_RE.findall(body):
                            add(m)
                        for rel in re.findall(r"""[^\s"'<>]+\.m3u8[^\s"'<>]*""", body, flags=re.I):
                            if rel.lower().startswith("http"):
                                add(rel)
                            else:
                                add(urljoin(res.url, rel))
            except Exception:
                pass

        page.on("response", on_response)

        page.goto(url, wait_until="domcontentloaded", timeout=int(timeout_seconds * 1000))
        elapsed = 0.0
        step = 0.25
        while elapsed < wait_seconds and not found:
            page.wait_for_timeout(int(step * 1000))
            elapsed += step

        page.close()
        ctx.close()
    except Exception:
        return None

    matches = [u for u in found if "chunks.m3u8" in u.lower()]
    if matches:
        return sorted(matches)[0]
    return None

def scrape_shark() -> pd.DataFrame:
    session = _get_session()
    r = session.get(BASE_URL_SHARK, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        return pd.DataFrame()

    soup = BeautifulSoup(r.text, "html.parser")
    rows = []
    m3u8_cache: dict[str, str | None] = {}
    lookup_count = 0
    browser = None
    playwright = None

    if SHARK_M3U8_MAX_LOOKUPS > 0:
        try:
            playwright = sync_playwright().start()
            browser = playwright.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
        except Exception as exc:
            installed = _ensure_playwright_chromium()
            if installed:
                try:
                    playwright = sync_playwright().start()
                    browser = playwright.chromium.launch(
                        headless=True,
                        args=["--no-sandbox", "--disable-dev-shm-usage"],
                    )
                except Exception as retry_exc:
                    print(
                        "[scraper] SharkStreams m3u8 lookups disabled (Playwright failed to launch "
                        "after attempting Chromium install). Error: "
                        f"{retry_exc}"
                    )
                    browser = None
            else:
                print(
                    "[scraper] SharkStreams m3u8 lookups disabled (Playwright failed to launch). "
                    "Ensure Playwright browsers are installed (e.g. `playwright install chromium`) "
                    f"and required system deps are available. Error: {exc}"
                )
                browser = None

    eligible = []
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

        eligible.append((div, dt, name_span, cat_span))

    total_items = len(eligible)
    for idx, (div, dt, name_span, cat_span) in enumerate(eligible, start=1):
        if total_items:
            bar_len = 24
            filled = int(bar_len * idx / total_items)
            bar = "=" * filled + "-" * (bar_len - filled)
            percent = int(idx * 100 / total_items)
            print(f"[scraper] SharkStreams progress [{bar}] {idx}/{total_items} ({percent}%)")

        embed_urls = []
        for a in div.find_all("a"):
            onclick = a.get("onclick", "")
            m = _OPENEMBED_RE.search(onclick) or _WINDOWOPEN_RE.search(onclick)
            if m:
                embed_urls.append(urljoin(BASE_URL_SHARK, m.group(1)))
            for attr in ("data-href", "data-embed", "data-url", "href"):
                href = a.get(attr)
                if href and href != "#":
                    embed_urls.append(urljoin(BASE_URL_SHARK, href))
            text = (a.get_text(strip=True) or "").lower()
            if text in ("watch", "embed") or "watch" in text or "embed" in text:
                href = a.get("href")
                if href and href != "#":
                    embed_urls.append(urljoin(BASE_URL_SHARK, href))

        # scan inline scripts within the row for openEmbed calls
        for script in div.find_all("script"):
            text = script.string or script.text or ""
            for m in _OPENEMBED_RE.finditer(text):
                embed_urls.append(urljoin(BASE_URL_SHARK, m.group(1)))
            for m in _WINDOWOPEN_RE.finditer(text):
                embed_urls.append(urljoin(BASE_URL_SHARK, m.group(1)))
            for m in _HREF_URL_RE.finditer(text):
                maybe = m.group(0)
                if "embed" in maybe or "player" in maybe or "watch" in maybe:
                    embed_urls.append(urljoin(BASE_URL_SHARK, maybe))

        # Normalize and deduplicate
        norm_urls = []
        seen_urls = set()
        for u in embed_urls:
            if not u:
                continue
            u = _force_https(u)
            if u in seen_urls:
                continue
            seen_urls.add(u)
            norm_urls.append(u)

        if not norm_urls:
            continue

        resolved_urls = []
        for u in norm_urls:
            cached = m3u8_cache.get(u)
            if cached is None and u not in m3u8_cache:
                if browser and lookup_count < SHARK_M3U8_MAX_LOOKUPS:
                    cached = _find_m3u8_from_page(
                        u,
                        browser=browser,
                        wait_seconds=M3U8_WAIT_SECONDS,
                        timeout_seconds=M3U8_TIMEOUT_SECONDS,
                    )
                    lookup_count += 1
                m3u8_cache[u] = cached
            if cached:
                resolved_urls.append((u, cached))

        if not resolved_urls:
            continue

        streams = [{
            "label": "SharkStreams",
            "embed_url": resolved,
            "watch_url": original,
            "origin": "scraped",
        } for original, resolved in resolved_urls]

        rows.append({
            "source": "sharkstreams",
            "date_header": dt.strftime("%A, %B %d, %Y"),
            "sport": cat_span.get_text(strip=True) if cat_span else "Other",
            "time_unix": int(dt.timestamp() * 1000),
            "time": dt,
            "tournament": None,
            "tournament_url": None,
            "matchup": name_span.text.strip(),
            "watch_url": resolved_urls[0][0],
            "streams": streams,
            "embed_url": resolved_urls[0][1],
            "is_live": False,
        })

    if browser:
        try:
            browser.close()
        except Exception:
            pass
        try:
            if playwright:
                playwright.stop()
        except Exception:
            pass

    return pd.DataFrame(rows)

# ---------------- MERGE + WRITE ----------------

def merge_streams(new, old):
    manual = [s for s in old if _is_manual(s)]
    scraped = [s for s in new if not _is_manual(s)]
    return _dedup_streams(manual + scraped)

def main():
    df_streamed = scrape_streamed_api()
    df_shark = scrape_shark()

    source_counts = {
        "streamed.pk": len(df_streamed),
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
                    "time_unix": row.get("time_unix"),
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

    print(f"[scraper] Wrote {len(out_rows)} games (streamed.pk={source_counts['streamed.pk']}, sharkstreams={source_counts['sharkstreams']})")

if __name__ == "__main__":
    main()
