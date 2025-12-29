#!/usr/bin/env python3
"""
scrape_games.py

FAST + SAFE scraper for:
  - Streamed.pk Matches + Streams APIs (preferred, no HTML parsing)
  - topembed.pw: today + tomorrow, ALL embeds
  - sharkstreams.net: today + next 7 days, 1 embed per game (legacy)

Guarantees:
  - Manual streams preserved
  - Scraped streams replaced each run
  - No stream explosion
  - Locked CSV writes
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import pytz
import re
from urllib.parse import urljoin
import ast
import os
import fcntl
import hashlib

# ---------------- CONFIG ----------------

BASE_URL_TOP_EMBED = os.environ.get("TOP_EMBED_BASE", "https://topembed.pw")
TOP_EMBED_EVENTS_PATH = os.environ.get("TOP_EMBED_EVENTS_PATH", "/api/events")
BASE_URL_SHARK   = "https://sharkstreams.net/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; StreamScraper/1.0)"}
REQUEST_TIMEOUT = 8  # seconds
STREAMED_API_BASE = os.environ.get("STREAMED_API_BASE", "https://streamed.pk")
STREAMED_MATCHES_PATH = os.environ.get("STREAMED_MATCHES_PATH", "/api/matches/all-today")
STREAMED_SPORTS_PATH = os.environ.get("STREAMED_SPORTS_PATH", "/api/sports")

_SESSION = None


def _get_session():
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update(HEADERS)
    return _SESSION

EST = pytz.timezone("US/Eastern")
UTC = pytz.UTC

OUTPUT_FILE = Path(
    os.environ.get(
        "GAMES_CSV_PATH",
        Path(__file__).parent / "data" / "today_games_with_all_streams.csv",
    )
)

MAX_STREAMS_PER_GAME = 8

CSV_COLS = [
    "source", "date_header", "sport", "time_unix", "time",
    "tournament", "tournament_url", "matchup", "watch_url",
    "is_live", "streams", "embed_url"
]

# ---------------- HELPERS ----------------

def _parse_streams_cell(val):
    if isinstance(val, list):
        return val
    if not isinstance(val, str) or not val.strip():
        return []
    try:
        parsed = ast.literal_eval(val)
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

def _streams_to_cell(streams):
    return repr(_dedup_streams(streams))

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

def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    for c in CSV_COLS:
        if c not in df.columns:
            df[c] = None
    return df

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

_IFRAME_RE = re.compile(r'https?://[^\s"\']+')
_EMBED_LIKE_RE = re.compile(r"https?://[^\s\"']*(?:embed|player|channel|topembed|m3u8)[^\s\"']*", re.I)
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
    keywords = ("topembed", "/channel/", "embed", "player", "m3u8", "hls", "manifest", ".m3u8")
    return any(k in u for k in keywords)


def _collect_embeds_from_html(base_url: str, soup: BeautifulSoup) -> list[dict]:
    streams = []

    # direct iframes
    for iframe in soup.find_all("iframe"):
        # Skip chat iframes explicitly housed in the chat container
        if iframe.find_parent(id="chat-container") or "chat" in (iframe.get("id") or "").lower():
            continue
        src = iframe.get("src") or iframe.get("data-src")
        if not src:
            continue
        full = urljoin(base_url, src)
        if _looks_like_chat(full) or not _is_probable_stream_url(full):
            continue
        streams.append({
            "label": "Stream",
            "embed_url": _force_https(full),
            "watch_url": base_url,
            "origin": "scraped",
        })

    # script-embedded URLs
    for script in soup.find_all("script"):
        text = script.string or script.text or ""
        for m in _IFRAME_RE.findall(text):
            if ("embed" in m or "player" in m) and not _looks_like_chat(m) and _is_probable_stream_url(m):
                streams.append({
                    "label": "Stream",
                    "embed_url": _force_https(urljoin(base_url, m)),
                    "watch_url": base_url,
                    "origin": "scraped",
                })
        for m in _EMBED_LIKE_RE.findall(text):
            if not _looks_like_chat(m) and _is_probable_stream_url(m):
                streams.append({
                    "label": "Stream",
                    "embed_url": _force_https(urljoin(base_url, m)),
                    "watch_url": base_url,
                    "origin": "scraped",
                })

    # embeds exposed via data- attributes on buttons/links
    for el in soup.find_all(["a", "button"]):
        for attr in ("data-embed", "data-src", "data-url", "data-href", "href", "src"):
            val = el.get(attr)
            if not val:
                continue
            full = urljoin(base_url, val)
            if _looks_like_chat(full) or not _is_probable_stream_url(full):
                continue
            if "embed" in full or "channel" in full or "player" in full or full.startswith("http"):
                streams.append({
                    "label": (el.get_text(strip=True) or "Stream")[:64] or "Stream",
                    "embed_url": _force_https(full),
                    "watch_url": base_url,
                    "origin": "scraped",
                })

    return streams


_URL_RE = re.compile(r"https?://[^'\"\\s]+", re.I)


def _extract_first_url(text: str | None) -> str | None:
    if not text:
        return None
    m = _URL_RE.search(text)
    if not m:
        return None
    return _force_https(m.group(0))


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
        # keep today + a small lookahead (similar to topembed behavior)
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


def _extract_topembed_streams(card, default_watch: str) -> list[dict]:
    channels = card.select(".channel-item-compact")
    streams: list[dict] = []
    for ch in channels:
        name_el = ch.select_one(".channel-name-compact")
        label = (name_el.get_text(" ", strip=True) if name_el else "Stream") or "Stream"
        preview_btn = ch.select_one(".btn-preview-inline-small")
        embed_url = _extract_first_url(preview_btn.get("onclick") if preview_btn else None)
        if not embed_url:
            for btn in ch.select(".btn-copy-icon"):
                embed_url = _extract_first_url(btn.get("onclick"))
                if embed_url:
                    break
        if not embed_url:
            continue
        streams.append({
            "label": label[:64] or "Stream",
            "embed_url": embed_url,
            "watch_url": default_watch or embed_url,
            "origin": "api",
        })
    return _dedup_streams(streams)


def scrape_topembed() -> pd.DataFrame:
    session = _get_session()
    events_url = urljoin(BASE_URL_TOP_EMBED, TOP_EMBED_EVENTS_PATH)
    r = session.get(events_url, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        return pd.DataFrame()

    soup = BeautifulSoup(r.text, "html.parser")
    cards = soup.select(".event-card")
    if not cards:
        return pd.DataFrame()

    rows = []
    now_utc = datetime.now(UTC)

    for card in cards:
        try:
            ts_sec = int(float(card.get("data-timestamp", "0")))
            event_dt = datetime.fromtimestamp(ts_sec, tz=UTC).astimezone(EST)
        except Exception:
            continue

        if not _within_days(event_dt, days_ahead=1):
            continue

        matchup_el = card.select_one(".event-title")
        matchup = matchup_el.get_text(" ", strip=True) if matchup_el else "Unknown match"

        league_attr = card.get("data-league") or ""
        league_el = card.select_one(".event-league")
        league_text = league_el.get_text(strip=True) if league_el else ""
        league = league_attr or league_text or None

        # Build watch URL from copy buttons or fallback to event slug
        watch_url = None
        embed_actions = card.select_one(".event-embed-actions")
        if embed_actions:
            for btn in embed_actions.find_all("button"):
                maybe_url = _extract_first_url(btn.get("onclick"))
                if maybe_url and "/event/" in maybe_url:
                    watch_url = maybe_url
                    break
        if not watch_url:
            channels_div = card.select_one(".event-channels")
            slug = None
            if channels_div and channels_div.has_attr("id"):
                slug = channels_div["id"].replace("event-", "", 1)
            if slug:
                watch_url = urljoin(BASE_URL_TOP_EMBED, f"/event/{slug}")

        streams = _extract_topembed_streams(card, watch_url)
        if not streams:
            continue
        embed_url = streams[0]["embed_url"] if streams else None

        sport = card.get("data-category") or _guess_sport(matchup)
        status = (card.get("data-status") or "").lower()
        is_live = (status == "live") or ((event_dt - timedelta(minutes=15)) <= now_utc <= (event_dt + timedelta(hours=5)))

        rows.append({
            "source": "topembed",
            "date_header": event_dt.strftime("%A, %B %d, %Y"),
            "sport": sport,
            "time_unix": ts_sec * 1000,
            "time": event_dt,
            "tournament": league,
            "tournament_url": None,
            "matchup": matchup,
            "watch_url": watch_url or embed_url,
            "streams": streams,
            "embed_url": embed_url,
            "is_live": is_live,
        })

    return pd.DataFrame(rows)

# ---------------- SHARKSTREAMS ----------------

_OPENEMBED_RE = re.compile(r"openEmbed\(\s*[\"']([^\"']+)[\"']\s*\)", re.I)
_WINDOWOPEN_RE = re.compile(r"window\.open\(\s*[\"']([^\"']+)[\"']\s*,", re.I)
_HREF_URL_RE = re.compile(r"https?://[^\\s\"']+", re.I)

def scrape_shark() -> pd.DataFrame:
    session = _get_session()
    r = session.get(BASE_URL_SHARK, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
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

        streams = [{
            "label": "SharkStreams",
            "embed_url": u,
            "watch_url": u,
            "origin": "scraped",
        } for u in norm_urls]

        rows.append({
            "source": "sharkstreams",
            "date_header": dt.strftime("%A, %B %d, %Y"),
            "sport": cat_span.get_text(strip=True) if cat_span else "Other",
            "time_unix": int(dt.timestamp() * 1000),
            "time": dt,
            "tournament": None,
            "tournament_url": None,
            "matchup": name_span.text.strip(),
            "watch_url": norm_urls[0],
            "streams": streams,
            "embed_url": norm_urls[0],
            "is_live": False,
        })

    return pd.DataFrame(rows)

# ---------------- MERGE + WRITE ----------------

def merge_streams(new, old):
    manual = [s for s in old if _is_manual(s)]
    scraped = [s for s in new if not _is_manual(s)]
    return _dedup_streams(manual + scraped)

def main():
    df_streamed = scrape_streamed_api()
    df_topembed = scrape_topembed()
    df_shark = scrape_shark()

    source_counts = {
        "streamed.pk": len(df_streamed),
        "topembed": len(df_topembed),
        "sharkstreams": len(df_shark),
    }

    scraped_frames = [df for df in (df_streamed, df_topembed, df_shark) if not df.empty]
    if not scraped_frames:
        print("[scraper] No games found.")
        return

    df_new = pd.concat(scraped_frames, ignore_index=True)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not OUTPUT_FILE.exists():
        pd.DataFrame(columns=CSV_COLS).to_csv(OUTPUT_FILE, index=False)

    with OUTPUT_FILE.open("r+", encoding="utf-8") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        df_old = _ensure_cols(pd.read_csv(fh))
        df_new = _ensure_cols(df_new)

        # Build a lookup of existing rows by stable ID so we don't drop manual edits
        old_map = {}
        for _, row in df_old.iterrows():
            rd = row.to_dict()
            old_map[_stable_game_id(rd)] = rd

        # Deduplicate new scrape across sources before merging into existing rows
        new_map = {}
        for _, row in df_new.iterrows():
            rd = row.to_dict()
            gid = _stable_game_id(rd)
            existing = new_map.get(gid)
            if existing:
                merged_streams = merge_streams(rd.get("streams") or [], _parse_streams_cell(existing.get("streams")))
                rd["streams"] = merged_streams
                rd["embed_url"] = rd.get("embed_url") or (merged_streams[0]["embed_url"] if merged_streams else None)
                rd["is_live"] = existing.get("is_live") or _normalize_bool(rd.get("is_live"))
            new_map[gid] = rd

        df_new = pd.DataFrame(new_map.values())

        out_rows = []

        # Merge scraped rows with existing where possible
        for _, row in df_new.iterrows():
            rd = row.to_dict()
            gid = _stable_game_id(rd)
            old = old_map.pop(gid, None)

            old_streams = _parse_streams_cell(old.get("streams")) if old else []
            merged = merge_streams(rd.get("streams") or [], old_streams)

            rd["streams"] = merged
            rd["embed_url"] = rd.get("embed_url") or (merged[0]["embed_url"] if merged else None)
            rd["is_live"] = _normalize_bool(old.get("is_live") if old else rd.get("is_live"))

            out_rows.append(rd)

        # Carry forward unmatched (typically manual) rows so they are not wiped out
        for _, rd in old_map.items():
            if not _should_keep_existing(rd):
                continue
            streams = _parse_streams_cell(rd.get("streams"))
            rd["streams"] = streams
            rd["embed_url"] = rd.get("embed_url") or (streams[0]["embed_url"] if streams else None)
            rd["is_live"] = _normalize_bool(rd.get("is_live"))
            out_rows.append(rd)

        fh.seek(0)
        fh.truncate()
        pd.DataFrame(out_rows)[CSV_COLS].to_csv(fh, index=False)
        fcntl.flock(fh, fcntl.LOCK_UN)

    print(f"[scraper] Wrote {len(out_rows)} games (streamed.pk={source_counts['streamed.pk']}, topembed={source_counts['topembed']}, sharkstreams={source_counts['sharkstreams']})")

if __name__ == "__main__":
    main()
