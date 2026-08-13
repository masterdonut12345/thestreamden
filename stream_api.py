"""Public + private streaming API and private admin dashboard.

Part of thestreamden. Reads the same games.db the site uses (via
streaming_site.load_games_cached) so the 30-minute scraper is the single
source of truth.

Endpoints
---------
Public (no auth):
  GET  /api/v1/sports               -> list of sports with counts
  GET  /api/v1/channels             -> searchable/filterable stream list
  GET  /api/v1/channels/<id>        -> single stream detail
  GET  /api/v1/play/<id>            -> hosted HTML player WITH ads

Private (Authorization: Bearer <api_key>):
  GET  /api/v1/private/channels     -> same listing
  GET  /api/v1/private/play/<id>    -> hosted HTML player, ad-free
  GET  /api/v1/private/me           -> key/limit info

Admin dashboard (password + TOTP, cookie session):
  GET  /admin/setup                 -> first-run: set password + TOTP
  POST /admin/setup
  GET  /admin/login                 -> password step
  POST /admin/login
  GET  /admin/login/totp            -> TOTP step
  POST /admin/login/totp
  GET  /admin/logout
  GET  /admin                       -> dashboard (manage users + keys)
  POST /admin/users                 -> create user
  POST /admin/users/<id>/toggle     -> activate/deactivate user
  POST /admin/users/<id>/keys       -> issue API key (shown once)
  POST /admin/keys/<id>/toggle      -> activate/revoke key
  POST /admin/keys/<id>/rate_limit  -> set per-key limit

Security
--------
* API keys: 32 bytes of urandom, stored as SHA-256, compared with
  hmac.compare_digest (constant time). Full key shown only once at issue.
* Per-key rate limiting (sliding window, in-memory); default editable per key.
* Public endpoints rate-limited per IP.
* Admin: password hashed (werkzeug), TOTP enforced, login attempts limited,
  session cookie HttpOnly + SameSite=Strict (Secure when behind TLS).
"""

from __future__ import annotations

import hashlib
import hmac
import io
import os
import re
import secrets
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyotp
import requests
from flask import (
    Blueprint,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

from streaming_site import (
    GAMES_DB_PATH,
    build_embed_player_fallback_url,
    load_games_cached,
    normalize_sport_name,
    sport_is_invalid,
)

api_bp = Blueprint("api", __name__)

# Admin account can be configured via env vars (for ephemeral deploys) or via
# setup_admin.py (stored in api.db). Env vars take priority.
ADMIN_PASSWORD_HASH = os.environ.get("ADMIN_PASSWORD_HASH", "")
ADMIN_TOTP_SECRET = os.environ.get("ADMIN_TOTP_SECRET", "")

# =============================================================================
# DB / storage
# =============================================================================
API_DB_PATH = Path(os.environ.get("API_DB_PATH", Path(__file__).parent / "data" / "api.db")).expanduser()

# ---- Adsterra ad units (public player only) ---------------------------------
# Banner: 728x90 leaderboard injected above the player.
ADSTERRA_BANNER_KEY = os.environ.get("ADSTERRA_BANNER_KEY", "9bf5ed86717fc77ea7a6a0e5755d1f46")
ADSTERRA_BANNER_FORMAT = os.environ.get("ADSTERRA_BANNER_FORMAT", "iframe")
ADSTERRA_BANNER_WIDTH = int(os.environ.get("ADSTERRA_BANNER_WIDTH", "728"))
ADSTERRA_BANNER_HEIGHT = int(os.environ.get("ADSTERRA_BANNER_HEIGHT", "90"))
# Social bar: floating sticky ad script.
ADSTERRA_SOCIAL_BAR_SRC = os.environ.get(
    "ADSTERRA_SOCIAL_BAR_SRC",
    "https://pl30832434.effectivecpmnetwork.com/56/4d/92/564d92302e80067679a09beb884354d6.js",
)

# ---- Tunables ---------------------------------------------------------------
PUBLIC_RATE_LIMIT_PER_MIN = int(os.environ.get("PUBLIC_RATE_LIMIT_PER_MIN", "30"))
DEFAULT_KEY_RATE_LIMIT_PER_MIN = int(os.environ.get("DEFAULT_KEY_RATE_LIMIT_PER_MIN", "120"))
LOGIN_ATTEMPTS_PER_MIN = int(os.environ.get("LOGIN_ATTEMPTS_PER_MIN", "5"))
MAX_CHANNEL_PER_PAGE = int(os.environ.get("MAX_CHANNEL_PER_PAGE", "100"))
API_CACHE_MAX_AGE = int(os.environ.get("API_CACHE_MAX_AGE", "60"))
# Admin account is provisioned only via setup_admin.py; there is no web
# /admin/setup route.

API_DB_LOCK = threading.Lock()

# In-memory sliding-window rate limiter. Keyed by api-key-id for private,
# "ip:<addr>" for public, "login:<addr>" for admin logins.
_RATE_HITS: dict[str, list[float]] = {}
_RATE_LOCK = threading.Lock()


# =============================================================================
# DB helpers
# =============================================================================
def _ensure_api_db() -> None:
    API_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with API_DB_LOCK, sqlite3.connect(API_DB_PATH) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS admin_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                password_hash TEXT NOT NULL,
                totp_secret TEXT NOT NULL,
                created_at REAL NOT NULL
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS api_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                created_at REAL NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES api_users(id),
                name TEXT NOT NULL,
                key_hash TEXT NOT NULL UNIQUE,
                key_prefix TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL,
                rate_limit_per_min INTEGER NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                last_used_at REAL
            )"""
        )
        conn.commit()


def _api_db_conn() -> sqlite3.Connection:
    _ensure_api_db()
    conn = sqlite3.connect(API_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


# =============================================================================
# Rate limiting
# =============================================================================
def _rate_allow(bucket: str, limit_per_min: int) -> bool:
    if limit_per_min <= 0:
        return True
    now = time.time()
    with _RATE_LOCK:
        hits = [t for t in _RATE_HITS.get(bucket, []) if t > now - 60]
        if len(hits) >= limit_per_min:
            _RATE_HITS[bucket] = hits
            return False
        hits.append(now)
        _RATE_HITS[bucket] = hits
        return True


def _client_ip() -> str:
    fwd = request.headers.get("X-Forwarded-For", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.remote_addr or "0.0.0.0"


def _rate_limit_public() -> bool:
    return _rate_allow(f"ip:{_client_ip()}", PUBLIC_RATE_LIMIT_PER_MIN)


# =============================================================================
# API key auth (private endpoints)
# =============================================================================
def _bearer_key() -> str:
    header = request.headers.get("Authorization", "")
    m = re.match(r"(?i)^Bearer\s+(\S+)$", header.strip())
    return m.group(1).strip() if m else ""


def _authenticate_key() -> tuple[sqlite3.Row | None, sqlite3.Row | None, str | None]:
    """Validate a Bearer key. Returns (user_row, key_row, error_msg)."""
    raw_key = _bearer_key()
    if not raw_key:
        return None, None, "Missing Authorization: Bearer <api_key> header."

    with _api_db_conn() as conn:
        key_row = conn.execute(
            "SELECT * FROM api_keys WHERE key_hash = ?", (_hash_key(raw_key),)
        ).fetchone()

    if not key_row:
        return None, None, "Invalid API key."
    if not key_row["active"]:
        return None, None, "API key is revoked."
    if key_row["expires_at"] and key_row["expires_at"] < time.time():
        return None, None, "API key has expired."

    if not _rate_allow(f"key:{key_row['id']}", key_row["rate_limit_per_min"]):
        return None, None, "Rate limit exceeded for this API key."

    with _api_db_conn() as conn:
        user_row = conn.execute(
            "SELECT * FROM api_users WHERE id = ?", (key_row["user_id"],)
        ).fetchone()
    if not user_row or not user_row["active"]:
        return None, None, "API key owner is disabled."

    with _api_db_conn() as conn:
        conn.execute("UPDATE api_keys SET last_used_at = ? WHERE id = ?", (time.time(), key_row["id"]))
        conn.commit()

    return user_row, key_row, None


def require_api_key():
    _, _, error = _authenticate_key()
    if error:
        return jsonify({"error": error, "detail": "Provide Authorization: Bearer <api_key>."}), 401
    return None


# -----------------------------------------------------------------------------
# Public data helpers
# -----------------------------------------------------------------------------
def _channel_payload(
    game: dict[str, Any],
    with_streams: bool = False,
    private: bool = False,
) -> dict[str, Any]:
    streams = game.get("streams") or []
    payload: dict[str, Any] = {
        "id": game.get("id"),
        "slug": game.get("slug"),
        "sport": normalize_sport_name(game.get("sport")),
        "tournament": game.get("tournament"),
        "matchup": game.get("matchup"),
        "is_live": bool(game.get("is_live")),
        "time": game.get("time"),
        "time_unix": game.get("time_unix"),
        "home_team": game.get("home_team"),
        "away_team": game.get("away_team"),
        "home_abbr": game.get("home_abbr"),
        "away_abbr": game.get("away_abbr"),
        "game_status": game.get("game_status"),
    }
    if private:
        payload["source"] = game.get("source")
    if with_streams:
        payload["streams"] = [
            {
                "label": s.get("label"),
                "embed_url": s.get("embed_url"),
                "watch_url": s.get("watch_url"),
            }
            for s in streams
        ]
    play_endpoint = "api.private_play" if private else "api.public_play"
    payload["play_url"] = url_for(play_endpoint, channel_id=game["id"], _external=True)
    return payload


def _filter_channels(
    games: list[dict[str, Any]], q: str, sport: str, source: str, is_live: bool | None
) -> list[dict[str, Any]]:
    q_l = q.lower().strip()
    sport_l = sport.lower().strip()
    source_l = source.lower().strip()
    out: list[dict[str, Any]] = []
    for g in games:
        if sport_l:
            norm = normalize_sport_name(g.get("sport")).lower()
            if norm != sport_l and sport_l not in norm:
                continue
        if source_l and str(g.get("source") or "").lower() != source_l:
            continue
        if is_live is not None and bool(g.get("is_live")) != is_live:
            continue
        if q_l:
            haystack = " ".join(
                str(g.get(k) or "") for k in ("matchup", "tournament", "sport", "home_team", "away_team")
            ).lower()
            if q_l not in haystack:
                continue
        out.append(g)
    return out


def _paginate(items: list[Any], page: int, per_page: int) -> tuple[list[Any], int, int]:
    total = len(items)
    per_page = max(1, min(per_page, MAX_CHANNEL_PER_PAGE))
    page = max(1, page)
    start = (page - 1) * per_page
    return items[start : start + per_page], total, per_page


# =============================================================================
# Public routes
# =============================================================================
@api_bp.route("/api/v1/sports")
def public_sports():
    if not _rate_limit_public():
        return jsonify({"error": "Rate limit exceeded."}), 429
    games = load_games_cached()
    counts: dict[str, int] = {}
    for g in games:
        sport = normalize_sport_name(g.get("sport"))
        if sport_is_invalid(sport):
            continue
        counts[sport] = counts.get(sport, 0) + 1
    sports = [{"sport": s, "count": c} for s, c in sorted(counts.items(), key=lambda x: x[1], reverse=True)]
    return _json_response({"sports": sports, "total_streams": sum(counts.values())})


@api_bp.route("/api/v1/channels")
def public_channels():
    if not _rate_limit_public():
        return jsonify({"error": "Rate limit exceeded."}), 429
    q = request.args.get("q", "")
    sport = request.args.get("sport", "")
    source = request.args.get("source", "")
    live = request.args.get("live", "")
    is_live = None if live in ("", "any") else live.lower() in ("1", "true", "yes", "on")
    try:
        page = int(request.args.get("page", "1"))
        per_page = int(request.args.get("per_page", "30"))
    except ValueError:
        return jsonify({"error": "page/per_page must be integers."}), 400

    filtered = _filter_channels(load_games_cached(), q, sport, source, is_live)
    filtered.sort(key=lambda g: (not bool(g.get("is_live")), g.get("time_unix") or 0))
    page_items, total, per_page = _paginate(filtered, page, per_page)
    return _json_response(
        {
            "channels": [_channel_payload(g) for g in page_items],
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page if per_page else 0,
        }
    )


@api_bp.route("/api/v1/channels/<int:channel_id>")
def public_channel_detail(channel_id: int):
    if not _rate_limit_public():
        return jsonify({"error": "Rate limit exceeded."}), 429
    for g in load_games_cached():
        if g.get("id") == channel_id:
            return _json_response({"channel": _channel_payload(g)})
    return jsonify({"error": "Stream not found."}), 404


@api_bp.route("/api/v1/play/<int:channel_id>")
def public_play(channel_id: int):
    if not _rate_limit_public():
        return jsonify({"error": "Rate limit exceeded."}), 429
    return _render_player(channel_id, show_ads=True)


# =============================================================================
# Private routes (Bearer API key)
# =============================================================================
@api_bp.route("/api/v1/private/channels")
def private_channels():
    err = require_api_key()
    if err is not None:
        return err
    q = request.args.get("q", "")
    sport = request.args.get("sport", "")
    source = request.args.get("source", "")
    live = request.args.get("live", "")
    is_live = None if live in ("", "any") else live.lower() in ("1", "true", "yes", "on")
    try:
        page = int(request.args.get("page", "1"))
        per_page = int(request.args.get("per_page", "30"))
    except ValueError:
        return jsonify({"error": "page/per_page must be integers."}), 400

    filtered = _filter_channels(load_games_cached(), q, sport, source, is_live)
    filtered.sort(key=lambda g: (not bool(g.get("is_live")), g.get("time_unix") or 0))
    page_items, total, per_page = _paginate(filtered, page, per_page)
    return _json_response(
        {
            "channels": [_channel_payload(g, with_streams=True, private=True) for g in page_items],
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page if per_page else 0,
        }
    )


@api_bp.route("/api/v1/private/play/<int:channel_id>")
def private_play(channel_id: int):
    err = require_api_key()
    if err is not None:
        return err
    return _render_player(channel_id, show_ads=False)


@api_bp.route("/api/v1/private/me")
def private_me():
    user, key, error = _authenticate_key()
    if error:
        return jsonify({"error": error}), 401
    return _json_response(
        {
            "user": user["name"],
            "key_name": key["name"],
            "rate_limit_per_min": key["rate_limit_per_min"],
            "expires_at": key["expires_at"],
            "created_at": key["created_at"],
            "last_used_at": key["last_used_at"],
        }
    )


# =============================================================================
# Player page
# =============================================================================
def _render_player(channel_id: int, show_ads: bool):
    game = next((g for g in load_games_cached() if g.get("id") == channel_id), None)
    if not game:
        return jsonify({"error": "Stream not found."}), 404

    streams = game.get("streams") or []
    active = game.get("embed_url") or (streams[0]["embed_url"] if streams else "")
    all_embeds = [s.get("embed_url", "") for s in streams if s.get("embed_url")]
    player_src = build_embed_player_fallback_url(active, all_embeds)

    return render_template(
        "api_player.html",
        game=game,
        player_src=player_src,
        show_ads=show_ads,
        adsterra_banner_key=ADSTERRA_BANNER_KEY,
        adsterra_banner_format=ADSTERRA_BANNER_FORMAT,
        adsterra_banner_width=ADSTERRA_BANNER_WIDTH,
        adsterra_banner_height=ADSTERRA_BANNER_HEIGHT,
        adsterra_social_bar_src=ADSTERRA_SOCIAL_BAR_SRC,
    )


def _json_response(data: Any, status: int = 200) -> Response:
    resp = jsonify(data)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Cache-Control"] = f"public, max-age={API_CACHE_MAX_AGE}"
    return resp, status


# =============================================================================
# Admin auth helpers
# =============================================================================
def _env_admin() -> dict | None:
    """Return admin config from env vars if both are set."""
    if ADMIN_PASSWORD_HASH and ADMIN_TOTP_SECRET:
        return {"password_hash": ADMIN_PASSWORD_HASH, "totp_secret": ADMIN_TOTP_SECRET}
    return None


def _admin_row() -> sqlite3.Row | None:
    _ensure_api_db()
    with sqlite3.connect(API_DB_PATH, check_same_thread=False) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute("SELECT * FROM admin_settings WHERE id = 1").fetchone()


def _admin_configured() -> bool:
    return _env_admin() is not None or _admin_row() is not None


def _get_admin_config() -> dict:
    """Get admin config (password_hash, totp_secret) from env or DB."""
    env = _env_admin()
    if env:
        return env
    row = _admin_row()
    if row:
        return {"password_hash": row["password_hash"], "totp_secret": row["totp_secret"]}
    return {}


def _admin_logged_in() -> bool:
    return bool(session.get("admin_auth"))


def _admin_login_allowed() -> bool:
    return _rate_allow(f"login:{_client_ip()}", LOGIN_ATTEMPTS_PER_MIN)


def _require_admin_session():
    if not _admin_logged_in():
        return redirect(url_for("api.admin_login"))


def _require_admin() -> Response | None:
    if not _admin_logged_in():
        return redirect(url_for("api.admin_login"))
    return None


# =============================================================================
# Admin dashboard
# =============================================================================
@api_bp.route("/admin")
def admin_index():
    if not _admin_configured():
        return "Admin is not configured. Provision it on the server with setup_admin.py.", 503
    err = _require_admin()
    if err is not None:
        return err

    with _api_db_conn() as conn:
        users = conn.execute(
            "SELECT * FROM api_users ORDER BY created_at DESC"
        ).fetchall()
        keys = conn.execute(
            """SELECT k.*, u.name AS user_name
               FROM api_keys k JOIN api_users u ON u.id = k.user_id
               ORDER BY k.created_at DESC"""
        ).fetchall()
    return render_template(
        "admin_dashboard.html",
        users=[dict(u) for u in users],
        keys=[dict(k) for k in keys],
        default_rate_limit=DEFAULT_KEY_RATE_LIMIT_PER_MIN,
        now=time.time(),
    )


def _totp_qr_data_uri(totp_uri: str) -> str:
    """Render the otpauth URI as an SVG QR and return it as a data URI.

    qrcode's SvgPathImage emits only the black modules on a transparent
    background, which is invisible on the dark admin page, so we inject a
    white background rect covering the whole viewBox.
    """
    import base64
    import io

    import qrcode
    from qrcode.image.svg import SvgPathImage

    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M)
    qr.add_data(totp_uri)
    qr.make(fit=True)
    img = qr.make_image(image_factory=SvgPathImage)
    buf = io.BytesIO()
    img.save(buf)
    svg = buf.getvalue().decode("utf-8")

    m = re.search(r'viewBox="([^"]+)"', svg)
    if m:
        _, _, width, height = [float(x) for x in m.group(1).split()]
        rect = (
            f'<rect x="0" y="0" width="{width}" height="{height}" '
            f'fill="#ffffff" />'
        )
        svg = svg.replace("><path", f">{rect}<path", 1)

    b64 = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return f"data:image/svg+xml;base64,{b64}"


@api_bp.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if _admin_logged_in():
        return redirect(url_for("api.admin_index"))
    if not _admin_configured():
        return "Admin is not configured. Provision it on the server with setup_admin.py.", 503
    if not _admin_login_allowed():
        return render_template("admin_login.html", error="Too many attempts. Try again in a minute.", step="password")

    if request.method == "POST":
        password = request.form.get("password", "")
        admin = _get_admin_config()
        if admin and check_password_hash(admin["password_hash"], password):
            session["admin_pw_ok"] = True
            return redirect(url_for("api.admin_totp"))
        return render_template("admin_login.html", error="Incorrect password.", step="password")

    return render_template("admin_login.html", error=None, step="password")


@api_bp.route("/admin/login/totp", methods=["GET", "POST"])
def admin_totp():
    if _admin_logged_in():
        return redirect(url_for("api.admin_index"))
    if not session.get("admin_pw_ok"):
        return redirect(url_for("api.admin_login"))
    if not _admin_login_allowed():
        return render_template("admin_login_totp.html", error="Too many attempts. Try again in a minute.")

    if request.method == "POST":
        code = re.sub(r"\s+", "", request.form.get("code", ""))
        admin = _get_admin_config()
        if admin and pyotp.TOTP(admin["totp_secret"]).verify(code, valid_window=1):
            session.pop("admin_pw_ok", None)
            session["admin_auth"] = True
            session.permanent = True
            return redirect(url_for("api.admin_index"))
        return render_template("admin_login_totp.html", error="Invalid verification code.")

    return render_template("admin_login_totp.html", error=None)


@api_bp.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("api.admin_login"))


# -----------------------------------------------------------------------------
# Admin user/key management
# -----------------------------------------------------------------------------
@api_bp.route("/admin/users", methods=["POST"])
def admin_create_user():
    if (r := _require_admin()) is not None:
        return r
    name = (request.form.get("name") or "").strip()
    if not name:
        return _admin_error("User name is required.")
    with _api_db_conn() as conn:
        conn.execute("INSERT INTO api_users (name, created_at, active) VALUES (?, ?, 1)", (name, time.time()))
        conn.commit()
    return redirect(url_for("api.admin_index"))


@api_bp.route("/admin/users/<int:user_id>/toggle", methods=["POST"])
def admin_toggle_user(user_id: int):
    if (r := _require_admin()) is not None:
        return r
    with _api_db_conn() as conn:
        conn.execute("UPDATE api_users SET active = 1 - active WHERE id = ?", (user_id,))
        conn.commit()
    return redirect(url_for("api.admin_index"))


@api_bp.route("/admin/users/<int:user_id>/keys", methods=["POST"])
def admin_create_key(user_id: int):
    if (r := _require_admin()) is not None:
        return r
    if user_id == 0:
        try:
            user_id = int(request.form.get("user_id") or "0")
        except ValueError:
            return _admin_error("Invalid user.")
    name = (request.form.get("name") or "").strip() or "default"
    expires_at = None
    expires_raw = (request.form.get("expires_at") or "").strip()
    if expires_raw:
        try:
            expires_at = float(expires_raw)
        except ValueError:
            return _admin_error("expires_at must be a unix timestamp (seconds) or empty.")
    try:
        rate_limit = int(request.form.get("rate_limit") or DEFAULT_KEY_RATE_LIMIT_PER_MIN)
    except ValueError:
        return _admin_error("rate_limit must be an integer.")
    if rate_limit < 1:
        return _admin_error("rate_limit must be >= 1.")

    raw_key = "td_" + secrets.token_urlsafe(32)
    with _api_db_conn() as conn:
        existing = conn.execute("SELECT id FROM api_users WHERE id = ?", (user_id,)).fetchone()
        if not existing:
            return _admin_error("User not found.")
        conn.execute(
            """INSERT INTO api_keys (user_id, name, key_hash, key_prefix, created_at, expires_at, rate_limit_per_min, active)
               VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
            (user_id, name, _hash_key(raw_key), raw_key[:8], time.time(), expires_at, rate_limit),
        )
        conn.commit()
    return render_template(
        "admin_key_issued.html",
        user_name=name,
        api_key=raw_key,
        rate_limit=rate_limit,
        expires_at=expires_at,
    )


@api_bp.route("/admin/keys/<int:key_id>/toggle", methods=["POST"])
def admin_toggle_key(key_id: int):
    if (r := _require_admin()) is not None:
        return r
    with _api_db_conn() as conn:
        conn.execute("UPDATE api_keys SET active = 1 - active WHERE id = ?", (key_id,))
        conn.commit()
    return redirect(url_for("api.admin_index"))


@api_bp.route("/admin/keys/<int:key_id>/rate_limit", methods=["POST"])
def admin_set_key_rate_limit(key_id: int):
    if (r := _require_admin()) is not None:
        return r
    try:
        rate_limit = int(request.form.get("rate_limit") or DEFAULT_KEY_RATE_LIMIT_PER_MIN)
    except ValueError:
        return _admin_error("rate_limit must be an integer.")
    if rate_limit < 1:
        return _admin_error("rate_limit must be >= 1.")
    with _api_db_conn() as conn:
        conn.execute("UPDATE api_keys SET rate_limit_per_min = ? WHERE id = ?", (rate_limit, key_id))
        conn.commit()
    return redirect(url_for("api.admin_index"))


def _admin_error(message: str) -> Response:
    return render_template("admin_dashboard.html", error=message), 400
