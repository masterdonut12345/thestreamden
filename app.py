from __future__ import annotations

import eventlet

eventlet.monkey_patch()

import json
import os
import re
import secrets
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Optional
import redis
from flask import (
    Flask,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_socketio import SocketIO, emit, join_room

from den_store import Den, den_store
from streaming_site import (
    DEN_EXPIRATION_HOURS,
    den_expiration_cutoff,
    get_session_id,
    load_games_cached,
    streaming_bp,
)

app = Flask(__name__)

APP_SECRET = os.environ.get("APP_SECRET", "dev-secret-key")
REDIS_URL = os.environ.get("REDIS_URL")
CHAT_MAX_MESSAGES = int(os.environ.get("CHAT_MAX_MESSAGES", "50"))
CHAT_MAX_LENGTH = int(os.environ.get("CHAT_MAX_LENGTH", "400"))
CLEANUP_INTERVAL_SECONDS = int(os.environ.get("CLEANUP_INTERVAL_SECONDS", "3600"))
SESSION_COOKIE_SECURE = os.environ.get("SESSION_COOKIE_SECURE", "0").lower() in ("1", "true", "yes")
SESSION_COOKIE_SAMESITE = os.environ.get("SESSION_COOKIE_SAMESITE", "Lax")

app.secret_key = APP_SECRET
app.config.update(
    SESSION_COOKIE_SECURE=SESSION_COOKIE_SECURE,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE=SESSION_COOKIE_SAMESITE,
)

socketio = SocketIO(
    app,
    async_mode="eventlet",
    cors_allowed_origins="*",
    message_queue=REDIS_URL,
)

app.register_blueprint(streaming_bp)

_cleanup_thread_started = False


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.environ.get(key)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


class ChatStore:
    """Persist recent chat messages per den, with Redis fanout when available."""

    def __init__(self, redis_url: str | None, max_messages: int):
        self.max_messages = max_messages
        self._redis = None
        self._local: dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()

        if redis_url:
            try:
                self._redis = redis.from_url(redis_url, decode_responses=True)
            except Exception as exc:
                print(f"[chat] Failed to init Redis ({redis_url}): {exc}")
                self._redis = None

    def _key(self, den_id: str) -> str:
        return f"denchat:{den_id}"

    def _recent_local(self, den_id: str, limit: int | None = None) -> list[dict]:
        limit = limit or self.max_messages
        with self._lock:
            bucket = self._local[den_id]
            return list(reversed(list(bucket)[-limit:]))

    def recent(self, den_id: str, limit: int | None = None) -> list[dict]:
        limit = limit or self.max_messages
        if self._redis:
            try:
                raw = self._redis.lrange(self._key(den_id), 0, limit - 1)
                parsed = [json.loads(item) for item in raw]
                return list(reversed(parsed))
            except Exception as exc:
                print(f"[chat] Redis recent fallback: {exc}")
        return self._recent_local(den_id, limit)

    def add(self, den_id: str, message: dict) -> None:
        if self._redis:
            try:
                pipe = self._redis.pipeline()
                pipe.lpush(self._key(den_id), json.dumps(message))
                pipe.ltrim(self._key(den_id), 0, self.max_messages - 1)
                pipe.execute()
                return
            except Exception as exc:
                print(f"[chat] Redis add fallback: {exc}")

        with self._lock:
            bucket = self._local[den_id]
            bucket.appendleft(message)
            while len(bucket) > self.max_messages:
                bucket.pop()


chat_store = ChatStore(REDIS_URL, CHAT_MAX_MESSAGES)


# -----------------------------
# CSRF helpers
# -----------------------------


def generate_csrf_token() -> str:
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["csrf_token"] = token
    return token


def require_csrf():
    token = session.get("csrf_token")
    submitted = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token")
    if not token or not submitted or token != submitted:
        abort(400)


@app.context_processor
def inject_csrf():
    return {"csrf_token": generate_csrf_token()}


# -----------------------------
# Utility helpers
# -----------------------------


def _start_cleanup_worker():
    global _cleanup_thread_started
    if _cleanup_thread_started:
        return

    def _loop():
        while True:
            try:
                cutoff = den_expiration_cutoff()
                removed = den_store.cleanup(cutoff)
                if removed:
                    print(f"[cleanup] Removed {removed} expired dens")
            except Exception as exc:
                print(f"[cleanup] Error during cleanup: {exc}")
            time.sleep(CLEANUP_INTERVAL_SECONDS)

    t = threading.Thread(target=_loop, daemon=True, name="den-cleanup")
    t.start()
    _cleanup_thread_started = True


def find_game_by_id(game_id: int) -> Optional[dict]:
    try:
        games = load_games_cached()
    except Exception:
        return None
    for g in games:
        try:
            if int(g.get("id")) == int(game_id):
                return g
        except Exception:
            continue
    return None


def serialize_den(den: Den) -> dict:
    return {
        "id": den.id,
        "name": den.name,
        "slug": den.slug,
        "is_public": den.is_public,
        "invite_code": den.invite_code or "",
        "owner_session": den.owner_session,
        "game_id": den.game_id,
        "stream_url": den.stream_url,
        "created_at": den.created_at,
    }


def _chat_room(den_id: str) -> str:
    return f"den:{den_id}"


def _build_chat_identity() -> dict:
    viewer_id = get_session_id()
    anon_label = f"Fan-{viewer_id[:6]}" if viewer_id else "Fan"
    return {
        "display": anon_label,
        "viewer_id": viewer_id,
        "is_guest": True,
    }


# -----------------------------
# Request lifecycle
# -----------------------------


@app.before_request
def before_any_request():
    generate_csrf_token()
    _start_cleanup_worker()


# -----------------------------
# Den routes
# -----------------------------


@app.route("/dens/create", methods=["POST"])
def create_den():
    require_csrf()
    session_id = get_session_id()

    name = (request.form.get("name") or "").strip()
    stream_url = (request.form.get("stream_url") or "").strip()
    game_id_raw = request.form.get("game_id") or ""
    is_public = (request.form.get("is_public") or "on").lower() in ("on", "true", "1", "yes")

    if not name or len(name) > 160:
        abort(400)
    if stream_url and len(stream_url) > 2048:
        abort(400)

    game_id = None
    try:
        if game_id_raw:
            game_id = int(game_id_raw)
    except Exception:
        game_id = None

    if not game_id and not stream_url:
        abort(400)

    selected_game = find_game_by_id(game_id) if game_id else None
    if game_id and not selected_game:
        abort(400)
    if selected_game and not stream_url:
        stream_url = selected_game.get("watch_url") or ""
        if not stream_url:
            streams = selected_game.get("streams") or []
            if streams:
                stream_url = streams[0].get("watch_url") or streams[0].get("embed_url") or ""

    if not stream_url:
        abort(400)
    if len(stream_url) > 2048:
        abort(400)

    den = den_store.create(
        name=name,
        stream_url=stream_url,
        game_id=game_id,
        is_public=is_public,
        owner_session=session_id,
    )

    return redirect(url_for("den_page", slug=den.slug))


def _den_or_404(slug: str) -> Den:
    den = den_store.get(slug)
    if den is None or den.created_at < den_expiration_cutoff():
        abort(404)
    return den


@app.route("/dens/<slug>")
def den_page(slug):
    session_id = get_session_id()
    den = _den_or_404(slug)
    invite_token = (request.args.get("invite") or "").strip()

    is_member = session_id in den.members
    if not is_member and den.is_public:
        den_store.join(slug, session_id)
        is_member = True
    if not is_member and invite_token and invite_token == (den.invite_code or ""):
        den_store.join(slug, session_id)
        is_member = True

    share_url = request.url_root.rstrip("/") + url_for("den_page", slug=den.slug)
    invite_url = share_url
    if den.invite_code:
        invite_url = f"{share_url}?invite={den.invite_code}"

    if not is_member and not den.is_public:
        return render_template(
            "den.html",
            den=serialize_den(den),
            invite_required=True,
            access_denied=True,
            current_user="Guest",
            current_user_id=None,
            share_url=share_url,
            invite_url=invite_url,
            chat_max_length=CHAT_MAX_LENGTH,
        )

    game = find_game_by_id(den.game_id) if den.game_id else None
    den_streams = game.get("streams") if game else []
    active_stream_url = den.stream_url or ""
    if not active_stream_url and den_streams:
        first_stream = den_streams[0]
        if first_stream:
            active_stream_url = first_stream.get("embed_url") or first_stream.get("watch_url") or ""

    games_for_dens = [g for g in load_games_cached() if g.get("streams")]
    recent_messages = chat_store.recent(str(den.id))

    return render_template(
        "den.html",
        den=serialize_den(den),
        messages=recent_messages,
        current_user="Guest",
        current_user_id=session_id,
        game=game,
        den_streams=den_streams,
        active_stream_url=active_stream_url,
        share_url=share_url,
        invite_url=invite_url,
        chat_max_length=CHAT_MAX_LENGTH,
        games_for_dens=games_for_dens,
    )


@app.route("/dens/<slug>/join", methods=["POST"])
def join_den(slug):
    require_csrf()
    session_id = get_session_id()
    den = _den_or_404(slug)
    invite_code = (request.form.get("invite_code") or "").strip()

    if not den.is_public:
        if not invite_code or invite_code != (den.invite_code or ""):
            share_url = request.url_root.rstrip("/") + url_for("den_page", slug=den.slug)
            invite_url = share_url
            if den.invite_code:
                invite_url = f"{share_url}?invite={den.invite_code}"
            return render_template(
                "den.html",
                den=serialize_den(den),
                invite_required=True,
                access_denied=True,
                invite_error="Invalid invite code.",
                current_user="Guest",
                current_user_id=session_id,
                share_url=share_url,
                invite_url=invite_url,
                chat_max_length=CHAT_MAX_LENGTH,
            )

    den_store.join(slug, session_id)
    return redirect(url_for("den_page", slug=slug))


@app.route("/dens/<slug>/delete", methods=["POST"])
def delete_den(slug):
    require_csrf()
    session_id = get_session_id()
    den = _den_or_404(slug)
    if den.owner_session != session_id:
        abort(403)
    den_store.delete(slug, session_id)
    return redirect(url_for("streaming.index"))


@app.route("/dens/<slug>/stream", methods=["POST"])
def update_den_stream(slug):
    require_csrf()
    session_id = get_session_id()
    den = _den_or_404(slug)
    if den.owner_session != session_id:
        abort(403)

    stream_url = (request.form.get("stream_url") or "").strip()
    game_id_raw = request.form.get("game_id") or ""
    if not stream_url or len(stream_url) > 2048:
        abort(400)

    game_id = None
    try:
        if game_id_raw:
            game_id = int(game_id_raw)
    except Exception:
        game_id = None

    den_store.update_stream(slug, session_id, stream_url, game_id)
    return redirect(url_for("den_page", slug=slug))


# -----------------------------
# Socket events
# -----------------------------


@socketio.on("join")
def socket_join(payload):
    den_id_raw = (payload or {}).get("den_id")
    try:
        den_id = int(den_id_raw)
    except Exception:
        return

    den = den_store.get_by_id(den_id)
    if not den or den.created_at < den_expiration_cutoff():
        emit("chat_error", {"error": "not_allowed"})
        return

    session_id = get_session_id()
    if not den.is_public and session_id not in den.members:
        emit("chat_error", {"error": "not_allowed"})
        return

    join_room(_chat_room(str(den_id)))
    recent = chat_store.recent(str(den_id)) or []
    emit("recent_messages", recent)


@socketio.on("send_message")
def socket_send_message(payload):
    den_id_raw = (payload or {}).get("den_id")
    text = (payload or {}).get("text") or ""
    text = text.strip()

    try:
        den_id = int(den_id_raw)
    except Exception:
        return

    if not den_id or not text:
        return

    text = text[:CHAT_MAX_LENGTH]
    den = den_store.get_by_id(den_id)
    if not den or den.created_at < den_expiration_cutoff():
        emit("chat_error", {"error": "not_allowed"})
        return

    session_id = get_session_id()
    if not den.is_public and session_id not in den.members:
        emit("chat_error", {"error": "not_allowed"})
        return

    message = {
        "id": int(time.time() * 1000),
        "den_id": den_id,
        "text": text,
        "ts": time.time(),
        "user": _build_chat_identity(),
    }
    chat_store.add(str(den_id), message)
    emit("chat_message", message, room=_chat_room(str(den_id)))


if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
