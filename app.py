from __future__ import annotations

import hmac
import os
import re
import secrets
import threading
import time
import json
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit, quote
from markupsafe import Markup
from flask import (
    Flask,
    abort,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_socketio import SocketIO, emit, join_room
import redis
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from werkzeug.exceptions import HTTPException
from werkzeug.security import check_password_hash, generate_password_hash

import embed_streams
from cleanup_expired import cleanup_expired_threads
from db_models import (
    Category,
    Post,
    SessionLocal,
    Thread,
    User,
    Den,
    DenMembership,
    DenChatMessage,
    init_db,
)
from pathlib import Path
from streaming_site import streaming_bp, get_session_id, load_games_cached

app = Flask(__name__)

ADMIN_GATE = os.environ.get("ADMIN_GATE")
EXP_CHOICES = ["1 day", "3 days", "1 week", "1 month"]
DEFAULT_TAG = "general"
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")
TWITCH_PARENT_HOST = "thestreamden.com"
CLEANUP_INTERVAL_SECONDS = int(os.environ.get("CLEANUP_INTERVAL_SECONDS", "3600"))
SESSION_COOKIE_SECURE = os.environ.get("SESSION_COOKIE_SECURE", "0").lower() in ("1", "true", "yes")
_cleanup_thread_started = False
_seed_started = False

REDIS_URL = os.environ.get("REDIS_URL")
CHAT_MAX_MESSAGES = int(os.environ.get("CHAT_MAX_MESSAGES", "25"))
CHAT_MAX_LENGTH = int(os.environ.get("CHAT_MAX_LENGTH", "400"))

def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.environ.get(key)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")

SESSION_COOKIE_SECURE = _env_bool("SESSION_COOKIE_SECURE", default=False)
SESSION_COOKIE_SAMESITE = os.environ.get("SESSION_COOKIE_SAMESITE", "Lax")

app.secret_key = os.environ.get("APP_SECRET", "dev-secret-key")
app.config.update(
    SESSION_COOKIE_SECURE=SESSION_COOKIE_SECURE,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE=SESSION_COOKIE_SAMESITE,
)

app.register_blueprint(streaming_bp)

socketio = SocketIO(
    app,
    async_mode="eventlet",
    cors_allowed_origins="*",
    message_queue=REDIS_URL,
)

# Ensure database tables exist on startup
init_db()


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
# DB/session helpers
# -----------------------------

def get_db():
    if "db" not in g:
        g.db = SessionLocal()
    return g.db


@app.teardown_appcontext
def shutdown_session(exception=None):
    db = g.pop("db", None)
    if db is not None:
        if exception:
            db.rollback()
        db.close()


# -----------------------------
# Auth + CSRF helpers
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
    if not token or not submitted or not hmac.compare_digest(token, submitted):
        abort(400)

@app.teardown_appcontext
def shutdown_session(exception=None):
    db = g.pop("db", None)
    if db is not None:
        if exception:
            db.rollback()
        db.close()

@app.context_processor
def inject_csrf():
    return {"csrf_token": generate_csrf_token()}

# -----------------------------
# Auth + CSRF helpers
# -----------------------------

def get_current_user(db):
    uid = session.get("user_id")
    if not uid:
        return None
    return db.get(User, uid)

def require_csrf():
    token = session.get("csrf_token")
    submitted = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token")
    if not token or not submitted or not hmac.compare_digest(token, submitted):
        abort(400)

def require_login(next_url: str = "/"):
    db = get_db()
    user = get_current_user(db)
    if not user:
        return redirect(f"/login?next={next_url}")
    if getattr(user, "is_banned", False):
        abort(403)
    return user

@app.context_processor
def inject_csrf():
    return {"csrf_token": generate_csrf_token()}


def is_admin_authed() -> bool:
    return bool(session.get("admin_authed"))


def require_admin():
    if not is_admin_authed():
        abort(403)


def _chat_room(den_id: str) -> str:
    return f"den:{den_id}"


def _build_chat_identity() -> dict:
    db = SessionLocal()
    try:
        user = get_current_user(db)
        if user:
            display = user.username or f"User-{user.id}"
            return {
                "display": display,
                "user_id": user.id,
                "is_guest": False,
            }
    except Exception as exc:
        print(f"[chat] user lookup failed: {exc}")
    finally:
        db.close()

    viewer_id = get_session_id()
    anon_label = f"Fan-{viewer_id[:6]}" if viewer_id else "Fan"
    return {
        "display": anon_label,
        "viewer_id": viewer_id,
        "is_guest": True,
    }


def user_is_den_member(db, den: Den, user: User) -> bool:
    if not den or not user:
        return False
    if den.owner_id == user.id:
        return True
    return (
        db.execute(
            select(DenMembership).where(DenMembership.den_id == den.id, DenMembership.user_id == user.id)
        )
        .scalars()
        .first()
        is not None
    )


def ensure_den_membership(db, den: Den, user: User) -> None:
    if user_is_den_member(db, den, user):
        return
    membership = DenMembership(den_id=den.id, user_id=user.id)
    db.add(membership)
    db.commit()


def serialize_den_message(msg: DenChatMessage, user_lookup: dict[int, User]) -> dict:
    user = user_lookup.get(msg.user_id)
    username = user.username if user else f"User-{msg.user_id}"
    return {
        "id": msg.id,
        "den_id": msg.den_id,
        "text": msg.body,
        "ts": msg.created_at.timestamp() if msg.created_at else time.time(),
        "user": {
            "display": username,
            "user_id": msg.user_id,
            "is_guest": False,
        },
    }


@socketio.on("join")
def socket_join(payload):
    den_id_raw = (payload or {}).get("den_id")
    try:
        den_id = int(den_id_raw)
    except Exception:
        return

    db = SessionLocal()
    try:
        user = get_current_user(db)
        if not user:
            emit("chat_error", {"error": "login_required"})
            return

        den = db.get(Den, den_id)
        if den is None or not user_is_den_member(db, den, user):
            emit("chat_error", {"error": "not_allowed"})
            return

        join_room(_chat_room(str(den_id)))

        recent_db = (
            db.execute(
                select(DenChatMessage)
                .where(DenChatMessage.den_id == den.id)
                .order_by(DenChatMessage.created_at.desc())
                .limit(CHAT_MAX_MESSAGES)
            )
            .scalars()
            .all()
        )
        user_ids = {m.user_id for m in recent_db}
        users = (
            db.execute(select(User).where(User.id.in_(user_ids))).scalars().all() if user_ids else []
        )
        user_lookup = {u.id: u for u in users}
        recent = [serialize_den_message(m, user_lookup) for m in reversed(recent_db)]

        cached = chat_store.recent(str(den_id)) or []
        if cached:
            recent = cached

        emit("recent_messages", recent or [])
    finally:
        db.close()


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

    db = SessionLocal()
    try:
        user = get_current_user(db)
        if not user:
            emit("chat_error", {"error": "login_required"})
            return

        den = db.get(Den, den_id)
        if den is None or not user_is_den_member(db, den, user):
            emit("chat_error", {"error": "not_allowed"})
            return

        db_msg = DenChatMessage(den_id=den.id, user_id=user.id, body=text)
        db.add(db_msg)
        db.flush()
        db.refresh(db_msg)
        db.commit()

        message = serialize_den_message(db_msg, {user.id: user})
        chat_store.add(str(den_id), message)
        emit("chat_message", message, room=_chat_room(str(den_id)))
    finally:
        db.close()


# -----------------------------
# Utility helpers
# -----------------------------

def ensure_seed_categories():
    """Ensure DB categories match the seed hierarchy in data/categories.json."""
    seed_path = Path("data/categories.json")
    if not seed_path.exists():
        return

    try:
        seed = json.loads(seed_path.read_text(encoding="utf-8")).get("categories", [])
    except Exception as exc:
        print(f"[seed] Failed to read categories.json: {exc}")
        return

    # Map seed id -> seed entry and seed slug -> seed entry
    seed_by_id = {c["id"]: c for c in seed if "id" in c}
    seed_by_slug = {c["slug"]: c for c in seed if "slug" in c}

    def parent_slug(c):
        pid = c.get("parent_id")
        if pid is None:
            return None
        parent = seed_by_id.get(pid)
        return parent.get("slug") if parent else None

    remaining = list(seed)
    slug_to_db_obj: dict[str, Category] = {}

    db = SessionLocal()
    try:
        while remaining:
            progressed = False
            next_round = []
            for c in remaining:
                p_slug = parent_slug(c)
                if p_slug and p_slug not in seed_by_slug:
                    # parent missing from seed; skip this entry
                    continue
                if p_slug and p_slug not in slug_to_db_obj:
                    next_round.append(c)
                    continue

                parent_obj = slug_to_db_obj.get(p_slug)
                parent_id = parent_obj.id if parent_obj else None

                existing = (
                    db.execute(select(Category).where(Category.slug == c["slug"])).scalar_one_or_none()
                )
                if existing:
                    existing.name = c.get("name", existing.name)
                    existing.desc = c.get("desc", existing.desc)
                    existing.parent_id = parent_id
                    obj = existing
                else:
                    obj = Category(
                        name=c.get("name", "Category"),
                        slug=c.get("slug", "category"),
                        desc=c.get("desc", ""),
                        parent_id=parent_id,
                    )
                    db.add(obj)

                db.flush()
                slug_to_db_obj[c["slug"]] = obj
                progressed = True

            if not progressed:
                # Avoid infinite loop if there are inconsistent seed entries
                break
            remaining = next_round

        db.commit()
    except Exception as exc:
        db.rollback()
        print(f"[seed] Failed to ensure seed categories: {exc}")
    finally:
        db.close()


def slugify(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or "item"


def unique_den_slug(db, name: str) -> str:
    base = slugify(name) or "den"
    slug = base
    suffix = 1
    while db.execute(select(Den).where(Den.slug == slug)).scalar_one_or_none():
        slug = f"{base}-{suffix}"
        suffix += 1
    return slug


def serialize_den(den: Den) -> dict:
    return {
        "id": den.id,
        "name": den.name,
        "slug": den.slug,
        "is_public": bool(den.is_public),
        "invite_code": den.invite_code or "",
        "owner_id": den.owner_id,
        "game_id": den.game_id,
        "stream_url": den.stream_url,
        "created_at": den.created_at.isoformat() if den.created_at else "",
    }


def parse_expiration_choice(choice: str) -> Optional[timedelta]:
    mapping = {
        "1 day": timedelta(days=1),
        "3 days": timedelta(days=3),
        "1 week": timedelta(weeks=1),
        "1 month": timedelta(days=30),
    }
    return mapping.get(choice)


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


def serialize_category(cat: Category) -> dict:
    return {
        "id": cat.id,
        "name": cat.name,
        "slug": cat.slug,
        "desc": cat.desc,
        "parent_id": cat.parent_id,
        "created_at": cat.created_at.isoformat() if cat.created_at else "",
        "updated_at": cat.updated_at.isoformat() if cat.updated_at else "",
    }


def serialize_thread(thread: Thread) -> dict:
    return {
        "id": thread.id,
        "category_id": thread.category_id,
        "title": thread.title,
        "slug": thread.slug,
        "stream_link": thread.stream_link,
        "created_at": thread.created_at.isoformat() if thread.created_at else "",
        "expires_at": thread.expires_at.isoformat() if thread.expires_at else "",
        "expires_choice": thread.expires_choice,
        "tag": thread.tag or DEFAULT_TAG,
        "reply_count": thread.reply_count or 0,
        "clicks": thread.clicks or 0,
        "user": thread.user.username if thread.user else "Anonymous",
    }


def serialize_post(post: Post) -> dict:
    return {
        "id": post.id,
        "thread_id": post.thread_id,
        "parent_id": post.parent_id,
        "body": post.body,
        "created_at": post.created_at.isoformat() if post.created_at else "",
        "user": post.user.username if post.user else "Anonymous",
    }

def load_category_indexes(db):
    categories = db.execute(select(Category)).scalars().all()
    serialized = [serialize_category(c) for c in categories]
    cat_by_id = {c["id"]: c for c in serialized}
    children_by_parent = defaultdict(list)
    for c in serialized:
        children_by_parent[c["parent_id"]].append(c)
    return serialized, cat_by_id, children_by_parent

def load_category_indexes(db):
    categories = db.execute(select(Category)).scalars().all()
    serialized = [serialize_category(c) for c in categories]
    cat_by_id = {c["id"]: c for c in serialized}
    children_by_parent = defaultdict(list)
    for c in serialized:
        children_by_parent[c["parent_id"]].append(c)
    return serialized, cat_by_id, children_by_parent

def load_category_indexes(db):
    categories = db.execute(select(Category)).scalars().all()
    serialized = [serialize_category(c) for c in categories]
    cat_by_id = {c["id"]: c for c in serialized}
    children_by_parent = defaultdict(list)
    for c in serialized:
        children_by_parent[c["parent_id"]].append(c)
    return serialized, cat_by_id, children_by_parent

def load_category_indexes(db):
    categories = db.execute(select(Category)).scalars().all()
    serialized = [serialize_category(c) for c in categories]
    cat_by_id = {c["id"]: c for c in serialized}
    children_by_parent = defaultdict(list)
    for c in serialized:
        children_by_parent[c["parent_id"]].append(c)
    return serialized, cat_by_id, children_by_parent


def build_category_path(cat: dict, cat_by_id: dict[int, dict]) -> str:
    slugs = []
    current = cat
    while current:
        slug = current.get("slug")
        if slug:
            slugs.append(slug)
        parent_id = current.get("parent_id")
        current = cat_by_id.get(parent_id)
    return "/".join(reversed(slugs))


def get_descendant_ids(children_map, cat_id):
    ids = []
    for c in children_map.get(cat_id, []):
        ids.append(c["id"])
        ids.extend(get_descendant_ids(children_map, c["id"]))
    return ids


def get_category_by_path(db, path: str) -> Optional[dict]:
    path = (path or "").strip("/")
    if not path:
        return None
    parts = [p for p in path.split("/") if p]
    parent_id = None
    current = None
    for slug in parts:
        current = (
            db.execute(
                select(Category).where(Category.parent_id == parent_id, Category.slug == slug)
            )
            .scalars()
            .first()
        )
        if current is None:
            return None
        parent_id = current.id
    return serialize_category(current)


def get_category_children(children_map, cat):
    if cat is None:
        return children_map.get(None, [])
    return children_map.get(cat["id"], [])


def build_thread_counts(db, include_descendants=False, children=None):
    result = db.execute(select(Thread.category_id, func.count(Thread.id)).group_by(Thread.category_id)).all()
    base_counts = {cat_id: count for cat_id, count in result}
    if not include_descendants:
        return base_counts
    children = children or defaultdict(list)
    memo = {}

    def total(cid: int) -> int:
        if cid in memo:
            return memo[cid]
        subtotal = base_counts.get(cid, 0)
        for child in children.get(cid, []):
            subtotal += total(child["id"])
        memo[cid] = subtotal
        return subtotal

    for cid in base_counts.keys() | set(children.keys()):
        total(cid)
    return memo


def build_forum_stats(db, limit: int = 5) -> dict:
    categories_total = db.scalar(select(func.count(Category.id))) or 0
    threads_total = db.scalar(select(func.count(Thread.id))) or 0
    posts_total = db.scalar(select(func.count(Post.id))) or 0

    user_totals: dict[str, dict] = {}

    def ensure_user(name: str):
        key = name or "Anonymous"
        if key not in user_totals:
            user_totals[key] = {
                "user_id": None,
                "threads": 0,
                "posts": 0,
                "latest_post": None,
                "latest_post_dt": datetime.min.replace(tzinfo=timezone.utc),
                "latest_thread": None,
                "latest_thread_dt": datetime.min.replace(tzinfo=timezone.utc),
            }
        return user_totals[key]

    threads = (
        db.execute(select(Thread).options(joinedload(Thread.user)))
        .scalars()
        .all()
    )
    for t in threads:
        info = ensure_user(t.user.username if t.user else "Anonymous")
        if t.user:
            info["user_id"] = t.user.id
        info["threads"] += 1
        dt = t.created_at or datetime.min.replace(tzinfo=timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if dt > info["latest_thread_dt"]:
            info["latest_thread_dt"] = dt
            info["latest_thread"] = serialize_thread(t)

    posts = (
        db.execute(select(Post).options(joinedload(Post.user)))
        .scalars()
        .all()
    )
    for p in posts:
        info = ensure_user(p.user.username if p.user else "Anonymous")
        if p.user:
            info["user_id"] = p.user.id
        info["posts"] += 1
        dt = p.created_at or datetime.min.replace(tzinfo=timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if dt > info["latest_post_dt"]:
            info["latest_post_dt"] = dt
            info["latest_post"] = serialize_post(p)

    leaderboard = []
    for username, details in user_totals.items():
        total = details["threads"] + details["posts"]
        link = None
        if details.get("user_id"):
            link = f"/users/{quote(username)}"
        leaderboard.append(
            {
                "username": username,
                "threads": details["threads"],
                "posts": details["posts"],
                "total": total,
                "link": link,
            }
        )

    leaderboard = sorted(
        leaderboard,
        key=lambda item: (-item["total"], -item["posts"], item["username"].lower()),
    )[:limit]

    return {
        "categories": categories_total,
        "threads": threads_total,
        "posts": posts_total,
        "leaderboard": leaderboard,
    }


def search_items(db, query: str, thread_counts: dict[int, int], cat_lookup: dict[int, dict], categories: list[dict]) -> dict:
    q = (query or "").strip().lower()
    if not q:
        return {"categories": [], "threads": []}

    cat_results = []
    for c in categories:
        blob = f"{c.get('name','')} {c.get('desc','')}".lower()
        if q in blob:
            path = build_category_path(c, cat_lookup)
            url = "/forum" + (f"/{path}" if path else "")
            cat_results.append(
                {
                    "name": c.get("name", ""),
                    "desc": c.get("desc", ""),
                    "url": url,
                    "threads": thread_counts.get(c.get("id"), 0),
                }
            )

    thread_results = (
        db.execute(
            select(Thread, Category)
            .join(Category, Thread.category_id == Category.id)
            .where(
                or_(
                    func.lower(Thread.title).like(f"%{q}%"),
                    func.lower(Thread.stream_link).like(f"%{q}%"),
                )
            )
            .order_by(Thread.clicks.desc(), Thread.created_at.desc())
            .limit(8)
        )
        .all()
    )

    out_threads = []
    for t, cat in thread_results:
        cat_path = build_category_path(serialize_category(cat), cat_lookup) if cat else ""
        out_threads.append(
            {
                "title": t.title,
                "clicks": t.clicks or 0,
                "created_at": t.created_at.isoformat() if t.created_at else "",
                "thread_url": f"/thread/{t.id}",
                "category_url": "/forum" + (f"/{cat_path}" if cat_path else ""),
                "category_name": cat.name if cat else "Unknown",
            }
        )

    cat_results = sorted(
        cat_results, key=lambda c: (-c.get("threads", 0), c.get("name", "").lower())
    )[:8]
    return {"categories": cat_results, "threads": out_threads}


def normalize_tag(tag: str) -> str:
    cleaned = slugify(tag)
    return cleaned or DEFAULT_TAG


def group_threads_by_tag(threads: list[dict]) -> tuple[dict[str, list[dict]], list[str]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for t in threads:
        tag = normalize_tag(t.get("tag") or DEFAULT_TAG)
        grouped[tag].append(t)

    # Sort threads newest first within each tag
    def created_key(td):
        return td.get("created_at") or ""

    for tag in grouped:
        grouped[tag].sort(key=created_key, reverse=True)

    tag_list = sorted(grouped.keys())
    if DEFAULT_TAG in tag_list:
        tag_list = [DEFAULT_TAG] + [t for t in tag_list if t != DEFAULT_TAG]
    return grouped, tag_list


def build_post_tree(db, thread_id: int) -> list[dict]:
    posts = (
        db.execute(
            select(Post)
            .where(Post.thread_id == thread_id)
            .options(joinedload(Post.user))
            .order_by(Post.created_at.asc(), Post.id.asc())
        )
        .scalars()
        .all()
    )
    by_parent = defaultdict(list)
    for p in posts:
        by_parent[p.parent_id].append(serialize_post(p))

    def recurse(parent_id):
        children = [c for c in by_parent.get(parent_id, []) if c.get("thread_id") == thread_id]
        result = []
        for child in sorted(children, key=lambda p: p.get("created_at", "")):
            node = dict(child)
            node["replies"] = recurse(child.get("id"))
            result.append(node)
        return result

    return recurse(None)


def append_post(db, thread_id: int, body: str, parent_id: int | None, user: User):
    thread = db.get(Thread, thread_id)
    if thread is None:
        abort(404)
    if parent_id is not None:
        parent_post = db.get(Post, parent_id)
        if parent_post is None or parent_post.thread_id != thread_id:
            abort(400)

    now = datetime.now(timezone.utc)
    new_post = Post(
        thread_id=thread_id,
        parent_id=parent_id,
        body=body,
        user_id=user.id,
        created_at=now,
    )
    db.add(new_post)
    thread.reply_count = (thread.reply_count or 0) + 1
    db.commit()
    db.refresh(new_post)
    return serialize_post(new_post)


def append_category(db, parent_path_str, name, desc):
    parent_path_str = (parent_path_str or "").strip("/")
    parent_cat = get_category_by_path(db, parent_path_str) if parent_path_str else None
    parent_id = parent_cat["id"] if parent_cat else None
    slug = slugify(name)

    existing = db.execute(
        select(Category).where(Category.parent_id == parent_id, Category.slug == slug)
    ).scalar_one_or_none()
    if existing:
        abort(409, description="Category already exists under this parent")

    new_category = Category(
        name=name,
        slug=slug,
        desc=desc,
        parent_id=parent_id,
    )
    db.add(new_category)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        abort(409, description="Category already exists under this parent")
    db.refresh(new_category)
    return serialize_category(new_category)


def append_thread(
    db,
    category_path_str: str,
    title: str,
    stream_link: str,
    expires_choice: str,
    user: User,
    tag: str | None = None,
):
    category_path_str = (category_path_str or "").strip("/")
    cat = get_category_by_path(db, category_path_str)
    if cat is None:
        abort(400)
    expires_delta = parse_expiration_choice(expires_choice)
    if expires_delta is None:
        abort(400)
    normalized_tag = normalize_tag(tag or DEFAULT_TAG)
    if len(normalized_tag) > 64:
        abort(400)

    now = datetime.now(timezone.utc)
    expires_at = now + expires_delta

    new_thread = Thread(
        category_id=cat["id"],
        user_id=user.id,
        title=title,
        slug=slugify(title),
        stream_link=stream_link,
        created_at=now,
        expires_at=expires_at,
        expires_choice=expires_choice,
        tag=normalized_tag,
        reply_count=0,
        clicks=0,
    )
    db.add(new_thread)
    db.commit()
    db.refresh(new_thread)
    db.refresh(user)
    return serialize_thread(new_thread)


def get_top_threads(db, limit: int = 10) -> list[dict]:
    threads = (
        db.execute(
            select(Thread)
            .options(joinedload(Thread.user))
            .order_by(Thread.clicks.desc(), Thread.created_at.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return [serialize_thread(t) for t in threads]


def get_top_threads_for_ids(db, cat_ids: list[int], limit: int = 10) -> list[dict]:
    if not cat_ids:
        return []
    threads = (
        db.execute(
            select(Thread)
            .where(Thread.category_id.in_(cat_ids))
            .options(joinedload(Thread.user))
            .order_by(Thread.clicks.desc(), Thread.created_at.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return [serialize_thread(t) for t in threads]


def get_threads_for_category(db, cat: dict) -> list[dict]:
    if cat is None:
        return []
    threads = (
        db.execute(
            select(Thread)
            .where(Thread.category_id == cat["id"])
            .options(joinedload(Thread.user))
            .order_by(Thread.created_at.desc())
        )
        .scalars()
        .all()
    )
    return [serialize_thread(t) for t in threads]


def user_threads_and_posts(db, user: User):
    threads = (
        db.execute(
            select(Thread)
            .where(Thread.user_id == user.id)
            .order_by(Thread.created_at.desc())
        )
        .scalars()
        .all()
    )
    posts = (
        db.execute(
            select(Post)
            .where(Post.user_id == user.id)
            .order_by(Post.created_at.desc())
        )
        .scalars()
        .all()
    )
    return [serialize_thread(t) for t in threads], [serialize_post(p) for p in posts]


def delete_thread_owned(db, thread_id: int, user: User):
    thread = db.get(Thread, thread_id)
    if thread is None:
        abort(404)
    if thread.user_id != user.id:
        abort(403)
    db.delete(thread)
    db.commit()


def delete_post_owned(db, post_id: int, user: User):
    post = db.get(Post, post_id)
    if post is None:
        abort(404)
    if post.user_id != user.id:
        abort(403)
    thread = db.get(Thread, post.thread_id)
    db.delete(post)
    db.commit()
    if thread:
        thread.reply_count = max((thread.reply_count or 1) - 1, 0)
        db.commit()

def detect_stream_embed(url: str, parent_host: str) -> dict:
    from urllib.parse import urlparse, parse_qs

    if not url:
        return {"type": "link", "src": url}

    u = urlparse(url)
    host = (u.netloc or "").lower()
    path = (u.path or "").lower()

    if "x.com" in host or "twitter.com" in host:
        post_url = url
        html = (
            '<blockquote class="twitter-tweet">'
            f'<p lang="en" dir="ltr"><a href="{post_url}">View on X</a></p>'
            "</blockquote>"
        )
        return {"type": "x", "html": Markup(html), "title": "X post"}

    if path.endswith(".m3u8"):
        return {"type": "m3u8", "src": url}
    if path.endswith((".mp4", ".webm", ".ogg")):
        return {"type": "video", "src": url}

    if "youtube.com" in host or "youtu.be" in host:
        video_id = None
        if "youtu.be" in host:
            video_id = u.path.strip("/")
        else:
            qs = parse_qs(u.query)
            video_id = qs.get("v", [None])[0]
        if video_id:
            return {"type": "iframe", "src": f"https://www.youtube.com/embed/{video_id}", "title": "YouTube"}

    if "twitch.tv" in host:
        slug = u.path.strip("/")
        if slug:
            return {"type": "iframe", "src": f"https://player.twitch.tv/?channel={slug}&parent={parent_host}", "title": "Twitch"}

    return {"type": "iframe", "src": url, "title": "Stream"}

def render_forum_page(
    category_path: str,
    categories: list,
    threads: list,
    parent_path: str,
    depth: int,
    allow_posting: bool,
    show_categories_section: bool,
    thread_title: str,
    thread_subtext: str,
    thread_counts: dict[int, int],
    search_query: str,
    search_results: dict[str, list[dict]],
    cat_lookup: dict[int, dict],
    current_user: str | None = None,
):
    grouped, tags = group_threads_by_tag(threads) if allow_posting else ({}, [])

    prefill_stream = request.args.get("prefill_stream", "")
    prefill_title = request.args.get("prefill_title", "")
    prefill_exp = request.args.get("prefill_exp", "")
    prefill_tag = request.args.get("prefill_tag", DEFAULT_TAG)
    open_thread_form = request.args.get("open_thread_form", "") == "1"
    forum_stats = build_forum_stats(get_db())
    show_post_panel = show_categories_section
    postable_categories = []
    if show_post_panel:
        for cat in cat_lookup.values():
            if cat.get('parent_id') is None:
                continue
            path = build_category_path(cat, cat_lookup)
            if not path:
                continue
            postable_categories.append(
                {
                    "path": path,
                    "name": cat.get("name", path),
                    "label": f"{cat.get('name', path)} ({path})",
                }
            )
        postable_categories.sort(key=lambda c: c["label"].lower())

    return render_template(
        "index.html",
        categories=categories,
        current_path=category_path,
        parent_path=parent_path,
        breadcrumbs=build_breadcrumbs(category_path, cat_lookup),
        threads_grouped=grouped,
        tag_buckets=tags,
        prefill_stream=prefill_stream,
        open_thread_form=open_thread_form and allow_posting,
        show_top_threads=not allow_posting,
        allow_posting=allow_posting,
        show_categories_section=show_categories_section,
        thread_title=thread_title,
        thread_subtext=thread_subtext,
        threads_flat=threads if not allow_posting else [],
        thread_counts=thread_counts,
        search_query=search_query,
        search_results=search_results,
        cat_lookup=cat_lookup,
        current_user=current_user,
        prefill_title=prefill_title,
        prefill_exp=prefill_exp,
        prefill_tag=prefill_tag,
        forum_stats=forum_stats,
        exp_choices=EXP_CHOICES,
        default_tag=DEFAULT_TAG,
        postable_categories=postable_categories,
        default_category_path=category_path,
        show_post_panel=show_post_panel,
    )


def build_breadcrumbs(current_path: str, cat_lookup: dict[int, dict]):
    crumbs = [
        {"name": "Home", "url": "/"},
        {"name": "Forum", "url": "/forum"},
    ]
    current_path = (current_path or "").strip("/")
    if not current_path:
        return crumbs

    parts = [p for p in current_path.split("/") if p]
    parent_id = None
    path_so_far = []

    for slug in parts:
        cat = next(
            (c for c in cat_lookup.values() if c["slug"] == slug and c["parent_id"] == parent_id),
            None,
        )
        if cat is None:
            break
        path_so_far.append(slug)
        crumbs.append({"name": cat.get("name", slug), "url": "/forum/" + "/".join(path_so_far)})
        parent_id = cat["id"]

    return crumbs


# -----------------------------
# Background cleanup worker
# -----------------------------


def _start_cleanup_worker():
    global _cleanup_thread_started
    if _cleanup_thread_started:
        return

    def _loop():
        while True:
            try:
                deleted = cleanup_expired_threads()
                if deleted:
                    print(f"[cleanup] Removed {deleted} expired threads")
            except Exception as exc:
                print(f"[cleanup] Error during cleanup: {exc}")
            time.sleep(CLEANUP_INTERVAL_SECONDS)

    t = threading.Thread(target=_loop, daemon=True, name="expired-cleanup")
    t.start()
    _cleanup_thread_started = True


# -----------------------------
# Routes
# -----------------------------


@app.before_request
def attach_user():
    db = get_db()
    g.current_user = get_current_user(db)
    generate_csrf_token()
    _start_cleanup_worker()


@app.route("/forum")
def forum_redirect():
    """Forum has been retired; redirect to the homepage."""
    return redirect("/")


@app.route("/forum/<path:category_path>")
def forum_category_redirect(category_path):
    """Forum has been retired; redirect to the homepage."""
    return redirect("/")


@app.route("/add_thread", methods=["POST"])
def add_thread():
    abort(410)


@app.route("/api/threads", methods=["POST"])
def api_create_thread():
    return jsonify({"error": "forum_removed"}), 410


@app.route("/dens/create", methods=["POST"])
def create_den():
    require_csrf()
    db = get_db()
    user = require_login("/")
    if not isinstance(user, User):
        return user

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

    slug = unique_den_slug(db, name)
    invite_code = None if is_public else secrets.token_urlsafe(6)
    den = Den(
        name=name,
        slug=slug,
        owner_id=user.id,
        is_public=is_public,
        invite_code=invite_code,
        stream_url=stream_url,
        game_id=game_id,
    )
    db.add(den)
    db.flush()
    db.refresh(den)
    ensure_den_membership(db, den, user)
    db.commit()

    return redirect(url_for("den_page", slug=slug))


def _den_or_404(db, slug: str) -> Den:
    den = (
        db.execute(select(Den).where(Den.slug == slug))
        .scalars()
        .first()
    )
    if den is None:
        abort(404)
    return den


@app.route("/dens/<slug>")
def den_page(slug):
    db = get_db()
    user = require_login(url_for("den_page", slug=slug))
    if not isinstance(user, User):
        return user

    den = _den_or_404(db, slug)
    invite_token = (request.args.get("invite") or "").strip()

    is_member = user_is_den_member(db, den, user)
    if not is_member and den.is_public:
        ensure_den_membership(db, den, user)
        is_member = True
    if not is_member and invite_token and invite_token == (den.invite_code or ""):
        ensure_den_membership(db, den, user)
        is_member = True

    if not is_member and not den.is_public:
        share_url = request.url_root.rstrip("/") + url_for("den_page", slug=den.slug)
        invite_url = share_url
        if den.invite_code:
            invite_url = f"{share_url}?invite={den.invite_code}"
        return render_template(
            "den.html",
            den=serialize_den(den),
            invite_required=True,
            access_denied=True,
            current_user=user.username,
            share_url=share_url,
            invite_url=invite_url,
            chat_max_length=CHAT_MAX_LENGTH,
        )

    messages = (
        db.execute(
            select(DenChatMessage)
            .where(DenChatMessage.den_id == den.id)
            .order_by(DenChatMessage.created_at.desc())
            .limit(CHAT_MAX_MESSAGES)
        )
        .scalars()
        .all()
    )
    user_ids = {m.user_id for m in messages}
    users = db.execute(select(User).where(User.id.in_(user_ids))).scalars().all() if user_ids else []
    user_lookup = {u.id: u for u in users}
    serialized_messages = [serialize_den_message(m, user_lookup) for m in reversed(messages)]

    game = find_game_by_id(den.game_id) if den.game_id else None
    share_url = request.url_root.rstrip("/") + url_for("den_page", slug=den.slug)
    invite_url = share_url
    if den.invite_code:
        invite_url = f"{share_url}?invite={den.invite_code}"
    return render_template(
        "den.html",
        den=serialize_den(den),
        messages=serialized_messages,
        current_user=user.username,
        game=game,
        share_url=share_url,
        invite_url=invite_url,
        chat_max_length=CHAT_MAX_LENGTH,
    )


@app.route("/dens/<slug>/join", methods=["POST"])
def join_den(slug):
    require_csrf()
    db = get_db()
    user = require_login(url_for("den_page", slug=slug))
    if not isinstance(user, User):
        return user

    den = _den_or_404(db, slug)
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
                current_user=user.username,
                share_url=share_url,
                invite_url=invite_url,
                chat_max_length=CHAT_MAX_LENGTH,
            )

    ensure_den_membership(db, den, user)
    return redirect(url_for("den_page", slug=slug))


@app.route("/account")
def account():
    db = get_db()
    user = require_login("/account")
    if not isinstance(user, User):
        return user

    owned_dens = (
        db.execute(
            select(Den).where(Den.owner_id == user.id).order_by(Den.created_at.desc())
        )
        .scalars()
        .all()
    )
    joined_dens = (
        db.execute(
            select(Den)
            .join(DenMembership, DenMembership.den_id == Den.id)
            .where(DenMembership.user_id == user.id, Den.owner_id != user.id)
            .order_by(Den.created_at.desc())
        )
        .scalars()
        .all()
    )
    return render_template(
        "account.html",
        current_user=user.username,
        owned_dens=owned_dens,
        joined_dens=joined_dens,
    )


@app.route("/users/<username>")
def user_profile(username):
    abort(410)


@app.route("/account/delete_thread/<int:thread_id>", methods=["POST"])
def account_delete_thread(thread_id):
    abort(410)


@app.route("/account/edit_thread/<int:thread_id>", methods=["POST"])
def account_edit_thread(thread_id: int):
    abort(410)


@app.route("/account/delete_post/<int:post_id>", methods=["POST"])
def account_delete_post(post_id):
    abort(410)


@app.route("/login", methods=["GET", "POST"])
def login():
    db = get_db()
    next_url = request.args.get("next") or request.form.get("next") or "/"
    error = None
    if request.method == "POST":
        require_csrf()
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        user = db.execute(select(User).where(User.username == username)).scalar_one_or_none()
        if user and user.is_banned:
            error = "This account has been banned."
        elif user and check_password_hash(user.password_hash, password):
            session["user_id"] = user.id
            session["username"] = user.username
            return redirect(next_url)
        error = "Invalid credentials"
    return render_template("login.html", next_url=next_url, error=error)


@app.route("/logout")
def logout():
    session.pop("user_id", None)
    session.pop("username", None)
    return redirect("/")


@app.route("/signup", methods=["GET", "POST"])
def signup():
    db = get_db()
    error = None
    next_url = request.args.get("next") or "/"
    if request.method == "POST":
        require_csrf()
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        if not username or not password:
            error = "Username and password required"
        else:
            hashed = generate_password_hash(password, method="pbkdf2:sha256")
            new_user = User(username=username, password_hash=hashed)
            db.add(new_user)
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                error = "Username already exists"
            else:
                session["user_id"] = new_user.id
                session["username"] = new_user.username
                return redirect(next_url)
    return render_template("signup.html", error=error, next_url=next_url)

@app.before_request
def admin_gate():
    """Optional gate to hide admin endpoints behind a shared secret."""
    if not ADMIN_GATE:
        # No gate configured; do not block admin routes.
        return
    if request.path.startswith('/admin'):
        gate_param = request.args.get('gate')
        if gate_param:
            session['admin_gate_token'] = gate_param
        gate_token = session.get('admin_gate_token')
        if not gate_token or not hmac.compare_digest(str(gate_token), str(ADMIN_GATE)):
            abort(404)

@app.route("/admin", methods=["GET", "POST"])
def admin_panel():
    db = get_db()
    login_error = None
    message = request.args.get("message", "")
    user_lookup = None
    thread_detail = None

    if not ADMIN_TOKEN:
        login_error = "Admin token is not configured on the server."

    if not is_admin_authed():
        if request.method == "POST":
            require_csrf()
            token = (request.form.get("admin_token") or "").strip()
            if login_error:
                pass
            elif token and hmac.compare_digest(token, ADMIN_TOKEN):
                session["admin_authed"] = True
                return redirect("/admin")
            else:
                login_error = "Invalid admin token."
        return render_template(
            "admin.html",
            admin_authed=False,
            login_error=login_error,
            message=message,
            csrf_token=generate_csrf_token(),
        )

    categories, cat_lookup, _ = load_category_indexes(db)
    categories = sorted(categories, key=lambda c: build_category_path(c, cat_lookup))
    category_paths = {c["id"]: build_category_path(c, cat_lookup) for c in categories}

    thread_id_param = request.args.get("thread_id")
    if thread_id_param:
        try:
            tid = int(thread_id_param)
        except ValueError:
            tid = None
        if tid:
            thread_obj = (
                db.execute(
                    select(Thread)
                    .where(Thread.id == tid)
                    .options(joinedload(Thread.user), joinedload(Thread.category))
                )
                .scalars()
                .first()
            )
            if thread_obj:
                thread_detail = serialize_thread(thread_obj)
                if thread_obj.category:
                    thread_detail["category_path"] = build_category_path(
                        serialize_category(thread_obj.category), cat_lookup
                    )
                thread_detail["user"] = thread_obj.user.username if thread_obj.user else "Anonymous"

    user_query = (request.args.get("user_search") or "").strip()
    if user_query:
        filters = [User.username == user_query]
        if user_query.isdigit():
            filters.append(User.id == int(user_query))
        user_lookup = (
            db.execute(
                select(User).where(or_(*filters))
            )
            .scalars()
            .first()
        )

    return render_template(
        "admin.html",
        admin_authed=True,
        categories=categories,
        cat_lookup=cat_lookup,
        category_paths=category_paths,
        thread_detail=thread_detail,
        user_lookup=user_lookup,
        message=message,
        exp_choices=EXP_CHOICES,
        default_tag=DEFAULT_TAG,
        csrf_token=generate_csrf_token(),
    )


@app.route("/admin/logout")
def admin_logout():
    session.pop("admin_authed", None)
    return redirect("/admin")


@app.route("/admin/categories/add", methods=["POST"])
def admin_add_category_form():
    require_csrf()
    require_admin()
    db = get_db()

    parent_path = (request.form.get("parent_path") or "").strip("/")
    name = (request.form.get("name") or "").strip()
    desc = (request.form.get("desc") or "").strip()

    if not name or len(name) > 120 or not desc or len(desc) > 2000:
        abort(400)

    append_category(db, parent_path, name, desc)
    return redirect("/admin?message=Category%20added")


@app.route("/admin/categories/<int:cat_id>/update", methods=["POST"])
def admin_update_category(cat_id: int):
    require_csrf()
    require_admin()
    db = get_db()

    name = (request.form.get("name") or "").strip()
    desc = (request.form.get("desc") or "").strip()
    if not name or len(name) > 120 or not desc or len(desc) > 2000:
        abort(400)

    cat = db.get(Category, cat_id)
    if cat is None:
        abort(404)

    cat.name = name
    cat.desc = desc
    db.commit()
    return redirect("/admin?message=Category%20updated")


@app.route("/admin/users/ban", methods=["POST"])
def admin_ban_user():
    require_csrf()
    require_admin()
    db = get_db()
    user_id_raw = request.form.get("user_id")
    action = (request.form.get("action") or "ban").strip().lower()
    try:
        user_id = int(user_id_raw)
    except Exception:
        abort(400)

    user = db.get(User, user_id)
    if user is None:
        abort(404)

    user.is_banned = action == "ban"
    db.commit()

    qs = urlencode({"user_search": user.username, "message": "User updated"})
    return redirect(f"/admin?{qs}")


@app.route("/admin/threads/<int:thread_id>/update", methods=["POST"])
def admin_update_thread(thread_id: int):
    require_csrf()
    require_admin()
    db = get_db()

    title = (request.form.get("title") or "").strip()
    stream_link = (request.form.get("stream_link") or "").strip()
    tag_raw = (request.form.get("tag") or DEFAULT_TAG).strip()

    if not title or len(title) > 200 or not stream_link or len(stream_link) > 1024:
        abort(400)
    if len(tag_raw) > 64:
        abort(400)

    thread = db.get(Thread, thread_id)
    if thread is None:
        abort(404)

    new_slug = slugify(title)
    existing = (
        db.execute(
            select(Thread).where(
                Thread.category_id == thread.category_id,
                Thread.slug == new_slug,
                Thread.id != thread_id,
            )
        )
        .scalars()
        .first()
    )
    if existing:
        abort(409)

    thread.title = title
    thread.slug = new_slug
    thread.stream_link = stream_link
    thread.tag = normalize_tag(tag_raw)
    db.commit()

    qs = urlencode({"thread_id": thread_id, "message": "Thread updated"})
    return redirect(f"/admin?{qs}")


@app.route("/api/admin/categories", methods=["POST"])
def api_add_category():
    db = get_db()
    token = request.headers.get("X-Admin-Token") or request.args.get("token")
    if not token or (ADMIN_TOKEN and token != ADMIN_TOKEN):
        abort(403)

    data = request.get_json(silent=True) or {}
    parent_path = (data.get("parent_path") or "").strip("/")
    name = (data.get("name") or "").strip()
    desc = (data.get("desc") or "").strip()

    if not name or len(name) > 12:
        abort(400)
    if not desc:
        abort(400)

    new_cat = append_category(db, parent_path, name, desc)
    return jsonify({"category": new_cat}), 201


@app.route("/embed_stream", methods=["GET", "POST"])
def embed_stream():
    return_to = request.args.get("return_to", "/forum")
    category_path = request.args.get("category_path", "")
    prefill_title = request.args.get("prefill_title", "")
    prefill_exp = request.args.get("prefill_exp", "")
    prefill_stream = request.args.get("prefill_stream", "")

    source = request.values.get("source", "twitch")
    user_input = request.values.get("user_input", "")

    candidates = []
    if request.method == "POST":
        require_csrf()
        candidates = embed_streams.get_embed_candidates(source, user_input, TWITCH_PARENT_HOST)

    def ensure_param(url: str, key: str, value: str) -> str:
        if f"{key}=" in url:
            return url
        sep = "&" if "?" in url else "?"
        return url + sep + urlencode({key: value})

    return_to = ensure_param(return_to, "open_thread_form", "1")
    if prefill_title:
        return_to = ensure_param(return_to, "prefill_title", prefill_title)
    if prefill_exp:
        return_to = ensure_param(return_to, "prefill_exp", prefill_exp)
    if prefill_stream:
        return_to = ensure_param(return_to, "prefill_stream", prefill_stream)

    return render_template(
        "embed_stream.html",
        return_to=return_to,
        category_path=category_path,
        source=source,
        user_input=user_input,
        candidates=candidates,
        prefill_title=prefill_title,
        prefill_exp=prefill_exp,
        prefill_stream=prefill_stream,
    )


@app.route("/choose_stream", methods=["POST"])
def choose_stream():
    return redirect("/")


@app.route("/thread/<int:thread_id>")
def thread_page(thread_id):
    abort(410)


@app.route("/thread/<int:thread_id>/reply", methods=["POST"])
def reply_thread(thread_id):
    abort(410)


if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
