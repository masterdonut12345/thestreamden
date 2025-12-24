from __future__ import annotations

import hmac
import os
import re
import secrets
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

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
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from werkzeug.security import check_password_hash, generate_password_hash

import embed_streams
from cleanup_expired import cleanup_expired_threads
from db_models import Category, Post, SessionLocal, Thread, User, init_db
from pathlib import Path

app = Flask(__name__)

EXP_CHOICES = ["1 day", "3 days", "1 week", "1 month"]
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")
TWITCH_PARENT_HOST = "thestreamden.com"
CLEANUP_INTERVAL_SECONDS = int(os.environ.get("CLEANUP_INTERVAL_SECONDS", "3600"))
_cleanup_thread_started = False

app.secret_key = os.environ.get("APP_SECRET", "dev-secret-key")
app.config.update(
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)

# Ensure database tables exist on startup
init_db()
ensure_seed_categories()


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


@app.context_processor
def inject_csrf():
    return {"csrf_token": generate_csrf_token()}


def get_current_user(db):
    uid = session.get("user_id")
    if not uid:
        return None
    return db.get(User, uid)


def require_login(next_url: str = "/forum"):
    db = get_db()
    user = get_current_user(db)
    if not user:
        return redirect(f"/login?next={next_url}")
    return user


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


def parse_expiration_choice(choice: str) -> Optional[timedelta]:
    mapping = {
        "1 day": timedelta(days=1),
        "3 days": timedelta(days=3),
        "1 week": timedelta(weeks=1),
        "1 month": timedelta(days=30),
    }
    return mapping.get(choice)


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
        if details["latest_post"]:
            lp = details["latest_post"]
            link = f"/thread/{lp.get('thread_id')}#post-{lp.get('id')}"
        elif details["latest_thread"]:
            link = f"/thread/{details['latest_thread'].get('id')}"
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


def group_threads_by_choice(threads: list[dict]) -> dict[str, list[dict]]:
    out = {k: [] for k in EXP_CHOICES}
    now = datetime.now(timezone.utc)

    for t in threads:
        choice = t.get("expires_choice")
        if choice in out:
            out[choice].append(t)
            continue

        exp_s = t.get("expires_at") or ""
        try:
            exp_dt = datetime.fromisoformat(exp_s)
            if exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=timezone.utc)
        except Exception:
            out["1 month"].append(t)
            continue

        delta = exp_dt - now
        secs = delta.total_seconds()
        if secs <= 0:
            out["1 day"].append(t)
        elif secs <= 24 * 3600:
            out["1 day"].append(t)
        elif secs <= 3 * 24 * 3600:
            out["3 days"].append(t)
        elif secs <= 7 * 24 * 3600:
            out["1 week"].append(t)
        else:
            out["1 month"].append(t)

    def exp_key(td):
        return td.get("expires_at") or ""

    for k in out:
        out[k].sort(key=exp_key)
    return out


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
    new_category = Category(
        name=name,
        slug=slugify(name),
        desc=desc,
        parent_id=parent_id,
    )
    db.add(new_category)
    db.commit()
    db.refresh(new_category)
    return serialize_category(new_category)


def append_thread(db, category_path_str: str, title: str, stream_link: str, expires_choice: str, user: User):
    category_path_str = (category_path_str or "").strip("/")
    cat = get_category_by_path(db, category_path_str)
    if cat is None:
        abort(400)
    expires_delta = parse_expiration_choice(expires_choice)
    if expires_delta is None:
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
            return {
                "type": "iframe",
                "src": f"https://www.youtube.com/embed/{video_id}",
                "title": "YouTube",
            }

    if "twitch.tv" in host:
        slug = u.path.strip("/")
        if slug:
            return {
                "type": "iframe",
                "src": f"https://player.twitch.tv/?channel={slug}&parent={parent_host}",
                "title": "Twitch",
            }

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
    grouped = group_threads_by_choice(threads) if allow_posting else {}

    prefill_stream = request.args.get("prefill_stream", "")
    prefill_title = request.args.get("prefill_title", "")
    prefill_exp = request.args.get("prefill_exp", "")
    open_thread_form = request.args.get("open_thread_form", "") == "1"
    forum_stats = build_forum_stats(get_db())

    return render_template(
        "index.html",
        categories=categories,
        current_path=category_path,
        parent_path=parent_path,
        breadcrumbs=build_breadcrumbs(category_path, cat_lookup),
        threads_grouped=grouped,
        exp_choices=EXP_CHOICES,
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
        forum_stats=forum_stats,
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


@app.route("/")
def home():
    return redirect("/forum")


@app.route("/forum")
def index():
    db = get_db()
    categories, cat_lookup, children = load_category_indexes(db)
    top_ids = get_descendant_ids(children, None)
    threads = get_top_threads_for_ids(db, top_ids, 10)
    thread_counts = build_thread_counts(db, include_descendants=True, children=children)
    search_query = request.args.get("search", "")
    search_results = (
        search_items(db, search_query, thread_counts, cat_lookup, categories)
        if search_query
        else {"categories": [], "threads": []}
    )
    return render_forum_page(
        category_path="",
        categories=categories,
        threads=threads,
        parent_path="",
        depth=0,
        allow_posting=False,
        show_categories_section=True,
        thread_title="Top Threads",
        thread_subtext="Most-clicked streams across all categories.",
        thread_counts=thread_counts,
        search_query=search_query,
        search_results=search_results,
        cat_lookup=cat_lookup,
        current_user=g.current_user.username if g.current_user else None,
    )


@app.route("/forum/<path:category_path>")
def forum(category_path):
    db = get_db()
    category_path = (category_path or "").strip("/")
    parts = [p for p in category_path.split("/") if p]
    parent_path = "/".join(parts[:-1])
    depth = len(parts)

    temp_cat = get_category_by_path(db, category_path)
    if temp_cat is None:
        abort(404)

    categories, cat_lookup, children = load_category_indexes(db)
    thread_counts = build_thread_counts(db, include_descendants=True, children=children)
    search_query = request.args.get("search", "")
    search_results = (
        search_items(db, search_query, thread_counts, cat_lookup, categories)
        if search_query
        else {"categories": [], "threads": []}
    )

    if depth == 1:
        ids = get_descendant_ids(children, temp_cat["id"])
        threads = get_top_threads_for_ids(db, ids, 10)
        allow_posting = False
        show_categories_section = True
        thread_title = "Top Threads"
        thread_subtext = f"Most-clicked streams inside {temp_cat.get('name', 'this category')}."
    else:
        threads = get_threads_for_category(db, temp_cat)
        allow_posting = True
        show_categories_section = False
        thread_title = "Threads"
        thread_subtext = "Streams in this category."

    return render_forum_page(
        category_path=category_path,
        categories=get_category_children(children, temp_cat),
        threads=threads,
        parent_path=parent_path,
        depth=depth,
        allow_posting=allow_posting,
        show_categories_section=show_categories_section,
        thread_title=thread_title,
        thread_subtext=thread_subtext,
        thread_counts=thread_counts,
        search_query=search_query,
        search_results=search_results,
        cat_lookup=cat_lookup,
        current_user=g.current_user.username if g.current_user else None,
    )


@app.route("/add_thread", methods=["POST"])
def add_thread():
    require_csrf()
    db = get_db()
    user = require_login("/forum")
    if not isinstance(user, User):
        return user

    category_path = (request.form.get("category_path") or "").strip("/")
    title = (request.form.get("thread-title") or "").strip()
    stream_link = (request.form.get("thread-stream-link") or "").strip()
    expires_choice = (request.form.get("thread-expiration") or "").strip()

    if not category_path:
        abort(400)
    if not title or len(title) > 80:
        abort(400)
    if not stream_link or len(stream_link) > 500:
        abort(400)
    if expires_choice not in EXP_CHOICES:
        abort(400)

    append_thread(db, category_path, title, stream_link, expires_choice, user)
    return redirect(f"/forum/{category_path}")


@app.route("/account")
def account():
    db = get_db()
    user = require_login("/account")
    if not isinstance(user, User):
        return user

    threads, posts = user_threads_and_posts(db, user)
    _, cat_lookup, _ = load_category_indexes(db)
    all_threads = (
        db.execute(select(Thread).options(joinedload(Thread.user)))
        .scalars()
        .all()
    )
    thread_lookup = {t.id: serialize_thread(t) for t in all_threads}
    return render_template(
        "account.html",
        current_user=user.username,
        threads=threads,
        posts=posts,
        thread_lookup=thread_lookup,
        cat_lookup=cat_lookup,
        build_category_path=build_category_path,
    )


@app.route("/account/delete_thread/<int:thread_id>", methods=["POST"])
def account_delete_thread(thread_id):
    require_csrf()
    db = get_db()
    user = require_login("/account")
    if not isinstance(user, User):
        return user
    delete_thread_owned(db, thread_id, user)
    return redirect("/account")


@app.route("/account/delete_post/<int:post_id>", methods=["POST"])
def account_delete_post(post_id):
    require_csrf()
    db = get_db()
    user = require_login("/account")
    if not isinstance(user, User):
        return user
    delete_post_owned(db, post_id, user)
    return redirect("/account")


@app.route("/login", methods=["GET", "POST"])
def login():
    db = get_db()
    next_url = request.args.get("next") or request.form.get("next") or "/forum"
    error = None
    if request.method == "POST":
        require_csrf()
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        user = db.execute(select(User).where(User.username == username)).scalar_one_or_none()
        if user and check_password_hash(user.password_hash, password):
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
    next_url = request.args.get("next") or "/forum"
    if request.method == "POST":
        require_csrf()
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        if not username or not password:
            error = "Username and password required"
        else:
            hashed = generate_password_hash(password)
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
    require_csrf()
    chosen = (request.form.get("chosen_value") or "").strip()
    return_to = request.form.get("return_to", "/forum")

    if not chosen:
        abort(400)

    parts = urlsplit(return_to)
    qs_map = dict(parse_qsl(parts.query, keep_blank_values=True))
    qs_map["prefill_stream"] = chosen
    qs_map["open_thread_form"] = "1"
    new_query = urlencode(qs_map)
    updated_return = urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))
    return redirect(updated_return)


@app.route("/thread/<int:thread_id>")
def thread_page(thread_id):
    db = get_db()
    t = (
        db.execute(
            select(Thread)
            .where(Thread.id == thread_id)
            .options(joinedload(Thread.user), joinedload(Thread.category))
        )
        .scalars()
        .first()
    )
    if t is None:
        abort(404)

    t.clicks = (t.clicks or 0) + 1
    db.commit()
    db.refresh(t)

    posts_tree = build_post_tree(db, thread_id)
    embed_info = detect_stream_embed(t.stream_link, TWITCH_PARENT_HOST)
    categories, cat_lookup, _ = load_category_indexes(db)
    return render_template(
        "thread.html",
        thread=serialize_thread(t),
        posts_tree=posts_tree,
        cat_lookup=cat_lookup,
        build_category_path=build_category_path,
        embed_info=embed_info,
        current_user=g.current_user.username if g.current_user else None,
    )


@app.route("/thread/<int:thread_id>/reply", methods=["POST"])
def reply_thread(thread_id):
    require_csrf()
    db = get_db()
    user = require_login(f"/thread/{thread_id}")
    if not isinstance(user, User):
        return user

    t = db.get(Thread, thread_id)
    if t is None:
        abort(404)

    body = (request.form.get("body") or "").strip()
    if not body or len(body) > 2000:
        abort(400)

    parent_raw = request.form.get("parent_id")
    parent_id = int(parent_raw) if parent_raw not in (None, "", "None") else None

    new_post = append_post(db, thread_id, body, parent_id, user)
    return redirect(f"/thread/{thread_id}#post-{new_post['id']}")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
