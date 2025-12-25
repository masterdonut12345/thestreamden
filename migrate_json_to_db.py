from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import func, select, text
from werkzeug.security import generate_password_hash

from db_models import Category, Post, SessionLocal, Thread, User, init_db


DATA_DIR = Path("data")


def load_json(path: Path, key: str):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    return raw.get(key, [])


def parse_dt(value: str):
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return datetime.now(timezone.utc)


def main():
    init_db()
    session = SessionLocal()

    existing_categories = session.scalar(select(func.count(Category.id))) or 0
    if existing_categories > 0:
        print("Database already has categories; aborting import to avoid duplicates.")
        session.close()
        return

    users = load_json(DATA_DIR / "users.json", "users")
    categories = load_json(DATA_DIR / "categories.json", "categories")
    threads = load_json(DATA_DIR / "threads.json", "threads")
    posts = load_json(DATA_DIR / "posts.json", "posts")

    # ---- users ----
    user_lookup = {}
    existing_users = session.scalar(select(func.count(User.id))) or 0
    if existing_users == 0:
        for u in users:
            username = u.get("username") or "Anonymous"
            password = u.get("password") or "changeme"
            user = User(username=username, password_hash=generate_password_hash(password))
            session.add(user)
            session.flush()
            user_lookup[username] = user.id

        # Ensure an Anonymous placeholder exists for missing lookups
        if "Anonymous" not in user_lookup:
            anon = User(username="Anonymous", password_hash=generate_password_hash("changeme"))
            session.add(anon)
            session.flush()
            user_lookup["Anonymous"] = anon.id
    else:
        # Reuse existing users; map by username
        for user in session.execute(select(User)).scalars().all():
            user_lookup[user.username] = user.id
        if "Anonymous" not in user_lookup:
            anon = User(username="Anonymous", password_hash=generate_password_hash("changeme"))
            session.add(anon)
            session.flush()
            user_lookup["Anonymous"] = anon.id

    # ---- categories ----
    cat_lookup = {}
    for c in categories:
        cat = Category(
            id=c.get("id"),
            name=c.get("name", "Category"),
            slug=c.get("slug") or "category",
            desc=c.get("desc") or "",
            parent_id=c.get("parent_id"),
            created_at=parse_dt(c.get("created_at", "")),
            updated_at=parse_dt(c.get("updated_at", "")),
        )
        session.add(cat)
        session.flush()
        cat_lookup[cat.id] = cat

    # ---- threads ----
    thread_lookup = {}
    reply_counts = {}
    for t in threads:
        username = t.get("user") or "Anonymous"
        user_id = user_lookup.get(username, user_lookup["Anonymous"])
        thread = Thread(
            id=t.get("id"),
            category_id=t.get("category_id"),
            user_id=user_id,
            title=t.get("title") or "Untitled",
            slug=t.get("slug") or "thread",
            stream_link=t.get("stream_link") or "",
            expires_at=parse_dt(t.get("expires_at", "")),
            expires_choice=t.get("expires_choice"),
            tag=t.get("tag") or "general",
            reply_count=t.get("reply_count") or 0,
            clicks=t.get("clicks") or 0,
            created_at=parse_dt(t.get("created_at", "")),
        )
        session.add(thread)
        session.flush()
        thread_lookup[thread.id] = thread
        reply_counts[thread.id] = 0

    # ---- posts ----
    for p in posts:
        username = p.get("user") or "Anonymous"
        user_id = user_lookup.get(username, user_lookup["Anonymous"])
        post = Post(
            id=p.get("id"),
            thread_id=p.get("thread_id"),
            user_id=user_id,
            parent_id=p.get("parent_id"),
            body=p.get("body") or "",
            created_at=parse_dt(p.get("created_at", "")),
        )
        session.add(post)
        reply_counts[post.thread_id] = reply_counts.get(post.thread_id, 0) + 1

    # sync reply counts
    for tid, count in reply_counts.items():
        thread = thread_lookup.get(tid)
        if thread:
            thread.reply_count = count
    session.commit()

    # align sequences for PostgreSQL
    if session.bind.dialect.name == "postgresql":
        for table in ("users", "categories", "threads", "posts"):
            session.execute(
                text(
                    "SELECT setval("
                    "pg_get_serial_sequence(:tbl, 'id'), "
                    "(SELECT COALESCE(MAX(id), 1) FROM " + table + "), true)"
                ),
                {"tbl": table},
            )
        session.commit()

    session.close()
    print("Import complete.")


if __name__ == "__main__":
    main()
