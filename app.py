from __future__ import annotations

import os

from flask import Flask, render_template, request, redirect, url_for, abort, jsonify
import re
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

import embed_streams  # your helper module

app = Flask(__name__)

CATEGORIES_PATH = Path("data/categories.json")
THREADS_PATH = Path("data/threads.json")

EXP_CHOICES = ["1 day", "3 days", "1 week", "1 month"]
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")


# -----------------------------
# Loaders
# -----------------------------

def load_categories():
    try:
        with open(CATEGORIES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        category_data = data.get("categories", [])
        cat_by_id = {c["id"]: c for c in category_data}
        max_id = max((c["id"] for c in category_data), default=-1)

        children_by_parent = defaultdict(list)
        cat_sp_id = {}  # (parent_id, slug) -> category dict

        for c in category_data:
            children_by_parent[c.get("parent_id")].append(c)
            key = (c.get("parent_id"), c.get("slug"))
            cat_sp_id[key] = c

        return {
            "data": data,
            "categories": category_data,
            "cat_by_id": cat_by_id,
            "children_by_parent": children_by_parent,
            "cat_sp_id": cat_sp_id,
            "max_id": max_id,
        }

    except FileNotFoundError:
        return {
            "data": {"categories": []},
            "categories": [],
            "cat_by_id": {},
            "children_by_parent": defaultdict(list),
            "cat_sp_id": {},
            "max_id": -1,
        }
    except json.JSONDecodeError as e:
        print(f"error decoding categories.json: {e}")
        return {}


def load_threads():
    try:
        with open(THREADS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        thread_data = data.get("threads", [])
        thread_by_id = {t["id"]: t for t in thread_data}
        max_id = max((t["id"] for t in thread_data), default=-1)

        threads_by_category = defaultdict(list)
        for t in thread_data:
            threads_by_category[t.get("category_id")].append(t)

        return {
            "raw": data,
            "threads": thread_data,
            "thread_by_id": thread_by_id,
            "threads_by_category": threads_by_category,
            "max_id": max_id,
        }

    except FileNotFoundError:
        return {
            "raw": {"threads": []},
            "threads": [],
            "thread_by_id": {},
            "threads_by_category": defaultdict(list),
            "max_id": -1,
        }
    except json.JSONDecodeError as e:
        print(f"error decoding threads.json: {e}")
        return {}


CATEGORY_DATA = load_categories()
THREAD_DATA = load_threads()


# -----------------------------
# Helpers
# -----------------------------

def slugify(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or "item"


def save_categories_json(data):
    CATEGORIES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CATEGORIES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def save_threads_json(data):
    THREADS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(THREADS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def ensure_thread_defaults():
    """Backfill missing thread fields for older JSON."""
    changed = False
    for t in THREAD_DATA.get("threads", []):
        if "clicks" not in t:
            t["clicks"] = 0
            changed = True

    if changed:
        THREAD_DATA["raw"]["threads"] = THREAD_DATA["threads"]
        save_threads_json(THREAD_DATA["raw"])


ensure_thread_defaults()


def build_thread_counts() -> dict[int, int]:
    """Return a mapping of category_id -> thread count."""
    return {
        cat_id: len(items)
        for cat_id, items in THREAD_DATA.get("threads_by_category", {}).items()
    }


def get_top_threads(limit: int = 10) -> list[dict]:
    """Return the top threads across all categories by clicks (desc), then recent."""
    threads = THREAD_DATA.get("threads", [])

    def sort_key(t):
        try:
            clicks = int(t.get("clicks", 0))
        except Exception:
            clicks = 0
        created = t.get("created_at", "")
        return (-clicks, created)

    return sorted(threads, key=sort_key)[:limit]


def get_top_threads_for_ids(cat_ids: list[int], limit: int = 10) -> list[dict]:
    threads = THREAD_DATA.get("threads", [])
    filtered = [t for t in threads if t.get("category_id") in cat_ids]

    def sort_key(t):
        try:
            clicks = int(t.get("clicks", 0))
        except Exception:
            clicks = 0
        created = t.get("created_at", "")
        return (-clicks, created)

    return sorted(filtered, key=sort_key)[:limit]


def get_category_by_path(path: str):
    path = (path or "").strip("/")
    if not path:
        return None

    split_path = [p for p in path.split("/") if p]
    idx = CATEGORY_DATA.get("cat_sp_id", {})
    parent_id = None
    current = None

    for slug in split_path:
        current = idx.get((parent_id, slug))
        if current is None:
            return None
        parent_id = current["id"]

    return current


def get_category_children(cat):
    children = CATEGORY_DATA["children_by_parent"]
    if cat is None:
        return children.get(None, [])
    return children.get(cat["id"], [])


def get_descendant_ids(cat_id):
    """Return all descendant category IDs beneath cat_id (None for root)."""
    ids = []
    children = CATEGORY_DATA["children_by_parent"].get(cat_id, [])
    for c in children:
        ids.append(c["id"])
        ids.extend(get_descendant_ids(c["id"]))
    return ids


def build_breadcrumbs(current_path: str):
    crumbs = [{"name": "Main", "url": "/forum"}]
    current_path = (current_path or "").strip("/")
    if not current_path:
        return crumbs

    parts = [p for p in current_path.split("/") if p]
    idx = CATEGORY_DATA.get("cat_sp_id", {})
    parent_id = None
    path_so_far = []

    for slug in parts:
        cat = idx.get((parent_id, slug))
        if cat is None:
            break
        path_so_far.append(slug)
        crumbs.append({
            "name": cat.get("name", slug),
            "url": "/forum/" + "/".join(path_so_far)
        })
        parent_id = cat["id"]

    return crumbs


def parse_expiration_choice(choice: str) -> timedelta | None:
    mapping = {
        "1 day": timedelta(days=1),
        "3 days": timedelta(days=3),
        "1 week": timedelta(weeks=1),
        "1 month": timedelta(days=30),
    }
    return mapping.get(choice)


def get_threads_for_category(cat):
    by_cat = THREAD_DATA.get("threads_by_category", defaultdict(list))
    if cat is None:
        return []  # no root threads by design
    return by_cat.get(cat["id"], [])

def group_threads_by_choice(threads: list[dict]) -> dict[str, list[dict]]:
    out = {k: [] for k in EXP_CHOICES}

    now = datetime.now(timezone.utc)

    for t in threads:
        # 1) if explicit choice exists and is valid, use it
        choice = t.get("expires_choice")
        if choice in out:
            out[choice].append(t)
            continue

        # 2) otherwise infer bucket from expires_at
        exp_s = t.get("expires_at") or ""
        try:
            exp_dt = datetime.fromisoformat(exp_s)  # works with +00:00
            if exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=timezone.utc)
        except Exception:
            # can't parse -> shove in 1 month as a safe default
            out["1 month"].append(t)
            continue

        delta = exp_dt - now
        secs = delta.total_seconds()

        if secs <= 0:
            # already expired: you can choose to hide these instead
            out["1 day"].append(t)
        elif secs <= 24 * 3600:
            out["1 day"].append(t)
        elif secs <= 3 * 24 * 3600:
            out["3 days"].append(t)
        elif secs <= 7 * 24 * 3600:
            out["1 week"].append(t)
        else:
            out["1 month"].append(t)

    # Sort inside each bucket by soonest expiration
    def exp_key(td):
        return td.get("expires_at") or ""

    for k in out:
        out[k].sort(key=exp_key)

    return out

# -----------------------------
# Mutations
# -----------------------------

def append_category(parent_path_str, name, desc):
    parent_path_str = (parent_path_str or "").strip("/")
    parent_cat = get_category_by_path(parent_path_str) if parent_path_str else None

    next_id = CATEGORY_DATA["max_id"] + 1
    slug = slugify(name)
    parent_id = parent_cat["id"] if parent_cat else None

    if parent_cat is not None:
        parent_cat["count"] = int(parent_cat.get("count", 0)) + 1

    new_category_data = {
        "id": next_id,
        "name": name,
        "slug": slug,
        "desc": desc,
        "count": 0,
        "parent_id": parent_id,
    }

    CATEGORY_DATA["max_id"] += 1
    CATEGORY_DATA["categories"].append(new_category_data)
    CATEGORY_DATA["cat_by_id"][next_id] = new_category_data
    CATEGORY_DATA["children_by_parent"][parent_id].append(new_category_data)
    CATEGORY_DATA["cat_sp_id"][(parent_id, slug)] = new_category_data
    CATEGORY_DATA["data"]["categories"] = CATEGORY_DATA["categories"]
    save_categories_json(CATEGORY_DATA["data"])
    return new_category_data


def append_thread(category_path_str: str, title: str, stream_link: str, expires_choice: str):
    category_path_str = (category_path_str or "").strip("/")
    cat = get_category_by_path(category_path_str)
    if cat is None:
        abort(400)

    expires_delta = parse_expiration_choice(expires_choice)
    if expires_delta is None:
        abort(400)

    now = datetime.now(timezone.utc)
    created_at = now.isoformat()
    expires_at = (now + expires_delta).isoformat()

    next_id = THREAD_DATA["max_id"] + 1
    slug = slugify(title)
    category_id = cat["id"]

    new_thread = {
        "id": next_id,
        "category_id": category_id,
        "title": title,
        "slug": slug,
        "stream_link": stream_link,
        "created_at": created_at,
        "expires_at": expires_at,
        "expires_choice": expires_choice,
        "reply_count": 0,
        "clicks": 0,
    }

    THREAD_DATA["max_id"] += 1
    THREAD_DATA["threads"].append(new_thread)
    THREAD_DATA["thread_by_id"][next_id] = new_thread
    THREAD_DATA["threads_by_category"][category_id].append(new_thread)

    THREAD_DATA["raw"]["threads"] = THREAD_DATA["threads"]
    save_threads_json(THREAD_DATA["raw"])
    return new_thread


# -----------------------------
# Routes
# -----------------------------

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
):
    grouped = group_threads_by_choice(threads) if allow_posting else {}

    prefill_stream = request.args.get("prefill_stream", "")
    open_thread_form = request.args.get("open_thread_form", "") == "1"

    return render_template(
        "index.html",
        categories=categories,
        current_path=category_path,
        parent_path=parent_path,
        breadcrumbs=build_breadcrumbs(category_path),
        threads_grouped=grouped,
        exp_choices=EXP_CHOICES,
        prefill_stream=prefill_stream,
        open_thread_form=open_thread_form and allow_posting,
        show_top_threads=not allow_posting,
        allow_posting=allow_posting,
        show_categories_section=show_categories_section,
        thread_title=thread_title,
        thread_subtext=thread_subtext,
        cat_lookup=CATEGORY_DATA.get("cat_by_id", {}),
        threads_flat=threads if not allow_posting else [],
        thread_counts=thread_counts,
    )


@app.route("/forum")
def index():
    categories = get_category_children(None)
    top_ids = get_descendant_ids(None)
    top_threads = get_top_threads_for_ids(top_ids, 10)
    thread_counts = build_thread_counts()
    return render_forum_page(
        category_path="",
        categories=categories,
        threads=top_threads,
        parent_path="",
        depth=0,
        allow_posting=False,
        show_categories_section=True,
        thread_title="Top Threads",
        thread_subtext="Most-clicked streams across all categories.",
        thread_counts=thread_counts,
    )


@app.route("/forum/<path:category_path>")
def forum(category_path):
    category_path = (category_path or "").strip("/")
    parts = [p for p in category_path.split("/") if p]
    parent_path = "/".join(parts[:-1])
    depth = len(parts)

    temp_cat = get_category_by_path(category_path)
    if temp_cat is None:
        abort(404)

    categories = get_category_children(temp_cat)
    thread_counts = build_thread_counts()

    if depth == 1:
        ids = get_descendant_ids(temp_cat["id"])
        threads = get_top_threads_for_ids(ids, 10)
        allow_posting = False
        show_categories_section = True
        thread_title = "Top Threads"
        thread_subtext = f"Most-clicked streams inside {temp_cat.get('name', 'this category')}."
    else:
        threads = get_threads_for_category(temp_cat)
        allow_posting = True
        show_categories_section = False
        thread_title = "Threads"
        thread_subtext = "Streams in this category."

    return render_forum_page(
        category_path=category_path,
        categories=categories,
        threads=threads,
        parent_path=parent_path,
        depth=depth,
        allow_posting=allow_posting,
        show_categories_section=show_categories_section,
        thread_title=thread_title,
        thread_subtext=thread_subtext,
        thread_counts=thread_counts,
    )


@app.route("/add_category", methods=["POST"])
def add_category():
    abort(404)


@app.route("/add_thread", methods=["POST"])
def add_thread():
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

    append_thread(category_path, title, stream_link, expires_choice)
    return redirect(f"/forum/{category_path}")


@app.route("/api/admin/categories", methods=["POST"])
def api_add_category():
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

    new_cat = append_category(parent_path, name, desc)
    return jsonify({"category": new_cat}), 201


@app.route("/embed_stream", methods=["GET", "POST"])
def embed_stream():
    # where to go back to
    return_to = request.args.get("return_to", "/forum")
    category_path = request.args.get("category_path", "")

    source = request.values.get("source", "twitch")
    user_input = request.values.get("user_input", "")

    candidates = []
    if request.method == "POST":
        parent_host = request.host.split(":")[0]  # needed for twitch "parent="
        candidates = embed_streams.get_embed_candidates(source, user_input, parent_host)

    return render_template(
        "embed_stream.html",
        return_to=return_to,
        category_path=category_path,
        source=source,
        user_input=user_input,
        candidates=candidates,
    )


@app.route("/choose_stream", methods=["POST"])
def choose_stream():
    chosen = (request.form.get("chosen_value") or "").strip()
    return_to = request.form.get("return_to", "/forum")

    if not chosen:
        abort(400)

    # ensure we return with:
    #  - prefill_stream=...
    #  - open_thread_form=1   (so UI opens add-thread area automatically)
    sep = "&" if "?" in return_to else "?"
    qs = urlencode({"prefill_stream": chosen, "open_thread_form": "1"})
    return redirect(f"{return_to}{sep}{qs}")


@app.route("/thread/<int:thread_id>")
def thread_page(thread_id):
    t = THREAD_DATA.get("thread_by_id", {}).get(thread_id)
    if t is None:
        abort(404)
    # Track click counts for the stream link
    try:
        t["clicks"] = int(t.get("clicks", 0)) + 1
    except Exception:
        t["clicks"] = 1

    THREAD_DATA["raw"]["threads"] = THREAD_DATA["threads"]
    save_threads_json(THREAD_DATA["raw"])

    stream_link = t.get("stream_link")
    if stream_link:
        return redirect(stream_link)
    return f"Thread page placeholder: {t.get('title')} (id={thread_id})"


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
