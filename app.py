from __future__ import annotations

from flask import Flask, render_template, request, redirect, url_for, abort
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
        "reply_count": 0
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

def render_forum_page(category_path: str, categories: list, threads: list, parent_path: str):
    grouped = group_threads_by_choice(threads)

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
        open_thread_form=open_thread_form,
    )


@app.route("/forum")
def index():
    categories = get_category_children(None)
    return render_forum_page("", categories, [], "")


@app.route("/forum/<path:category_path>")
def forum(category_path):
    category_path = (category_path or "").strip("/")
    parts = [p for p in category_path.split("/") if p]
    parent_path = "/".join(parts[:-1])

    temp_cat = get_category_by_path(category_path)
    if temp_cat is None:
        abort(404)

    categories = get_category_children(temp_cat)
    threads = get_threads_for_category(temp_cat)

    return render_forum_page(category_path, categories, threads, parent_path)


@app.route("/add_category", methods=["POST"])
def add_category():
    parent_path = request.form.get("parent_path", "")
    name = (request.form.get("category-name") or "").strip()
    desc = (request.form.get("category-desc") or "").strip()

    if not name or len(name) > 12:
        abort(400)
    if not desc:
        abort(400)

    append_category(parent_path, name, desc)

    parent_path = (parent_path or "").strip("/")
    if parent_path:
        return redirect(f"/forum/{parent_path}")
    return redirect(url_for("index"))


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
    return f"Thread page placeholder: {t.get('title')} (id={thread_id})"


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
