from __future__ import annotations

import os
import re
import secrets
import threading
import time
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit, quote
from markupsafe import Markup
from flask import (
    Flask,
    abort,
    g,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)
import requests
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from werkzeug.exceptions import HTTPException
from werkzeug.security import check_password_hash, generate_password_hash

from flask import Flask, session

from streaming_site import streaming_bp

app = Flask(__name__)

APP_SECRET = os.environ.get("APP_SECRET", "dev-secret-key")
SESSION_COOKIE_SECURE = os.environ.get("SESSION_COOKIE_SECURE", "0").lower() in ("1", "true", "yes")
SESSION_COOKIE_SAMESITE = os.environ.get("SESSION_COOKIE_SAMESITE", "Lax")

app.secret_key = APP_SECRET
app.config.update(
    SESSION_COOKIE_SECURE=SESSION_COOKIE_SECURE,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE=SESSION_COOKIE_SAMESITE,
)

app.register_blueprint(streaming_bp)

STREAM_PROXY_TIMEOUT = 12
STREAM_PROXY_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/143.0.0.0 Safari/537.36"
)


def stream_proxy_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    try:
        parsed = urlsplit(raw_url)
    except Exception:
        return raw_url
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return raw_url
    return url_for("stream_proxy", url=raw_url)


def _inject_popup_guard(html: str, base_url: str) -> str:
    html = re.sub(
        r"<meta[^>]+http-equiv=[\"']?Content-Security-Policy[\"']?[^>]*>",
        "",
        html,
        flags=re.IGNORECASE,
    )
    guard = """
    <script>
      (() => {
        const noop = () => null;
        try {
          Object.defineProperty(window, "open", {
            value: noop,
            writable: false,
            configurable: false,
          });
        } catch (e) {
          window.open = noop;
        }
        try {
          Object.defineProperty(Window.prototype, "open", {
            value: noop,
            writable: false,
            configurable: false,
          });
        } catch (e) {}

        document.addEventListener("click", (event) => {
          const anchor = event.target?.closest?.("a");
          if (!anchor) return;
          const target = (anchor.getAttribute("target") || "").toLowerCase();
          if (target === "_blank" || target === "_new") {
            event.preventDefault();
            event.stopPropagation();
          }
        }, true);

        try {
          const locationProto = Object.getPrototypeOf(window.location);
          if (locationProto) {
            ["assign", "replace"].forEach((name) => {
              try {
                Object.defineProperty(window.location, name, {
                  value: noop,
                  writable: false,
                  configurable: false,
                });
              } catch (e) {}
            });
          }
        } catch (e) {}
      })();
    </script>
    """
    base = f'<base href="{base_url}">'
    head_match = re.search(r"<head[^>]*>", html, re.IGNORECASE)
    if head_match:
        insert_at = head_match.end()
        return html[:insert_at] + base + guard + html[insert_at:]
    body_match = re.search(r"<body[^>]*>", html, re.IGNORECASE)
    if body_match:
        insert_at = body_match.end()
        return html[:insert_at] + guard + html[insert_at:]
    return base + guard + html


@app.route("/stream-proxy")
def stream_proxy():
    raw_url = (request.args.get("url") or "").strip()
    if not raw_url:
        abort(400)

    try:
        parsed = urlsplit(raw_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            abort(400)

        headers = {
            "User-Agent": STREAM_PROXY_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": raw_url,
            "Origin": f"{parsed.scheme}://{parsed.netloc}",
        }

        resp = requests.get(
            raw_url,
            headers=headers,
            timeout=STREAM_PROXY_TIMEOUT,
            allow_redirects=True,
        )
    except requests.RequestException:
        abort(502)
    except Exception:
        abort(502)

    content_type = resp.headers.get("Content-Type", "")
    body = resp.content
    is_html = "text/html" in content_type.lower()
    if not is_html:
        snippet = body[:500].lstrip()
        is_html = snippet.startswith(b"<") or snippet.startswith(b"<!doctype")
    if is_html:
        try:
            html = resp.text
            injected = _inject_popup_guard(html, str(resp.url))
            body = injected.encode(resp.encoding or "utf-8", errors="replace")
        except Exception:
            body = resp.content

    response = make_response(body, resp.status_code)
    response.headers["Content-Type"] = content_type or "text/html; charset=utf-8"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


app.jinja_env.globals["stream_proxy_url"] = stream_proxy_url


@app.route("/robots.txt")
def robots_txt():
    return send_from_directory(app.static_folder, "robots.txt")


# -----------------------------
# CSRF helpers
# -----------------------------


def generate_csrf_token() -> str:
    token = session.get("csrf_token")
    if not token:
        token = os.urandom(32).hex()
        session["csrf_token"] = token
    return token


@app.context_processor
def inject_csrf():
    return {"csrf_token": generate_csrf_token()}


_cleanup_worker_lock = threading.Lock()
_cleanup_worker_started = False


def _start_cleanup_worker() -> None:
    """Placeholder for background cleanup to avoid request-time failures."""
    global _cleanup_worker_started
    if _cleanup_worker_started:
        return
    with _cleanup_worker_lock:
        if _cleanup_worker_started:
            return
        _cleanup_worker_started = True


@app.before_request
def before_any_request():
    generate_csrf_token()
    _start_cleanup_worker()


@app.route("/forum")
def index():
    db = get_db()
    categories, cat_lookup, children = load_category_indexes(db)
    top_categories = get_category_children(children, None)
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
        categories=top_categories,
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
    tag = (request.form.get("thread-tag") or DEFAULT_TAG).strip()

    if not category_path:
        abort(400)
    if not title or len(title) > 80:
        abort(400)
    if not stream_link or len(stream_link) > 500:
        abort(400)
    if expires_choice not in EXP_CHOICES:
        abort(400)
    if len(tag) > 64:
        abort(400)

    append_thread(db, category_path, title, stream_link, expires_choice, user, tag=tag)
    return redirect(f"/forum/{category_path}")


@app.route("/api/threads", methods=["POST"])
def api_create_thread():
    db = get_db()
    user = get_current_user(db)
    if not user:
        return jsonify({"error": "auth_required"}), 401
    if getattr(user, "is_banned", False):
        abort(403)

    data = request.get_json(silent=True) or {}
    category_path = (data.get("category_path") or "").strip("/")
    title = (data.get("title") or "").strip()
    stream_link = (data.get("stream_link") or "").strip()
    expires_choice = (data.get("expires_choice") or "").strip()
    tag = (data.get("tag") or DEFAULT_TAG).strip()

    if not category_path:
        return jsonify({"error": "category_required"}), 400
    if not title or len(title) > 80:
        return jsonify({"error": "invalid_title"}), 400
    if not stream_link or len(stream_link) > 500:
        return jsonify({"error": "invalid_stream_link"}), 400
    if expires_choice not in EXP_CHOICES:
        return jsonify({"error": "invalid_expiration"}), 400
    if len(tag) > 64:
        return jsonify({"error": "invalid_tag"}), 400

    try:
        new_thread = append_thread(
            db,
            category_path,
            title,
            stream_link,
            expires_choice,
            user,
            tag=tag,
        )
    except Exception as exc:
        # Preserve existing behavior for missing categories/validation
        if isinstance(exc, HTTPException):
            raise
        abort(400)

    return jsonify({"thread": new_thread}), 201


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


@app.route("/users/<username>")
def user_profile(username):
    db = get_db()
    user_obj = db.execute(select(User).where(User.username == username)).scalar_one_or_none()
    if user_obj is None:
        abort(404)

    threads, posts = user_threads_and_posts(db, user_obj)
    _, cat_lookup, _ = load_category_indexes(db)
    all_threads = (
        db.execute(select(Thread).options(joinedload(Thread.user)))
        .scalars()
        .all()
    )
    thread_lookup = {t.id: serialize_thread(t) for t in all_threads}
    return render_template(
        "user_profile.html",
        profile_user=user_obj,
        threads=threads,
        posts=posts,
        cat_lookup=cat_lookup,
        thread_lookup=thread_lookup,
        build_category_path=build_category_path,
        current_user=g.current_user.username if g.current_user else None,
        is_self=g.current_user.id == user_obj.id if g.current_user else False,
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


@app.route("/account/edit_thread/<int:thread_id>", methods=["POST"])
def account_edit_thread(thread_id: int):
    require_csrf()
    db = get_db()
    user = require_login("/account")
    if not isinstance(user, User):
        return user

    thread = db.get(Thread, thread_id)
    if thread is None:
        abort(404)
    if thread.user_id != user.id:
        abort(403)

    title = (request.form.get("title") or "").strip()
    tag_raw = (request.form.get("tag") or DEFAULT_TAG).strip()

    if not title or len(title) > 80:
        abort(400)
    if len(tag_raw) > 64:
        abort(400)

    new_slug = slugify(title)
    conflict = (
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
    if conflict:
        abort(409)

    thread.title = title
    thread.slug = new_slug
    thread.tag = normalize_tag(tag_raw)
    db.commit()

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

@app.before_request
def admin_gate():
    if request.path.startswith('/admin'):
        gate_param = request.args.get('gate')
        if gate_param:
            session['admin_gate_token'] = gate_param
        gate_token = session.get('admin_gate_token')
        if not gate_token or not hmac.compare_digest(gate_token, ADMIN_GATE):
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
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
