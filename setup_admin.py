#!/usr/bin/env python3
"""CLI bootstrap for the thestreamden API admin.

Use this BEFORE taking the site public so the admin account, users, and
API keys are all provisioned without exposing /admin/setup to the internet.

Examples
--------
  python setup_admin.py                      # create admin (prompts for password)
  python setup_admin.py --password '...'     # non-interactive admin creation
  python setup_admin.py --user 'Acme Corp'   # add a user
  python setup_admin.py --user 'Acme Corp' --key-name prod --rate-limit 60
                                             # add a user AND issue an API key
  python setup_admin.py --list               # show users + keys (prefixes only)
  python setup_admin.py --disable-web-setup  # turn off /admin/setup via env hint
"""

from __future__ import annotations

import argparse
import getpass
import io
import os
import secrets
import sys
import time
from pathlib import Path

# Ensure the repo root is importable so stream_api's helpers resolve.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from stream_api import (  # noqa: E402
    API_DB_PATH,
    DEFAULT_KEY_RATE_LIMIT_PER_MIN,
    _api_db_conn,
    _ensure_api_db,
    _hash_key,
    _totp_qr_data_uri,
)
from werkzeug.security import generate_password_hash  # noqa: E402


def _admin_configured() -> bool:
    with _api_db_conn() as conn:
        return conn.execute("SELECT 1 FROM admin_settings WHERE id = 1").fetchone() is not None


def _create_admin(password: str) -> str:
    if _admin_configured():
        print("[!] Admin already configured. Run --reset-admin to replace it.")
        sys.exit(1)
    import pyotp

    totp_secret = pyotp.random_base32()
    _ensure_api_db()
    from stream_api import API_DB_LOCK, sqlite3

    with API_DB_LOCK, sqlite3.connect(API_DB_PATH) as conn:
        conn.execute(
            "INSERT INTO admin_settings (id, password_hash, totp_secret, created_at) VALUES (1, ?, ?, ?)",
            (generate_password_hash(password), totp_secret, time.time()),
        )
        conn.commit()

    totp_uri = pyotp.totp.TOTP(totp_secret).provisioning_uri(
        name="thestreamden-admin", issuer_name="thestreamden"
    )
    qr = _totp_qr_data_uri(totp_uri)
    print("=" * 60)
    print("Admin created.")
    print("TOTP secret (add to your authenticator app):")
    print(f"  {totp_secret}")
    print()
    print("Or scan this QR code:")
    print(f"  {qr}")
    print()
    print("QR is a data URI. Open it in a browser to scan, or enter the secret manually.")
    print("=" * 60)
    return totp_secret


def _reset_admin(password: str) -> None:
    import pyotp

    totp_secret = pyotp.random_base32()
    _ensure_api_db()
    from stream_api import API_DB_LOCK, sqlite3

    with API_DB_LOCK, sqlite3.connect(API_DB_PATH) as conn:
        conn.execute("DELETE FROM admin_settings WHERE id = 1")
        conn.execute(
            "INSERT INTO admin_settings (id, password_hash, totp_secret, created_at) VALUES (1, ?, ?, ?)",
            (generate_password_hash(password), totp_secret, time.time()),
        )
        conn.commit()

    totp_uri = pyotp.totp.TOTP(totp_secret).provisioning_uri(
        name="thestreamden-admin", issuer_name="thestreamden"
    )
    qr = _totp_qr_data_uri(totp_uri)
    print("=" * 60)
    print("Admin reset.")
    print(f"TOTP secret: {totp_secret}")
    print(f"QR (data URI): {qr}")
    print("=" * 60)


def _add_user(name: str) -> int:
    _ensure_api_db()
    from stream_api import API_DB_LOCK, sqlite3

    with API_DB_LOCK, sqlite3.connect(API_DB_PATH) as conn:
        cur = conn.execute(
            "INSERT INTO api_users (name, created_at, active) VALUES (?, ?, 1)",
            (name, time.time()),
        )
        conn.commit()
        return cur.lastrowid


def _issue_key(user_id: int, name: str, rate_limit: int, expires_at: float | None) -> str:
    _ensure_api_db()
    from stream_api import API_DB_LOCK, sqlite3

    raw_key = "td_" + secrets.token_urlsafe(32)
    with API_DB_LOCK, sqlite3.connect(API_DB_PATH) as conn:
        conn.execute(
            """INSERT INTO api_keys (user_id, name, key_hash, key_prefix, created_at, expires_at, rate_limit_per_min, active)
               VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
            (user_id, name, _hash_key(raw_key), raw_key[:8], time.time(), expires_at, rate_limit),
        )
        conn.commit()
    return raw_key


def _find_user(name: str) -> int | None:
    _ensure_api_db()
    with _api_db_conn() as conn:
        row = conn.execute("SELECT id FROM api_users WHERE name = ?", (name,)).fetchone()
        return row["id"] if row else None


def _list_all() -> None:
    _ensure_api_db()
    with _api_db_conn() as conn:
        users = conn.execute("SELECT * FROM api_users ORDER BY created_at").fetchall()
        keys = conn.execute("SELECT * FROM api_keys ORDER BY created_at").fetchall()
    print(f"Admin configured: {_admin_configured()}")
    print()
    print("USERS:")
    for u in users:
        print(f"  [{u['id']}] {u['name']} (active={u['active']}) created={u['created_at']:.0f}")
    print()
    print("KEYS (prefix only — full key is shown once at issuance):")
    for k in keys:
        print(
            f"  [{k['id']}] user={k['user_id']} {k['name']} "
            f"prefix={k['key_prefix']}… limit={k['rate_limit_per_min']}/min "
            f"active={k['active']}"
        )


def main() -> None:
    ap = argparse.ArgumentParser(description="Bootstrap the thestreamden API admin.")
    ap.add_argument("--password", help="Admin password (min 12 chars). Prompts if omitted.")
    ap.add_argument("--reset-admin", action="store_true", help="Replace existing admin creds.")
    ap.add_argument("--user", help="Name of a user to create (can be combined with --key-name).")
    ap.add_argument("--key-name", default="prod", help="Name for the issued API key.")
    ap.add_argument("--rate-limit", type=int, default=DEFAULT_KEY_RATE_LIMIT_PER_MIN)
    ap.add_argument("--expires-at", type=float, help="Optional unix expiry for the key.")
    ap.add_argument("--list", action="store_true", help="Show users + keys.")
    args = ap.parse_args()

    if args.list:
        _list_all()
        return

    if args.user:
        user_id = _find_user(args.user)
        if user_id is None:
            user_id = _add_user(args.user)
            print(f"Created user '{args.user}' (id={user_id}).")
        if args.key_name:
            raw = _issue_key(user_id, args.key_name, args.rate_limit, args.expires_at)
            print("=" * 60)
            print(f"API key for '{args.user}' / '{args.key_name}':")
            print(f"  {raw}")
            print("Copy it now — it is stored hashed and shown only once.")
            print("=" * 60)
        return

    if _admin_configured() and not args.reset_admin:
        print("Admin already configured.")
        _list_all()
        print()
        print("Use --reset-admin to change the admin password/TOTP.")
        return

    password = args.password
    if not password:
        password = getpass.getpass("Admin password (min 12 chars): ")
        confirm = getpass.getpass("Confirm password: ")
        if password != confirm:
            print("Passwords do not match.")
            sys.exit(1)
    if len(password) < 12:
        print("Password must be at least 12 characters.")
        sys.exit(1)

    if args.reset_admin:
        _reset_admin(password)
    else:
        _create_admin(password)


if __name__ == "__main__":
    main()