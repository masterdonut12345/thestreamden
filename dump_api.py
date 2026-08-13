#!/usr/bin/env python3
"""Dump the thestreamden API endpoints to JSON files for inspection.

By default it hits the production site (https://thestreamden.com). Override
with --base for a local server, and optionally pass a private key with
--key to also pull the private endpoints.

Usage
-----
  python dump_api.py --out api_dump
  python dump_api.py --base http://localhost:5000 --key td_xxxx
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

DEFAULT_BASE = "https://thestreamden.com"


def fetch(base: str, path: str, key: str | None = None) -> dict:
    url = base.rstrip("/") + path
    req = urllib.request.Request(url, headers={"User-Agent": "tsd-api-dump"})
    if key:
        req.add_header("Authorization", f"Bearer {key}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> None:
    ap = argparse.ArgumentParser(description="Dump thestreamden API endpoints to JSON files.")
    ap.add_argument("--base", default=DEFAULT_BASE, help="Base URL (default: %(default)s)")
    ap.add_argument("--key", help="Private API key to also pull private endpoints")
    ap.add_argument("--out", default="api_dump", help="Output directory (default: %(default)s)")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    jobs: list[tuple[str, str]] = [
        ("sports.json", "/api/v1/sports"),
        ("channels.json", "/api/v1/channels"),
        ("channels_live.json", "/api/v1/channels?live=1"),
        ("channels_soccer.json", "/api/v1/channels?sport=soccer"),
        ("channels_search.json", "/api/v1/channels?q=braves&per_page=5"),
    ]

    # Add a detail dump for the first channel id we find.
    try:
        listing = fetch(args.base, "/api/v1/channels?per_page=1")
        first_id = listing["channels"][0]["id"]
        jobs.append(("channel_detail.json", f"/api/v1/channels/{first_id}"))
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] could not resolve a channel id: {exc}")

    for name, path in jobs:
        try:
            data = fetch(args.base, path)
            (out / name).write_text(json.dumps(data, indent=2))
            print(f"[ok]   {name:<24} {path}")
        except Exception as exc:  # noqa: BLE001
            print(f"[fail] {name:<24} {path} -> {exc}")

    if args.key:
        priv_jobs: list[tuple[str, str]] = [
            ("private_channels.json", "/api/v1/private/channels"),
            ("private_channels_live.json", "/api/v1/private/channels?live=1"),
            ("private_me.json", "/api/v1/private/me"),
        ]
        for name, path in priv_jobs:
            try:
                data = fetch(args.base, path, key=args.key)
                (out / name).write_text(json.dumps(data, indent=2))
                print(f"[ok]   {name:<24} {path} (private)")
            except Exception as exc:  # noqa: BLE001
                print(f"[fail] {name:<24} {path} -> {exc}")

    # Print the first live channel's play URL so it can be opened in a browser.
    try:
        live = fetch(args.base, "/api/v1/channels?live=1&per_page=1")
        ch = live["channels"][0]
        print()
        print(f"First live channel: {ch['sport']} — {ch.get('matchup') or ch['id']}")
        print(f"Public player:      {ch['play_url']}")
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] could not print a play URL: {exc}")

    print(f"\nSaved to {out}/")


if __name__ == "__main__":
    main()