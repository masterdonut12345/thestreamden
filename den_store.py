from __future__ import annotations

import re
import secrets
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _slugify(name: str) -> str:
    value = (name or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = value.strip("-")
    return value or "den"


@dataclass
class Den:
    id: int
    name: str
    slug: str
    is_public: bool
    invite_code: str | None
    stream_url: str
    game_id: int | None
    owner_session: str
    created_at: datetime = field(default_factory=_now)
    members: set[str] = field(default_factory=set)


class DenStore:
    """A lightweight in-memory store for dens.

    This replaces the previous database-backed model so any visitor can spin up
    a den and invite others with a simple link.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._dens_by_slug: Dict[str, Den] = {}
        self._next_id = 1

    def _reserve_slug(self, name: str) -> str:
        base = _slugify(name)
        slug = base
        suffix = 1
        with self._lock:
            while slug in self._dens_by_slug:
                slug = f"{base}-{suffix}"
                suffix += 1
            return slug

    def create(
        self,
        name: str,
        stream_url: str,
        game_id: int | None,
        is_public: bool,
        owner_session: str,
    ) -> Den:
        slug = self._reserve_slug(name)
        invite_code = None if is_public else secrets.token_urlsafe(6)
        with self._lock:
            den = Den(
                id=self._next_id,
                name=name,
                slug=slug,
                is_public=is_public,
                invite_code=invite_code,
                stream_url=stream_url,
                game_id=game_id,
                owner_session=owner_session,
            )
            den.members.add(owner_session)
            self._dens_by_slug[slug] = den
            self._next_id += 1
            return den

    def get(self, slug: str) -> Optional[Den]:
        with self._lock:
            return self._dens_by_slug.get(slug)

    def get_by_id(self, den_id: int) -> Optional[Den]:
        with self._lock:
            for den in self._dens_by_slug.values():
                if den.id == den_id:
                    return den
            return None

    def join(self, slug: str, session_id: str) -> Optional[Den]:
        with self._lock:
            den = self._dens_by_slug.get(slug)
            if den:
                den.members.add(session_id)
            return den

    def delete(self, slug: str, requester: str) -> bool:
        with self._lock:
            den = self._dens_by_slug.get(slug)
            if not den or den.owner_session != requester:
                return False
            del self._dens_by_slug[slug]
            return True

    def update_stream(self, slug: str, requester: str, stream_url: str, game_id: int | None) -> bool:
        with self._lock:
            den = self._dens_by_slug.get(slug)
            if not den or den.owner_session != requester:
                return False
            den.stream_url = stream_url
            den.game_id = game_id
            return True

    def dens_for_session(self, session_id: str) -> tuple[list[Den], list[Den]]:
        with self._lock:
            owned = [d for d in self._dens_by_slug.values() if d.owner_session == session_id]
            joined = [d for d in self._dens_by_slug.values() if session_id in d.members and d.owner_session != session_id]
            return owned, joined

    def active_public(self, cutoff: datetime) -> list[Den]:
        with self._lock:
            return [d for d in self._dens_by_slug.values() if d.is_public and d.created_at >= cutoff]

    def cleanup(self, cutoff: datetime) -> int:
        with self._lock:
            stale = [slug for slug, den in self._dens_by_slug.items() if den.created_at < cutoff]
            for slug in stale:
                del self._dens_by_slug[slug]
            return len(stale)

    def all(self) -> Iterable[Den]:
        with self._lock:
            return list(self._dens_by_slug.values())


den_store = DenStore()
