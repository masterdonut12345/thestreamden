from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select

from db_models import SessionLocal, Thread, Den, init_db
from streaming_site import den_expiration_cutoff, DEN_EXPIRATION_HOURS


def cleanup_expired_threads() -> int:
    """Delete threads whose expires_at is in the past. Returns number deleted."""
    init_db()
    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        expired = (
            db.execute(
                select(Thread).where(Thread.expires_at.is_not(None), Thread.expires_at <= now)
            )
            .scalars()
            .all()
        )
        count = len(expired)
        for t in expired:
            db.delete(t)  # cascades posts via relationship
        if count:
            db.commit()
        return count
    finally:
        db.close()


def cleanup_expired_dens(expiration_hours: int = DEN_EXPIRATION_HOURS) -> int:
    """Delete dens older than the expiration window. Returns number deleted."""

    init_db()
    db = SessionLocal()
    try:
        cutoff = den_expiration_cutoff()
        expired = (
            db.execute(
                select(Den).where(Den.created_at.is_not(None), Den.created_at < cutoff)
            )
            .scalars()
            .all()
        )
        count = len(expired)
        for den in expired:
            db.delete(den)
        if count:
            db.commit()
        return count
    finally:
        db.close()


if __name__ == "__main__":
    deleted_threads = cleanup_expired_threads()
    deleted_dens = cleanup_expired_dens()
    print(f"Expired threads deleted: {deleted_threads}")
    print(f"Expired dens deleted: {deleted_dens}")
