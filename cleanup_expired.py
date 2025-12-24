from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select

from db_models import SessionLocal, Thread, init_db


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


if __name__ == "__main__":
    deleted = cleanup_expired_threads()
    print(f"Expired threads deleted: {deleted}")
