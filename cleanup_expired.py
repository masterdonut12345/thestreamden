from __future__ import annotations

from datetime import datetime, timezone

from den_store import den_store
from streaming_site import den_expiration_cutoff, DEN_EXPIRATION_HOURS


def cleanup_expired_dens(expiration_hours: int = DEN_EXPIRATION_HOURS) -> int:
    """Delete dens older than the expiration window. Returns number deleted."""
    cutoff = den_expiration_cutoff()
    return den_store.cleanup(cutoff)


if __name__ == "__main__":
    deleted_dens = cleanup_expired_dens()
    print(f"Expired dens deleted: {deleted_dens}")
