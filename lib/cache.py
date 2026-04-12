"""
MediaFusion Bridge – lib/cache.py
Persistent JSON caches for:
  • StreamCache   – resolved stream URL + header map, keyed by media_id.
  • ProgressCache – resume time in seconds, keyed by media_id.

media_id convention:
  movie   → "tt1234567"
  episode → "tt1234567:1:2"   (imdb:season:episode)
"""

import json
import os
import time


class StreamCache:
    """Cache for MediaFusion stream URLs so we don't re-query every play."""

    # Treat cached URL as stale after this many seconds (default 6 h).
    # MediaFusion proxy URLs include the user's secret token and are stable
    # for the life of the Real-Debrid cache entry.
    DEFAULT_TTL = 6 * 3600

    def __init__(self, userdata_path, ttl=None):
        self._path = os.path.join(userdata_path, "stream_cache.json")
        self._ttl = ttl if ttl is not None else self.DEFAULT_TTL
        self._data = self._load()

    # ------------------------------------------------------------------ #
    # Private helpers
    # ------------------------------------------------------------------ #

    def _load(self):
        if os.path.isfile(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception:
                pass
        return {}

    def _save(self):
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def get(self, media_id):
        """Return cached entry dict or None if missing / expired."""
        entry = self._data.get(media_id)
        if not entry:
            return None
        age = time.time() - entry.get("timestamp", 0)
        if age > self._ttl:
            return None
        return entry  # {"url": ..., "headers": {...}, "timestamp": ...}

    def set(self, media_id, url, headers=None):
        """Save a stream entry.  headers must be a plain dict (may be empty)."""
        self._data[media_id] = {
            "url": url,
            "headers": headers or {},
            "timestamp": time.time(),
        }
        self._save()

    def clear(self, media_id=None):
        """Remove one entry (or all entries when media_id is None)."""
        if media_id:
            self._data.pop(media_id, None)
        else:
            self._data.clear()
        self._save()


class ProgressCache:
    """Cache for per-item resume positions (seconds)."""

    def __init__(self, userdata_path):
        self._path = os.path.join(userdata_path, "progress_cache.json")
        self._data = self._load()

    # ------------------------------------------------------------------ #
    # Private helpers
    # ------------------------------------------------------------------ #

    def _load(self):
        if os.path.isfile(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception:
                pass
        return {}

    def _save(self):
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def get_resume_time(self, media_id):
        """Return resume position in seconds, or 0.0 if watched / not tracked."""
        entry = self._data.get(media_id, {})
        if entry.get("watched"):
            return 0.0
        return float(entry.get("resume_time", 0.0))

    def set_progress(self, media_id, resume_time, total_time=0.0, watched=False):
        """Persist the current playback position for *media_id*."""
        self._data[media_id] = {
            "resume_time": resume_time,
            "total_time": total_time,
            "watched": watched,
            "updated": time.time(),
        }
        self._save()
