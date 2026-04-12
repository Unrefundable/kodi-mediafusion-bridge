"""
MediaFusion Bridge – lib/mediafusion.py
Thin client for the MediaFusion Kodi stream API.

Reads BASE_URL and SECRET_STR directly from the MediaFusion addon settings so
the user never has to enter their credentials twice.

Public entry point: fetch_best_stream(catalog_type, video_id)
  Returns a dict {"url": str, "headers": dict, "name": str} for the
  top-ranked stream, or None if nothing is available.

Sort order: the MediaFusion /kodi/stream/ API already applies the user's
custom sort/filter configured on the MediaFusion setup website (e.g.
cached → size → quality → language → resolution) server-side before
responding.  This client preserves that order exactly – no local re-sort.
"""

from urllib.parse import urljoin
import xbmc
import xbmcaddon
import xbmcgui


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[MFBridge] {msg}", level)


def _get_mf_settings():
    """Read MediaFusion base URL and secret string from its own settings."""
    try:
        mf = xbmcaddon.Addon("plugin.video.mediafusion")
        base_url = mf.getSetting("base_url").rstrip("/") + "/"
        secret = mf.getSetting("secret_string")
        return base_url, secret
    except Exception as exc:
        _log(f"Cannot read MediaFusion settings: {exc}", xbmc.LOGERROR)
        return None, None


def _parse_stream_options(stream_entries):
    """
    Convert raw entries from the MediaFusion /kodi/stream/ endpoint into a
    flat list of playable stream dicts.

    The order of stream_entries is preserved exactly as returned by the API.
    MediaFusion applies the user's custom sort/filter (configured on the
    MediaFusion setup website) server-side, so the first HTTP entry is already
    the user's #1 preferred stream.

    Only direct HTTP(S) streams are included; torrents/magnets that require
    Elementum are skipped (no url key in the stream dict).
    """
    results = []
    for entry in stream_entries:
        stream = entry.get("stream") or {}
        metadata = entry.get("metadata") or {}
        behavior_hints = stream.get("behaviorHints") or {}

        url = stream.get("url")
        if not url:
            continue  # torrent / magnet – skip

        # Build the headers dict from the proxyHeaders block.
        proxy_headers = (
            behavior_hints.get("proxyHeaders") or {}
        ).get("request") or {}

        results.append({
            "url": url,
            "headers": proxy_headers,
            "name": (
                behavior_hints.get("filename")
                or metadata.get("filename")
                or metadata.get("name")
                or "Stream"
            ),
        })
    return results


def fetch_best_stream(catalog_type, video_id):
    """
    Query MediaFusion's Kodi stream endpoint, apply smart sort, and return the
    best available stream dict: {"url": str, "headers": dict, "name": str}.
    Returns None on error or when no playable streams are found.
    """
    import requests  # from script.module.requests

    base_url, secret = _get_mf_settings()
    if not base_url or not secret:
        xbmcgui.Dialog().notification(
            "MF Bridge",
            "MediaFusion is not configured",
            xbmcgui.NOTIFICATION_ERROR,
        )
        return None

    api_url = urljoin(
        base_url,
        f"/{secret}/kodi/stream/{catalog_type}/{video_id}.json?page=1&page_size=25",
    )

    _log(f"Fetching streams → {catalog_type}/{video_id}")
    try:
        response = requests.get(api_url, timeout=20)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        _log(f"Stream fetch failed: {exc}", xbmc.LOGERROR)
        xbmcgui.Dialog().notification(
            "MF Bridge",
            "Failed to fetch streams from MediaFusion",
            xbmcgui.NOTIFICATION_ERROR,
        )
        return None

    stream_entries = data.get("streams") or []
    if not stream_entries:
        _log(f"No streams returned for {catalog_type}/{video_id}", xbmc.LOGWARNING)
        return None

    # Preserve server order – reflects the user's MediaFusion custom sort config.
    parsed = _parse_stream_options(stream_entries)

    if not parsed:
        _log(f"No direct (HTTP) streams for {catalog_type}/{video_id}", xbmc.LOGWARNING)
        return None

    best = parsed[0]
    _log(f"Selected stream (server order #1): {best['name']!r}")
    return {"url": best["url"], "headers": best["headers"], "name": best["name"]}
