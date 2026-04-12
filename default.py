"""
MediaFusion Bridge – default.py
Plugin entry point.

Called by TMDb Bingie Helper when the user presses Play or Resume.
Runs as a standard Kodi resolver plugin: always exits via
xbmcplugin.setResolvedUrl() so Kodi manages playback internally and
all Player-subclass callbacks (onAVStarted, onPlayBackStopped, …) fire
correctly in service.py.

Flow:
  1. Parse media ID from URL params (IMDB ID + optional season/episode).
  2. Look up a cached stream URL.  On cache miss, query MediaFusion's
     /kodi/stream/ API, smart-sort, pick best, and save to cache.
  3. Read locally-stored resume position (populated by service.py).
  4. Set window properties so the background service can apply the resume seek
     after A/V playback has actually started (onAVStarted).
  5. Resolve to Kodi via xbmcplugin.setResolvedUrl().
"""

import json
import os
import sys
from urllib.parse import parse_qsl, urlencode

import xbmc
import xbmcaddon
import xbmcgui
import xbmcplugin
import xbmcvfs

# ------------------------------------------------------------------ #
# Bootstrap: add lib/ to sys.path before importing local modules.
# ------------------------------------------------------------------ #
_ADDON = xbmcaddon.Addon()
_ADDON_ID = _ADDON.getAddonInfo("id")
_ADDON_PATH = _ADDON.getAddonInfo("path")
_USERDATA_PATH = xbmcvfs.translatePath(
    f"special://profile/addon_data/{_ADDON_ID}/"
)
# Plugin handle – valid when Kodi invoked us as a resolver.
ADDON_HANDLE = int(sys.argv[1]) if len(sys.argv) > 1 else -1
sys.path.insert(0, os.path.join(_ADDON_PATH, "lib"))

from cache import StreamCache, ProgressCache        # noqa: E402 (after sys.path)
from mediafusion import fetch_best_stream           # noqa: E402

# ------------------------------------------------------------------ #
# Constants – window property keys shared with service.py
# ------------------------------------------------------------------ #
WIN = xbmcgui.Window(10000)
PROP_MEDIA_ID = "mfbridge.media_id"
PROP_RESUME_TIME = "mfbridge.resume_time"


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[MFBridge] {msg}", level)


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def _build_final_url(url, headers_dict):
    """
    Produce the final URL string for xbmc.Player().play().

    If the stream has proxy headers (e.g. an Authorization bearer token for
    Real-Debrid), we append them after a pipe in the format that Kodi's
    inputstream.ffmpegdirect / HTTP handler understands:
        https://example.com/stream.mp4|Authorization=Bearer+abc
    """
    if not headers_dict:
        return url, False

    # urlencode the headers dict → "Key1=val1&Key2=val2"
    formatted = urlencode(headers_dict)
    if not formatted:
        return url, False
    return f"{url}|{formatted}", True


def _play_stream(media_id, url, headers_dict, imdb, season, episode):
    """
    Build the ListItem and resolve it to Kodi via setResolvedUrl.
    Using setResolvedUrl (instead of xbmc.Player().play) lets Kodi manage
    playback internally, which guarantees that onAVStarted / onPlayBackStopped
    are dispatched to the BridgePlayer instance in service.py.
    """
    final_url, has_headers = _build_final_url(url, headers_dict)

    li = xbmcgui.ListItem(path=final_url)
    li.setProperty("IsPlayable", "true")

    if has_headers:
        li.setProperty("inputstream", "inputstream.ffmpegdirect")
        if url.endswith(".ts"):
            li.setMimeType("video/mp2t")
        elif url.endswith(".mpd"):
            li.setMimeType("application/dash+xml")
        elif url.endswith(".m3u8"):
            li.setMimeType("application/vnd.apple.mpegurl")

    # Tag the item with its IMDB number so that script.trakt (and any other
    # Trakt-aware service) can scrobble progress correctly – exactly like
    # MediaFusion's own _resolve_playback() does.
    if imdb:
        li.setInfo("video", {"imdbnumber": imdb})
        WIN.setProperty("script.trakt.ids", json.dumps({"imdb": imdb}))

    if season and episode:
        try:
            li.setInfo("video", {"season": int(season), "episode": int(episode)})
        except (TypeError, ValueError):
            pass

    # Tell the service which item is playing so it can save progress later.
    WIN.setProperty(PROP_MEDIA_ID, media_id)

    # Queue a resume seek via window property; service.py reads it in onAVStarted.
    progress_cache = ProgressCache(_USERDATA_PATH)
    resume_time = progress_cache.get_resume_time(media_id)
    if resume_time > 5.0:
        _log(f"Queuing resume seek to {resume_time:.1f}s for {media_id}")
        WIN.setProperty(PROP_RESUME_TIME, str(resume_time))
    else:
        WIN.clearProperty(PROP_RESUME_TIME)

    _log(f"Resolving stream to Kodi: {final_url[:80]}…")
    xbmcplugin.setResolvedUrl(ADDON_HANDLE, True, li)


# ------------------------------------------------------------------ #
# Actions
# ------------------------------------------------------------------ #

def action_play(params):
    """
    Core handler for play_movie / play_episode.
    Resolves or re-uses a cached stream URL and starts playback.
    """
    action = params.get("action", "play_movie")
    imdb = params.get("imdb", "").strip()
    season = params.get("season", "").strip()
    episode = params.get("episode", "").strip()
    force_refresh = params.get("refresh", "0") == "1"

    # Determine MediaFusion catalog type and video_id.
    if action == "play_episode" and season and episode:
        catalog_type = "series"
        video_id = f"{imdb}:{season}:{episode}"
        media_id = video_id          # used as cache key
    else:
        catalog_type = "movie"
        video_id = imdb
        media_id = imdb

    if not media_id:
        xbmcgui.Dialog().notification(
            "MF Bridge", "No media ID – check player JSON config",
            xbmcgui.NOTIFICATION_ERROR,
        )
        return

    stream_cache = StreamCache(_USERDATA_PATH)

    # ---- 1. Try stream cache ---------------------------------------- #
    stream = None
    if not force_refresh:
        cached = stream_cache.get(media_id)
        if cached:
            _log(f"Cache hit for {media_id}")
            stream = cached           # {"url": ..., "headers": {...}}

    # ---- 2. Fetch fresh from MediaFusion if needed ------------------- #
    if stream is None:
        _log(f"Cache miss – fetching from MediaFusion for {media_id}")

        busy_dialog = xbmcgui.DialogProgress()
        busy_dialog.create("MediaFusion Bridge", "Finding best stream…")
        try:
            fresh = fetch_best_stream(catalog_type, video_id)
        finally:
            busy_dialog.close()

        if not fresh:
            xbmcgui.Dialog().notification(
                "MF Bridge",
                "No streams found – check MediaFusion / Real-Debrid",
                xbmcgui.NOTIFICATION_ERROR,
            )
            return

        stream_cache.set(media_id, fresh["url"], fresh.get("headers") or {})
        stream = {"url": fresh["url"], "headers": fresh.get("headers") or {}}
        _log(f"Cached new stream for {media_id}: {fresh['name']!r}")

    # ---- 3. Play ---------------------------------------------------- #
    _play_stream(
        media_id=media_id,
        url=stream["url"],
        headers_dict=stream.get("headers") or {},
        imdb=imdb,
        season=season,
        episode=episode,
    )


def action_clear_cache(params):
    """Clear the stream cache for one item (or all items)."""
    imdb = params.get("imdb", "").strip()
    season = params.get("season", "").strip()
    episode = params.get("episode", "").strip()

    if imdb and season and episode:
        media_id = f"{imdb}:{season}:{episode}"
    elif imdb:
        media_id = imdb
    else:
        media_id = None  # clear everything

    StreamCache(_USERDATA_PATH).clear(media_id)
    label = media_id if media_id else "all entries"
    xbmcgui.Dialog().notification(
        "MF Bridge",
        f"Stream cache cleared ({label})",
        xbmcgui.NOTIFICATION_INFO,
    )


def action_clear_progress(params):
    """Reset the locally-stored resume position for one item."""
    imdb = params.get("imdb", "").strip()
    season = params.get("season", "").strip()
    episode = params.get("episode", "").strip()

    if imdb and season and episode:
        media_id = f"{imdb}:{season}:{episode}"
    elif imdb:
        media_id = imdb
    else:
        xbmcgui.Dialog().notification(
            "MF Bridge", "Provide imdb param to clear progress",
            xbmcgui.NOTIFICATION_WARNING,
        )
        return

    pc = ProgressCache(_USERDATA_PATH)
    pc.set_progress(media_id, 0.0, watched=False)
    xbmcgui.Dialog().notification(
        "MF Bridge",
        f"Resume position cleared for {media_id}",
        xbmcgui.NOTIFICATION_INFO,
    )


# ------------------------------------------------------------------ #
# Router
# ------------------------------------------------------------------ #

def addon_router():
    params = dict(parse_qsl(sys.argv[2][1:])) if len(sys.argv) > 2 else {}
    action = params.get("action", "")

    if action in ("play_movie", "play_episode", "play"):
        action_play(params)
    elif action == "clear_cache":
        action_clear_cache(params)
    elif action == "clear_progress":
        action_clear_progress(params)
    else:
        xbmcgui.Dialog().notification(
            "MediaFusion Bridge",
            "Install player JSON into TMDb Bingie Helper players folder.",
            xbmcgui.NOTIFICATION_INFO,
        )


if __name__ == "__main__":
    addon_router()
