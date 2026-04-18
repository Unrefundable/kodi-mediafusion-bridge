"""
KDMM – default.py
Plugin entry point.

Called by TMDb Bingie Helper when the user presses Play or Resume.
Runs as a standard Kodi resolver plugin: always exits via
xbmcplugin.setResolvedUrl() so Kodi manages playback internally and
all Player-subclass callbacks (onAVStarted, onPlayBackStopped, …) fire
correctly in service.py.

Flow:
  1. Parse media ID from URL params (IMDB ID + optional season/episode).
  2. Look up a cached stream URL.  On cache miss, query DMM's torrent
     database, check RD availability, resolve best, and save to cache.
  3. Read locally-stored resume position (populated by service.py).
  4. Set window properties so the background service can apply the resume seek
     after A/V playback has actually started (onAVStarted).
  5. Resolve to Kodi via xbmcplugin.setResolvedUrl().
"""

import json
import os
import sys
import threading
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
# Kodi 18+ passes "resume:false" as sys.argv[3] when PlayMedia(..., noresume)
# is used (i.e. "Play from Beginning").  Any other value (or absent) means
# the normal resume-from-saved-position behaviour applies.
NO_RESUME = len(sys.argv) > 3 and "resume:false" in sys.argv[3].lower()
sys.path.insert(0, os.path.join(_ADDON_PATH, "lib"))

from cache import StreamCache, ProgressCache        # noqa: E402 (after sys.path)
from dmm import fetch_all_cached_streams, is_stream_accessible    # noqa: E402
from rd_auth import authorize as rd_authorize, revoke as rd_revoke  # noqa: E402

# ------------------------------------------------------------------ #
# Constants – window property keys shared with service.py
# ------------------------------------------------------------------ #
WIN = xbmcgui.Window(10000)
PROP_MEDIA_ID = "kdmm.media_id"
PROP_RESUME_TIME = "kdmm.resume_time"
PROP_CANDIDATES = "kdmm.candidates"   # JSON list of all cached stream candidates


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM] {msg}", level)


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def _build_final_url(url, headers_dict):
    """
    Produce the final URL string for xbmc.Player().play().

    If the stream has proxy headers (e.g. an Authorization bearer token),
    we append them after a pipe in the format that Kodi's HTTP handler
    understands:
        https://example.com/stream.mp4|Authorization=Bearer+abc
    """
    if not headers_dict:
        return url, False

    formatted = urlencode(headers_dict)
    if not formatted:
        return url, False
    return f"{url}|{formatted}", True


def _play_stream(media_id, url, headers_dict, imdb, season, episode, no_resume=False):
    """
    Build the ListItem and resolve it to Kodi via setResolvedUrl.
    """
    final_url, has_headers = _build_final_url(url, headers_dict)

    li = xbmcgui.ListItem(path=final_url)
    li.setProperty("IsPlayable", "true")

    # Only force inputstream.ffmpegdirect for adaptive / container formats that
    # Kodi's native HTTP player can't handle.
    _url_path = url.split("?")[0].lower()
    _ADAPTIVE_EXTS = (".ts", ".mpd", ".m3u8")
    _needs_ffmpegdirect = any(_url_path.endswith(ext) for ext in _ADAPTIVE_EXTS)

    if has_headers and _needs_ffmpegdirect:
        li.setProperty("inputstream", "inputstream.ffmpegdirect")
        if _url_path.endswith(".ts"):
            li.setMimeType("video/mp2t")
        elif _url_path.endswith(".mpd"):
            li.setMimeType("application/dash+xml")
        elif _url_path.endswith(".m3u8"):
            li.setMimeType("application/vnd.apple.mpegurl")

    # Tag the item with its IMDB number for Trakt scrobbling.
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
    if no_resume:
        _log(f"Play from beginning requested for {media_id} – skipping resume seek")
        WIN.clearProperty(PROP_RESUME_TIME)
    elif resume_time > 5.0:
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

    # Determine catalog type and video_id.
    if action == "play_episode" and season and episode:
        catalog_type = "series"
        video_id = f"{imdb}:{season}:{episode}"
        media_id = video_id
    else:
        catalog_type = "movie"
        video_id = imdb
        media_id = imdb

    if not media_id:
        xbmcgui.Dialog().notification(
            "KDMM", "No media ID – check player JSON config",
            xbmcgui.NOTIFICATION_ERROR,
        )
        return

    ttl_hours = int(_ADDON.getSetting("stream_cache_ttl_hours") or "6")
    stream_cache = StreamCache(_USERDATA_PATH, ttl=ttl_hours * 3600)

    # ---- 1. Try stream cache ---------------------------------------- #
    candidates = None
    if not force_refresh:
        cached = stream_cache.get(media_id)
        if cached:
            _log(f"Cache hit for {media_id}")
            candidates = cached
            if _ADDON.getSetting("notify_cache_hit").lower() == "true":
                xbmcgui.Dialog().notification(
                    "KDMM", "Using cached stream",
                    xbmcgui.NOTIFICATION_INFO, 2000,
                )

    # ---- 2. Fetch fresh from DMM + RD if needed -------------------- #
    if candidates is None:
        _log(f"Cache miss – querying DMM + RD for {media_id}")

        fetch_result = {}
        cancel_event = threading.Event()

        def _fetch():
            try:
                fetch_result["candidates"] = fetch_all_cached_streams(
                    catalog_type, video_id, cancel_event=cancel_event)
            except Exception as exc:
                fetch_result["error"] = exc

        t = threading.Thread(target=_fetch, daemon=True)
        t.start()

        busy_dialog = xbmcgui.DialogProgress()
        busy_dialog.create("KDMM", "Finding best stream…")
        dots = 0
        while t.is_alive():
            if busy_dialog.iscanceled():
                cancel_event.set()
                busy_dialog.close()
                xbmcplugin.setResolvedUrl(ADDON_HANDLE, False, xbmcgui.ListItem())
                return
            dots = (dots + 1) % 4
            busy_dialog.update(0, f"Finding best stream{'.' * dots}")
            xbmc.sleep(500)
        busy_dialog.close()

        if "error" in fetch_result:
            _log(f"fetch_all_cached_streams raised: {fetch_result['error']}", xbmc.LOGERROR)
            xbmcgui.Dialog().notification(
                "KDMM", f"Error: {fetch_result['error']}",
                xbmcgui.NOTIFICATION_ERROR, 8000)
            xbmcplugin.setResolvedUrl(ADDON_HANDLE, False, xbmcgui.ListItem())
            return

        candidates = fetch_result.get("candidates") or []

        if not candidates:
            xbmcgui.Dialog().notification(
                "KDMM",
                "No cached streams found – check RD authorization",
                xbmcgui.NOTIFICATION_ERROR, 8000)
            xbmcplugin.setResolvedUrl(ADDON_HANDLE, False, xbmcgui.ListItem())
            return

        stream_cache.set(media_id, candidates)
        _log(f"Stored {len(candidates)} candidate(s) for {media_id}: {candidates[0]['name']!r}")

    # ---- 3. Ensure candidates is a list ----------------------------- #
    if isinstance(candidates, dict):
        candidates = [candidates]

    # ---- 4. Store full candidate list for service.py retry ----------- #
    chosen_idx = 0
    for i, c in enumerate(candidates):
        if is_stream_accessible(c["url"], c.get("headers") or {}):
            if i > 0:
                _log(f"Skipped {i} inaccessible candidate(s); using: {c['name']!r}")
            chosen_idx = i
            break
        _log(f"Candidate {i} ({c['name']!r}) is too small – skipping", xbmc.LOGWARNING)
    else:
        _log("All candidates failed size check – falling back to first", xbmc.LOGWARNING)
        chosen_idx = 0

    remaining = candidates[chosen_idx:]
    WIN.setProperty(PROP_CANDIDATES, json.dumps(remaining))

    # ---- 5. Play first remaining candidate -------------------------- #
    stream = remaining[0]
    _play_stream(
        media_id=media_id,
        url=stream["url"],
        headers_dict=stream.get("headers") or {},
        imdb=imdb,
        season=season,
        episode=episode,
        no_resume=NO_RESUME,
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
        media_id = None

    StreamCache(_USERDATA_PATH).clear(media_id)
    label = media_id if media_id else "all entries"
    xbmcgui.Dialog().notification(
        "KDMM",
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
            "KDMM", "Provide imdb param to clear progress",
            xbmcgui.NOTIFICATION_WARNING,
        )
        return

    pc = ProgressCache(_USERDATA_PATH)
    pc.set_progress(media_id, 0.0, watched=False)
    xbmcgui.Dialog().notification(
        "KDMM",
        f"Resume position cleared for {media_id}",
        xbmcgui.NOTIFICATION_INFO,
    )


# ------------------------------------------------------------------ #
# Router
# ------------------------------------------------------------------ #

def action_main_menu():
    """
    Show a simple main menu when the addon is launched directly.
    """
    items = [
        ("Clear Stream Cache",
         "Force re-fetch stream URLs on next play",
         "plugin://plugin.video.kdmm/?action=clear_cache"),
    ]

    listing = []
    for label, label2, url in items:
        li = xbmcgui.ListItem(label=label, label2=label2)
        li.setProperty("IsPlayable", "false")
        listing.append((url, li, False))

    xbmcplugin.setContent(ADDON_HANDLE, "files")
    xbmcplugin.addDirectoryItems(ADDON_HANDLE, listing, len(listing))
    xbmcplugin.endOfDirectory(ADDON_HANDLE)


def addon_router():
    params = dict(parse_qsl(sys.argv[2][1:])) if len(sys.argv) > 2 else {}
    action = params.get("action", "")

    if action in ("play_movie", "play_episode", "play"):
        action_play(params)
    elif action == "authorize_rd":
        rd_authorize()
    elif action == "revoke_rd":
        rd_revoke()
        xbmcgui.Dialog().notification("KDMM", "Real-Debrid authorization revoked",
                                       xbmcgui.NOTIFICATION_INFO)
    elif action == "clear_cache":
        action_clear_cache(params)
    elif action == "clear_progress":
        action_clear_progress(params)
    else:
        action_main_menu()


if __name__ == "__main__":
    addon_router()
