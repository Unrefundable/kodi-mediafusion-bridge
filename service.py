"""
MediaFusion Bridge – service.py
Background service that runs for the lifetime of Kodi.

Responsibilities
────────────────
1. RESUME-SEEK
   When the bridge plugin resolves a stream it sets two global window
   properties before calling xbmc.Player().play():
       mfbridge.media_id    – e.g. "tt1234567" or "tt1234567:1:2"
       mfbridge.resume_time – float seconds (only when > 5.0)

   BridgePlayer.onAVStarted() reads these properties, clears them, and
   calls seekTime() so the video starts at the correct position.

2. PROGRESS TRACKING
   BridgePlayer.onPlayBackStopped() and .onPlayBackEnded() save the
   current playback time to ProgressCache so the bridge knows where to
   resume on next play.

3. BROKEN STREAM RECOVERY
   BridgePlayer.onPlayBackError() clears the cached stream URL for an
   item so the next play attempt fetches a fresh one from MediaFusion.
"""

import os
import sys

import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

# ------------------------------------------------------------------ #
# Bootstrap
# ------------------------------------------------------------------ #
_ADDON = xbmcaddon.Addon()
_ADDON_ID = _ADDON.getAddonInfo("id")
_ADDON_PATH = _ADDON.getAddonInfo("path")
_USERDATA_PATH = xbmcvfs.translatePath(
    f"special://profile/addon_data/{_ADDON_ID}/"
)
sys.path.insert(0, os.path.join(_ADDON_PATH, "lib"))

from cache import StreamCache, ProgressCache   # noqa: E402

# ------------------------------------------------------------------ #
# Window property keys (must match default.py)
# ------------------------------------------------------------------ #
WIN = xbmcgui.Window(10000)
PROP_MEDIA_ID = "mfbridge.media_id"
PROP_RESUME_TIME = "mfbridge.resume_time"

# How many seconds from end-of-file to count as "watched" (default 5 %).
# Computed per-item using total_time; this is the minimum absolute margin.
WATCHED_MARGIN_SECONDS = 60


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[MFBridge Service] {msg}", level)


# ------------------------------------------------------------------ #
# Player monitor
# ------------------------------------------------------------------ #

class BridgePlayer(xbmc.Player):
    """
    Subclass of xbmc.Player to receive playback lifecycle events.
    Only acts on playback that was initiated by the bridge plugin
    (identified by the PROP_MEDIA_ID window property being set).
    """

    def __init__(self):
        super().__init__()
        # Set when bridge-managed playback is active; cleared when done.
        self._current_media_id = None
        # Continuously updated while playing (getTime() returns 0 in stop callbacks).
        self._last_known_time = 0.0
        self._last_known_total = 0.0
        self._stream_cache = StreamCache(_USERDATA_PATH)
        self._progress_cache = ProgressCache(_USERDATA_PATH)

    # ---------------------------------------------------------------- #
    # Called by the monitor loop every few seconds while playing
    # ---------------------------------------------------------------- #

    def tick(self):
        """
        Poll getTime() while playback is active.
        Must NOT be called from within a Player callback – only from the
        monitor loop so we always have a fresh position for _save_progress.
        """
        if self._current_media_id and self.isPlaying():
            try:
                t = self.getTime()
                total = self.getTotalTime()
                if t > 0:
                    self._last_known_time = t
                if total > 0:
                    self._last_known_total = total
            except Exception:
                pass

    # ---------------------------------------------------------------- #
    # Playback lifecycle callbacks
    # ---------------------------------------------------------------- #

    def onAVStarted(self):
        """
        Called once the A/V tracks are decoded and playback has actually
        begun.  This is the right moment to seek because the player is
        ready to accept seekTime() calls.
        """
        # Only act when the bridge initiated this playback.
        media_id = WIN.getProperty(PROP_MEDIA_ID)
        if not media_id:
            return

        # Claim ownership so stop/end handlers know to save progress.
        self._current_media_id = media_id
        self._last_known_time = 0.0
        self._last_known_total = 0.0
        WIN.clearProperty(PROP_MEDIA_ID)

        # Apply the resume seek if one was queued.
        resume_str = WIN.getProperty(PROP_RESUME_TIME)
        WIN.clearProperty(PROP_RESUME_TIME)

        if resume_str:
            try:
                resume_time = float(resume_str)
            except (ValueError, TypeError):
                resume_time = 0.0

            if resume_time > 5.0:
                _log(f"Applying resume seek to {resume_time:.1f}s for {media_id}")
                # Run seek in a background thread so we don’t block the callback.
                import threading
                def _seek():
                    xbmc.sleep(1200)
                    try:
                        self.seekTime(resume_time)
                    except Exception as exc:
                        _log(f"seekTime() failed: {exc}", xbmc.LOGWARNING)
                threading.Thread(target=_seek, daemon=True).start()

    def onPlayBackStopped(self):
        """User stopped the video manually – save current position."""
        self._save_progress(is_ended=False)

    def onPlayBackEnded(self):
        """Video played to the end – mark as watched (clear resume)."""
        self._save_progress(is_ended=True)

    def onPlayBackError(self):
        """
        Playback failed.  Clear the cached stream URL so the next attempt
        re-fetches a fresh one from MediaFusion.
        """
        media_id = self._current_media_id or WIN.getProperty(PROP_MEDIA_ID)
        if media_id:
            _log(
                f"Playback error – clearing stream cache for {media_id}",
                xbmc.LOGWARNING,
            )
            self._stream_cache.clear(media_id)
            xbmcgui.Dialog().notification(
                "MF Bridge",
                "Stream failed – cached URL cleared.  Press play to retry.",
                xbmcgui.NOTIFICATION_WARNING,
            )
        self._current_media_id = None
        WIN.clearProperty(PROP_MEDIA_ID)
        WIN.clearProperty(PROP_RESUME_TIME)

    # ---------------------------------------------------------------- #
    # Private helpers
    # ---------------------------------------------------------------- #

    def _save_progress(self, is_ended):
        """
        Persist the playback position to ProgressCache.
        Uses _last_known_time (updated by tick()) instead of getTime() because
        getTime() returns 0 inside onPlayBackStopped – the player has already
        torn down by the time that callback fires.
        """
        media_id = self._current_media_id
        if not media_id:
            return
        self._current_media_id = None  # release ownership first

        current_time = self._last_known_time
        total_time = self._last_known_total

        if total_time <= 0 or current_time <= 0:
            _log(f"No valid time data for {media_id} – progress not saved", xbmc.LOGWARNING)
            return

        near_end = (total_time - current_time) < WATCHED_MARGIN_SECONDS
        if is_ended or near_end:
            _log(f"Marking {media_id} as watched")
            self._progress_cache.set_progress(
                media_id, 0.0, total_time=total_time, watched=True
            )
        elif current_time > 5.0:
            _log(f"Saving resume position {current_time:.1f}s / {total_time:.1f}s for {media_id}")
            self._progress_cache.set_progress(
                media_id, current_time, total_time=total_time, watched=False
            )


# ------------------------------------------------------------------ #
# Service entry point
# ------------------------------------------------------------------ #

class BridgeMonitor(xbmc.Monitor):
    def __init__(self):
        super().__init__()
        # Instantiate the player so it stays alive and can receive events.
        self.player = BridgePlayer()

    def run(self):
        _log("Service started")
        while not self.abortRequested():
            # Update the player’s last-known time every 5 s while playing.
            # This is the only reliable way to get the position at stop-time
            # because getTime() returns 0 inside onPlayBackStopped.
            self.player.tick()
            self.waitForAbort(5)
        _log("Service stopped")


if __name__ == "__main__":
    monitor = BridgeMonitor()
    monitor.run()
