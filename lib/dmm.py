"""
KDMM – lib/dmm.py
Debrid Media Manager torrent lookup + Real-Debrid stream resolver.

Flow:
  1.  Generate a DMM proof-of-work token (port of their JS generateTokenAndHash).
  2.  GET  debridmediamanager.com/api/torrents/movie (or tv)
      → returns every known torrent hash for the IMDB ID.
  3.  POST api.real-debrid.com/rest/1.0/torrents/instantAvailability/{hashes}
      → filter to only RD-cached hashes.
  4.  Sort by preferred groups, then by file size (largest first).
  5.  For the chosen hash, resolve a direct-play URL via:
        POST /torrents/addMagnet → POST /torrents/selectFiles →
        GET  /torrents/info      → POST /unrestrict/link
      → returns a direct CDN link.
"""

import math
import sys
import os
import time as _time

import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

_DMM_SALT = "debridmediamanager.com%%fe7#td00rA3vHz%VmI"
_DMM_BASE = "https://debridmediamanager.com"
_RD_BASE = "https://api.real-debrid.com/rest/1.0"

# Remux groups known for high-quality encode / remux work.
_PREFERRED_GROUPS = ("framestor", "cinephiles", "triton")

# Minimum file size to distinguish real content from RD error clips.
_MIN_STREAM_BYTES = 50 * 1024 * 1024  # 50 MB


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM] {msg}", level)


def _get_requests():
    """Import requests with all Kodi addon module paths on sys.path."""
    addon_dir = xbmcvfs.translatePath("special://home/addons")
    for mod in ("script.module.requests", "script.module.urllib3",
                "script.module.chardet", "script.module.certifi",
                "script.module.idna"):
        lib = os.path.join(addon_dir, mod, "lib")
        if os.path.isdir(lib) and lib not in sys.path:
            sys.path.insert(0, lib)
    import requests
    try:
        import certifi
        os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
    except Exception:
        pass
    return requests


# ------------------------------------------------------------------ #
# DMM token generation  (port of src/utils/token.ts)
# ------------------------------------------------------------------ #

def _dmm_hash(s):
    """Port of DMM's custom 32-bit hash function."""
    h1 = 0xDEADBEEF ^ len(s)
    h2 = 0x41C6CE57 ^ len(s)
    for ch in s:
        c = ord(ch)
        h1 = _imul(h1 ^ c, 0x9E3779B1) & 0xFFFFFFFF
        h2 = _imul(h2 ^ c, 0x5F356495) & 0xFFFFFFFF
        h1 = ((h1 << 5) | (h1 >> 27)) & 0xFFFFFFFF
        h2 = ((h2 << 5) | (h2 >> 27)) & 0xFFFFFFFF

    h1 = (h1 + _imul(h2, 0x5D588B65)) & 0xFFFFFFFF
    h2 = (h2 + _imul(h1, 0x78A76A79)) & 0xFFFFFFFF
    return format((h1 ^ h2) & 0xFFFFFFFF, "x")


def _imul(a, b):
    """Emulate JavaScript Math.imul (signed 32-bit multiply)."""
    a &= 0xFFFFFFFF
    b &= 0xFFFFFFFF
    result = (a * b) & 0xFFFFFFFF
    if result >= 0x80000000:
        result -= 0x100000000
    # we need unsigned for bit shifts later
    return result & 0xFFFFFFFF


def _combine_hashes(h1, h2):
    """Port of DMM's combineHashes (interleave + reverse)."""
    half = len(h1) // 2
    fp1, sp1 = h1[:half], h1[half:]
    fp2, sp2 = h2[:half], h2[half:]

    obfuscated = ""
    for i in range(half):
        obfuscated += fp1[i] + fp2[i]
    obfuscated += sp2[::-1] + sp1[::-1]
    return obfuscated


def _get_rd_timestamp(api_token=None):
    """
    Fetch the current unix timestamp from Real-Debrid's time API.
    DMM's server uses this same source for token validation, so we must too.
    Falls back to local time if the request fails.
    """
    try:
        requests = _get_requests()
        headers = _rd_headers(api_token) if api_token else {}
        resp = requests.get(f"{_RD_BASE}/time/iso", headers=headers, timeout=5)
        resp.raise_for_status()
        from datetime import datetime, timezone
        iso = resp.text.strip().strip('"')
        dt = datetime.fromisoformat(iso.replace('Z', '+00:00'))
        return int(dt.timestamp())
    except Exception as exc:
        _log(f"RD time fetch failed, using local clock: {exc}", xbmc.LOGWARNING)
        return int(_time.time())


def _generate_token_and_hash(api_token=None):
    """
    Generate a (tokenWithTimestamp, combinedHash) pair accepted by DMM's API.
    Timestamp is sourced from RD's time API to stay in sync with DMM's validation.
    """
    import random
    token = format(random.getrandbits(32), "x")
    timestamp = _get_rd_timestamp(api_token)
    token_with_ts = f"{token}-{timestamp}"
    ts_hash = _dmm_hash(token_with_ts)
    salt_hash = _dmm_hash(f"{_DMM_SALT}-{token}")
    return token_with_ts, _combine_hashes(ts_hash, salt_hash)


# ------------------------------------------------------------------ #
# Real-Debrid helpers
# ------------------------------------------------------------------ #

def _rd_key():
    """Return a valid RD access token (OAuth), refreshing if needed."""
    from rd_auth import get_access_token
    return get_access_token()


def _validate_rd_token(api_token):
    """
    Verify the token is accepted by RD by calling GET /user.
    Returns True if valid, False if 401/403, None on network error.
    """
    requests = _get_requests()
    try:
        resp = requests.get(f"{_RD_BASE}/user", headers=_rd_headers(api_token), timeout=10)
        if resp.status_code in (401, 403):
            _log(f"Token validation failed: HTTP {resp.status_code} – {resp.text[:80]}",
                 xbmc.LOGWARNING)
            return False
        return resp.status_code == 200
    except Exception as exc:
        _log(f"Token validation network error: {exc}", xbmc.LOGWARNING)
        return None  # can't confirm either way


def _rd_headers(api_token):
    return {"Authorization": f"Bearer {api_token}"}


def _rd_get(path, api_token, timeout=15):
    requests = _get_requests()
    r = requests.get(f"{_RD_BASE}{path}", headers=_rd_headers(api_token), timeout=timeout)
    r.raise_for_status()
    return r.json()


def _rd_post(path, api_token, data=None, timeout=15):
    requests = _get_requests()
    r = requests.post(f"{_RD_BASE}{path}", headers=_rd_headers(api_token),
                      data=data or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _rd_delete(path, api_token, timeout=15):
    requests = _get_requests()
    requests.delete(f"{_RD_BASE}{path}", headers=_rd_headers(api_token), timeout=timeout)


# ------------------------------------------------------------------ #
# DMM hash database query
# ------------------------------------------------------------------ #

def _fetch_dmm_hashes(imdb_id, media_type="movie", max_size=0, page=0, api_token=None):
    """
    Query DMM's torrent database for all known hashes for an IMDB ID.
    Returns list of dicts: [{hash, title, fileSize, files, ...}, ...]
    """
    token_ts, solution = _generate_token_and_hash(api_token)
    endpoint = "movie" if media_type == "movie" else "tv"
    url = (
        f"{_DMM_BASE}/api/torrents/{endpoint}"
        f"?imdbId={imdb_id}"
        f"&dmmProblemKey={token_ts}"
        f"&solution={solution}"
        f"&onlyTrusted=false"
        f"&maxSize={max_size}"
        f"&page={page}"
    )

    _log(f"Querying DMM hash DB for {imdb_id} ({media_type})")
    requests = _get_requests()
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results") or []
    _log(f"DMM returned {len(results)} torrent(s) for {imdb_id}")
    return results


# ------------------------------------------------------------------ #
# RD instant availability check
# ------------------------------------------------------------------ #

def _check_rd_availability(hashes, api_token):
    """
    Check which hashes are instantly available (cached) on Real-Debrid.
    Returns a dict {hash: files_list} for cached hashes, and
    raises PermissionError on 401/403 so the caller can handle auth failures.
    """
    requests = _get_requests()
    cached = {}
    video_exts = (".mkv", ".mp4", ".avi", ".m4v", ".webm", ".ts")

    for i in range(0, len(hashes), 100):
        batch = hashes[i:i + 100]
        hash_path = "/".join(batch)
        url = f"{_RD_BASE}/torrents/instantAvailability/{hash_path}"
        try:
            resp = requests.get(url, headers=_rd_headers(api_token), timeout=20)
            if resp.status_code in (401, 403):
                # Endpoint may be deprecated; caller decides if this is auth failure
                _log(f"instantAvailability returned {resp.status_code}: {resp.text[:80]}",
                     xbmc.LOGWARNING)
                return cached  # return what we have so far (probably empty)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            _log(f"RD availability check failed for batch {i}: {exc}", xbmc.LOGWARNING)
            continue

        for h in batch:
            # RD may return keys in upper or lower case
            info = data.get(h) or data.get(h.lower()) or data.get(h.upper()) or {}
            # Some RD responses wrap in a list
            if isinstance(info, list):
                info = info[0] if info else {}
            rd_entries = info.get("rd") or []
            if not rd_entries:
                continue
            best_variant = None
            best_size = 0
            for variant in rd_entries:
                for fid, finfo in variant.items():
                    fname = finfo.get("filename", "").lower()
                    fsize = finfo.get("filesize", 0)
                    if any(fname.endswith(e) for e in video_exts) and fsize > best_size:
                        best_variant = variant
                        best_size = fsize
            if best_variant:
                files = []
                for fid, finfo in best_variant.items():
                    files.append({
                        "file_id": int(fid),
                        "filename": finfo.get("filename", ""),
                        "filesize": finfo.get("filesize", 0),
                    })
                cached[h] = files

    return cached


def _availability_is_usable(api_token, hashes):
    """
    Returns (cached_dict, available) where available=False means the
    instantAvailability endpoint is broken/deprecated and we should skip it.
    """
    try:
        cached = _check_rd_availability(hashes, api_token)
        return cached, True
    except Exception as exc:
        _log(f"instantAvailability unavailable: {exc}", xbmc.LOGWARNING)
        return {}, False


def _resolve_by_direct_add(candidates_info, api_token, season=None, episode=None,
                           max_resolve=3, cancel_event=None):
    """
    Resolve streams by adding magnets to RD and checking for instant cache.
    Uses tight 5s timeouts per request.  Checks at most `len(candidates_info)`
    candidates (caller should already limit to 5).
    cancel_event: threading.Event – set it to abort early.
    """
    import time as _t
    resolved = []
    video_exts = (".mkv", ".mp4", ".avi", ".m4v", ".webm", ".ts")
    ep_markers = []
    if season and episode:
        ep_markers = [
            f"s{int(season):02d}e{int(episode):02d}",
            f"{season}x{int(episode):02d}",
        ]

    for idx, c in enumerate(candidates_info):
        if cancel_event and cancel_event.is_set():
            _log("Direct-add cancelled")
            break
        if len(resolved) >= max_resolve:
            break

        rd_id = None
        _log(f"Direct-add attempt {idx+1}/{len(candidates_info)}: {c['hash'][:8]}…")
        try:
            magnet = f"magnet:?xt=urn:btih:{c['hash']}"
            resp = _rd_post("/torrents/addMagnet", api_token,
                            data={"magnet": magnet}, timeout=8)
            rd_id = resp.get("id")
            if not rd_id:
                continue

            info = _rd_get(f"/torrents/info/{rd_id}", api_token, timeout=8)
            status = info.get("status", "")

            # If already downloaded (was in cache), skip file selection
            if status == "downloaded":
                pass  # fall through to link handling below
            elif status == "waiting_files_selection":
                # Pick the best video file
                files = info.get("files") or []
                best_file_id = None
                best_size = 0
                for f in files:
                    fname = f.get("path", "").lower()
                    fsize = f.get("bytes", 0)
                    if not any(fname.endswith(e) for e in video_exts):
                        continue
                    if ep_markers and not any(m in fname for m in ep_markers):
                        continue
                    if fsize > best_size:
                        best_size = fsize
                        best_file_id = f.get("id")
                if not best_file_id:
                    for f in files:
                        fname = f.get("path", "").lower()
                        fsize = f.get("bytes", 0)
                        if any(fname.endswith(e) for e in video_exts) and fsize > best_size:
                            best_size = fsize
                            best_file_id = f.get("id")
                if not best_file_id:
                    _rd_delete(f"/torrents/delete/{rd_id}", api_token, timeout=5)
                    continue

                _rd_post(f"/torrents/selectFiles/{rd_id}", api_token,
                         data={"files": str(best_file_id)}, timeout=8)
                _t.sleep(1.5)
                info = _rd_get(f"/torrents/info/{rd_id}", api_token, timeout=8)
                if info.get("status") != "downloaded":
                    _log(f"{c['hash'][:8]} not cached (status={info.get('status')!r})")
                    _rd_delete(f"/torrents/delete/{rd_id}", api_token, timeout=5)
                    continue
            else:
                # Not cached – magnet_conversion / queued / etc.
                _log(f"{c['hash'][:8]} status={status!r} – not instantly cached")
                _rd_delete(f"/torrents/delete/{rd_id}", api_token, timeout=5)
                continue

            links = info.get("links") or []
            if not links:
                _rd_delete(f"/torrents/delete/{rd_id}", api_token, timeout=5)
                continue

            unrestrict = _rd_post("/unrestrict/link", api_token,
                                   data={"link": links[0]}, timeout=8)
            url = unrestrict.get("download")
            filename = unrestrict.get("filename", c.get("title", "Stream"))
            _rd_delete(f"/torrents/delete/{rd_id}", api_token, timeout=5)

            if url:
                _log(f"Direct-add resolve OK: {filename!r}")
                resolved.append({"url": url, "headers": {}, "name": filename})

        except Exception as exc:
            _log(f"Direct-add failed for {c.get('hash','')[:8]}: {exc}", xbmc.LOGWARNING)
            if rd_id:
                try:
                    _rd_delete(f"/torrents/delete/{rd_id}", api_token, timeout=5)
                except Exception:
                    pass

    return resolved


# ------------------------------------------------------------------ #
# ------------------------------------------------------------------ #
# RD stream resolution (hash → playable URL)
# ------------------------------------------------------------------ #

def _resolve_rd_stream(torrent_hash, file_id, api_token):
    """
    Turn a cached torrent hash into a direct-play URL via Real-Debrid.
    Steps:  addMagnet → selectFiles → torrentInfo → unrestrictLink
    Returns (url, filename) or (None, None) on failure.
    """
    rd_id = None
    try:
        # 1. Add the magnet (using hash directly; RD accepts bare hashes)
        magnet = f"magnet:?xt=urn:btih:{torrent_hash}"
        resp = _rd_post("/torrents/addMagnet", api_token, data={"magnet": magnet})
        rd_id = resp.get("id")
        if not rd_id:
            _log("addMagnet returned no id", xbmc.LOGERROR)
            return None, None

        # 2. Select the target file
        _rd_post(f"/torrents/selectFiles/{rd_id}", api_token,
                 data={"files": str(file_id)})

        # 3. Wait briefly then get torrent info (with links)
        import time
        time.sleep(0.5)
        info = _rd_get(f"/torrents/info/{rd_id}", api_token)

        if info.get("status") != "downloaded":
            _log(f"Torrent status={info.get('status')!r}, expected 'downloaded'", xbmc.LOGWARNING)
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            return None, None

        links = info.get("links") or []
        if not links:
            _log("No links in torrent info", xbmc.LOGWARNING)
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            return None, None

        # 4. Unrestrict the first link to get a direct CDN URL
        unrestrict = _rd_post("/unrestrict/link", api_token, data={"link": links[0]})
        download_url = unrestrict.get("download")
        filename = unrestrict.get("filename", "Stream")

        # 5. Clean up – delete the torrent from RD library
        _rd_delete(f"/torrents/delete/{rd_id}", api_token)

        if not download_url:
            _log("unrestrict/link returned no download URL", xbmc.LOGERROR)
            return None, None

        return download_url, filename

    except Exception as exc:
        _log(f"RD resolve failed: {exc}", xbmc.LOGERROR)
        if rd_id:
            try:
                _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            except Exception:
                pass
        return None, None


# ------------------------------------------------------------------ #
# Public API
# ------------------------------------------------------------------ #

def is_stream_accessible(url, headers):
    """
    Return True when the stream URL points to real video content.
    Returns False only when Content-Length is known and too small.
    """
    _req = _get_requests()
    try:
        resp = _req.head(url, headers=headers, timeout=6, allow_redirects=True)
        cl = int(resp.headers.get("content-length", -1))
        if cl < 0:
            resp2 = _req.get(url, headers={**headers, "Range": "bytes=0-0"},
                             timeout=6, stream=True, allow_redirects=True)
            cr = resp2.headers.get("content-range", "")
            if "/" in cr:
                cl = int(cr.split("/")[-1])
        if cl < 0:
            return True  # unknown → allow
        accessible = cl >= _MIN_STREAM_BYTES
        if not accessible:
            _log(
                f"Stream rejected: {cl // 1024 // 1024} MB < "
                f"{_MIN_STREAM_BYTES // 1024 // 1024} MB threshold ({url[:60]}…)",
                xbmc.LOGWARNING,
            )
        return accessible
    except Exception:
        return True


def fetch_all_cached_streams(catalog_type, video_id, cancel_event=None):
    """
    Main entry point.  Queries DMM's hash database, checks RD availability,
    resolves each cached hash to a direct-play URL, and returns a sorted
    list of {"url", "headers", "name"} candidates.

    catalog_type: "movie" or "series"
    video_id:     "tt1234567" for movies, "tt1234567:1:2" for episodes
    """
    api_token = _rd_key()
    if not api_token:
        xbmcgui.Dialog().notification(
            "KDMM",
            "No Real-Debrid API token – authorize in addon settings",
            xbmcgui.NOTIFICATION_ERROR, 8000)
        return []

    # Validate token before doing anything else
    token_ok = _validate_rd_token(api_token)
    if token_ok is False:
        xbmcgui.Dialog().notification(
            "KDMM", "Real-Debrid auth failed – re-authorize in addon settings",
            xbmcgui.NOTIFICATION_ERROR, 8000)
        return []
    # token_ok is None means network error — let's try anyway

    # Parse video_id: for series it's "imdb:season:episode"
    parts = video_id.split(":")
    imdb_id = parts[0]
    season = parts[1] if len(parts) > 1 else None
    episode = parts[2] if len(parts) > 2 else None

    # 1. Get all hashes from DMM
    try:
        dmm_results = _fetch_dmm_hashes(
            imdb_id,
            media_type="movie" if catalog_type == "movie" else "tv",
            api_token=api_token,
        )
    except Exception as exc:
        _log(f"DMM hash fetch failed: {exc}", xbmc.LOGERROR)
        xbmcgui.Dialog().notification(
            "KDMM", f"DMM error: {type(exc).__name__}: {str(exc)[:120]}",
            xbmcgui.NOTIFICATION_ERROR,
        )
        return []

    if not dmm_results:
        _log(f"No torrents in DMM database for {video_id}")
        xbmcgui.Dialog().notification(
            "KDMM", f"DMM: no torrents found for {imdb_id}",
            xbmcgui.NOTIFICATION_WARNING, 5000)
        return []

    # Build hash → result map
    hash_map = {}
    for r in dmm_results:
        h = r.get("hash", "").lower()
        if h and len(h) == 40:
            hash_map[h] = r

    if not hash_map:
        _log("No valid hashes from DMM results")
        xbmcgui.Dialog().notification(
            "KDMM", "DMM returned results but no valid hashes",
            xbmcgui.NOTIFICATION_WARNING, 5000)
        return []

    # 2. Check RD instant availability
    _log(f"Checking RD availability for {len(hash_map)} hash(es)…")
    cached, avail_ok = _availability_is_usable(api_token, list(hash_map.keys()))
    _log(f"{len(cached)} / {len(hash_map)} hashes cached via instantAvailability (ok={avail_ok})")

    # Sort helper used by both paths
    def _sort_key(entry):
        name = (entry.get("title") or "").lower()
        group_prio = 0 if any(g in name for g in _PREFERRED_GROUPS) else 1
        size = entry.get("fileSize") or entry.get("filesize") or 0
        return (group_prio, -size)

    if not cached:
        # instantAvailability returned nothing – fall back to direct-add approach
        _log("instantAvailability returned 0 results, trying direct-add fallback…")
        xbmcgui.Dialog().notification(
            "KDMM", f"Checking top 5 streams directly on RD…",
            xbmcgui.NOTIFICATION_INFO, 4000)
        sorted_dmm = sorted(dmm_results, key=_sort_key)
        candidates_direct = [
            {"hash": (r.get("hash") or "").lower(), "title": r.get("title", "Unknown")}
            for r in sorted_dmm if len((r.get("hash") or "")) == 40
        ][:5]  # only top 5 — each needs multiple round-trips
        resolved = _resolve_by_direct_add(
            candidates_direct, api_token, season=season, episode=episode,
            cancel_event=cancel_event,
        )
        if not resolved:
            xbmcgui.Dialog().notification(
                "KDMM", "No cached streams found for this title on RD",
                xbmcgui.NOTIFICATION_WARNING, 6000)
        return resolved

    # 3. Build sorted candidate list from availability results
    candidates_info = []
    for h, files in cached.items():
        dmm_entry = hash_map.get(h, {})
        title = dmm_entry.get("title", "Unknown")
        video_exts = (".mkv", ".mp4", ".avi", ".m4v", ".webm", ".ts")
        best_file = None
        best_size = 0
        for f in files:
            if any(f["filename"].lower().endswith(e) for e in video_exts):
                if f["filesize"] > best_size:
                    best_file = f
                    best_size = f["filesize"]

        if not best_file:
            continue

        # For TV series, try to match the correct episode file
        if season and episode:
            ep_markers = [
                f"s{int(season):02d}e{int(episode):02d}",
                f"s{season}e{episode}",
                f"{season}x{int(episode):02d}",
            ]
            # Check all video files for episode match
            matched_file = None
            for f in files:
                fname_lower = f["filename"].lower()
                if any(fname_lower.endswith(e) for e in video_exts):
                    if any(m in fname_lower for m in ep_markers):
                        if not matched_file or f["filesize"] > matched_file["filesize"]:
                            matched_file = f
            if matched_file:
                best_file = matched_file
                best_size = matched_file["filesize"]

        candidates_info.append({
            "hash": h,
            "file_id": best_file["file_id"],
            "filename": best_file["filename"],
            "title": title,
            "size_mb": best_size / 1024 / 1024,
        })

    # Stable-sort: preferred groups first, then by size (largest first)
    def _sort_key_c(c):
        name = c["title"].lower()
        group_prio = 0 if any(g in name for g in _PREFERRED_GROUPS) else 1
        return (group_prio, -(c["size_mb"]))

    candidates_info.sort(key=_sort_key_c)

    _log(f"Resolving {len(candidates_info)} cached candidate(s) via RD…")

    # 4. Resolve each candidate to a direct-play URL
    resolved = []
    for c in candidates_info:
        url, filename = _resolve_rd_stream(c["hash"], c["file_id"], api_token)
        if url:
            resolved.append({
                "url": url,
                "headers": {},
                "name": filename or c["filename"] or c["title"],
            })
        if len(resolved) >= 10:
            break  # enough candidates – no need to resolve more

    _log(f"Resolved {len(resolved)} playable stream(s), top: {resolved[0]['name']!r}" if resolved else "No streams resolved")
    if not resolved:
        xbmcgui.Dialog().notification(
            "KDMM",
            f"RD: {len(cached)} cached but all resolve attempts failed",
            xbmcgui.NOTIFICATION_ERROR, 6000)
    return resolved
