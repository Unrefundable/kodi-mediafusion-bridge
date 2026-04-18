"""
KDMM – lib/rd_auth.py
Real-Debrid OAuth2 device-code authorization flow.

Uses the open-source client ID that community addons share.
Shows a QR code + user code dialog, polls for authorization,
then stores the resulting tokens in a JSON file in addon_data
(survives addon reinstalls/updates).
"""

import json
import os
import sys
import threading
import time

import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

_CLIENT_ID = "X245A4XAIBGVM"
_RD_OAUTH_BASE = "https://api.real-debrid.com/oauth/v2"
_ADDON_ID = "plugin.video.kdmm"


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM Auth] {msg}", level)


def _tokens_path():
    """Path to the persistent tokens JSON file (in userdata, not addon dir)."""
    userdata = xbmcvfs.translatePath(f"special://profile/addon_data/{_ADDON_ID}/")
    os.makedirs(userdata, exist_ok=True)
    return os.path.join(userdata, "rd_tokens.json")


def _load_tokens():
    """Load tokens dict from JSON file, or empty dict if missing/corrupt."""
    try:
        with open(_tokens_path(), "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_tokens(data):
    """Write tokens dict to JSON file."""
    try:
        with open(_tokens_path(), "w") as f:
            json.dump(data, f)
        _log("Tokens saved to rd_tokens.json")
    except Exception as exc:
        _log(f"Failed to save tokens: {exc}", xbmc.LOGERROR)


def _get_requests():
    """Import requests, ensuring all Kodi addon module paths are on sys.path."""
    addon_dir = xbmcvfs.translatePath("special://home/addons")
    for mod in ("script.module.requests", "script.module.urllib3",
                "script.module.chardet", "script.module.certifi",
                "script.module.idna"):
        lib = os.path.join(addon_dir, mod, "lib")
        if os.path.isdir(lib) and lib not in sys.path:
            sys.path.insert(0, lib)
    import requests
    # Point requests at Kodi's certifi CA bundle so SSL works
    try:
        import certifi
        os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
    except Exception:
        pass
    return requests


def authorize():
    """
    Run the full RD device-code OAuth flow.
    Returns True on success, False on cancel/error.
    """
    # Show a "connecting" dialog immediately so Kodi feels responsive
    busy = xbmcgui.DialogProgress()
    busy.create("KDMM – Real-Debrid", "Connecting to Real-Debrid…")

    # Fetch device code on a background thread so Kodi UI doesn't freeze
    fetch_result = {}

    def _fetch_device_code():
        try:
            requests = _get_requests()
            resp = requests.get(
                f"{_RD_OAUTH_BASE}/device/code",
                params={"client_id": _CLIENT_ID, "new_credentials": "yes"},
                timeout=15,
            )
            resp.raise_for_status()
            fetch_result["data"] = resp.json()
        except Exception as exc:
            fetch_result["error"] = exc

    t = threading.Thread(target=_fetch_device_code, daemon=True)
    t.start()

    # Wait on the background thread, updating busy dialog every 500ms
    while t.is_alive():
        if busy.iscanceled():
            busy.close()
            return False
        busy.update(0, "Connecting to Real-Debrid…")
        xbmc.sleep(500)

    busy.close()

    if "error" in fetch_result:
        exc = fetch_result["error"]
        _log(f"Failed to get device code: {exc}", xbmc.LOGERROR)
        xbmcgui.Dialog().ok("KDMM", f"RD error: {type(exc).__name__}: {str(exc)[:200]}")
        return False

    data = fetch_result.get("data", {})
    device_code = data.get("device_code", "")
    user_code = data.get("user_code", "")
    if not device_code or not user_code:
        xbmcgui.Dialog().ok("KDMM", "Unexpected response from Real-Debrid.")
        return False

    interval = data.get("interval", 5)
    expires_in = data.get("expires_in", 600)
    verification_url = data.get("verification_url", "https://real-debrid.com/device")
    direct_url = data.get("direct_verification_url", verification_url)

    # Start QR generation in background (non-blocking); dialog shows immediately
    qr_path = _generate_qr_image(direct_url)

    # Show dialog and poll (polling is also on a background thread in _show_auth_dialog)
    success = _show_auth_dialog(
        device_code=device_code,
        user_code=user_code,
        verification_url=verification_url,
        qr_image_path=qr_path,
        interval=interval,
        expires_in=expires_in,
    )

    # Clean up QR image
    if qr_path and os.path.isfile(qr_path):
        try:
            os.remove(qr_path)
        except Exception:
            pass

    return success


def _generate_qr_image(url):
    """Generate a QR code PNG in a background thread. Returns path immediately."""
    userdata = xbmcvfs.translatePath(f"special://profile/addon_data/{_ADDON_ID}/")
    os.makedirs(userdata, exist_ok=True)
    qr_path = os.path.join(userdata, "rd_qr.png")

    def _gen():
        try:
            import qrcode
            img = qrcode.make(url, box_size=10, border=2)
            img.save(qr_path)
            _log(f"QR code saved to {qr_path}")
        except Exception as exc:
            _log(f"QR generation failed: {exc}", xbmc.LOGWARNING)

    threading.Thread(target=_gen, daemon=True).start()
    return qr_path


def _show_auth_dialog(device_code, user_code, verification_url, qr_image_path,
                      interval, expires_in):
    """
    Show a progress dialog and poll RD for authorization.
    All network calls run on a daemon thread; the main thread only updates
    the dialog at 1-second ticks so Kodi's UI stays responsive.
    """
    # Shared state between main thread and poller thread
    result = {"status": "pending"}   # "pending" | "ok" | "error" | "timeout"
    result_lock = threading.Lock()

    def _poll_thread():
        requests = _get_requests()
        poll_url = f"{_RD_OAUTH_BASE}/device/credentials"
        deadline = time.time() + expires_in

        while time.time() < deadline:
            with result_lock:
                if result["status"] != "pending":
                    return

            time.sleep(interval)

            with result_lock:
                if result["status"] != "pending":
                    return

            try:
                resp = requests.get(
                    poll_url,
                    params={"client_id": _CLIENT_ID, "code": device_code},
                    timeout=10,
                )
            except Exception as exc:
                _log(f"Poll request failed: {exc}", xbmc.LOGWARNING)
                continue

            if resp.status_code == 200:
                creds = resp.json()
                client_id = creds.get("client_id", _CLIENT_ID)
                client_secret = creds.get("client_secret", "")
                token_data = _exchange_code(client_id, client_secret, device_code)
                with result_lock:
                    if token_data:
                        _save_tokens(client_id, client_secret, token_data)
                        result["status"] = "ok"
                    else:
                        result["status"] = "error"
                return
            elif resp.status_code != 403:
                _log(f"Unexpected poll response: {resp.status_code}", xbmc.LOGWARNING)

        with result_lock:
            if result["status"] == "pending":
                result["status"] = "timeout"

    # Start polling in background
    t = threading.Thread(target=_poll_thread, daemon=True)
    t.start()

    # Show dialog; update every second on the main thread (non-blocking ticks)
    dialog = xbmcgui.DialogProgress()
    dialog.create(
        "KDMM – Link Real-Debrid",
        f"Go to: [B]{verification_url}[/B]\n"
        f"Enter code: [B]{user_code}[/B]"
    )

    deadline = time.time() + expires_in
    while True:
        with result_lock:
            status = result["status"]

        if status != "pending":
            break

        if dialog.iscanceled():
            with result_lock:
                result["status"] = "cancelled"
            break

        remaining = max(0, int(deadline - time.time()))
        elapsed = expires_in - remaining
        percent = min(99, int((elapsed / expires_in) * 100))
        dialog.update(
            percent,
            f"Go to: [B]{verification_url}[/B]\n"
            f"Enter code: [B]{user_code}[/B]\n"
            f"Waiting… ({remaining}s remaining)"
        )
        xbmc.sleep(1000)  # 1-second tick — yields to Kodi UI

    dialog.close()

    with result_lock:
        status = result["status"]

    if status == "ok":
        xbmcgui.Dialog().notification("KDMM", "Real-Debrid authorized!",
                                      xbmcgui.NOTIFICATION_INFO, 3000)
        _log("Authorization successful")
        return True
    elif status == "cancelled":
        _log("User cancelled authorization")
        return False
    elif status == "timeout":
        xbmcgui.Dialog().ok("KDMM", "Authorization timed out. Please try again.")
        return False
    else:
        xbmcgui.Dialog().ok("KDMM", "Failed to get access token from Real-Debrid.")
        return False


def _exchange_code(client_id, client_secret, device_code):
    """Exchange the device code for an access + refresh token."""
    requests = _get_requests()
    try:
        resp = requests.post(
            f"{_RD_OAUTH_BASE}/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "code": device_code,
                "grant_type": "http://oauth.net/grant_type/device/1.0",
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        _log(f"Token exchange failed: {exc}", xbmc.LOGERROR)
        return None


def _save_tokens(client_id, client_secret, token_data):
    """Persist OAuth tokens to JSON file in addon_data."""
    _write_tokens({
        "client_id": client_id,
        "client_secret": client_secret,
        "access_token": token_data.get("access_token", ""),
        "refresh_token": token_data.get("refresh_token", ""),
        "expiry": int(time.time()) + token_data.get("expires_in", 0),
    })


def refresh_token():
    """
    Refresh the RD access token using the stored refresh token.
    Returns the new access token, or None on failure.
    """
    requests = _get_requests()

    tokens = _load_tokens()
    client_id = tokens.get("client_id", "")
    client_secret = tokens.get("client_secret", "")
    refresh_tok = tokens.get("refresh_token", "")

    if not all([client_id, client_secret, refresh_tok]):
        _log("Missing credentials for token refresh", xbmc.LOGWARNING)
        return None

    try:
        resp = requests.post(
            f"{_RD_OAUTH_BASE}/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "code": refresh_tok,
                "grant_type": "refresh_token",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        _log(f"Token refresh failed: {exc}", xbmc.LOGERROR)
        return None

    new_token = data.get("access_token", "")
    if new_token:
        tokens["access_token"] = new_token
        tokens["refresh_token"] = data.get("refresh_token", refresh_tok)
        tokens["expiry"] = int(time.time()) + data.get("expires_in", 0)
        _write_tokens(tokens)
        _log("Token refreshed successfully")
    return new_token or None


def get_access_token():
    """
    Return a valid RD access token.
    Priority:
      1. API key entered directly in addon settings (rd_api_key) – no expiry
      2. OAuth tokens stored in rd_tokens.json – auto-refreshed if expiring
    Returns None if neither is configured.
    """
    # 1. Check for a directly-entered API key in addon settings
    try:
        addon = xbmcaddon.Addon()
        api_key = addon.getSetting("rd_api_key").strip()
        if api_key:
            _log("Using API key from addon settings")
            return api_key
    except Exception as exc:
        _log(f"Could not read addon settings: {exc}", xbmc.LOGWARNING)

    # 2. Fall back to OAuth JSON tokens
    tokens = _load_tokens()
    token = tokens.get("access_token", "")
    if not token:
        return None

    # Refresh 5 min before expiry
    expiry = tokens.get("expiry", 0)
    if time.time() > (expiry - 300):
        _log("Access token expired or expiring soon, refreshing…")
        token = refresh_token()

    return token or None


def revoke():
    """Clear all stored RD tokens."""
    _write_tokens({})
    _log("RD authorization revoked")
