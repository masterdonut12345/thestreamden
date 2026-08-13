"""
CDP-based stream harvester + live-follow relay (Python port of restream-site/server.js).

Instead of proxying every HLS segment through the Flask server (which throttles under
load), this module drives a real headless browser to the embed page and captures the
master + media manifests as the site's own secure player fetches them. The Flask app
then serves only those tiny manifests (KB), and the viewer's browser plays the
segments directly from the CDN (kapwing/etc., which are CORS-open).

Because autoplay is blocked in headless tabs, the embedded player fetches a live
window only once; the "follower" reloads the embed page periodically to re-capture
the current live window so viewers track the real broadcast.
"""

import base64
import json
import os
import random
import re
import threading
import time

import requests
from websocket import create_connection

UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36'


def http_json(url, timeout=6):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def ensure_cdp_chrome(chrome_port):
    """Return an existing CDP chrome on chrome_port, or launch one. Never returns an error."""
    if chrome_port:
        try:
            http_json(f"http://127.0.0.1:{chrome_port}/json/version", timeout=3)
            return None  # already running
        except Exception:
            pass
    import subprocess
    chrome_candidates = [
        os.environ.get('CHROME_PATH'),
        '/opt/google/chrome/chrome',
        '/usr/bin/chromium',
        '/usr/bin/chromium-browser',
        '/usr/bin/google-chrome',
    ]
    bind = chrome_port or 9223
    for chrome in [c for c in chrome_candidates if c]:
        if not os.path.exists(chrome):
            continue
        args = [
            chrome, '--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
            f'--remote-debugging-port={bind}', f'--user-data-dir=/tmp/cdp-harvest-{bind}',
            '--remote-allow-origins=*',
            '--noerrdialogs', '--no-first-run', '--ozone-platform=headless',
            '--ozone-override-screen-size=800,600', '--use-angle=swiftshader-webgl',
            'about:blank',
        ]
        try:
            proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for _ in range(40):
                time.sleep(0.25)
                try:
                    http_json(f"http://127.0.0.1:{bind}/json/version", timeout=2)
                    return proc
                except Exception:
                    pass
        except Exception:
            continue
    return None  # will fail on connect; error surfaces in harvest


class CdpHarvester:
    """Captures + live-follows the manifests of one embed URL via the Chrome DevTools protocol."""

    def __init__(self, embed_url, chrome_port=9223,
                 harvest_timeout_ms=50000, media_wait_ms=20000,
                 follow_interval_ms=6000, refresh_timeout_ms=10000):
        self.embed_url = embed_url
        self.chrome_port = chrome_port
        self.harvest_timeout_ms = harvest_timeout_ms
        self.media_wait_ms = media_wait_ms
        self.follow_interval_ms = follow_interval_ms
        self.refresh_timeout_ms = refresh_timeout_ms

        self.ws = None
        self.page_session = None
        self._id_lock = threading.Lock()
        self._state_lock = threading.RLock()
        self.msg_id = 0
        self.pending = {}  # id -> (event, box)
        self.pull_cb = {}  # id -> callback for fire-and-forget getResponseBody
        self.send_lock = threading.Lock()
        self.recv_thread = None

        self.manifested = {}
        self.latest_req = {}
        self._url_ts = {}  # url -> last capture time
        self._master_updated_at = 0
        self.master_url = None
        self.master_live = {'body': '#EXTM3U\n'}
        self.media = {}  # {<key>: {"url": str, "body": str}}
        self.token = None
        self.stopped = False
        self.follow_timer = None

        self.secure_url = None
        self.prep_time_ms = None

    # ------------------------------------------------------------------ CDP plumbing
    def _connect(self):
        version = http_json(f"http://127.0.0.1:{self.chrome_port}/json/version")
        self.ws = create_connection(version['webSocketDebuggerUrl'], timeout=30,
                                    suppress_origin=True)
        self.recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self.recv_thread.start()

    def _send_raw(self, obj):
        with self.send_lock:
            self.ws.send(json.dumps(obj))

    def send(self, method, params=None, sess=None, timeout=15):
        with self._id_lock:
            self.msg_id += 1
            mid = self.msg_id
            ev, box = threading.Event(), {}
            self.pending[mid] = (ev, box)
        payload = {'id': mid, 'method': method, 'params': params or {}}
        if sess:
            payload['sessionId'] = sess
        self._send_raw(payload)
        if not ev.wait(timeout):
            with self._id_lock:
                self.pending.pop(mid, None)
            raise TimeoutError('cdp timeout: ' + method)
        if 'error' in box:
            raise RuntimeError(str(box['error']))
        return box.get('result', {})

    def _recv_loop(self):
        while not self.stopped:
            try:
                raw = self.ws.recv()
            except Exception:
                break
            if not raw:
                continue
            try:
                self._on_message(json.loads(raw))
            except Exception:
                pass
        # ws died under us -> mark the session dead so downstream re-harvests
        self.stopped = True

    def _on_message(self, m):
        if 'id' in m:
            with self._id_lock:
                cb = self.pull_cb.pop(m['id'], None)
            if cb:
                try:
                    cb(m)
                except Exception:
                    pass
                return
            with self._id_lock:
                entry = self.pending.pop(m['id'], None)
            if entry:
                ev, box = entry
                if 'error' in m:
                    box['error'] = m['error']
                else:
                    box.update(m)
                ev.set()
            return
        if m.get('sessionId') != self.page_session:
            return
        method = m.get('method')
        if method == 'Network.requestWillBeSent':
            u = (m['params'].get('request') or {}).get('url') or ''
            if '/secure/' in u and '.m3u8' in u:
                pass  # keepalive signal; bodies pulled on responseReceived
        elif method == 'Network.responseReceived':
            u = (m['params'].get('response') or {}).get('url') or ''
            if not self._is_manifest(u):
                return
            req_id = m['params']['requestId']
            self.latest_req[u] = req_id
            self._pull_body(u, req_id)

    @staticmethod
    def _is_manifest(u):
        return '/secure/' in u and '.m3u8' in u or ('.m3u8' in u and 'strmd.st' in u)

    def _pull_body(self, url, req_id):
        with self._id_lock:
            self.msg_id += 1
            mid = self.msg_id

        def cb(m):
            if 'error' in m:
                return
            try:
                body = m['result'].get('body') or ''
                if m['result'].get('base64Encoded'):
                    body = base64.b64decode(body).decode('latin1')
            except Exception:
                return
            if not body.startswith('#EXTM3U'):
                return
            if self.latest_req.get(url) != req_id:
                return  # superseded by a newer fetch
            with self._state_lock:
                self.manifested[url] = body
                self._url_ts[url] = time.time()
                if '#EXT-X-STREAM-INF' in body:
                    # fresh master -> it becomes the active master
                    self.master_url = url
                    self.master_live['body'] = body
                    self._master_updated_at = self._url_ts[url]
                if self.master_url:
                    self._sync()

        with self._id_lock:
            self.pull_cb[mid] = cb
        self._send_raw({'id': mid, 'method': 'Network.getResponseBody',
                        'params': {'requestId': req_id}, 'sessionId': self.page_session})

    def _sync(self):
        mb = self.manifested.get(self.master_url)
        if mb:
            self.master_live['body'] = mb
        best = {}
        for u, body in self.manifested.items():
            if not body or '#EXT-X-STREAM-INF' in body:
                continue
            key = self._key_for_url(u)
            if key not in best or self._url_ts.get(u, 0) > self._url_ts.get(best[key], 0):
                best[key] = u
        for key, u in best.items():
            self.media[key] = {'url': u, 'body': self.manifested[u]}

    def snapshot(self):
        with self._state_lock:
            return {
                'master': self.master_live.get('body') or '#EXTM3U\n',
                'media': {k: {'url': v['url'], 'body': v['body']} for k, v in self.media.items()},
            }

    @staticmethod
    def _key_for_url(u):
        if '/high/mono.m3u8' in u:
            return 'high'
        if '/low/mono.m3u8' in u:
            return 'low'
        return 'variant'

    def _has_master(self):
        return any('#EXT-X-STREAM-INF' in b for b in self.manifested.values())

    def _has_media(self):
        return bool(self.media)

    # ------------------------------------------------------------------ the harvest
    def run(self):
        started = time.time()
        self._connect()
        try:
            target = self.send('Target.createTarget', {'url': 'about:blank'})
        except Exception:
            try:
                target = self.send('Target.getTargets')['targetInfos']
                target = {'targetId': next(t['targetId'] for t in target if t['type'] == 'page')}
            except Exception:
                raise RuntimeError('no chrome page target available')
        attach = self.send('Target.attachToTarget', {'targetId': target['targetId'], 'flatten': True})
        self.page_session = attach['sessionId']
        self.send('Page.enable', sess=self.page_session)
        self.send('Runtime.enable', sess=self.page_session)
        self.send('Network.enable', sess=self.page_session)
        self.send('Page.navigate', {'url': self.embed_url}, sess=self.page_session)

        # phase 1: wait for the master manifest
        deadline = time.time() + self.harvest_timeout_ms / 1000
        while time.time() < deadline and not self._has_master():
            time.sleep(0.25)
        if not self._has_master():
            raise RuntimeError('no master .m3u8 within harvest window (stream offline / blocked?)')

        self.master_url = next(u for u, b in self.manifested.items() if '#EXT-X-STREAM-INF' in b)
        for u, b in self.manifested.items():
            if u == self.master_url:
                self.master_live['body'] = b
        self._sync()

        # phase 2: wait a bit more for the media (variant) manifest
        deadline2 = time.time() + self.media_wait_ms / 1000
        while time.time() < deadline2 and not self._has_media():
            time.sleep(0.25)
            self._sync()
        self._sync()

        m = self.master_url
        self.token = m.split('/secure/')[1].split('/')[0] if '/secure/' in m else None
        self.secure_url = self.master_url
        self.prep_time_ms = int((time.time() - started) * 1000)

        # live-follow: reload the embed page periodically to re-capture the current window.
        # A single Page.reload (not an about:blank bounce) is fast and reliable; we keep
        # serving the last good window if a cycle fails so the player never sees an empty
        # playlist while we retry.
        def refresh():
            before = self._master_updated_at
            try:
                self.send('Page.reload', {'ignoreCache': True}, sess=self.page_session, timeout=10)
            except Exception:
                try:
                    self.send('Page.navigate', {'url': self.embed_url}, sess=self.page_session, timeout=15)
                except Exception:
                    return
            time.sleep(0.3)
            self.latest_req.clear()
            dl = time.time() + self.refresh_timeout_ms / 1000
            while time.time() < dl and self._master_updated_at == before:
                time.sleep(0.4)
            # fresh master/body arrivals flowed in through the ws handler already

        def loop():
            while not self.stopped:
                time.sleep(self.follow_interval_ms / 1000)
                if not self.stopped:
                    try:
                        refresh()
                    except Exception:
                        pass

        self._refresh = refresh
        self.follow_timer = threading.Thread(target=loop, daemon=True)
        self.follow_timer.start()
        return self

    def refresh_now(self):
        f = getattr(self, '_refresh', None)
        if not f:
            return False
        try:
            f()
        except Exception:
            pass
        return True

    def stop(self):
        self.stopped = True
        try:
            if self.page_session:
                self.send('Page.navigate', {'url': 'about:blank'}, sess=self.page_session, timeout=3)
        except Exception:
            pass
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass


# ------------------------------------------------------------------ playlist rewriting
def rewrite_playlist(body, base_url, direct=True):
    out = []
    for line in body.split('\n'):
        t = line.strip()
        if t and not t.startswith('#'):
            try:
                href = requests.compat.urljoin(base_url, t)
                out.append(href if direct else '/api/proxy?url=' + href)
            except Exception:
                out.append(line)
        else:
            out.append(line)
    return '\n'.join(out)


def build_master(session_id, master_body):
    out = []
    for line in (master_body or '#EXTM3U\n').split('\n'):
        t = line.strip()
        if t and not t.startswith('#'):
            kind = 'high' if 'high/' in t else ('low' if 'low/' in t else 'variant')
            out.append(f'/api/playback/{session_id}/stream_{kind}.m3u8')
        else:
            out.append(line)
    return '\n'.join(out)
# ====================== CDN Live TV Harvester ======================
# Simple HTTP-based harvester for cdnlivetv.is
# Fetches channel list from API, extracts playlist URLs from player pages
# No headless browser - just HTTP requests with proper headers

CDNLIVETV_HEADERS = {
    "Referer": "https://streamsports99.tv/",
    "Origin": "https://streamsports99.tv/",
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
}

CDNLIVETV_JSON_HEADERS = dict(CDNLIVETV_HEADERS)
CDNLIVETV_JSON_HEADERS["Accept"] = "application/json"
CDNLIVETV_PAGE_HEADERS = dict(CDNLIVETV_HEADERS)
CDNLIVETV_PAGE_HEADERS.update({
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Upgrade-Insecure-Requests": "1",
})


def _cdnlivetv_b64decode(value: str) -> str:
    """Decode cdnlivetv's unpadded URL-safe base64 (uses - and _)."""
    s = value.replace("-", "+").replace("_", "/")
    s += "=" * (-len(s) % 4)
    return base64.b64decode(s).decode("utf-8")


# cdnlivetv.tv tightens up aggressively on burst traffic (429), so serialize
# player-page fetches and space them out; retry a few times when throttled.
_PAGE_MUTEX = threading.Lock()
_PAGE_LAST = 0.0
_PAGE_MIN_INTERVAL = float(os.environ.get("CDNLIVETV_PAGE_INTERVAL", "2.0"))
_PAGE_JITTER = float(os.environ.get("CDNLIVETV_PAGE_JITTER", "0.5"))


def _throttled_page_get(url: str, headers: dict, timeout: int = 15) -> requests.Response:
    """GET a cdnlivetv player page with a global rate limit + 429 backoff."""
    global _PAGE_LAST
    with _PAGE_MUTEX:
        now = time.time()
        wait = _PAGE_LAST + _PAGE_MIN_INTERVAL - now
        if wait > 0:
            time.sleep(wait)
        # add small random jitter
        if _PAGE_JITTER > 0:
            time.sleep(random.uniform(0, _PAGE_JITTER))
        for attempt in range(3):
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code != 429:
                break
            time.sleep(2 + attempt * 2)
        _PAGE_LAST = time.time()
        return r


def _extract_cdnlivetv_playlist_url(html: str) -> str | None:
    """Extract the m3u8 URL built from concatenated base64 var strings."""
    var_pattern = re.compile(r"var\s+(\w+)\s*=\s*['\"]([A-Za-z0-9\-_=]+)['\"];")
    vars_dict = dict(var_pattern.findall(html or ""))

    decode_match = re.search(r"function\s+(\w+)\s*\(s\)\s*\{", html or "")
    if not decode_match:
        return None
    decode_func = re.escape(decode_match.group(1))

    concat_match = re.search(r"var\s+(\w+)\s*=\s*[A-Za-z_][\w]*\([^;]+", html or "")
    if not concat_match:
        return None
    parts = re.findall(rf"{decode_func}\((\w+)\)", concat_match.group(0))
    if not parts:
        parts = re.findall(rf"{decode_func}\((\w+)\)", html or "")

    full_url = ""
    for part in parts:
        encoded = vars_dict.get(part)
        if not encoded:
            continue
        try:
            full_url += _cdnlivetv_b64decode(encoded)
        except Exception:
            continue

    if full_url and ".m3u8" in full_url:
        return full_url
    return None


class CdnLiveTvHarvester:
    """HTTP harvester for cdnlivetv channels (channel list + tokenized playlists)."""

    def __init__(self, base_url: str = "https://cdnlivetv.is"):
        self.base_url = base_url.rstrip("/")

    def get_channels(self) -> list[dict]:
        """Fetch online channels from the cdnlivetv API."""
        try:
            r = requests.get(
                "https://api.cdnlivetv.is/api/v1/channels/?user=cdnlivetv&plan=free",
                headers=CDNLIVETV_JSON_HEADERS,
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            return [
                {
                    "name": ch.get("name", ""),
                    "code": ch.get("code", ""),
                    "url": ch.get("url", ""),
                    "image": ch.get("image", ""),
                    "viewers": ch.get("viewers", 0),
                }
                for ch in data.get("channels", [])
                if ch.get("status") == "online"
            ]
        except Exception as e:
            print(f"[cdnlivetv] Error fetching channels: {e}")
            return []

    def get_playlist_url(self, channel_url: str) -> str | None:
        """Fetch the channel player page and extract its tokenized playlist URL."""
        try:
            if "cdnlivetv.tv" not in channel_url:
                channel_url = channel_url.replace("cdnlivetv.is", "cdnlivetv.tv")
            r = _throttled_page_get(channel_url, CDNLIVETV_PAGE_HEADERS, timeout=15)
            r.raise_for_status()
            return _extract_cdnlivetv_playlist_url(r.text)
        except Exception as e:
            print(f"[cdnlivetv] Error fetching playlist: {e}")
            return None
