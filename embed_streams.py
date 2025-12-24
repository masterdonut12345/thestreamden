from __future__ import annotations
from dataclasses import dataclass
from typing import List
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup


UA = (
    "Mozilla/5.0 (X11; Linux x86_64; rv:145.0) "
    "Gecko/20100101 Firefox/145.0"
)

DEFAULT_TIMEOUT = 15
MAX_OTHER_IFRAMES = 12
MAX_OTHER_VIDEOS = 12

@dataclass
class EmbedCandidate:
    kind: str                 # "twitch" | "youtube" | "x" | "other"
    label: str                # shown to user
    preview_html: str         # iframe/widget HTML
    chosen_value: str         # what we store back into thread form


def _is_probably_url(s: str) -> bool:
    try:
        u = urlparse(s.strip())
        return u.scheme in ("http", "https") and bool(u.netloc)
    except Exception:
        return False


def _normalize_url(base: str, maybe_relative: str) -> str:
    try:
        return urljoin(base, (maybe_relative or "").strip())
    except Exception:
        return (maybe_relative or "").strip()


def _dedup_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _twitch_candidates(user_input: str, parent_host: str) -> List[EmbedCandidate]:
    """
    Accepts:
      - channel name: "shroud"
      - twitch URL: "https://www.twitch.tv/shroud"
    Produces Twitch player iframe with required `parent=` param.
    """
    s = (user_input or "").strip()
    if not s:
        return []

    if _is_probably_url(s):
        path = urlparse(s).path.strip("/")
        channel = path.split("/")[0] if path else ""
    else:
        channel = s.strip("/")

    if not channel:
        return []

    embed_url = f"https://player.twitch.tv/?channel={channel}&parent={parent_host}"
    html = (
        f'<iframe src="{embed_url}" '
        f'width="800" height="450" frameborder="0" '
        f'allowfullscreen="true" scrolling="no"></iframe>'
    )

    return [
        EmbedCandidate(
            kind="twitch",
            label=f"Twitch channel: {channel}",
            preview_html=html,
            chosen_value=embed_url,
        )
    ]


def _youtube_candidates(user_input: str) -> List[EmbedCandidate]:
    """
    Accepts:
      - YouTube video URL: https://www.youtube.com/watch?v=VIDEOID
      - Short URL: https://youtu.be/VIDEOID
      - Embed URL: https://www.youtube.com/embed/VIDEOID
      - Raw ID: VIDEOID
    Produces YouTube embed iframe.
    """
    s = (user_input or "").strip()
    if not s:
        return []

    vid = ""

    if _is_probably_url(s):
        u = urlparse(s)
        if "youtu.be" in u.netloc:
            vid = u.path.strip("/").split("/")[0]
        elif "youtube.com" in u.netloc:
            if u.path.startswith("/embed/"):
                vid = u.path.split("/embed/")[1].split("/")[0]
            else:
                # parse v= from query
                qs = u.query.split("&")
                for part in qs:
                    if part.startswith("v="):
                        vid = part.split("=", 1)[1]
                        break
    else:
        vid = s

    vid = (vid or "").strip()
    if not vid:
        return []

    embed_url = f"https://www.youtube.com/embed/{vid}"
    html = (
        f'<iframe width="800" height="450" src="{embed_url}" '
        f'frameborder="0" allow="accelerometer; autoplay; clipboard-write; '
        f'encrypted-media; gyroscope; picture-in-picture; web-share" '
        f'allowfullscreen></iframe>'
    )

    return [
        EmbedCandidate(
            kind="youtube",
            label=f"YouTube video: {vid}",
            preview_html=html,
            chosen_value=embed_url,
        )
    ]


def _x_candidates(user_input: str) -> List[EmbedCandidate]:
    """
    Accepts:
      - X/Twitter status URL: https://x.com/user/status/123...
    Uses the official widget script (not an iframe), because X blocks framing.
    Still allows choosing the original post URL as the stored value.
    """
    s = (user_input or "").strip()
    if not s:
        return []

    if not _is_probably_url(s):
        # user might paste just "user/status/id" — try to salvage
        s = "https://x.com/" + s.strip("/")

    # widget preview
    html = (
        f'<blockquote class="twitter-tweet"><a href="{s}"></a></blockquote>'
        f'<script async src="https://platform.twitter.com/widgets.js" charset="utf-8"></script>'
    )

    return [
        EmbedCandidate(
            kind="x",
            label="X post",
            preview_html=html,
            chosen_value=s,  # store the post URL
        )
    ]

def _other_candidates(page_url: str) -> List[EmbedCandidate]:
    """
    Best-effort: fetch the page and return:
      - iframe src URLs
      - HTML5 video URLs from <video src> and <source src>
    Many targets will block embedding via X-Frame-Options/CSP — that's OK.
    We'll still show a preview attempt and allow choosing the URL.
    """
    url = (page_url or "").strip()
    if not _is_probably_url(url):
        return []

    headers = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"}
    r = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
    r.raise_for_status()

    final_url = str(r.url)
    soup = BeautifulSoup(r.text, "html.parser")

    # --- collect iframes ---
    iframe_srcs: List[str] = []
    for iframe in soup.find_all("iframe"):
        src = _normalize_url(final_url, iframe.get("src") or "")
        if not src:
            continue
        if src.lower().startswith("javascript:"):
            continue
        iframe_srcs.append(src)

    iframe_srcs = _dedup_keep_order(iframe_srcs)[:MAX_OTHER_IFRAMES]

    # --- collect videos ---
    video_srcs: List[str] = []

    # <video src="...">
    for video in soup.find_all("video"):
        vsrc = _normalize_url(final_url, video.get("src") or "")
        if vsrc and not vsrc.lower().startswith("javascript:"):
            video_srcs.append(vsrc)

        # <video><source src="..."></video>
        for source in video.find_all("source"):
            ssrc = _normalize_url(final_url, source.get("src") or "")
            if ssrc and not ssrc.lower().startswith("javascript:"):
                video_srcs.append(ssrc)

    video_srcs = _dedup_keep_order(video_srcs)[:MAX_OTHER_VIDEOS]

    out: List[EmbedCandidate] = []

    # Make iframe candidates first
    for i, src in enumerate(iframe_srcs, start=1):
        html = (
            f'<iframe src="{src}" width="800" height="450" frameborder="0" '
            f'allowfullscreen="true"></iframe>'
        )
        out.append(
            EmbedCandidate(
                kind="other",
                label=f"Found iframe #{i}",
                preview_html=html,
                chosen_value=src,
            )
        )

    # Then video candidates
    for i, src in enumerate(video_srcs, start=1):
        # Preview with HTML5 video player
        # Note: some URLs may require cookies/CORS and may not play — that's OK.
        html = (
            "<video width='800' height='450' controls "
            "style='border:1px solid #000; background:#000;'>"
            f"<source src='{src}'>"
            "Your browser does not support HTML5 video."
            "</video>"
            f"<div style='font-size:12px; margin-top:6px;'>video src: {src}</div>"
        )
        out.append(
            EmbedCandidate(
                kind="other",
                label=f"Found video #{i}",
                preview_html=html,
                chosen_value=src,
            )
        )

    if not out:
        out.append(
            EmbedCandidate(
                kind="other",
                label="No iframes or HTML5 videos found on that page",
                preview_html=(
                    "<div style='border:1px solid #000; padding:10px;'>"
                    "Could not find any <code>&lt;iframe&gt;</code> or "
                    "<code>&lt;video&gt;</code> / <code>&lt;source&gt;</code> tags on that page."
                    "</div>"
                ),
                chosen_value="",
            )
        )

    return out

def get_embed_candidates(source: str, user_input: str, parent_host: str) -> List[dict]:
    """
    Returns a list of dicts (Jinja-friendly) with:
      - kind, label, preview_html, chosen_value
    """
    source = (source or "").strip().lower()
    user_input = (user_input or "").strip()

    try:
        if source == "twitch":
            cands = _twitch_candidates(user_input, parent_host)
        elif source == "youtube":
            cands = _youtube_candidates(user_input)
        elif source == "x":
            cands = _x_candidates(user_input)
        elif source == "other":
            cands = _other_candidates(user_input)
        else:
            cands = []

    except Exception as e:
        # Show a single “error” candidate so the page doesn’t feel dead
        cands = [
            EmbedCandidate(
                kind=source or "other",
                label="Error",
                preview_html=(
                    "<div style='border:1px solid #000; padding:10px;'>"
                    f"Failed to fetch/parse: {str(e)}"
                    "</div>"
                ),
                chosen_value="",
            )
        ]

    # Convert dataclasses to dict for templates
    return [
        {
            "kind": c.kind,
            "label": c.label,
            "preview_html": c.preview_html,
            "chosen_value": c.chosen_value,
        }
        for c in cands
    ]
