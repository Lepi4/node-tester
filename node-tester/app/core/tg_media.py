"""Fetch public Telegram channel media URLs for testing.

Pre-fetched once (direct, without proxy) before the per-node test loop
so all nodes are tested on the same URLs for fair comparison.
"""
import re
import logging
import httpx

log = logging.getLogger("node_tester.tg_media")

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_IMG_EXTS = (".jpg", ".jpeg", ".png", ".webp")


def _parse_channel(raw: str) -> str:
    """Extract channel username from full URL or bare name.
    'https://t.me/homeasy' → 'homeasy'
    't.me/homeasy/'        → 'homeasy'
    '@homeasy'             → 'homeasy'
    'homeasy'              → 'homeasy'
    """
    raw = raw.strip().rstrip("/")
    if "/" in raw:
        raw = raw.rsplit("/", 1)[-1]
    return raw.lstrip("@")


def _parse_duration_secs(s: str) -> int:
    """'0:35' → 35, '1:23' → 83, '1:23:45' → 5025"""
    parts = s.strip().split(":")
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except ValueError:
        pass
    return 0


def _extract_video_posts(html: str, channel: str) -> list[tuple[str, int]]:
    """Return list of (embed_url, duration_secs) from t.me/s/ page HTML."""
    results = []
    # Split into individual post blocks by data-post attribute boundaries
    blocks = re.split(r'(?=data-post=")', html)
    for block in blocks:
        dur_m = re.search(
            r'message_video_duration[^"]*"[^>]*>(\d+:\d+(?::\d+)?)',
            block,
        )
        if not dur_m:
            continue
        duration = _parse_duration_secs(dur_m.group(1))
        id_m = re.search(r'data-post="[^/]+/(\d+)"', block)
        if not id_m:
            continue
        embed_url = f"https://t.me/{channel}/{id_m.group(1)}?embed=1"
        results.append((embed_url, duration))
    return results


def _extract_image_urls(html: str) -> list[str]:
    """Return image URLs from t.me/s/ page HTML.
    Matches any https:// URL ending in a known image extension — domain-agnostic
    so it works regardless of which CDN Telegram uses (telegram.org, telesco.pe, etc).
    """
    pattern = re.compile(r'https://[^\s"\'<>]+\.(?:jpg|jpeg|png|webp)')
    found: list[str] = []
    seen: set[str] = set()
    for m in pattern.finditer(html):
        url = m.group(0).rstrip(".,;:)}]\"'")
        low = url.lower().split("?")[0]
        if url not in seen and any(low.endswith(ext) for ext in _IMG_EXTS):
            seen.add(url)
            found.append(url)
    return found


def _find_oldest_id(html: str) -> str | None:
    """Return the smallest message ID on the page for pagination."""
    ids = re.findall(r'data-post="[^/]+/(\d+)"', html)
    if ids:
        return str(min(int(i) for i in ids))
    return None


async def _fetch_page(
    client: httpx.AsyncClient, channel: str, before: str | None
) -> str | None:
    url = f"https://t.me/s/{channel}"
    if before:
        url += f"?before={before}"
    try:
        r = await client.get(url)
        if r.status_code == 200:
            return r.text
        log.warning("[tg_media] %s → HTTP %s", url, r.status_code)
    except Exception as e:
        log.warning("[tg_media] fetch failed (%s): %s", url, e)
    return None


def _make_client(proxy_url: str | None) -> httpx.AsyncClient:
    kwargs: dict = {"headers": _HEADERS, "timeout": 15.0, "follow_redirects": True}
    if proxy_url:
        kwargs["proxy"] = proxy_url
    return httpx.AsyncClient(**kwargs)


async def _fetch_video_from_channel(
    channel: str,
    count: int,
    min_duration_s: int,
    max_pages: int,
    proxy_url: str | None = None,
) -> list[str]:
    """Try one channel. Returns list of embed URLs (may be empty)."""
    channel = _parse_channel(channel)
    found: list[str] = []
    before: str | None = None

    async with _make_client(proxy_url) as client:
        for page_num in range(max_pages):
            html = await _fetch_page(client, channel, before)
            if not html:
                break

            posts = _extract_video_posts(html, channel)
            for embed_url, dur in reversed(posts):   # newest first
                if dur >= min_duration_s and embed_url not in found:
                    found.append(embed_url)
                if len(found) >= count:
                    break

            log.debug("[tg_media] %s page %d: %d/%d video(s)", channel, page_num + 1, len(found), count)

            if len(found) >= count:
                break
            before = _find_oldest_id(html)
            if not before:
                break

    return found[:count]


async def _fetch_images_from_channel(
    channel: str,
    count: int,
    max_pages: int,
    proxy_url: str | None = None,
) -> list[str]:
    channel = _parse_channel(channel)
    found: list[str] = []
    seen: set[str] = set()
    before: str | None = None

    async with _make_client(proxy_url) as client:
        for page_num in range(max_pages):
            html = await _fetch_page(client, channel, before)
            if not html:
                break
            for url in _extract_image_urls(html):
                if url not in seen:
                    seen.add(url)
                    found.append(url)
                if len(found) >= count:
                    break
            log.debug("[tg_media] %s page %d: %d/%d image(s)", channel, page_num + 1, len(found), count)
            if len(found) >= count:
                break
            before = _find_oldest_id(html)
            if not before:
                break

    return found[:count]


async def fetch_video_urls(
    channels: list[str],
    count: int = 3,
    min_duration_s: int = 30,
    proxy_url: str | None = None,
) -> list[str]:
    """Progressive search: 3 rounds of (try all channels) with increasing scroll depth.

    Round 1 — 1 page  (no scroll)  → check all channels
    Round 2 — 3 pages (light scroll) → check all channels
    Round 3 — 8 pages (deep scroll)  → check all channels
    Still nothing → return [] (test skipped).

    proxy_url — route through Mihomo so t.me is reachable in blocked regions.
    All nodes are tested on the same returned URL list.
    """
    valid = [_parse_channel(ch) for ch in channels if ch and ch.strip()]
    valid = [ch for ch in valid if ch]
    if not valid:
        return []

    for round_pages in (1, 3, 8):
        for ch in valid:
            result = await _fetch_video_from_channel(
                ch, count, min_duration_s, round_pages, proxy_url=proxy_url
            )
            if result:
                log.info("[tg_media] video: round=%d ch=%s → %d URL(s)", round_pages, ch, len(result))
                return result
            log.debug("[tg_media] video: round=%d ch=%s → 0, next", round_pages, ch)

    log.warning("[tg_media] video: no videos found after all rounds — skipping TG video test")
    return []


def _extract_mp4_from_embed_html(html: str) -> str | None:
    """Extract direct mp4 CDN URL from TG embed page HTML."""
    m = re.search(r'<video[^>]+src=["\']([^"\']+\.mp4[^"\']*)["\']', html, re.I)
    if m:
        return m.group(1)
    m = re.search(r'"src"\s*:\s*"([^"]+\.mp4[^"]*)"', html)
    if m:
        return m.group(1).replace('\\/', '/')
    return None


async def _resolve_embed_to_direct(
    client: httpx.AsyncClient, embed_url: str
) -> str | None:
    try:
        r = await client.get(embed_url)
        if r.status_code == 200:
            return _extract_mp4_from_embed_html(r.text)
        log.debug("[tg_media] embed %s → HTTP %s", embed_url, r.status_code)
    except Exception as e:
        log.debug("[tg_media] embed fetch failed %s: %s", embed_url, e)
    return None


async def fetch_video_direct_urls(
    channels: list[str],
    count: int = 3,
    min_duration_s: int = 30,
    proxy_url: str | None = None,
) -> list[str]:
    """Find TG video posts, then resolve embed pages to direct mp4 CDN URLs.
    Returns direct URLs downloadable via httpx (no Playwright needed).
    """
    embed_urls = await fetch_video_urls(channels, count, min_duration_s, proxy_url)
    if not embed_urls:
        return []

    direct: list[str] = []
    async with _make_client(proxy_url) as client:
        for embed_url in embed_urls:
            mp4_url = await _resolve_embed_to_direct(client, embed_url)
            if mp4_url:
                direct.append(mp4_url)
                log.info("[tg_media] resolved embed → direct mp4 OK")
            else:
                log.warning("[tg_media] no mp4 URL in embed: %s", embed_url)

    log.info("[tg_media] video direct: %d/%d resolved", len(direct), len(embed_urls))
    return direct


async def fetch_image_urls(
    channels: list[str],
    count: int = 3,
    proxy_url: str | None = None,
) -> list[str]:
    """Progressive search for images: same 3-round strategy as video.
    proxy_url — route through Mihomo so t.me is reachable in blocked regions.
    """
    valid = [_parse_channel(ch) for ch in channels if ch and ch.strip()]
    valid = [ch for ch in valid if ch]
    if not valid:
        return []

    for round_pages in (1, 3, 5):
        for ch in valid:
            result = await _fetch_images_from_channel(
                ch, count, round_pages, proxy_url=proxy_url
            )
            if result:
                log.info("[tg_media] images: round=%d ch=%s → %d URL(s)", round_pages, ch, len(result))
                return result
            log.debug("[tg_media] images: round=%d ch=%s → 0, next", round_pages, ch)

    log.warning("[tg_media] images: no images found after all rounds — skipping TG image test")
    return []
