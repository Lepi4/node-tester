"""
Browser Test — Module 4.

Per node, via Mihomo proxy:
  1. Fetch BBC main page  → measure TTFB + download HTML
  2. Extract images from CDN (ichef.bbci.co.uk) — scans ALL occurrences in HTML,
     not just <img src=> so it catches Next.js JSON, data-attrs and srcset
  3. Fetch top-4 images in parallel with proper Referer header
  4. Repeat for TMDB (image.tmdb.org posters)

Run twice, take the best result.
"""
import asyncio
import logging
import re
import time
from urllib.parse import urljoin, urlparse

import httpx

import app.mihomo as mihomo
from app.core import scoring

log = logging.getLogger("node_tester.browser")

# ── Sites ─────────────────────────────────────────────────────────────────────
TEST_SITES = [
    {"name": "BBC",  "url": "https://www.bbc.com/",              "cdn": "ichef.bbci.co.uk"},
    {"name": "TMDB", "url": "https://www.themoviedb.org/movie/", "cdn": "image.tmdb.org"},
]
MAX_IMAGES      = 4
MAX_SCRIPTS     = 2
MAX_HTML_BYTES  = 262_144   # 256 KB — enough for Next.js SSR pages
PAGE_TIMEOUT    = 30.0
MAX_IMG_BYTES   = 400_000
MAX_RES_BYTES   = 200_000
BETWEEN_RUNS_S  = 3.0

_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
}

_IMG_EXTS  = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif")

# Accept header sent when fetching image resources
_IMG_ACCEPT = "image/webp,image/avif,image/apng,image/*,*/*;q=0.8"

# Classic HTML attributes
_SRC_RE    = re.compile(r'(?:src|data-src|data-lazy-src)=["\']([^"\']{10,})["\']', re.I)
_SRCSET_RE = re.compile(r'srcset=["\']([^"\']+)["\']', re.I)
# Scripts & stylesheets
_SCRIPT_RE = re.compile(r'<script[^>]+src=["\']([^"\']+\.js(?:\?[^"\']*)?)["\']', re.I)
_CSS_RE    = re.compile(r'<link[^>]+href=["\']([^"\']+\.css(?:\?[^"\']*)?)["\']', re.I)


def _proxy_url(cfg: dict) -> str:
    host = cfg["mihomo_host"].removeprefix("https://").removeprefix("http://").strip("/")
    return f"http://{host}:{cfg['mixed_port']}"


def _proxy(cfg: dict) -> httpx.Proxy:
    url  = _proxy_url(cfg)
    user = cfg.get("proxy_user", "")
    pw   = cfg.get("proxy_pass", "")
    return httpx.Proxy(url=url, auth=(user, pw) if user else None)


# ── HTML helpers ──────────────────────────────────────────────────────────────

def _abs(url: str, base: str) -> str | None:
    if not url or url.startswith("data:"):
        return None
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return urljoin(base, url)
    if url.startswith("http"):
        return url
    return None


def _looks_like_img(url: str) -> bool:
    low = url.lower().split("?")[0]
    return any(low.endswith(ext) for ext in _IMG_EXTS)


def _extract_images(html: str, base_url: str, cdn_hint: str, n: int) -> list[str]:
    """Return up to n absolute image URLs.

    Three-pass strategy:
    1. Classic <img src/data-src> and srcset attributes
    2. Raw CDN domain scan — catches URLs embedded in JSON (Next.js __NEXT_DATA__),
       data-* attributes and any other context without needing file extensions
    Prefers CDN-domain URLs.
    """
    candidates: list[str] = []

    # Pass 1: standard src / data-src attributes
    for m in _SRC_RE.finditer(html):
        url = _abs(m.group(1), base_url)
        if url and (_looks_like_img(url) or cdn_hint in url):
            candidates.append(url)

    # Pass 2: srcset values (img, source, etc.)
    for m in _SRCSET_RE.finditer(html):
        for part in m.group(1).split(","):
            part = part.strip()
            if not part:
                continue
            raw = part.split()[0]   # strip width descriptor "480w"
            url = _abs(raw, base_url)
            if url and (_looks_like_img(url) or cdn_hint in url):
                candidates.append(url)

    # Pass 3: scan entire HTML for CDN domain URLs (catches Next.js JSON, etc.)
    # BBC Next.js uses JSON-escaped slashes (\/) — unescape before scanning
    scan_html = html.replace('\\/', '/')
    cdn_pat = re.compile(
        r'https?://[^\s"\'<>]*' + re.escape(cdn_hint) + r'[^\s"\'<>]*'
    )
    for m in cdn_pat.finditer(scan_html):
        url = m.group(0).rstrip(".,;:)}]\"'")
        if url not in candidates:
            candidates.append(url)

    seen: set[str] = set()
    cdn, rest = [], []
    for u in candidates:
        if u in seen:
            continue
        seen.add(u)
        (cdn if cdn_hint in u else rest).append(u)
    return (cdn + rest)[:n]


def _extract_scripts(html: str, base_url: str, cdn_hint: str, n: int) -> list[str]:
    base_host = urlparse(base_url).netloc
    candidates: list[str] = []
    for pat in (_CSS_RE, _SCRIPT_RE):
        for m in pat.finditer(html):
            url = _abs(m.group(1), base_url)
            if url:
                candidates.append(url)
    seen: set[str] = set()
    same, cdn, rest = [], [], []
    for u in candidates:
        if u in seen:
            continue
        seen.add(u)
        host = urlparse(u).netloc
        if base_host in host or cdn_hint in host:
            same.append(u)
        elif cdn_hint in u:
            cdn.append(u)
        else:
            rest.append(u)
    return (same + cdn + rest)[:n]


# ── Fetch helpers ─────────────────────────────────────────────────────────────

async def _fetch_html(
    client: httpx.AsyncClient, url: str
) -> tuple[float | None, str | None, str | None]:
    t0 = time.monotonic()
    log.debug("[browser] GET %s …", url)
    try:
        buf = b""
        async with client.stream("GET", url) as r:
            ttfb = round((time.monotonic() - t0) * 1000, 1)
            log.debug("[browser] %s → HTTP %s ttfb=%.0fms", url, r.status_code, ttfb)
            if r.status_code >= 400:
                return None, None, f"HTTP {r.status_code}"
            async for chunk in r.aiter_bytes(8192):
                buf += chunk
                if len(buf) >= MAX_HTML_BYTES:
                    break
        log.debug("[browser] %s done %.0fms %dB", url, (time.monotonic()-t0)*1000, len(buf))
        return ttfb, buf.decode(errors="replace"), None
    except Exception as e:
        log.debug("[browser] %s error after %.0fms: %s", url, (time.monotonic()-t0)*1000, e)
        return None, None, str(e)[:60]


async def _fetch_resource(
    client: httpx.AsyncClient,
    url: str,
    max_bytes: int = MAX_IMG_BYTES,
    referer: str | None = None,
    is_img: bool = False,
) -> tuple[int, float | None, str | None]:
    t0 = time.monotonic()
    try:
        extra: dict[str, str] = {}
        if referer:
            extra["Referer"] = referer
        if is_img:
            extra["Accept"] = _IMG_ACCEPT
        total = 0
        async with client.stream("GET", url, headers=extra) as r:
            if r.status_code >= 400:
                return 0, None, f"HTTP {r.status_code}"
            async for chunk in r.aiter_bytes(16384):
                total += len(chunk)
                if total >= max_bytes:
                    break
        return total, round((time.monotonic() - t0) * 1000, 1), None
    except Exception as e:
        return 0, None, str(e)[:50]


# ── Per-site test ─────────────────────────────────────────────────────────────

async def _test_site(
    client: httpx.AsyncClient, site: dict, emit, node: str, run_idx: int
) -> dict:
    name = site["name"]
    url  = site["url"]
    cdn  = site["cdn"]

    ttfb, html, err = await _fetch_html(client, url)
    img_urls: list[str]    = []
    script_urls: list[str] = []
    if html:
        img_urls    = _extract_images(html, url, cdn, MAX_IMAGES)
        script_urls = _extract_scripts(html, url, cdn, MAX_SCRIPTS)

    await emit({
        "type": "step", "phase": "browser_site_html", "node": node, "run": run_idx,
        "site": name, "ttfb_ms": ttfb,
        "img_found": len(img_urls), "script_found": len(script_urls), "error": err,
    })

    img_ok     = 0
    script_ok  = 0
    res_total_kb = 0
    res_parallel_ms = None
    img_url_set = set(img_urls)
    all_urls    = img_urls + script_urls

    if all_urls:
        t0 = time.monotonic()
        tasks = [
            _fetch_resource(
                client, u,
                MAX_IMG_BYTES if u in img_url_set else MAX_RES_BYTES,
                referer=url,
                is_img=u in img_url_set,
            )
            for u in all_urls
        ]
        raw = await asyncio.gather(*tasks, return_exceptions=True)
        res_parallel_ms = round((time.monotonic() - t0) * 1000, 1)

        for i, res in enumerate(raw):
            if isinstance(res, Exception):
                continue
            kb, elapsed, err2 = res
            if kb > 0:
                if all_urls[i] in img_url_set:
                    img_ok += 1
                else:
                    script_ok += 1
                res_total_kb += kb // 1024

        await emit({
            "type": "step", "phase": "browser_site_resources", "node": node, "run": run_idx,
            "site": name,
            "res_total": len(all_urls), "res_ok": img_ok + script_ok,
            "imgs": len(img_urls), "scripts": len(script_urls),
            "img_ok": img_ok, "script_ok": script_ok,
            "res_kb": res_total_kb, "parallel_ms": res_parallel_ms,
        })

    return {
        "name":            name,
        "ttfb_ms":         ttfb,
        "img_ok":          img_ok,
        "img_total":       len(img_urls),
        "script_ok":       script_ok,
        "script_total":    len(script_urls),
        "res_ok":          img_ok + script_ok,
        "res_total":       len(all_urls),
        "res_kb":          res_total_kb,
        "res_parallel_ms": res_parallel_ms,
        "ok":              ttfb is not None,
    }


# ── Single run ────────────────────────────────────────────────────────────────

async def _run_once(
    cfg: dict, emit, node: str, run_idx: int,
    tg_images: list[str] | None = None,
) -> dict:
    proxy = _proxy(cfg)

    async with httpx.AsyncClient(
        proxy=proxy,
        timeout=httpx.Timeout(PAGE_TIMEOUT),
        follow_redirects=True,
        headers=_HEADERS,
    ) as client:
        site_results = []
        for site in TEST_SITES:
            r = await _test_site(client, site, emit, node, run_idx)
            site_results.append(r)

        tg_result = None
        if tg_images:
            tg_result = await _test_tg_images(client, tg_images, emit, node, run_idx)

    all_sites     = site_results + ([tg_result] if tg_result else [])
    ttfbs         = [r["ttfb_ms"] for r in all_sites if r.get("ttfb_ms") is not None]
    ttfb_avg      = round(sum(ttfbs) / len(ttfbs), 1) if ttfbs else None
    success_pct   = round(sum(1 for r in site_results if r["ok"]) / len(TEST_SITES) * 100, 1)
    total_res_kb  = sum(r.get("res_kb", 0) for r in site_results)
    all_img_ok    = sum(r["img_ok"]    for r in site_results)
    all_img_total = sum(r["img_total"] for r in site_results)
    # Always include TG in media count when configured — failure is a real penalty.
    if tg_result and tg_result.get("img_total", 0) > 0:
        all_img_ok    += tg_result["img_ok"]    * 2   # double weight for TG
        all_img_total += tg_result["img_total"] * 2
    media_ok_pct  = round(all_img_ok / all_img_total * 100, 1) if all_img_total > 0 else 0.0

    return {
        "ttfb_avg":     ttfb_avg,
        "success_pct":  success_pct,
        "media_ok_pct": media_ok_pct,
        "total_res_kb": total_res_kb,
        "total_img_kb": total_res_kb,
        "sites":        site_results,
        "tg_result":    tg_result,
    }


# ── Public API ────────────────────────────────────────────────────────────────

async def _test_tg_images(
    client: httpx.AsyncClient, image_urls: list[str], emit, node: str, run_idx: int
) -> dict:
    """Download pre-fetched Telegram CDN images through the proxy."""
    if not image_urls:
        return {"name": "Telegram", "ok": False, "ttfb_ms": None, "img_ok": 0, "img_total": 0}
    t0 = time.monotonic()
    tasks = [_fetch_resource(client, u, MAX_IMG_BYTES, is_img=True) for u in image_urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    elapsed_ms = round((time.monotonic() - t0) * 1000, 1)

    img_ok  = 0
    ttfbs: list[float] = []
    for res in results:
        if isinstance(res, Exception):
            continue
        kb, elapsed, err = res
        if kb > 0:
            img_ok += 1
        if elapsed is not None:
            ttfbs.append(elapsed)

    ttfb_avg = round(sum(ttfbs) / len(ttfbs), 1) if ttfbs else None
    r = {
        "name":       "Telegram",
        "ok":         img_ok > 0,
        "ttfb_ms":    ttfb_avg,
        "img_ok":     img_ok,
        "img_total":  len(image_urls),
        "parallel_ms": elapsed_ms,
    }
    await emit({
        "type": "step", "phase": "browser_site_resources", "node": node, "run": run_idx,
        "site": "Telegram", "img_ok": img_ok, "img_total": len(image_urls),
        "ttfb_ms": ttfb_avg, "parallel_ms": elapsed_ms,
    })
    return r


async def test_node(
    node: str, cfg: dict,
    log_q: asyncio.Queue | None = None,
    tg_images: list[str] | None = None,
) -> dict:
    """Browser page-load test: BBC + TMDB with images. Runs twice, takes best."""
    host, port, secret, group = (
        cfg["mihomo_host"], cfg["mihomo_port"],
        cfg["mihomo_secret"], cfg["proxy_group"],
    )

    async def emit(ev: dict) -> None:
        if log_q:
            await log_q.put(ev)

    original = await mihomo.get_selector_now(host, port, secret, group)
    samples: list[dict] = []

    try:
        log.debug("[browser] node=%s set_proxy → %s", node, node)
        await mihomo.set_proxy(host, port, secret, group, node)
        await asyncio.sleep(0.5)

        # Warmup: prime proxy connection before first run
        t_wu = time.monotonic()
        warmup_ok = False
        try:
            async with httpx.AsyncClient(
                proxy=_proxy(cfg), timeout=httpx.Timeout(15.0)
            ) as _wc:
                resp = await _wc.head("https://www.gstatic.com/generate_204")
                warmup_ok = resp.status_code < 400
        except Exception as exc:
            log.debug("[browser] node=%s warmup error (%.1fs): %s",
                      node, time.monotonic() - t_wu, exc)
        log.debug("[browser] node=%s warmup ok=%s (%.1fs)",
                  node, warmup_ok, time.monotonic() - t_wu)

        await emit({"type": "step", "phase": "browser_start", "node": node})

        async def _do_run(run_num: int) -> None:
            t_run = time.monotonic()
            log.debug("[browser] node=%s run=%d start", node, run_num)
            r  = await _run_once(cfg, emit, node, run_num, tg_images=tg_images)
            tg_ttfb = (r.get("tg_result") or {}).get("ttfb_ms")
            sc = scoring.browser_score(r["ttfb_avg"], r["success_pct"], r["media_ok_pct"],
                                       tg_ttfb_ms=tg_ttfb)
            gr = scoring.browser_grade(sc)
            log.debug("[browser] node=%s run=%d done (%.1fs) ttfb=%s success=%s score=%s",
                      node, run_num, time.monotonic() - t_run,
                      r["ttfb_avg"], r["success_pct"], sc)
            samples.append({**r, "score": sc, "grade": gr})
            await emit({
                "type": "step", "phase": "browser_run_done", "node": node,
                "run": run_num, "ttfb_avg": r["ttfb_avg"],
                "success_pct": r["success_pct"], "media_ok_pct": r["media_ok_pct"],
                "score": sc, "grade": gr, "total_res_kb": r["total_res_kb"],
                "tg_ttfb_ms": tg_ttfb,
            })

        await _do_run(1)
        await asyncio.sleep(BETWEEN_RUNS_S)
        await _do_run(2)

        # 3rd run only if EXACTLY one of the two failed (one passed, one didn't)
        failed_count = sum(1 for s in samples if s["score"] == 0)
        if failed_count == 1:
            log.debug("[browser] node=%s run=3 (retry: one of two runs failed)", node)
            await asyncio.sleep(BETWEEN_RUNS_S)
            await _do_run(3)
        elif failed_count == 2:
            log.debug("[browser] node=%s both runs failed — no 3rd run", node)
    finally:
        if original:
            try:
                await mihomo.set_proxy(host, port, secret, group, original)
            except Exception:
                pass

    # Build final pair: [successful_run, run3] or both runs or empty
    if len(samples) == 3:
        final_pair = [s for s in samples[:2] if s["score"] > 0] + [samples[2]]
    else:
        final_pair = samples

    def _avg_nn(vals: list) -> float | None:
        v = [x for x in vals if x is not None]
        return round(sum(v) / len(v), 1) if v else None

    n = len(final_pair) or 1
    avg_score    = round(sum(s["score"] for s in final_pair) / n, 1)
    grade        = scoring.browser_grade(round(avg_score))
    ttfb_avg     = _avg_nn([s["ttfb_avg"]     for s in final_pair])
    success_pct  = round(sum((s["success_pct"] or 0)  for s in final_pair) / n, 1)
    media_ok_pct = round(sum((s["media_ok_pct"] or 0) for s in final_pair) / n, 1)
    total_res_kb = round(sum((s["total_res_kb"] or 0) for s in final_pair) / n)
    tg_results   = [s.get("tg_result") or {} for s in final_pair]
    tg_img_ok    = round(sum(t.get("img_ok", 0)    for t in tg_results) / n)
    tg_img_total = (tg_results[0].get("img_total", 0) if tg_results else 0)
    tg_ttfb_ms   = _avg_nn([t.get("ttfb_ms") for t in tg_results])

    return {
        "name":           node,
        "ttfb_avg":       ttfb_avg,
        "success_pct":    success_pct,
        "media_ok_pct":   media_ok_pct,
        "total_res_kb":   total_res_kb,
        "total_img_kb":   total_res_kb,
        "score":          avg_score,
        "grade":          grade,
        "tg_img_ok":      tg_img_ok,
        "tg_img_total":   tg_img_total,
        "tg_ttfb_ms":     tg_ttfb_ms,
        "samples": [
            {
                "ttfb_avg":     s["ttfb_avg"],
                "success_pct":  s["success_pct"],
                "media_ok_pct": s["media_ok_pct"],
                "score":        s["score"],
                "sites":        s["sites"],
                "tg_result":    s.get("tg_result"),
            }
            for s in samples
        ],
    }
