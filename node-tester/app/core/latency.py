import asyncio
from urllib.parse import quote

import httpx

from app.core import scoring

# (name, url, max_pts) — max_pts reflects practical importance.
# Weights are relative; LAT_MAX is auto-computed for normalization.
TEST_URLS = [
    ("Telegram",   "https://api.telegram.org",           16),  # Critical
    ("YouTube",    "https://www.youtube.com",             16),  # Critical
    ("Instagram",  "https://www.instagram.com",           12),  # Critical — blocked RU
    ("Twitter/X",  "https://twitter.com",                 12),  # Critical — blocked RU
    ("WhatsApp",   "https://web.whatsapp.com",            10),  # Important
    ("Google",     "https://gstatic.com/generate_204",    10),  # Important
    ("Apple",      "https://www.apple.com",                8),  # Important
    ("Microsoft",  "https://www.microsoft.com",            8),  # Important
    ("Cloudflare", "https://www.cloudflare.com",           6),  # Standard
    ("Facebook",   "https://www.facebook.com",             4),  # Standard
    ("YT API",     "https://www.googleapis.com",           4),  # Minor
    ("Netflix",    "https://www.netflix.com",              4),  # Minor
]

# Auto-computed — normalizes latency regardless of URL count.
LAT_MAX = sum(max_pts for _, _, max_pts in TEST_URLS)   # = 110

LATENCY_TIMEOUT      = 5000  # ms per URL via Mihomo /delay
URL_DELAY_MS         = 300   # pause between sequential URL tests (ms)
STABILITY_PINGS      = 20    # sequential pings for true jitter/loss measurement
STABILITY_DELAY_MS   = 200   # pause between stability pings (ms)


def _api_base(host: str, port: int) -> str:
    h = host.removeprefix("https://").removeprefix("http://").strip("/")
    return f"http://{h}:{port}"


def _headers(secret: str) -> dict:
    return {"Authorization": f"Bearer {secret}"} if secret else {}


async def _delay(client: httpx.AsyncClient, base: str, hdrs: dict,
                 node: str, url: str, timeout_ms: int) -> int | None:
    endpoint = f"{base}/proxies/{quote(node, safe='')}/delay"
    try:
        r = await client.get(endpoint, headers=hdrs,
                             params={"timeout": timeout_ms, "url": url})
        if r.status_code == 200:
            return r.json().get("delay")
        return None
    except Exception:
        return None


async def test_node(
    node: str, cfg: dict, log_q: asyncio.Queue | None = None
) -> dict:
    base = _api_base(cfg["mihomo_host"], cfg["mihomo_port"])
    hdrs = _headers(cfg["mihomo_secret"])

    async def emit(event: dict) -> None:
        if log_q is not None:
            await log_q.put(event)

    async with httpx.AsyncClient(timeout=httpx.Timeout(9.0)) as client:

        await emit({"type": "step", "phase": "url_start", "node": node})

        # Phase 1: test each service URL sequentially.
        # Strategy: 2 attempts with 1s gap, take the best result.
        # If both timed out: wait 3s and do a 3rd decisive attempt.
        url_results = []
        for name, url, max_pts in TEST_URLS:
            ms1 = await _delay(client, base, hdrs, node, url, LATENCY_TIMEOUT)
            await asyncio.sleep(1.0)
            ms2 = await _delay(client, base, hdrs, node, url, LATENCY_TIMEOUT)

            ok = [m for m in (ms1, ms2) if m is not None]
            if ok:
                ms       = min(ok)   # best of two
                attempts = 2
            else:
                # Both failed → decisive 3rd attempt after a longer pause
                await asyncio.sleep(3.0)
                ms       = await _delay(client, base, hdrs, node, url, LATENCY_TIMEOUT)
                attempts = 3

            raw      = scoring.latency_score(ms)
            weighted = round(raw / 10 * max_pts, 1)
            url_results.append({
                "name": name, "ms": ms,
                "score": weighted, "raw_score": raw, "max_pts": max_pts,
                "attempts": attempts,
            })
            await emit({"type": "step", "phase": "url", "node": node,
                        "service": name, "ms": ms, "ms1": ms1, "ms2": ms2,
                        "max_pts": max_pts, "attempts": attempts})
            await asyncio.sleep(URL_DELAY_MS / 1000)

        # Phase 2: stability pings — use only infrastructure-grade URLs.
        # Social/messaging services (TG, Instagram, WhatsApp, etc.) rate-limit
        # or drop repeated pings, making stability results unreliable.
        _STAB_PREFERRED = {"Google", "Cloudflare", "YouTube", "Apple", "Microsoft", "YT API"}
        ok_pairs = [
            (r["ms"], url, r["name"])
            for r, (_, url, _) in zip(url_results, TEST_URLS)
            if r["ms"] is not None
        ]
        preferred = [(ms, url, name) for ms, url, name in ok_pairs if name in _STAB_PREFERRED]
        candidates = preferred if preferred else ok_pairs  # fallback: any working URL

        if candidates:
            best_ms, best_url, best_name = min(candidates, key=lambda x: x[0])
            await emit({"type": "step", "phase": "stability_start",
                        "node": node, "url_name": best_name, "base_ms": best_ms,
                        "preferred": best_name in _STAB_PREFERRED})

            # Warmup — discard
            await _delay(client, base, hdrs, node, best_url, LATENCY_TIMEOUT)
            await asyncio.sleep(0.15)

            stab_samples: list[int] = []
            actual_fails = 0
            for i in range(STABILITY_PINGS):
                ms = await _delay(client, base, hdrs, node, best_url, LATENCY_TIMEOUT)
                if ms is not None:
                    stab_samples.append(ms)
                else:
                    actual_fails += 1
                # Emit every 4 pings and always on the last
                if (i + 1) % 4 == 0 or i == STABILITY_PINGS - 1:
                    await emit({"type": "step", "phase": "stability_progress",
                                "node": node, "done": i + 1,
                                "total": STABILITY_PINGS, "ms": ms})
                await asyncio.sleep(STABILITY_DELAY_MS / 1000)

            # Trimmed jitter: drop top ~15% outliers
            if len(stab_samples) >= 10:
                n_drop = max(2, len(stab_samples) // 7)
                trimmed = sorted(stab_samples)[:-n_drop]
                jitter_ms = trimmed[-1] - trimmed[0]
            elif len(stab_samples) >= 4:
                trimmed = sorted(stab_samples)[:-1]
                jitter_ms = trimmed[-1] - trimmed[0]
            elif len(stab_samples) >= 2:
                jitter_ms = max(stab_samples) - min(stab_samples)
            else:
                jitter_ms = None

            # Grace of 1: forgive a single isolated timeout
            effective_fails = max(0, actual_fails - 1)
            loss_pct = round(effective_fails / STABILITY_PINGS * 100, 1)
        else:
            jitter_ms = None
            loss_pct  = 100.0

    return _build(node, url_results, jitter_ms, loss_pct)


def _build(node: str, url_results: list[dict], jitter_ms: int | None, loss_pct: float) -> dict:
    lat_raw  = round(sum(r["score"] for r in url_results))   # 0-80
    ok_ms    = [r["ms"] for r in url_results if r["ms"] is not None]
    avg_ms   = round(sum(ok_ms) / len(ok_ms)) if ok_ms else None

    jitter_sc = scoring.jitter_score(jitter_ms)     # 0-5
    loss_sc   = scoring.packet_loss_score(loss_pct)  # 0-15
    stab_raw  = jitter_sc + loss_sc                  # 0-20

    # Block weights for Quick mode (from 5-block plan: latency=30, stability=20).
    # Normalized to 100pts: latency contributes 60%, stability 40%.
    # This means stability has 2× more impact than in the old 80/20 split.
    lat_contrib  = round(lat_raw  / LAT_MAX * 60)   # 0-60
    stab_contrib = round(stab_raw / 20 * 40)   # 0-40
    total        = lat_contrib + stab_contrib   # 0-100

    lat_failed = lat_raw == 0
    if lat_failed:
        block_failed = True
        fail_reason  = "Latency block: 0 — all URLs unreachable"
    else:
        block_failed = False
        fail_reason  = None

    return {
        "name":          node,
        "lat_raw":       lat_raw,          # 0-80 raw latency score
        "stab_raw":      stab_raw,         # 0-20 raw stability score
        "lat_contrib":   lat_contrib,      # 0-60 weighted contribution
        "stab_contrib":  stab_contrib,     # 0-40 weighted contribution
        "total_score":   total,            # 0-100
        "grade":         scoring.grade(total),
        "block_failed":  block_failed,
        "fail_reason":   fail_reason,
        "avg_ms":        avg_ms,
        "jitter_ms":     jitter_ms,
        "loss_pct":      loss_pct,
        "url_results":   url_results,
    }
