import asyncio
import logging
import time

import httpx

from app.core import scoring

log = logging.getLogger("node_tester.speed")

# 5 MB download from Cloudflare's speed-test endpoint — globally reachable, no auth
SPEED_URL       = "https://speed.cloudflare.com/__down?bytes=5242880"
SPEED_BYTES     = 5_242_880
DOWNLOAD_TIMEOUT = 30.0   # seconds
BETWEEN_RUNS_S   = 3.0    # pause between the two download attempts


def _proxy_url(cfg: dict) -> str:
    host = cfg["mihomo_host"].removeprefix("https://").removeprefix("http://").strip("/")
    return f"http://{host}:{cfg['mixed_port']}"


def _proxy(cfg: dict) -> httpx.Proxy:
    url  = _proxy_url(cfg)
    user = cfg.get("proxy_user", "")
    pw   = cfg.get("proxy_pass", "")
    return httpx.Proxy(url=url, auth=(user, pw) if user else None)


async def _measure(proxy: httpx.Proxy, node: str) -> tuple[float | None, str | None]:
    """Download SPEED_BYTES through the proxy; return (Mbps, error_str)."""
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(
            proxy=proxy,
            timeout=httpx.Timeout(DOWNLOAD_TIMEOUT),
            follow_redirects=True,
        ) as client:
            total = 0
            async with client.stream("GET", SPEED_URL) as r:
                log.debug("[speed] node=%s HTTP %s (connect %.1fs)", node, r.status_code,
                          time.monotonic() - t0)
                if r.status_code != 200:
                    return None, f"HTTP {r.status_code}"
                async for chunk in r.aiter_bytes(65536):
                    total += len(chunk)
            elapsed = time.monotonic() - t0
            log.debug("[speed] node=%s downloaded %dB in %.1fs", node, total, elapsed)
            if elapsed < 0.1 or total < 10_000:
                return None, f"too small: {total}B in {elapsed:.1f}s"
            return round(total * 8 / elapsed / 1_000_000, 2), None
    except Exception as exc:
        log.debug("[speed] node=%s error after %.1fs: %s", node, time.monotonic() - t0, exc)
        return None, str(exc)


async def test_node(node: str, cfg: dict, log_q: asyncio.Queue | None = None) -> dict:
    """
    Speed test for a single node:
      1. Switch Mihomo selector to this node.
      2. Download twice (3 s gap), take the best Mbps.
      3. Restore original selector choice.
    """
    import app.mihomo as mihomo

    base_host = cfg["mihomo_host"]
    port      = cfg["mihomo_port"]
    secret    = cfg["mihomo_secret"]
    group     = cfg["proxy_group"]

    async def emit(ev: dict) -> None:
        if log_q:
            await log_q.put(ev)

    # Save current selection so we can restore it
    original = await mihomo.get_selector_now(base_host, port, secret, group)
    log.debug("[speed] node=%s original=%s", node, original)

    try:
        await mihomo.set_proxy(base_host, port, secret, group, node)
        await asyncio.sleep(0.5)   # let Mihomo settle

        # Warmup: prime proxy connection before measuring speed
        t_warmup = time.monotonic()
        warmup_ok = False
        try:
            async with httpx.AsyncClient(
                proxy=_proxy(cfg), timeout=httpx.Timeout(15.0)
            ) as _wc:
                resp = await _wc.head("https://www.gstatic.com/generate_204")
                warmup_ok = resp.status_code < 400
        except Exception as exc:
            log.debug("[speed] node=%s warmup failed (%.1fs): %s",
                      node, time.monotonic() - t_warmup, exc)
        log.debug("[speed] node=%s warmup ok=%s (%.1fs)",
                  node, warmup_ok, time.monotonic() - t_warmup)

        await emit({"type": "step", "phase": "speed_start", "node": node})

        proxy = _proxy(cfg)
        samples: list[float] = []

        async def _do_attempt(attempt: int) -> None:
            log.debug("[speed] node=%s attempt=%d start", node, attempt)
            mbps, err = await _measure(proxy, node)
            log.debug("[speed] node=%s attempt=%d mbps=%s err=%s", node, attempt, mbps, err)
            if mbps is not None:
                samples.append(mbps)
            await emit({
                "type": "step", "phase": "speed_sample",
                "node": node, "attempt": attempt, "mbps": mbps,
                "error": err, "proxy_url": _proxy_url(cfg),
            })

        await _do_attempt(1)
        await asyncio.sleep(BETWEEN_RUNS_S)
        await _do_attempt(2)

        # 3rd attempt only if EXACTLY one of the two succeeded (one failed, one passed)
        if len(samples) == 1:
            log.debug("[speed] node=%s attempt=3 (retry: one of two runs failed)", node)
            await asyncio.sleep(BETWEEN_RUNS_S)
            await _do_attempt(3)
        elif len(samples) == 0:
            log.debug("[speed] node=%s both attempts failed — no 3rd run", node)

    finally:
        if original:
            try:
                await mihomo.set_proxy(base_host, port, secret, group, original)
            except Exception:
                pass

    n = len(samples) or 1
    avg_mbps = round(sum(samples) / n, 2) if samples else None
    log.debug("[speed] node=%s avg=%.2f samples=%s", node, avg_mbps or 0, samples)
    return {
        "name":    node,
        "dl_mbps": avg_mbps,
        "score":   scoring.speed_score(avg_mbps),
        "grade":   scoring.speed_grade(avg_mbps),
        "samples": samples,
    }
