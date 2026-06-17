"""TCP 16-20 DPI bypass test.

Sends HEAD + 64 KB POST to each host from the hyperion-cs suite through the proxy.
A timeout on POST means DPI froze the connection (or the proxy can't pass large data).
"""
import asyncio
import logging
import os
import time

import httpx

from app.core import scoring

log = logging.getLogger("node_tester.dpi")

# Full suite from hyperion-cs/dpi-checkers suite.v2.json
DPI_HOSTS: list[dict] = [
    {"id": "US.GH-HPRN",  "provider": "Self check",       "host": "hyperion-cs.github.io"},
    {"id": "PL.AKM-01",   "provider": "Akamai",            "host": "www.mobil.com.se"},
    {"id": "SE.AKM-01",   "provider": "Akamai",            "host": "cdn.apple-mapkit.com"},
    {"id": "DE.AWS-01",   "provider": "AWS",               "host": "amplifon.com"},
    {"id": "US.AWS-01",   "provider": "AWS",               "host": "optout.aboutads.info"},
    {"id": "US.CDN77-01", "provider": "CDN77",             "host": "cdn.eso.org"},
    {"id": "CA.CF-01",    "provider": "Cloudflare",        "host": "go.coveo.com"},
    {"id": "CA.CF-02",    "provider": "Cloudflare",        "host": "justice.gov"},
    {"id": "US.CF-01",    "provider": "Cloudflare",        "host": "img.wzstats.gg"},
    {"id": "US.CF-02",    "provider": "Cloudflare",        "host": "esm.sh"},
    {"id": "FR.CNTB-01",  "provider": "Contabo",           "host": "antoniotartaglia.it"},
    {"id": "FR.CNTB-02",  "provider": "Contabo",           "host": "status.moow.info"},
    {"id": "DE.DO-01",    "provider": "DigitalOcean",      "host": "ui-arts.com"},
    {"id": "UK.DO-01",    "provider": "DigitalOcean",      "host": "app.thecuriositylibrary.com"},
    {"id": "UK.DO-02",    "provider": "DigitalOcean",      "host": "admin.survey54.com"},
    {"id": "CA.FST-01",   "provider": "Fastly",            "host": "ssl.p.jwpcdn.com"},
    {"id": "US.FST-01",   "provider": "Fastly",            "host": "www.jetblue.com"},
    {"id": "US.FTBVM-01", "provider": "FT/BuyVM",          "host": "buyvm.net"},
    {"id": "US.FTBVM-02", "provider": "FT/BuyVM",          "host": "dmvideo.download"},
    {"id": "LU.GCORE-01", "provider": "Gcore",             "host": "gcore.com"},
    {"id": "US.GC-01",    "provider": "Google Cloud",      "host": "api.usercentrics.eu"},
    {"id": "US.GC-02",    "provider": "Google Cloud",      "host": "widgets.reputation.com"},
    {"id": "DE.HE-01",    "provider": "Hetzner",           "host": "king.hr"},
    {"id": "DE.HE-02",    "provider": "Hetzner",           "host": "mail.server.apaone.com"},
    {"id": "FI.HE-01",    "provider": "Hetzner",           "host": "nioges.com"},
    {"id": "FI.HE-02",    "provider": "Hetzner",           "host": "5fd8bdae.nip.io"},
    {"id": "FI.HE-03",    "provider": "Hetzner",           "host": "net4u.de"},
    {"id": "US.MBCOM-01", "provider": "Melbicom",          "host": "elecane.com"},
    {"id": "NL.MS-01",    "provider": "Microsoft/Azure",   "host": "store.takeda.com"},
    {"id": "ES.OR-01",    "provider": "Oracle",            "host": "sh00065.hostgator.com"},
    {"id": "SG.OR-01",    "provider": "Oracle",            "host": "ged.com.sg"},
    {"id": "FR.OVH-01",   "provider": "OVH",              "host": "www.adwin.fr"},
    {"id": "FR.OVH-02",   "provider": "OVH",              "host": "www.emca.be"},
    {"id": "NL.SW-01",    "provider": "Scaleway",          "host": "www.velivole.fr"},
    {"id": "DE.VLTR-01",  "provider": "Vultr",             "host": "askit-app.de"},
    {"id": "US.VLTR-01",  "provider": "Vultr",             "host": "us.rudder.qntmnet.com"},
]

ALIVE_TIMEOUT = 10.0        # HEAD check timeout (s)
POST_TIMEOUT  = 20.0        # POST 64 KB timeout (s) — DPI freezes connection here
POST_SIZE     = 64 * 1024   # 64 KB payload
CONCURRENCY   = 18          # parallel host checks


def _proxy_url(cfg: dict) -> str:
    host = cfg["mihomo_host"].removeprefix("https://").removeprefix("http://").strip("/")
    return f"http://{host}:{cfg['mixed_port']}"


def _make_proxy(cfg: dict) -> httpx.Proxy:
    url  = _proxy_url(cfg)
    user = cfg.get("proxy_user", "")
    pw   = cfg.get("proxy_pass", "")
    return httpx.Proxy(url=url, auth=(user, pw) if user else None)


async def _check_host(
    host_info: dict,
    proxy: httpx.Proxy,
    emit,
    node: str,
) -> dict:
    host = host_info["host"]
    hid  = host_info["id"]
    prov = host_info["provider"]
    url  = f"https://{host}/"
    result = {"id": hid, "provider": prov, "host": host,
              "alive": False, "dpi_passed": False, "error": None}

    # 1. Alive check — HEAD
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(proxy=proxy, timeout=ALIVE_TIMEOUT, verify=False) as cl:
            r = await cl.head(url, follow_redirects=True)
            result["alive"] = r.status_code < 600
        log.debug("[dpi] node=%s %s alive=%s (%.1fs)",
                  node, host, result["alive"], time.monotonic() - t0)
    except Exception as exc:
        result["error"] = f"alive: {str(exc)[:80]}"
        log.debug("[dpi] node=%s %s alive=err %.1fs: %s", node, host, time.monotonic() - t0, exc)
        await emit({"type": "step", "phase": "dpi_host", "node": node,
                    "id": hid, "provider": prov, "host": host,
                    "alive": False, "dpi": None, "error": result["error"]})
        return result

    if not result["alive"]:
        await emit({"type": "step", "phase": "dpi_host", "node": node,
                    "id": hid, "provider": prov, "host": host,
                    "alive": False, "dpi": None})
        return result

    # 2. DPI check — POST 64 KB
    payload = os.urandom(POST_SIZE)
    t1 = time.monotonic()
    try:
        async with httpx.AsyncClient(proxy=proxy, timeout=POST_TIMEOUT, verify=False) as cl:
            await cl.post(url, content=payload, follow_redirects=False)
        result["dpi_passed"] = True
        log.debug("[dpi] node=%s %s POST ok (%.1fs)", node, host, time.monotonic() - t1)
    except httpx.TimeoutException:
        result["dpi_passed"] = False
        result["error"] = "post: timeout (DPI detected)"
        log.debug("[dpi] node=%s %s POST TIMEOUT=DPI blocked (%.1fs)", node, host, time.monotonic() - t1)
    except Exception as exc:
        # Connection reset/refused ≠ DPI freeze — count as passed
        result["dpi_passed"] = True
        log.debug("[dpi] node=%s %s POST non-timeout err (%.1fs): %s",
                  node, host, time.monotonic() - t1, exc)

    await emit({"type": "step", "phase": "dpi_host", "node": node,
                "id": hid, "provider": prov, "host": host,
                "alive": result["alive"], "dpi": result["dpi_passed"],
                "error": result.get("error")})
    return result


async def test_node(node: str, cfg: dict, log_q: asyncio.Queue | None = None) -> dict:
    import app.mihomo as mihomo

    base_host = cfg["mihomo_host"]
    port      = cfg["mihomo_port"]
    secret    = cfg["mihomo_secret"]
    group     = cfg["proxy_group"]
    proxy     = _make_proxy(cfg)

    async def emit(ev: dict) -> None:
        if log_q:
            await log_q.put(ev)

    original = await mihomo.get_selector_now(base_host, port, secret, group)
    log.debug("[dpi] node=%s original=%s", node, original)
    results: list[dict] = []

    try:
        await mihomo.set_proxy(base_host, port, secret, group, node)
        await asyncio.sleep(0.5)

        # Warmup — prime proxy TCP before tests
        t_wu = time.monotonic()
        warmup_ok = False
        try:
            async with httpx.AsyncClient(proxy=proxy, timeout=12.0, verify=False) as wc:
                r = await wc.head("https://www.gstatic.com/generate_204")
                warmup_ok = r.status_code < 400
        except Exception as exc:
            log.debug("[dpi] node=%s warmup err %.1fs: %s", node, time.monotonic() - t_wu, exc)
        log.debug("[dpi] node=%s warmup ok=%s %.1fs", node, warmup_ok, time.monotonic() - t_wu)

        await emit({"type": "step", "phase": "dpi_start", "node": node,
                    "total": len(DPI_HOSTS)})

        sem = asyncio.Semaphore(CONCURRENCY)

        async def _bounded(h: dict) -> dict:
            async with sem:
                return await _check_host(h, proxy, emit, node)

        results = list(await asyncio.gather(*[_bounded(h) for h in DPI_HOSTS]))

    finally:
        if original:
            try:
                await mihomo.set_proxy(base_host, port, secret, group, original)
            except Exception:
                pass

    alive_count  = sum(1 for r in results if r["alive"])
    passed_count = sum(1 for r in results if r["dpi_passed"])
    sc = scoring.dpi_score(passed_count, alive_count)
    gr = scoring.dpi_grade(sc)
    log.debug("[dpi] node=%s alive=%d passed=%d score=%s grade=%s",
              node, alive_count, passed_count, sc, gr)

    await emit({"type": "step", "phase": "dpi_done", "node": node,
                "alive_count": alive_count, "passed_count": passed_count,
                "total": len(DPI_HOSTS), "score": sc, "grade": gr})

    return {
        "name":         node,
        "alive_count":  alive_count,
        "passed_count": passed_count,
        "total_hosts":  len(DPI_HOSTS),
        "score":        sc,
        "grade":        gr,
        "results": [
            {"id":       r["id"],
             "provider": r["provider"],
             "host":     r["host"],
             "alive":    r["alive"],
             "dpi":      r["dpi_passed"],
             "error":    r.get("error")}
            for r in results
        ],
    }
