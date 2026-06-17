"""
Video Test — Module 5.

Persistent Chromium: single browser instance for all nodes (standalone runner),
fresh context per node. Single run. DASH-IF only.

HTML + dash.js served locally via page.route() — только DASH manifest/segments
идут через Mihomo (это и есть тест). dash.js скачивается один раз при старте.
"""
import asyncio
import logging
import time

import httpx
from playwright.async_api import async_playwright, Browser

import app.mihomo as mihomo
from app.core import scoring

log = logging.getLogger("node_tester.video")

DASH_MANIFEST    = "https://storage.googleapis.com/shaka-demo-assets/angel-one/dash.mpd"
_FAKE_PAGE_URL   = "http://videotest.local/"
_DASHJS_CDN_URL  = "https://cdn.jsdelivr.net/npm/dashjs@4.7.4/dist/dash.all.min.js"
# Точный URL — без glob, Playwright перехватит именно этот запрос
_DASHJS_ROUTE    = _DASHJS_CDN_URL

_dashjs_bytes: bytes | None = None
_dashjs_lock = asyncio.Lock()


async def _ensure_dashjs() -> bytes:
    """Скачивает dash.js один раз (напрямую, без прокси) и кэширует в памяти."""
    global _dashjs_bytes
    if _dashjs_bytes:
        return _dashjs_bytes
    async with _dashjs_lock:
        if _dashjs_bytes:
            return _dashjs_bytes
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            r = await client.get(_DASHJS_CDN_URL)
            r.raise_for_status()
            _dashjs_bytes = r.content
    return _dashjs_bytes

# Страница обслуживается через page.route() — браузер "видит" нормальный URL,
# загрузку HTML Playwright перехватывает сам (без прокси),
# dash.js и DASH-видеопоток — через Mihomo (это и есть тест).
_VIDEO_TEST_HTML = b"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Video Test</title>
<style>body{margin:0;background:#000;}video{width:100%;height:100vh;}</style>
</head>
<body>
<video id="v" autoplay muted playsinline></video>
<script>
(function() {
  var s = document.createElement('script');
  s.src = 'https://cdn.jsdelivr.net/npm/dashjs@4.7.4/dist/dash.all.min.js';
  s.onload = function() {
    try {
      dashjs.MediaPlayer().create().initialize(
        document.getElementById('v'),
        'https://storage.googleapis.com/shaka-demo-assets/angel-one/dash.mpd',
        true
      );
      console.log('dashjs:init:ok');
    } catch(e) { console.error('dashjs:init:error:' + e.message); }
  };
  s.onerror = function() { console.error('dashjs:load:failed'); };
  document.head.appendChild(s);
  console.log('dashjs:loading');
})();
</script>
</body>
</html>"""

VIDEO_SITES = [
    {
        "name":            "DASH-IF",
        "player_selector": "video",
        "video_selector":  "video",
    },
]

PAGE_TIMEOUT     = 15_000   # ms
VIDEO_POLL_S     = 1        # интервал опроса — чем меньше, тем быстрее реагирует на Stop
VIDEO_MAX_WAIT_S = 40       # максимальное ожидание начала буферизации

CHROMIUM_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-software-rasterizer",
    "--disable-extensions",
    "--disable-blink-features=AutomationControlled",
    "--autoplay-policy=no-user-gesture-required",
]

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _proxy_url_str(cfg: dict) -> str:
    """httpx-compatible proxy URL with auth credentials."""
    host = cfg["mihomo_host"].removeprefix("https://").removeprefix("http://").strip("/")
    user = cfg.get("proxy_user", "")
    pw   = cfg.get("proxy_pass", "")
    if user and pw:
        return f"http://{user}:{pw}@{host}:{cfg['mixed_port']}"
    return f"http://{host}:{cfg['mixed_port']}"


def _proxy_cfg(cfg: dict) -> dict:
    host = cfg["mihomo_host"].removeprefix("https://").removeprefix("http://").strip("/")
    d: dict = {"server": f"http://{host}:{cfg['mixed_port']}"}
    if cfg.get("proxy_user"):
        d["username"] = cfg["proxy_user"]
        d["password"] = cfg.get("proxy_pass", "")
    return d


async def _disable_cache(page) -> None:
    try:
        cdp = await page.context.new_cdp_session(page)
        await cdp.send("Network.enable", {})
        await cdp.send("Network.setCacheDisabled", {"cacheDisabled": True})
        await cdp.send("Network.clearBrowserCache", {})
    except Exception:
        pass


async def _video_state(page, selector: str) -> dict | None:
    try:
        return await page.evaluate(f"""() => {{
            const v = document.querySelector('{selector}');
            if (!v) return null;
            return {{
                readyState:   v.readyState,
                buffered_len: v.buffered.length,
                buffered_end: v.buffered.length > 0 ? v.buffered.end(0) : 0,
                current_time: v.currentTime,
                paused:       v.paused,
                error:        v.error ? v.error.code : null,
                src:          v.src || v.currentSrc || null,
            }};
        }}""")
    except Exception:
        return None


async def _wait_for_buffering(
    page, selector: str, stop: asyncio.Event | None = None
) -> tuple[bool, float, int | None]:
    """Опрашивает видео каждые VIDEO_POLL_S секунд до буферизации или таймаута.
    Возвращает (buffered, buffered_secs, ttb_ms)."""
    t0 = time.monotonic()
    deadline = t0 + VIDEO_MAX_WAIT_S
    while time.monotonic() < deadline:
        if stop and stop.is_set():
            return False, 0.0, None
        await asyncio.sleep(VIDEO_POLL_S)
        vs = await _video_state(page, selector)
        if vs:
            if (vs.get("readyState") or 0) >= 3 or (vs.get("buffered_len") or 0) > 0:
                ttb_ms = round((time.monotonic() - t0) * 1000)
                return True, round(vs.get("buffered_end") or 0.0, 2), ttb_ms
    return False, 0.0, None


# ── Per-site test ─────────────────────────────────────────────────────────────

async def _test_site(
    page, site: dict, emit, node: str,
    dashjs_bytes: bytes | None = None,
    stop: asyncio.Event | None = None,
) -> dict:
    name    = site["name"]
    vid_sel = site["video_selector"]

    await _disable_cache(page)

    # HTML — без прокси
    async def _serve_html(route):
        await route.fulfill(content_type="text/html; charset=utf-8", body=_VIDEO_TEST_HTML)
    await page.route(_FAKE_PAGE_URL, _serve_html)

    # dash.js — локально (скачан заранее без прокси); DASH-поток идёт через прокси
    if dashjs_bytes:
        async def _serve_dashjs(route):
            await route.fulfill(
                content_type="application/javascript; charset=utf-8",
                body=dashjs_bytes,
            )
        await page.route(_DASHJS_ROUTE, _serve_dashjs)

    player_ok      = False
    video_buffered = False
    buffered_secs  = 0.0
    ttb_ms: int | None = None
    err            = None
    console_msgs: list[str] = []

    def _on_console(msg):
        console_msgs.append(f"[{msg.type}] {msg.text[:120]}")

    page.on("console", _on_console)
    page.on("pageerror", lambda e: console_msgs.append(f"[pageerror] {str(e)[:120]}"))

    try:
        await page.goto(_FAKE_PAGE_URL, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")

        try:
            await page.wait_for_selector(site["player_selector"], timeout=10_000)
            player_ok = True
        except Exception:
            player_ok = False

        if player_ok and not (stop and stop.is_set()):
            video_buffered, buffered_secs, ttb_ms = await _wait_for_buffering(
                page, vid_sel, stop
            )
            # Measure buffer growth for _DASH_MEASURE_S seconds after detection
            if video_buffered and not (stop and stop.is_set()):
                for _ in range(_DASH_MEASURE_S):
                    if stop and stop.is_set():
                        break
                    await asyncio.sleep(1)
                vs = await _video_state(page, vid_sel)
                if vs and (vs.get("buffered_end") or 0) > buffered_secs:
                    buffered_secs = round(vs["buffered_end"], 2)

        if not video_buffered:
            # Дамп финального состояния для диагностики
            vs = await _video_state(page, vid_sel)
            diag = {
                "dashjs_local": bool(dashjs_bytes),
                "video_state": vs,
                "console": console_msgs[-10:],
            }
            await emit({"type": "step", "phase": "video_diag", "node": node, "site": name, "diag": diag})

        await emit({
            "type": "step", "phase": "video_site_done",
            "node": node, "site": name,
            "ok": player_ok, "video_buffered": video_buffered,
            "buffered_secs": buffered_secs, "ttb_ms": ttb_ms,
        })

    except Exception as exc:
        err = str(exc)[:100]
        await emit({
            "type": "step", "phase": "video_site_err",
            "node": node, "site": name, "error": err,
            "console": console_msgs[-5:],
        })

    return {
        "name":           name,
        "ok":             player_ok,
        "video_buffered": video_buffered,
        "buffered_secs":  buffered_secs,
        "ttb_ms":         ttb_ms,
        "error":          err,
    }


# ── Telegram direct download test ────────────────────────────────────────────

_DASH_MEASURE_S      = 8          # seconds to keep measuring after DASH starts
_TG_DIRECT_MAX_BYTES = 10 * 1024 * 1024   # 10 MB per video — confirms CDN access + measures speed


async def _test_tg_direct(
    proxy_url: str, mp4_url: str, emit, node: str,
    stop: asyncio.Event | None = None,
) -> dict:
    """Download TG video MP4 directly via httpx through the node proxy.
    Much more reliable than Playwright embed: cdn4.telesco.pe goes through proxy,
    no autoplay/GPU issues, fast and measurable.
    """
    t0 = time.monotonic()
    ok = False
    ttb_ms: int | None = None
    downloaded = 0
    error = None

    try:
        async with httpx.AsyncClient(
            proxy=proxy_url, timeout=30.0, follow_redirects=True
        ) as client:
            async with client.stream("GET", mp4_url) as r:
                if r.status_code >= 400:
                    error = f"HTTP {r.status_code}"
                else:
                    async for chunk in r.aiter_bytes(65536):
                        if stop and stop.is_set():
                            break
                        if ttb_ms is None:
                            ttb_ms = round((time.monotonic() - t0) * 1000)
                        downloaded += len(chunk)
                        if downloaded >= _TG_DIRECT_MAX_BYTES:
                            ok = True
                            break
                    if downloaded > 0:
                        ok = True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        error = str(e)[:80]

    elapsed = round((time.monotonic() - t0) * 1000)
    speed_mbps = round(downloaded * 8 / elapsed / 1000, 2) if elapsed > 0 and downloaded > 0 else None
    downloaded_mb = round(downloaded / 1_048_576, 2)
    log.debug("[video] tg_direct node=%s ok=%s ttb=%s speed=%s elapsed=%dms err=%s",
              node, ok, ttb_ms, speed_mbps, elapsed, error)

    await emit({
        "type": "step", "phase": "tg_video_done",
        "node": node, "url": mp4_url,
        "ok": ok, "ttb_ms": ttb_ms,
        "speed_mbps": speed_mbps, "downloaded_mb": downloaded_mb,
        "error": error,
    })
    return {"url": mp4_url, "ok": ok, "ttb_ms": ttb_ms, "speed_mbps": speed_mbps}


# ── Result builder ────────────────────────────────────────────────────────────

def _build_result(node: str, site_results: list[dict], tg_results: list[dict] | None = None) -> dict:
    videos_buffered = sum(1 for r in site_results if r["video_buffered"])
    total           = len(VIDEO_SITES)
    ttbs            = [r["ttb_ms"] for r in site_results if r.get("ttb_ms") is not None]
    ttb_avg         = round(sum(ttbs) / len(ttbs), 1) if ttbs else None
    buf_list        = [r["buffered_secs"] for r in site_results if (r.get("buffered_secs") or 0) > 0]
    buf_secs_avg    = round(sum(buf_list) / len(buf_list), 2) if buf_list else 0.0

    # Telegram component
    tg_ok  = sum(1 for r in (tg_results or []) if r.get("ok"))
    tg_ttbs = [r["ttb_ms"] for r in (tg_results or []) if r.get("ttb_ms") is not None]
    tg_ttb_avg = round(sum(tg_ttbs) / len(tg_ttbs), 1) if tg_ttbs else None
    tg_total = len(tg_results) if tg_results else 0

    sc = scoring.video_score(ttb_avg, videos_buffered, total, tg_ttb_avg, tg_ok, tg_total,
                             buf_secs=buf_secs_avg)
    gr = scoring.video_grade(sc)

    return {
        "name":            node,
        "ttb_avg":         ttb_avg,
        "buf_secs":        buf_secs_avg,
        "pages_ok":        sum(1 for r in site_results if r["ok"]),
        "videos_buffered": videos_buffered,
        "total":           total,
        "tg_ok":           tg_ok,
        "tg_total":        tg_total,
        "tg_ttb_avg":      tg_ttb_avg,
        "score":           sc,
        "grade":           gr,
        "samples": [{
            "ttb_avg":         ttb_avg,
            "pages_ok":        sum(1 for r in site_results if r["ok"]),
            "videos_buffered": videos_buffered,
            "tg_ok":           tg_ok,
            "tg_total":        tg_total,
            "score":           sc,
            "grade":           gr,
            "sites":           site_results,
            "tg_sites":        tg_results or [],
        }],
    }


# ── Per-node test (shared browser) ───────────────────────────────────────────

async def _test_node_with_browser(
    browser: Browser, node: str, cfg: dict, emit,
    dashjs_bytes: bytes | None = None,
    tg_sources: list[str] | None = None,
    stop: asyncio.Event | None = None,
    run_num: int = 1,
) -> dict:
    """Тест одной ноды в уже запущенном браузере. Свежий контекст на каждую ноду."""
    await emit({"type": "step", "phase": "video_start", "node": node, "run": run_num})

    context = await browser.new_context(
        user_agent=_UA,
        viewport={"width": 1280, "height": 720},
        ignore_https_errors=True,
        java_script_enabled=True,
        extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
    )
    await context.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    )

    site_results: list[dict] = []
    tg_results:   list[dict] = []
    try:
        for site in VIDEO_SITES:
            if stop and stop.is_set():
                break
            page = await context.new_page()
            r = await _test_site(page, site, emit, node, dashjs_bytes=dashjs_bytes, stop=stop)
            site_results.append(r)
            await page.close()

        # Telegram video tests: direct httpx download (no Playwright needed)
        _proxy = _proxy_url_str(cfg)
        for mp4_url in (tg_sources or []):
            if stop and stop.is_set():
                break
            r = await _test_tg_direct(_proxy, mp4_url, emit, node, stop=stop)
            tg_results.append(r)
    finally:
        try:
            await context.close()
        except Exception:
            pass

    result = _build_result(node, site_results, tg_results)
    log.debug("[video] node=%s run=%d DASH buf=%d/%d ttb=%s tg=%d/%d score=%s",
              node, run_num,
              result["videos_buffered"], result["total"], result["ttb_avg"],
              result["tg_ok"], result["tg_total"], result["score"])
    await emit({
        "type": "step", "phase": "video_run_done",
        "node": node, "run": run_num,
        "ttb_avg": result["ttb_avg"],
        "pages_ok": result["pages_ok"],
        "videos_buffered": result["videos_buffered"],
        "total": result["total"],
        "tg_ok": result["tg_ok"],
        "tg_total": result["tg_total"],
        "score": result["score"],
        "grade": result["grade"],
    })
    return result


async def _run_video_twice(
    browser: Browser, node: str, cfg: dict, emit,
    dashjs_bytes: bytes | None = None,
    tg_sources: list[str] | None = None,
    stop: asyncio.Event | None = None,
) -> dict:
    """Запустить видео-тест дважды, вернуть лучший результат, объединить samples."""
    # Warmup: establish proxy connection before first run.
    # Slow-handshake protocols (amnezia, reality, etc.) need first TCP to settle.
    _proxy = _proxy_url_str(cfg)
    t_wu = time.monotonic()
    warmup_ok = False
    try:
        async with httpx.AsyncClient(proxy=_proxy, timeout=15.0) as _wc:
            resp = await _wc.head("https://www.gstatic.com/generate_204")
            warmup_ok = resp.status_code < 400
    except Exception as exc:
        log.debug("[video] node=%s warmup error (%.1fs): %s", node, time.monotonic() - t_wu, exc)
    log.debug("[video] node=%s warmup ok=%s (%.1fs)", node, warmup_ok, time.monotonic() - t_wu)
    await asyncio.sleep(0.5)

    async def _do_run(run_num: int) -> dict:
        return await _test_node_with_browser(
            browser, node, cfg, emit,
            dashjs_bytes=dashjs_bytes, tg_sources=tg_sources,
            stop=stop, run_num=run_num,
        )

    run_results: list[dict] = []
    for run_num in range(1, 3):
        if stop and stop.is_set():
            break
        run_results.append(await _do_run(run_num))
        if run_num == 1 and not (stop and stop.is_set()):
            await asyncio.sleep(2.0)

    if not run_results:
        return _build_result(node, [])

    # 3rd run only if EXACTLY one of the two failed (score=0)
    if not (stop and stop.is_set()):
        failed_count = sum(1 for r in run_results if r.get("score", 0) == 0)
        if failed_count == 1:
            log.debug("[video] node=%s run=3 (retry: one of two runs failed)", node)
            await asyncio.sleep(2.0)
            run_results.append(await _do_run(3))
        elif failed_count == 2:
            log.debug("[video] node=%s both runs failed — no 3rd run", node)

    # Build final pair: [successful_run, run3] if 3rd was done, else both runs
    if len(run_results) == 3:
        final_pair = [r for r in run_results[:2] if r.get("score", 0) > 0] + [run_results[2]]
    else:
        final_pair = run_results

    def _avg_nn(vals: list) -> float | None:
        v = [x for x in vals if x is not None]
        return round(sum(v) / len(v), 1) if v else None

    n = len(final_pair) or 1
    avg_score = round(sum(r.get("score", 0) for r in final_pair) / n, 1)
    all_samples = [s for r in run_results for s in r.get("samples", [])]
    base = final_pair[0]
    return {
        **base,
        "score":           avg_score,
        "grade":           scoring.video_grade(round(avg_score)),
        "videos_buffered": round(sum((r.get("videos_buffered") or 0) for r in final_pair) / n),
        "buf_secs":        _avg_nn([r.get("buf_secs")    for r in final_pair]),
        "ttb_avg":         _avg_nn([r.get("ttb_avg")     for r in final_pair]),
        "tg_ok":           round(sum((r.get("tg_ok") or 0) for r in final_pair) / n),
        "tg_ttb_avg":      _avg_nn([r.get("tg_ttb_avg") for r in final_pair]),
        "samples":         all_samples,
    }


# ── Public API ────────────────────────────────────────────────────────────────

async def test_all_nodes(
    nodes: list[str], cfg: dict, ev_q: asyncio.Queue,
    stop: asyncio.Event | None = None,
    tg_sources: list[str] | None = None,
) -> None:
    """Standalone видео-runner: один Chromium на все ноды, свежий контекст на каждую.
    stop — asyncio.Event для немедленной остановки (нажатие Stop в UI).
    Кладёт {"_result": ...} в ev_q для каждой завершённой ноды."""
    host, port, secret, group = (
        cfg["mihomo_host"], cfg["mihomo_port"],
        cfg["mihomo_secret"], cfg["proxy_group"],
    )

    async def emit(ev: dict) -> None:
        await ev_q.put(ev)

    # Скачиваем dash.js один раз (напрямую, без прокси)
    try:
        dashjs_bytes = await _ensure_dashjs()
    except Exception:
        dashjs_bytes = None

    proxy_cfg = _proxy_cfg(cfg)
    log.debug("[test_all_nodes] starting, nodes=%d, proxy=%s user=%s",
              len(nodes), proxy_cfg.get("server"), proxy_cfg.get("username", "<none>"))
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, args=CHROMIUM_ARGS, proxy=proxy_cfg,
        )
        try:
            for node in nodes:
                if stop and stop.is_set():
                    log.debug("[test_all_nodes] stop_event is set, breaking before node=%s", node)
                    break
                try:
                    log.debug("[test_all_nodes] set_proxy -> %s", node)
                    await mihomo.set_proxy(host, port, secret, group, node)
                    await asyncio.sleep(0.5)
                    r = await _run_video_twice(
                        browser, node, cfg, emit,
                        dashjs_bytes=dashjs_bytes,
                        tg_sources=tg_sources,
                        stop=stop,
                    )
                    log.debug("[test_all_nodes] node=%s done, score=%s", node, r.get("score"))
                except asyncio.CancelledError:
                    log.debug("[test_all_nodes] CancelledError in node=%s, re-raising", node)
                    raise
                except Exception as exc:
                    log.debug("[test_all_nodes] Exception in node=%s: %s", node, exc)
                    r = _build_result(node, [])
                    r["error"] = str(exc)[:100]
                await ev_q.put({"_result": r})
        finally:
            log.debug("[test_all_nodes] finally: closing browser")
            try:
                await browser.close()
                log.debug("[test_all_nodes] browser closed")
            except Exception as e:
                log.debug("[test_all_nodes] browser.close error: %s", e)


async def launch_shared_browser(cfg: dict):
    """Запустить один Chromium для использования во всех нодах deep test.
    Возвращает (playwright_instance, browser). Закрывать через close_shared_browser()."""
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=True, args=CHROMIUM_ARGS, proxy=_proxy_cfg(cfg),
    )
    return pw, browser


async def close_shared_browser(pw, browser) -> None:
    """Закрыть общий браузер и playwright instance."""
    try:
        await browser.close()
    except Exception:
        pass
    try:
        await pw.stop()
    except Exception:
        pass


async def test_node_in_browser(
    browser, node: str, cfg: dict,
    log_q=None, tg_sources: list[str] | None = None,
    stop: asyncio.Event | None = None,
) -> dict:
    """Тест одной ноды в уже запущенном общем браузере (deep test).
    Браузер не запускается/закрывается — только переключается прокси."""
    host, port, secret, group = (
        cfg["mihomo_host"], cfg["mihomo_port"],
        cfg["mihomo_secret"], cfg["proxy_group"],
    )

    async def emit(ev: dict) -> None:
        if log_q:
            await log_q.put(ev)

    try:
        dashjs_bytes = await _ensure_dashjs()
    except Exception:
        dashjs_bytes = None

    original = await mihomo.get_selector_now(host, port, secret, group)
    try:
        await mihomo.set_proxy(host, port, secret, group, node)
        await asyncio.sleep(0.5)
        return await _run_video_twice(
            browser, node, cfg, emit,
            dashjs_bytes=dashjs_bytes, tg_sources=tg_sources, stop=stop,
        )
    finally:
        if original:
            try:
                await mihomo.set_proxy(host, port, secret, group, original)
            except Exception:
                pass


async def test_node(node: str, cfg: dict, log_q=None, tg_sources: list[str] | None = None) -> dict:
    """Тест одной ноды с собственным браузером. Fallback если shared browser недоступен."""
    host, port, secret, group = (
        cfg["mihomo_host"], cfg["mihomo_port"],
        cfg["mihomo_secret"], cfg["proxy_group"],
    )

    async def emit(ev: dict) -> None:
        if log_q:
            await log_q.put(ev)

    try:
        dashjs_bytes = await _ensure_dashjs()
    except Exception:
        dashjs_bytes = None

    original = await mihomo.get_selector_now(host, port, secret, group)
    try:
        await mihomo.set_proxy(host, port, secret, group, node)
        await asyncio.sleep(0.5)

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True, args=CHROMIUM_ARGS, proxy=_proxy_cfg(cfg),
            )
            try:
                return await _run_video_twice(
                    browser, node, cfg, emit,
                    dashjs_bytes=dashjs_bytes, tg_sources=tg_sources,
                )
            finally:
                try:
                    await browser.close()
                except Exception:
                    pass
    finally:
        if original:
            try:
                await mihomo.set_proxy(host, port, secret, group, original)
            except Exception:
                pass
