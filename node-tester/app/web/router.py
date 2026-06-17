import asyncio
import json
import logging
from pathlib import Path
from zoneinfo import available_timezones

import httpx
from typing import List
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

import app.config as config
import app.db as db
import app.mihomo as mihomo
import app.monitor as monitor
import app.mqtt as mqtt
import app.store as store
from app.core import latency as lat_core
from app.core import speed as spd_core
from app.core import ws_test as ws_core
from app.core import browser_test as br_core
from app.core import video_test as vid_core
from app.core import dpi_test as dpi_core
from app.core import scoring

log = logging.getLogger("node_tester.router")

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _time_ago(ts: str) -> str:
    tz = config.load().get("timezone", "Europe/Moscow")
    return store.time_ago(ts, tz)


templates.env.globals["time_ago"] = _time_ago

_ALL_TIMEZONES = sorted(available_timezones())


def _apply_exclusions(nodes: list[str], cfg: dict) -> list[str]:
    groups = cfg.get("node_groups") or {}
    return [n for n in nodes if groups.get(n, "main") != "excluded"]


def _tg_proxy_url(cfg: dict) -> str | None:
    """Mihomo proxy URL for TG pre-fetch so t.me is reachable in blocked regions."""
    host = cfg.get("mihomo_host", "").removeprefix("https://").removeprefix("http://").strip("/")
    port = cfg.get("mixed_port")
    if not host or not port:
        return None
    user = (cfg.get("proxy_user") or "").strip()
    pw   = (cfg.get("proxy_pass") or "").strip()
    if user and pw:
        return f"http://{user}:{pw}@{host}:{port}"
    return f"http://{host}:{port}"

def _redir(request: Request, path: str, status_code: int = 303) -> RedirectResponse:
    """Redirect that preserves HA ingress path prefix."""
    ing = request.headers.get("x-ingress-path", "")
    return RedirectResponse(f"{ing}{path}", status_code=status_code)


# Глобальный реестр stop-событий и abort-коллбэков по типу теста
_stop_events: dict[str, asyncio.Event] = {}
_abort_callbacks: dict[str, callable] = {}

# Текущий статус теста (для индикатора на главной)
_test_status: dict = {}   # {"type": "quick"|"deep"|..., "done": int, "total": int}


def _set_test_status(test_type: str, done: int, total: int,
                     node: str | None = None, score=None) -> None:
    if "type" not in _test_status:
        _test_status.update({"type": test_type, "done": 0, "total": total, "nodes": []})
    _test_status["done"]  = done
    _test_status["total"] = total
    if node is not None:
        nodes = _test_status.setdefault("nodes", [])
        for n in nodes:
            if n["name"] == node:
                n["done"] = True
                n["score"] = score
                break
        else:
            nodes.append({"name": node, "done": True, "score": score})
    asyncio.create_task(_publish_test_status())


def _add_pending_node(node: str) -> None:
    """Register a node as in-progress before result arrives."""
    nodes = _test_status.setdefault("nodes", [])
    if not any(n["name"] == node for n in nodes):
        nodes.append({"name": node, "done": False, "score": None})


def _clear_test_status() -> None:
    _test_status.clear()
    asyncio.create_task(_publish_test_status())


async def _publish_test_status() -> None:
    import app.mqtt as _mqtt
    if not _mqtt.is_enabled():
        return
    tt    = _test_status.get("type", "")
    done  = _test_status.get("done", 0)
    total = _test_status.get("total", 0)
    prefix = _mqtt._prefix()
    await _mqtt._queue.put({
        "topic":   f"{prefix}/test/status",
        "payload": tt if tt else "idle",
        "retain":  True,
    })
    if tt == "quick":
        await _mqtt._queue.put({
            "topic":   f"{prefix}/test/quick_progress",
            "payload": f"{done}/{total}" if total else "0/0",
            "retain":  True,
        })
    elif tt == "deep":
        await _mqtt._queue.put({
            "topic":   f"{prefix}/test/deep_progress",
            "payload": f"{done}/{total}" if total else "0/0",
            "retain":  True,
        })


def _fresh_stop_event(name: str) -> asyncio.Event:
    ev = asyncio.Event()
    _stop_events[name] = ev
    _abort_callbacks.pop(name, None)
    return ev


@router.post("/api/stop/{test_type}")
async def stop_test(test_type: str):
    ev = _stop_events.get(test_type)
    log.debug("[stop] POST /api/stop/%s, event found=%s", test_type, ev is not None)
    if ev:
        ev.set()
    # Вызываем _abort напрямую из endpoint — cleanup запускается независимо от generator
    cb = _abort_callbacks.get(test_type)
    if cb:
        cb()
    return JSONResponse({"ok": True})


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    cfg = config.load()
    status = None
    all_nodes: list[str] = []
    alive_set: set[str] = set()
    current_proxy: str | None = None
    node_results: dict = {}

    if config.is_configured():
        try:
            ver = await mihomo.get_version(cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"])
            if cfg.get("proxy_group"):
                all_nodes = await mihomo.get_nodes_in_group(
                    cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
                )
                groups = cfg.get("node_groups") or {}
                testable_nodes = [n for n in all_nodes if groups.get(n, "main") != "excluded"]
                active_nodes, _ = await mihomo.split_active_nodes(
                    cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                    cfg["proxy_group"], testable_nodes,
                )
                alive_set = set(active_nodes)
                try:
                    current_proxy = await mihomo.get_active_leaf_node(
                        cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
                    )
                except Exception:
                    current_proxy = None
                node_results = store.get_node_results(all_nodes)
                # Load scoring weights from config
                wq   = cfg.get("weight_quick",   30)
                wbr  = cfg.get("weight_browser",  20)
                wsp  = cfg.get("weight_speed",    15)
                wws  = cfg.get("weight_ws",       10)
                wdpi = cfg.get("weight_dpi",      25)
                httpx_pct = cfg.get("weight_browser_httpx", 60)

                # Compute deep score from available module results (6 modules total)
                for r in node_results.values():
                    q   = r.get("quick")
                    br  = r.get("browser")
                    vid = r.get("video")
                    spd = r.get("speed")
                    ws  = r.get("ws")
                    dpi = r.get("dpi")
                    stored_deep = r.get("deep")
                    if stored_deep:
                        r["computed_deep"] = {**stored_deep, "complete": True, "modules": 6}
                    elif q:
                        br_s  = br["score"]  if br  else 0
                        vid_s = vid["score"] if vid else 0
                        if br and vid:
                            combined_br = scoring.combined_browser_score(br_s, vid_s, httpx_pct)
                        else:
                            combined_br = br_s or vid_s
                        sc = scoring.deep_score(
                            q["score"]   if q   else 0,
                            combined_br,
                            spd["score"] if spd else 0,
                            ws["score"]  if ws  else 0,
                            dpi["score"] if dpi else 0,
                            wq, wbr, wsp, wws, wdpi,
                        )
                        mods = sum(x is not None for x in [q, br, vid, spd, ws, dpi])
                        r["computed_deep"] = {
                            "score": sc, "grade": scoring.deep_grade(sc),
                            "complete": mods == 6, "modules": mods,
                        }
            status = {"ok": True, "version": ver.get("version", "?"), "meta": ver.get("meta", False)}
        except Exception as e:
            status = {"ok": False, "error": str(e)}

    node_groups = cfg.get("node_groups") or {}

    def sort_key(name: str):
        r           = node_results.get(name, {})
        deep_score  = r["computed_deep"]["score"] if r.get("computed_deep") else -1
        quick_score = r["quick"]["score"]         if r.get("quick")         else -1
        # Main group ranks above backup; within group — best score first
        gp = 0 if node_groups.get(name, "main") != "backup" else -1
        return (gp, -deep_score, -quick_score, name.lower())

    excl_set   = {n for n, g in node_groups.items() if g == "excluded"}
    testable   = [n for n in all_nodes if n not in excl_set]
    alive_main = sorted([n for n in testable if n in alive_set     and node_groups.get(n, "main") != "backup"], key=sort_key)
    alive_back = sorted([n for n in testable if n in alive_set     and node_groups.get(n, "main") == "backup"], key=sort_key)
    dead_main  = sorted([n for n in testable if n not in alive_set and node_groups.get(n, "main") != "backup"])
    dead_back  = sorted([n for n in testable if n not in alive_set and node_groups.get(n, "main") == "backup"])
    dead_nodes = dead_main + dead_back
    disabled_nodes = sorted([n for n in all_nodes if n in excl_set])

    return templates.TemplateResponse("index.html", {
        "request":        request,
        "cfg":            cfg,
        "configured":     config.is_configured(),
        "status":         status,
        "alive_nodes":    alive_main,
        "alive_backup":   alive_back,
        "dead_nodes":     dead_nodes,
        "disabled_nodes": disabled_nodes,
        "alive_set":      alive_set,
        "current_proxy":  current_proxy,
        "node_results":   node_results,
        "page":           "dashboard",
    })


@router.get("/api/nodes/status")
async def nodes_status():
    """Returns cached node alive/dead state from background monitor."""
    cfg   = config.load()
    cache  = monitor.get_cache()
    groups = cfg.get("node_groups") or {}
    excl   = {n for n, g in groups.items() if g == "excluded"}
    return JSONResponse({
        "alive":       [n for n in cache["alive"] if n not in excl],
        "dead":        [n for n in cache["dead"] + cache["uncertain"] if n not in excl],
        "updated_at":  cache["updated_at"],
        "error":       cache["error"],
        "last_switch": cache.get("last_switch"),
    })


@router.get("/api/test-status")
async def test_status_api():
    """Returns currently running test progress (empty dict if idle)."""
    return JSONResponse(_test_status)


@router.post("/api/monitor/poll")
async def monitor_poll():
    """Force an immediate monitor poll (manual refresh)."""
    await monitor.poll_once()
    cache = monitor.get_cache()
    return JSONResponse({
        "alive":      cache["alive"],
        "dead":       cache["dead"] + cache["uncertain"],
        "updated_at": cache["updated_at"],
        "error":      cache["error"],
    })


@router.post("/api/set-proxy")
async def api_set_proxy(node: str = Form(...)):
    cfg = config.load()
    if not cfg.get("proxy_group"):
        return JSONResponse({"ok": False, "error": "proxy_group not configured"}, status_code=400)
    try:
        await mihomo.set_proxy(
            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
            cfg["proxy_group"], node,
        )
        return JSONResponse({"ok": True, "node": node})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    cfg   = config.load()
    cache = monitor.get_cache()
    # Use cached nodes as fallback; try to fetch all nodes (incl. excluded) from Mihomo
    proxy_nodes = sorted(cache["alive"] + cache["dead"] + cache["uncertain"])
    current_proxy = None
    if config.is_configured() and cfg.get("proxy_group"):
        try:
            all_raw = await mihomo.get_nodes_in_group(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )
            proxy_nodes = sorted(all_raw)
            current_proxy = await mihomo.get_active_leaf_node(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )
        except Exception:
            pass
    return templates.TemplateResponse("settings.html", {
        "request":       request,
        "cfg":           cfg,
        "all_timezones": _ALL_TIMEZONES,
        "proxy_nodes":   proxy_nodes,
        "current_proxy": current_proxy,
        "alive_nodes":   cache["alive"],
        "saved":         request.query_params.get("saved") == "1",
        "page":          "settings",
    })


@router.post("/settings")
async def settings_save(
    request: Request,
    mihomo_host:          str = Form(...),
    mihomo_port:          int = Form(9090),
    mihomo_secret:        str = Form(""),
    proxy_group:          str = Form(""),
    mixed_port:           int = Form(7893),
    proxy_user:           str = Form(""),
    proxy_pass:           str = Form(""),
    weight_quick:         int = Form(30),
    weight_browser:       int = Form(20),
    weight_speed:         int = Form(15),
    weight_ws:            int = Form(10),
    weight_dpi:           int = Form(25),
    weight_browser_httpx:   int = Form(60),
    log_level:              str = Form("WARNING"),
    timezone:               str = Form("Europe/Moscow"),
    monitor_interval_min:   int = Form(5),
    auto_node_mode:         str = Form("off"),
    auto_switch_dead:       str = Form("off"),
    auto_direct_fallback:   str = Form("off"),
    manual_node:            str = Form(""),
    ng_node:                List[str] = Form([]),
    ng_group:               List[str] = Form([]),
    mqtt_enabled:         str = Form("off"),
    mqtt_host:            str = Form(""),
    mqtt_port:            int = Form(1883),
    mqtt_user:            str = Form(""),
    mqtt_pass:            str = Form(""),
    mqtt_topic_prefix:    str = Form("node-tester"),
    mqtt_ha_discovery:    str = Form("off"),
    mqtt_top_nodes:       int = Form(10),
    tg_video_channels:    List[str] = Form([]),
    tg_image_channels:    List[str] = Form([]),
    schedule_quick_enabled:  str = Form("off"),
    schedule_quick_mode:     str = Form("interval"),
    schedule_quick_interval: int = Form(8),
    schedule_quick_hour:     int = Form(2),
    schedule_quick_minute:   int = Form(0),
    schedule_quick_days:     List[str] = Form([]),
    schedule_deep_enabled:   str = Form("off"),
    schedule_deep_mode:      str = Form("interval"),
    schedule_deep_interval:  int = Form(24),
    schedule_deep_hour:      int = Form(3),
    schedule_deep_minute:    int = Form(0),
    schedule_deep_days:      List[str] = Form([]),
):
    def _w(v: int) -> int:
        return max(0, min(100, v))

    level = log_level.upper() if log_level.upper() in ("DEBUG", "INFO", "WARNING", "ERROR") else "WARNING"
    tz   = timezone if timezone in _ALL_TIMEZONES else "Europe/Moscow"
    mode = auto_node_mode if auto_node_mode in ("off", "deep", "quick", "any") else "off"
    def _sched_mode(v): return v if v in ("interval", "daily", "weekly") else "interval"
    def _sched_days(lst): return sorted({int(d) for d in lst if d.isdigit() and 0 <= int(d) <= 6})
    config.save({
        "mihomo_host":          mihomo_host.strip(),
        "mihomo_port":          mihomo_port,
        "mihomo_secret":        mihomo_secret,
        "proxy_group":          proxy_group,
        "mixed_port":           mixed_port,
        "proxy_user":           proxy_user,
        "proxy_pass":           proxy_pass,
        "weight_quick":         _w(weight_quick),
        "weight_browser":       _w(weight_browser),
        "weight_speed":         _w(weight_speed),
        "weight_ws":            _w(weight_ws),
        "weight_dpi":           _w(weight_dpi),
        "weight_browser_httpx": _w(weight_browser_httpx),
        "log_level":            level,
        "timezone":             tz,
        "monitor_interval_min": max(0, monitor_interval_min),
        "auto_node_mode":       mode,
        "auto_switch_dead":     auto_switch_dead == "on",
        "auto_direct_fallback": auto_direct_fallback == "on",
        "mqtt_enabled":              mqtt_enabled == "on",
        "mqtt_host":                 mqtt_host.strip(),
        "mqtt_port":                 max(1, min(65535, mqtt_port)),
        "mqtt_user":                 mqtt_user,
        "mqtt_pass":                 mqtt_pass,
        "mqtt_topic_prefix":         mqtt_topic_prefix.strip() or "node-tester",
        "mqtt_ha_discovery":         mqtt_ha_discovery == "on",
        "mqtt_top_nodes":            max(1, mqtt_top_nodes),
        "tg_video_channels":         [ch.strip().lstrip("@") for ch in tg_video_channels],
        "tg_image_channels":         [ch.strip().lstrip("@") for ch in tg_image_channels],
        "node_groups":               {n: (g if g in ("main", "backup", "excluded") else "main")
                                         for n, g in zip(ng_node, ng_group)},
        "schedule_quick_enabled":    schedule_quick_enabled == "on",
        "schedule_quick_mode":       _sched_mode(schedule_quick_mode),
        "schedule_quick_interval":   max(1, schedule_quick_interval),
        "schedule_quick_hour":       max(0, min(23, schedule_quick_hour)),
        "schedule_quick_minute":     max(0, min(59, schedule_quick_minute)),
        "schedule_quick_days":       _sched_days(schedule_quick_days),
        "schedule_deep_enabled":     schedule_deep_enabled == "on",
        "schedule_deep_mode":        _sched_mode(schedule_deep_mode),
        "schedule_deep_interval":    max(1, schedule_deep_interval),
        "schedule_deep_hour":        max(0, min(23, schedule_deep_hour)),
        "schedule_deep_minute":      max(0, min(59, schedule_deep_minute)),
        "schedule_deep_days":        _sched_days(schedule_deep_days),
    })
    # Manual node switch — apply immediately if a node is selected
    if manual_node.strip():
        cfg2 = config.load()
        if cfg2.get("proxy_group") and config.is_configured():
            try:
                await mihomo.set_proxy(
                    cfg2["mihomo_host"], cfg2["mihomo_port"], cfg2["mihomo_secret"],
                    cfg2["proxy_group"], manual_node.strip(),
                )
            except Exception:
                pass
    # Применяем уровень логов немедленно
    from app.main import apply_log_level
    apply_log_level(level)
    log.info("log_level changed to %s", level)
    return _redir(request, "/settings?saved=1")


@router.post("/api/mqtt/resync")
async def mqtt_resync():
    """Re-publish HA autodiscovery for all known nodes."""
    cache = monitor.get_cache()
    alive     = cache.get("alive")     or []
    dead      = cache.get("dead")      or []
    uncertain = cache.get("uncertain") or []
    from app.mqtt import _publish_ha_discovery, publish_state  # type: ignore[attr-defined]
    await _publish_ha_discovery(alive + dead + uncertain)
    await publish_state(alive, dead, uncertain)
    return JSONResponse({"ok": True, "nodes": len(alive) + len(dead) + len(uncertain)})


@router.post("/api/test-connection")
async def test_connection(
    host:   str = Form(...),
    port:   int = Form(9090),
    secret: str = Form(""),
):
    try:
        ver    = await mihomo.get_version(host.strip(), port, secret)
        groups = await mihomo.get_selector_groups(host.strip(), port, secret)
        return JSONResponse({
            "ok":      True,
            "version": ver.get("version", "?"),
            "meta":    ver.get("meta", False),
            "groups":  groups,
        })
    except httpx.HTTPStatusError as e:
        msg = "Unauthorized — check secret" if e.response.status_code == 401 else f"HTTP {e.response.status_code}"
        return JSONResponse({"ok": False, "error": msg})
    except httpx.ConnectError:
        return JSONResponse({"ok": False, "error": "Connection refused — check host and port"})
    except httpx.TimeoutException:
        return JSONResponse({"ok": False, "error": "Timeout — host unreachable"})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@router.get("/run/quick", response_class=HTMLResponse)
async def run_quick_page(request: Request):
    cfg = config.load()
    if not config.is_configured() or not cfg.get("proxy_group"):
        return _redir(request, "/settings")
    return templates.TemplateResponse("run.html", {
        "request": request,
        "cfg":     cfg,
        "page":    "run",
    })


@router.get("/api/run/quick")
async def run_quick_stream(request: Request):
    cfg = config.load()
    if not config.is_configured():
        return JSONResponse({"error": "not configured"}, status_code=400)

    async def stream():
        try:
            all_nodes = await mihomo.get_nodes_in_group(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )
            all_nodes = _apply_exclusions(all_nodes, cfg)

            confirmed_active, confirmed_dead, uncertain = await mihomo.classify_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], all_nodes
            )

            ping_active, ping_dead, ping_reasons = await mihomo.ping_filter_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], uncertain
            )

            active  = confirmed_active + ping_active
            skipped = (
                [{"name": n, "reason": "Mihomo URLTest: consistently dead", "source": "mihomo"}
                 for n in confirmed_dead]
                +
                [{"name": n, "reason": ping_reasons.get(n, "ping timeout"), "source": "ping"}
                 for n in ping_dead]
            )

            total_active = len(active)
            yield _sse({
                "type":    "start",
                "total":   total_active,
                "skipped": skipped,
                "group":   cfg["proxy_group"],
            })

            if not active:
                yield _sse({"type": "done"})
                return

            ev_q: asyncio.Queue = asyncio.Queue()
            sem = asyncio.Semaphore(1)
            stop_ev = _fresh_stop_event("quick")
            ctr = [0]  # shared mutable counter across tasks
            _set_test_status("quick", 0, total_active)
            for n in active:
                _add_pending_node(n)

            async def run_one(node: str):
                if stop_ev.is_set():
                    return
                async with sem:
                    if stop_ev.is_set():
                        return
                    result = await lat_core.test_node(node, cfg, ev_q)
                    if not result.get("block_failed"):
                        store.save_quick(result["name"], result)
                    ctr[0] += 1
                    _set_test_status("quick", ctr[0], total_active,
                                     node=result["name"], score=result.get("total_score"))
                    await ev_q.put({"_result": result})

            tasks = [asyncio.create_task(run_one(n)) for n in active]

            def _abort():
                stop_ev.set()
                for t in tasks: t.cancel()

            _abort_callbacks["quick"] = _abort

            # Finalize always runs in background — test continues even if client disconnects
            async def _finalize():
                await asyncio.gather(*tasks, return_exceptions=True)
                _clear_test_status()
                if not stop_ev.is_set():
                    await monitor.apply_best_node("quick", tested_nodes=active)

            finalize_task = asyncio.create_task(_finalize())

            # Stream SSE to client; disconnect just stops streaming, not the test
            received = 0
            while received < total_active:
                try:
                    item = await asyncio.wait_for(ev_q.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    if await request.is_disconnected():
                        return  # test continues in background via finalize_task
                    continue

                if await request.is_disconnected():
                    return  # test continues in background

                if "_result" in item:
                    received += 1
                    r = item["_result"]
                    yield _sse({"type": "node", "data": r,
                                "progress": ctr[0], "total": total_active})
                else:
                    yield _sse(item)

            await finalize_task
            yield _sse({"type": "done"})

        except Exception as e:
            yield _sse({"type": "error", "message": str(e)})

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/run/speed", response_class=HTMLResponse)
async def run_speed_page(request: Request):
    cfg = config.load()
    if not config.is_configured() or not cfg.get("proxy_group"):
        return _redir(request, "/settings")
    return templates.TemplateResponse("run_speed.html", {
        "request": request,
        "cfg":     cfg,
        "page":    "run",
    })


@router.get("/api/run/speed")
async def run_speed_stream(request: Request):
    cfg = config.load()
    if not config.is_configured():
        return JSONResponse({"error": "not configured"}, status_code=400)

    async def stream():
        try:
            all_nodes = await mihomo.get_nodes_in_group(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )
            all_nodes = _apply_exclusions(all_nodes, cfg)
            confirmed_active, confirmed_dead, uncertain = await mihomo.classify_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], all_nodes
            )
            ping_active, ping_dead, ping_reasons = await mihomo.ping_filter_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], uncertain
            )
            active  = confirmed_active + ping_active
            skipped = (
                [{"name": n, "reason": "Mihomo URLTest: consistently dead", "source": "mihomo"}
                 for n in confirmed_dead]
                + [{"name": n, "reason": ping_reasons.get(n, "ping timeout"), "source": "ping"}
                   for n in ping_dead]
            )
            total_active = len(active)
            yield _sse({"type": "start", "total": total_active,
                        "skipped": skipped, "group": cfg["proxy_group"]})

            if not active:
                yield _sse({"type": "done"})
                return

            ev_q: asyncio.Queue = asyncio.Queue()
            sem = asyncio.Semaphore(1)
            stop_ev = _fresh_stop_event("speed")
            completed = 0
            _original_proxy = await mihomo.get_selector_now(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )

            async def run_one(node: str):
                if stop_ev.is_set():
                    return
                async with sem:
                    if stop_ev.is_set():
                        return
                    result = await spd_core.test_node(node, cfg, ev_q)
                    await ev_q.put({"_result": result})

            tasks = [asyncio.create_task(run_one(n)) for n in active]
            aborted = False

            async def _cleanup():
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True), timeout=10.0
                    )
                except Exception:
                    pass
                if _original_proxy:
                    try:
                        await mihomo.set_proxy(
                            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                            cfg["proxy_group"], _original_proxy,
                        )
                    except Exception:
                        pass

            def _abort():
                nonlocal aborted
                if aborted:
                    return
                aborted = True
                stop_ev.set()
                for t in tasks: t.cancel()
                asyncio.create_task(_cleanup())

            _abort_callbacks["speed"] = _abort

            while completed < total_active:
                try:
                    item = await asyncio.wait_for(ev_q.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    if await request.is_disconnected():
                        _abort(); break
                    continue
                if await request.is_disconnected():
                    _abort(); break
                if "_result" in item:
                    completed += 1
                    r = item["_result"]
                    store.save_speed(r["name"], r)
                    yield _sse({"type": "node", "data": r,
                                "progress": completed, "total": total_active})
                else:
                    yield _sse(item)

            if not aborted:
                await asyncio.gather(*tasks, return_exceptions=True)
                if _original_proxy:
                    try:
                        await mihomo.set_proxy(
                            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                            cfg["proxy_group"], _original_proxy,
                        )
                    except Exception:
                        pass
                yield _sse({"type": "done"})

        except Exception as e:
            yield _sse({"type": "error", "message": str(e)})

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/run/browser", response_class=HTMLResponse)
async def run_browser_page(request: Request):
    cfg = config.load()
    if not config.is_configured() or not cfg.get("proxy_group"):
        return _redir(request, "/settings")
    return templates.TemplateResponse("run_browser.html", {
        "request": request, "cfg": cfg, "page": "run",
    })


@router.get("/api/run/browser")
async def run_browser_stream(request: Request):
    cfg = config.load()
    if not config.is_configured():
        return JSONResponse({"error": "not configured"}, status_code=400)

    async def stream():
        try:
            all_nodes = await mihomo.get_nodes_in_group(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )
            all_nodes = _apply_exclusions(all_nodes, cfg)
            confirmed_active, confirmed_dead, uncertain = await mihomo.classify_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], all_nodes
            )
            ping_active, ping_dead, ping_reasons = await mihomo.ping_filter_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], uncertain
            )
            active  = confirmed_active + ping_active
            skipped = (
                [{"name": n, "reason": "Mihomo URLTest: consistently dead", "source": "mihomo"}
                 for n in confirmed_dead]
                + [{"name": n, "reason": ping_reasons.get(n, "ping timeout"), "source": "ping"}
                   for n in ping_dead]
            )
            total_active = len(active)
            yield _sse({"type": "start", "total": total_active,
                        "skipped": skipped, "group": cfg["proxy_group"]})
            if not active:
                yield _sse({"type": "done"})
                return

            from app.core.tg_media import fetch_image_urls as _tg_imgs
            _tg_image_chs = cfg.get("tg_image_channels") or []
            tg_image_sources: list[str] = []
            if any(ch.strip() for ch in _tg_image_chs):
                try:
                    tg_image_sources = await asyncio.wait_for(
                        _tg_imgs(_tg_image_chs, proxy_url=_tg_proxy_url(cfg)), timeout=60)
                except Exception as _e:
                    log.warning("[browser] TG image pre-fetch failed: %s", _e)
            if tg_image_sources:
                yield _sse({"type": "step", "phase": "tg_prefetch",
                            "image_count": len(tg_image_sources)})

            ev_q: asyncio.Queue = asyncio.Queue()
            sem = asyncio.Semaphore(1)
            stop_ev = _fresh_stop_event("browser")
            completed = 0
            _original_proxy = await mihomo.get_selector_now(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )

            async def run_one(node: str):
                if stop_ev.is_set():
                    return
                async with sem:
                    if stop_ev.is_set():
                        return
                    result = await br_core.test_node(
                        node, cfg, ev_q,
                        tg_images=tg_image_sources or None,
                    )
                    await ev_q.put({"_result": result})

            tasks = [asyncio.create_task(run_one(n)) for n in active]
            aborted = False

            async def _cleanup():
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True), timeout=10.0
                    )
                except Exception:
                    pass
                if _original_proxy:
                    try:
                        await mihomo.set_proxy(
                            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                            cfg["proxy_group"], _original_proxy,
                        )
                    except Exception:
                        pass

            def _abort():
                nonlocal aborted
                if aborted:
                    return
                aborted = True
                stop_ev.set()
                for t in tasks: t.cancel()
                asyncio.create_task(_cleanup())

            _abort_callbacks["browser"] = _abort

            while completed < total_active:
                try:
                    item = await asyncio.wait_for(ev_q.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    if await request.is_disconnected():
                        _abort(); break
                    continue
                if await request.is_disconnected():
                    _abort(); break
                if "_result" in item:
                    completed += 1
                    r = item["_result"]
                    store.save_browser(r["name"], r)
                    yield _sse({"type": "node", "data": r,
                                "progress": completed, "total": total_active})
                else:
                    yield _sse(item)

            if not aborted:
                await asyncio.gather(*tasks, return_exceptions=True)
                if _original_proxy:
                    try:
                        await mihomo.set_proxy(
                            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                            cfg["proxy_group"], _original_proxy,
                        )
                    except Exception:
                        pass
                yield _sse({"type": "done"})
        except Exception as e:
            yield _sse({"type": "error", "message": str(e)})

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/run/deep", response_class=HTMLResponse)
async def run_deep_page(request: Request):
    cfg = config.load()
    if not config.is_configured() or not cfg.get("proxy_group"):
        return _redir(request, "/settings")
    return templates.TemplateResponse("run_deep.html", {
        "request": request, "cfg": cfg, "page": "run",
    })


@router.get("/api/run/deep")
async def run_deep_stream(request: Request):
    cfg = config.load()
    if not config.is_configured():
        return JSONResponse({"error": "not configured"}, status_code=400)

    async def stream():
        try:
            log.debug("[deep] stream started")
            all_nodes = await mihomo.get_nodes_in_group(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )
            all_nodes = _apply_exclusions(all_nodes, cfg)
            log.debug("[deep] all_nodes=%d", len(all_nodes))
            confirmed_active, confirmed_dead, uncertain = await mihomo.classify_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], all_nodes
            )
            log.debug("[deep] confirmed_active=%d dead=%d uncertain=%d",
                      len(confirmed_active), len(confirmed_dead), len(uncertain))
            if uncertain:
                yield _sse({"type": "scanning", "uncertain": len(uncertain),
                            "confirmed": len(confirmed_active)})
            try:
                ping_active, ping_dead, ping_reasons = await asyncio.wait_for(
                    mihomo.ping_filter_nodes(
                        cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], uncertain
                    ),
                    timeout=60,
                )
            except asyncio.TimeoutError:
                log.warning("[deep] ping_filter_nodes timed out, treating %d uncertain as dead", len(uncertain))
                ping_active, ping_dead, ping_reasons = [], uncertain, {n: "ping phase timeout" for n in uncertain}
            log.debug("[deep] ping_active=%d ping_dead=%d", len(ping_active), len(ping_dead))
            active  = confirmed_active + ping_active
            skipped = (
                [{"name": n, "reason": "Mihomo URLTest: consistently dead", "source": "mihomo"}
                 for n in confirmed_dead]
                + [{"name": n, "reason": ping_reasons.get(n, "ping timeout"), "source": "ping"}
                   for n in ping_dead]
            )
            total_active = len(active)
            yield _sse({"type": "start", "total": total_active,
                        "skipped": skipped, "group": cfg["proxy_group"]})
            if not active:
                yield _sse({"type": "done"})
                return

            # Pre-fetch Telegram media URLs once — all nodes tested on the same content
            from app.core.tg_media import fetch_video_direct_urls as _tg_vids, fetch_image_urls as _tg_imgs
            _tg_video_chs = cfg.get("tg_video_channels") or []
            _tg_image_chs = cfg.get("tg_image_channels") or []
            tg_video_sources: list[str] = []
            tg_image_sources: list[str] = []
            if any(ch.strip() for ch in _tg_video_chs):
                try:
                    tg_video_sources = await asyncio.wait_for(
                        _tg_vids(_tg_video_chs, proxy_url=_tg_proxy_url(cfg)), timeout=90)
                except Exception as e:
                    log.warning("[deep] TG video pre-fetch failed: %s", e)
            if any(ch.strip() for ch in _tg_image_chs):
                try:
                    tg_image_sources = await asyncio.wait_for(
                        _tg_imgs(_tg_image_chs, proxy_url=_tg_proxy_url(cfg)), timeout=60)
                except Exception as e:
                    log.warning("[deep] TG image pre-fetch failed: %s", e)
            if tg_video_sources or tg_image_sources:
                yield _sse({
                    "type": "tg_media_ready",
                    "video_count": len(tg_video_sources),
                    "image_count": len(tg_image_sources),
                })

            ev_q: asyncio.Queue = asyncio.Queue()
            sem = asyncio.Semaphore(1)
            stop_ev = _fresh_stop_event("deep")
            ctr = [0]
            _set_test_status("deep", 0, total_active)
            for n in active:
                _add_pending_node(n)
            _original_proxy = await mihomo.get_selector_now(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )

            # Общий Chromium для всех видео-тестов — экономит ~2-3с на каждую ноду
            _shared_pw = None
            _shared_browser = None
            try:
                _shared_pw, _shared_browser = await vid_core.launch_shared_browser(cfg)
                log.debug("[deep] shared browser launched OK")
            except Exception as _e:
                log.warning("[deep] shared browser launch failed, fallback per-node: %s", _e)

            async def run_one_deep(node: str):
                if stop_ev.is_set():
                    return
                async with sem:
                    if stop_ev.is_set():
                        return
                    log.debug("[deep] node=%s step=quick start", node)
                    await ev_q.put({"type": "step", "phase": "quick_start", "node": node})
                    quick_r = await lat_core.test_node(node, cfg, ev_q)
                    log.debug("[deep] node=%s step=quick done", node)
                    if not quick_r.get("block_failed"):
                        store.save_quick(quick_r["name"], quick_r)
                    await ev_q.put({"_module": "quick", "node": node, "data": quick_r})
                    if stop_ev.is_set():
                        return

                    log.debug("[deep] node=%s step=speed start", node)
                    speed_r = await spd_core.test_node(node, cfg, ev_q)
                    log.debug("[deep] node=%s step=speed done", node)
                    store.save_speed(speed_r["name"], speed_r)
                    await ev_q.put({"_module": "speed", "node": node, "data": speed_r})
                    if stop_ev.is_set():
                        return

                    log.debug("[deep] node=%s step=ws start", node)
                    ws_r = await ws_core.test_node(node, cfg, ev_q)
                    log.debug("[deep] node=%s step=ws done", node)
                    store.save_ws(ws_r["name"], ws_r)
                    await ev_q.put({"_module": "ws", "node": node, "data": ws_r})
                    if stop_ev.is_set():
                        return

                    log.debug("[deep] node=%s step=browser start", node)
                    br_r = await br_core.test_node(node, cfg, ev_q, tg_images=tg_image_sources)
                    log.debug("[deep] node=%s step=browser done", node)
                    store.save_browser(br_r["name"], br_r)
                    await ev_q.put({"_module": "browser", "node": node, "data": br_r})
                    if stop_ev.is_set():
                        return

                    log.debug("[deep] node=%s step=video start", node)
                    if _shared_browser:
                        vid_r = await vid_core.test_node_in_browser(
                            _shared_browser, node, cfg, ev_q,
                            tg_sources=tg_video_sources, stop=stop_ev,
                        )
                    else:
                        vid_r = await vid_core.test_node(node, cfg, ev_q, tg_sources=tg_video_sources)
                    log.debug("[deep] node=%s step=video done", node)
                    store.save_video(vid_r["name"], vid_r)
                    await ev_q.put({"_module": "video", "node": node, "data": vid_r})
                    if stop_ev.is_set():
                        return

                    log.debug("[deep] node=%s step=dpi start", node)
                    dpi_r = await dpi_core.test_node(node, cfg, ev_q)
                    log.debug("[deep] node=%s step=dpi done score=%s", node, dpi_r["score"])
                    store.save_dpi(dpi_r["name"], dpi_r)
                    await ev_q.put({"_module": "dpi", "node": node, "data": dpi_r})

                    _httpx_pct = cfg.get("weight_browser_httpx", 60)
                    combined_browser_s = scoring.combined_browser_score(
                        br_r["score"], vid_r["score"], _httpx_pct
                    )
                    sc = scoring.deep_score(
                        quick_r.get("total_score", 0) if not quick_r.get("block_failed") else 0,
                        combined_browser_s,
                        speed_r["score"],
                        ws_r["score"],
                        dpi_r["score"],
                        cfg.get("weight_quick",   30),
                        cfg.get("weight_browser",  20),
                        cfg.get("weight_speed",    15),
                        cfg.get("weight_ws",       10),
                        cfg.get("weight_dpi",      25),
                    )
                    deep_result = {
                        "name": node, "score": sc, "grade": scoring.deep_grade(sc),
                        "quick_score":            quick_r.get("total_score", 0),
                        "browser_score":          br_r["score"],
                        "video_score":            vid_r["score"],
                        "combined_browser_score": combined_browser_s,
                        "speed_score":            speed_r["score"],
                        "ws_score":               ws_r["score"],
                        "dpi_score":              dpi_r["score"],
                    }
                    store.save_deep(node, deep_result)
                    ctr[0] += 1
                    _set_test_status("deep", ctr[0], total_active,
                                     node=node, score=sc)
                    await ev_q.put({"_result": deep_result})

            tasks = [asyncio.create_task(run_one_deep(n)) for n in active]

            def _abort():
                stop_ev.set()
                for t in tasks: t.cancel()

            _abort_callbacks["deep"] = _abort

            # Finalize always runs in background — restores proxy and cleans status
            async def _finalize():
                await asyncio.gather(*tasks, return_exceptions=True)
                _clear_test_status()
                # Закрываем общий браузер после всех нод
                if _shared_pw and _shared_browser:
                    await vid_core.close_shared_browser(_shared_pw, _shared_browser)
                    log.debug("[deep] shared browser closed")
                if _original_proxy:
                    try:
                        await mihomo.set_proxy(
                            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                            cfg["proxy_group"], _original_proxy,
                        )
                    except Exception:
                        pass
                if not stop_ev.is_set():
                    await monitor.apply_best_node("deep", tested_nodes=active)

            finalize_task = asyncio.create_task(_finalize())

            # Stream SSE to client; disconnect just stops streaming, not the test
            received = 0
            while received < total_active:
                try:
                    item = await asyncio.wait_for(ev_q.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    if await request.is_disconnected():
                        return  # test continues in background via finalize_task
                    continue
                if await request.is_disconnected():
                    return  # test continues in background
                if "_result" in item:
                    received += 1
                    r = item["_result"]
                    yield _sse({"type": "node", "data": r,
                                "progress": ctr[0], "total": total_active})
                elif "_module" in item:
                    yield _sse({"type": "node_module", "module": item["_module"],
                                "node": item["node"], "data": item["data"]})
                else:
                    yield _sse(item)

            await finalize_task
            yield _sse({"type": "done"})
        except Exception as e:
            yield _sse({"type": "error", "message": str(e)})

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/run/ws", response_class=HTMLResponse)
async def run_ws_page(request: Request):
    cfg = config.load()
    if not config.is_configured() or not cfg.get("proxy_group"):
        return _redir(request, "/settings")
    return templates.TemplateResponse("run_ws.html", {
        "request": request,
        "cfg":     cfg,
        "page":    "run",
    })


@router.get("/api/run/ws")
async def run_ws_stream(request: Request):
    cfg = config.load()
    if not config.is_configured():
        return JSONResponse({"error": "not configured"}, status_code=400)

    async def stream():
        try:
            all_nodes = await mihomo.get_nodes_in_group(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )
            all_nodes = _apply_exclusions(all_nodes, cfg)
            confirmed_active, confirmed_dead, uncertain = await mihomo.classify_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], all_nodes
            )
            ping_active, ping_dead, ping_reasons = await mihomo.ping_filter_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], uncertain
            )
            active  = confirmed_active + ping_active
            skipped = (
                [{"name": n, "reason": "Mihomo URLTest: consistently dead", "source": "mihomo"}
                 for n in confirmed_dead]
                + [{"name": n, "reason": ping_reasons.get(n, "ping timeout"), "source": "ping"}
                   for n in ping_dead]
            )
            total_active = len(active)
            yield _sse({"type": "start", "total": total_active,
                        "skipped": skipped, "group": cfg["proxy_group"]})

            if not active:
                yield _sse({"type": "done"})
                return

            ev_q: asyncio.Queue = asyncio.Queue()
            sem = asyncio.Semaphore(1)
            stop_ev = _fresh_stop_event("ws")
            completed = 0
            _original_proxy = await mihomo.get_selector_now(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )

            async def run_one(node: str):
                if stop_ev.is_set():
                    return
                async with sem:
                    if stop_ev.is_set():
                        return
                    result = await ws_core.test_node(node, cfg, ev_q)
                    await ev_q.put({"_result": result})

            tasks = [asyncio.create_task(run_one(n)) for n in active]
            aborted = False

            async def _cleanup():
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True), timeout=10.0
                    )
                except Exception:
                    pass
                if _original_proxy:
                    try:
                        await mihomo.set_proxy(
                            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                            cfg["proxy_group"], _original_proxy,
                        )
                    except Exception:
                        pass

            def _abort():
                nonlocal aborted
                if aborted:
                    return
                aborted = True
                stop_ev.set()
                for t in tasks: t.cancel()
                asyncio.create_task(_cleanup())

            _abort_callbacks["ws"] = _abort

            while completed < total_active:
                try:
                    item = await asyncio.wait_for(ev_q.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    if await request.is_disconnected():
                        _abort(); break
                    continue
                if await request.is_disconnected():
                    _abort(); break
                if "_result" in item:
                    completed += 1
                    r = item["_result"]
                    store.save_ws(r["name"], r)
                    yield _sse({"type": "node", "data": r,
                                "progress": completed, "total": total_active})
                else:
                    yield _sse(item)

            if not aborted:
                await asyncio.gather(*tasks, return_exceptions=True)
                if _original_proxy:
                    try:
                        await mihomo.set_proxy(
                            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                            cfg["proxy_group"], _original_proxy,
                        )
                    except Exception:
                        pass
                yield _sse({"type": "done"})

        except Exception as e:
            yield _sse({"type": "error", "message": str(e)})

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/run/dpi", response_class=HTMLResponse)
async def run_dpi_page(request: Request):
    cfg = config.load()
    if not config.is_configured() or not cfg.get("proxy_group"):
        return _redir(request, "/settings")
    return templates.TemplateResponse("run_dpi.html", {
        "request": request, "cfg": cfg, "page": "run",
    })


@router.get("/api/run/dpi")
async def run_dpi_stream(request: Request):
    cfg = config.load()
    if not config.is_configured():
        return JSONResponse({"error": "not configured"}, status_code=400)

    async def stream():
        try:
            all_nodes = await mihomo.get_nodes_in_group(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )
            all_nodes = _apply_exclusions(all_nodes, cfg)
            confirmed_active, confirmed_dead, uncertain = await mihomo.classify_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], all_nodes
            )
            ping_active, ping_dead, ping_reasons = await mihomo.ping_filter_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], uncertain
            )
            active  = confirmed_active + ping_active
            skipped = (
                [{"name": n, "reason": "Mihomo URLTest: consistently dead", "source": "mihomo"}
                 for n in confirmed_dead]
                + [{"name": n, "reason": ping_reasons.get(n, "ping timeout"), "source": "ping"}
                   for n in ping_dead]
            )
            total_active = len(active)
            yield _sse({"type": "start", "total": total_active,
                        "skipped": skipped, "group": cfg["proxy_group"]})

            if not active:
                yield _sse({"type": "done"})
                return

            ev_q: asyncio.Queue = asyncio.Queue()
            sem = asyncio.Semaphore(1)
            stop_ev = _fresh_stop_event("dpi")
            completed = 0
            _original_proxy = await mihomo.get_selector_now(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )

            async def run_one(node: str):
                if stop_ev.is_set():
                    return
                async with sem:
                    if stop_ev.is_set():
                        return
                    result = await dpi_core.test_node(node, cfg, ev_q)
                    await ev_q.put({"_result": result})

            tasks = [asyncio.create_task(run_one(n)) for n in active]
            aborted = False

            async def _cleanup():
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True), timeout=10.0
                    )
                except Exception:
                    pass
                if _original_proxy:
                    try:
                        await mihomo.set_proxy(
                            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                            cfg["proxy_group"], _original_proxy,
                        )
                    except Exception:
                        pass

            def _abort():
                nonlocal aborted
                if aborted:
                    return
                aborted = True
                stop_ev.set()
                for t in tasks: t.cancel()
                asyncio.create_task(_cleanup())

            _abort_callbacks["dpi"] = _abort

            while completed < total_active:
                try:
                    item = await asyncio.wait_for(ev_q.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    if await request.is_disconnected():
                        _abort(); break
                    continue
                if await request.is_disconnected():
                    _abort(); break
                if "_result" in item:
                    completed += 1
                    r = item["_result"]
                    store.save_dpi(r["name"], r)
                    yield _sse({"type": "node", "data": r,
                                "progress": completed, "total": total_active})
                else:
                    yield _sse(item)

            if not aborted:
                await asyncio.gather(*tasks, return_exceptions=True)
                if _original_proxy:
                    try:
                        await mihomo.set_proxy(
                            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                            cfg["proxy_group"], _original_proxy,
                        )
                    except Exception:
                        pass
                yield _sse({"type": "done"})

        except Exception as e:
            yield _sse({"type": "error", "message": str(e)})

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/run/video", response_class=HTMLResponse)
async def run_video_page(request: Request):
    cfg = config.load()
    if not config.is_configured() or not cfg.get("proxy_group"):
        return _redir(request, "/settings")
    return templates.TemplateResponse("run_video.html", {
        "request": request, "cfg": cfg, "page": "run",
    })


@router.get("/api/run/video")
async def run_video_stream(request: Request):
    cfg = config.load()
    if not config.is_configured():
        return JSONResponse({"error": "not configured"}, status_code=400)

    async def stream():
        try:
            all_nodes = await mihomo.get_nodes_in_group(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )
            all_nodes = _apply_exclusions(all_nodes, cfg)
            confirmed_active, confirmed_dead, uncertain = await mihomo.classify_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], all_nodes
            )
            ping_active, ping_dead, ping_reasons = await mihomo.ping_filter_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], uncertain
            )
            active  = confirmed_active + ping_active
            skipped = (
                [{"name": n, "reason": "Mihomo URLTest: consistently dead", "source": "mihomo"}
                 for n in confirmed_dead]
                + [{"name": n, "reason": ping_reasons.get(n, "ping timeout"), "source": "ping"}
                   for n in ping_dead]
            )
            total_active = len(active)
            yield _sse({"type": "start", "total": total_active,
                        "skipped": skipped, "group": cfg["proxy_group"]})
            if not active:
                yield _sse({"type": "done"})
                return

            from app.core.tg_media import fetch_video_direct_urls as _tg_vids
            _tg_video_chs = cfg.get("tg_video_channels") or []
            tg_video_sources: list[str] = []
            if any(ch.strip() for ch in _tg_video_chs):
                try:
                    tg_video_sources = await asyncio.wait_for(
                        _tg_vids(_tg_video_chs, proxy_url=_tg_proxy_url(cfg)), timeout=90)
                except Exception as _e:
                    log.warning("[video] TG video pre-fetch failed: %s", _e)
            if tg_video_sources:
                yield _sse({"type": "step", "phase": "tg_prefetch",
                            "video_count": len(tg_video_sources)})

            ev_q: asyncio.Queue = asyncio.Queue()
            stop_event = _fresh_stop_event("video")
            completed = 0
            _original_proxy = await mihomo.get_selector_now(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )
            log.debug("[video] original_proxy=%s", _original_proxy)

            task = asyncio.create_task(vid_core.test_all_nodes(
                active, cfg, ev_q, stop_event,
                tg_sources=tg_video_sources or None,
            ))
            aborted = False

            async def _cleanup():
                log.debug("[video] _cleanup: waiting for task to finish")
                try:
                    await asyncio.wait_for(
                        asyncio.gather(task, return_exceptions=True), timeout=12.0
                    )
                    log.debug("[video] _cleanup: task finished")
                except Exception as e:
                    log.debug("[video] _cleanup: task wait error: %s", e)
                if _original_proxy:
                    log.debug("[video] _cleanup: restoring proxy to %s", _original_proxy)
                    try:
                        await mihomo.set_proxy(
                            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                            cfg["proxy_group"], _original_proxy,
                        )
                        log.debug("[video] _cleanup: proxy restored OK")
                    except Exception as e:
                        log.debug("[video] _cleanup: proxy restore failed: %s", e)
                else:
                    log.debug("[video] _cleanup: _original_proxy is None/empty, skip restore")

            def _abort():
                nonlocal aborted
                if aborted:
                    return
                aborted = True
                log.debug("[video] _abort: task.cancel + _cleanup task spawned")
                task.cancel()
                asyncio.create_task(_cleanup())

            _abort_callbacks["video"] = _abort

            while completed < total_active:
                try:
                    item = await asyncio.wait_for(ev_q.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    if stop_event.is_set():
                        log.debug("[video] stop_event set (timeout), calling _abort")
                        _abort(); break
                    if await request.is_disconnected():
                        log.debug("[video] disconnect detected (timeout), calling _abort")
                        _abort(); break
                    continue
                if stop_event.is_set():
                    log.debug("[video] stop_event set (item), calling _abort")
                    _abort(); break
                if await request.is_disconnected():
                    log.debug("[video] disconnect detected (item), calling _abort")
                    _abort(); break
                if "_result" in item:
                    completed += 1
                    r = item["_result"]
                    store.save_video(r["name"], r)
                    yield _sse({"type": "node", "data": r,
                                "progress": completed, "total": total_active})
                else:
                    yield _sse(item)

            if not aborted:
                # Нормальное завершение — cleanup inline
                await asyncio.gather(task, return_exceptions=True)
                if _original_proxy:
                    try:
                        await mihomo.set_proxy(
                            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                            cfg["proxy_group"], _original_proxy,
                        )
                    except Exception:
                        pass
                yield _sse({"type": "done"})
        except Exception as e:
            yield _sse({"type": "error", "message": str(e)})

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


# ── Charts ────────────────────────────────────────────────────────────────────

@router.get("/charts", response_class=HTMLResponse)
async def charts_page(request: Request):
    cfg = config.load()
    return templates.TemplateResponse("charts.html", {
        "request": request, "cfg": cfg, "page": "charts",
    })


@router.get("/api/charts/data")
async def charts_data(module: str = "quick", days: int = 30):
    allowed = {"quick", "deep", "speed", "browser", "ws", "video"}
    if module not in allowed:
        return JSONResponse({"error": "unknown module"}, status_code=400)
    days = max(1, min(365, days))
    return JSONResponse(db.get_history(module, days))
