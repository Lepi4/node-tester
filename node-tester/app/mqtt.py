"""MQTT publisher + command subscriber for Mihomo proxy monitor."""
import asyncio
import json
import logging
from typing import Any

import app.config as config

log = logging.getLogger("node_tester.mqtt")

_queue: asyncio.Queue = asyncio.Queue()

_TOP_LABELS = [
    "Best", "Second", "Third", "Fourth", "Fifth",
    "Sixth", "Seventh", "Eighth", "Ninth", "Tenth",
]


def is_enabled() -> bool:
    cfg = config.load()
    return bool(cfg.get("mqtt_enabled") and cfg.get("mqtt_host"))


def _prefix() -> str:
    return config.load().get("mqtt_topic_prefix", "node-tester").rstrip("/")


def _sanitize(name: str) -> str:
    return name.replace(" ", "_").replace("/", "_")


def _top_label(i: int) -> str:
    """Human label for rank i (1-based): '1.Best', '2.Second', etc."""
    name = _TOP_LABELS[i - 1] if i <= len(_TOP_LABELS) else f"Top{i}"
    return f"{i}.{name}"


def _ranked_nodes(nodes: list[str]) -> list[str]:
    """Sort nodes best-first: main group before backup, then by max score."""
    import app.store as _store
    if not nodes:
        return []
    results = _store.get_node_results(nodes)
    groups  = config.load().get("node_groups") or {}
    def _score(n: str) -> tuple:
        r  = results.get(n, {})
        q  = r["quick"]["score"] if r.get("quick") else -1
        d  = r["deep"]["score"]  if r.get("deep")  else -1
        gp = 0 if groups.get(n, "main") != "backup" else -1
        return (gp, max(q, d), q, d)
    return sorted(nodes, key=_score, reverse=True)


def _node_label(node: str, results: dict) -> str:
    """Format sensor value: 'A node-name' or just 'node-name' if no grade."""
    r = results.get(node, {})
    grade = None
    if r.get("quick") and r["quick"].get("grade"):
        grade = r["quick"]["grade"][0]   # first char: A/B/C/D/F
    if r.get("deep") and r["deep"].get("grade"):
        grade = r["deep"]["grade"][0]
    return f"{grade} {node}" if grade else node


# ── public async API (enqueue messages) ──────────────────────────────────────

async def publish_active_node(node: str) -> None:
    if not is_enabled():
        return
    prefix = _prefix()
    await _queue.put({"topic": f"{prefix}/active_node", "payload": node, "retain": True})
    # Keep the select entity in sync
    await _queue.put({"topic": f"{prefix}/select/node/state", "payload": node, "retain": True})
    # If not DIRECT, turn off the direct switch
    if node != "DIRECT":
        await _queue.put({"topic": f"{prefix}/switch/direct/state", "payload": "OFF", "retain": True})
    else:
        await _queue.put({"topic": f"{prefix}/switch/direct/state", "payload": "ON", "retain": True})


async def publish_state(
    nodes_alive: list[str],
    nodes_dead:  list[str],
    nodes_uncertain: list[str],
) -> None:
    """Publish top-N ranking. Called after each monitor poll."""
    if not is_enabled():
        return
    cfg = config.load()
    prefix = cfg.get("mqtt_topic_prefix", "node-tester").rstrip("/")
    top_n = int(cfg.get("mqtt_top_nodes", 10))

    import app.store as _store
    all_nodes = nodes_alive + nodes_dead + nodes_uncertain
    ranked = _ranked_nodes(nodes_alive) + _ranked_nodes(nodes_dead + nodes_uncertain)
    results = _store.get_node_results(all_nodes) if all_nodes else {}

    actual_n = min(top_n, len(ranked)) if ranked else 0
    for i in range(1, max(top_n, actual_n) + 1):
        payload = _node_label(ranked[i - 1], results) if i <= len(ranked) else ""
        await _queue.put({
            "topic":   f"{prefix}/top/{i}",
            "payload": payload,
            "retain":  True,
        })

    # All-nodes-dead status
    all_dead = len(nodes_alive) == 0 and (len(nodes_dead) + len(nodes_uncertain)) > 0
    await _queue.put({
        "topic":   f"{prefix}/all_nodes_dead",
        "payload": "ON" if all_dead else "OFF",
        "retain":  True,
    })


# ── HA discovery ─────────────────────────────────────────────────────────────

async def _publish_ha_discovery(nodes: list[str]) -> None:
    cfg = config.load()
    if not cfg.get("mqtt_ha_discovery"):
        return
    prefix = cfg.get("mqtt_topic_prefix", "node-tester").rstrip("/")
    top_n  = max(1, min(int(cfg.get("mqtt_top_nodes", 10)), len(nodes) if nodes else 0))
    device = {
        "identifiers":  ["mihomo_node_tester"],
        "name":         "Mihomo",
        "model":        "Proxy Monitor",
        "manufacturer": "node-tester",
    }

    async def _enq(disc_type: str, uid: str, payload: dict) -> None:
        await _queue.put({
            "topic":   f"homeassistant/{disc_type}/{uid}/config",
            "payload": json.dumps(payload, ensure_ascii=False),
            "retain":  True,
        })

    # ── sensors ──────────────────────────────────────────────────────────────
    await _enq("sensor", "mihomo_active_node", {
        "unique_id":   "mihomo_active_node",
        "name":        "Active Node",
        "state_topic": f"{prefix}/active_node",
        "icon":        "mdi:router-network",
        "device":      device,
    })

    for i in range(1, top_n + 1):
        await _enq("sensor", f"mihomo_top_{i}", {
            "unique_id":   f"mihomo_top_{i}",
            "name":        _top_label(i),
            "state_topic": f"{prefix}/top/{i}",
            "icon":        "mdi:medal" if i == 1 else "mdi:numeric-{}-box".format(i),
            "device":      device,
        })

    # ── binary sensor: all nodes dead ────────────────────────────────────────
    await _enq("binary_sensor", "mihomo_all_nodes_dead", {
        "unique_id":    "mihomo_all_nodes_dead",
        "name":         "All Nodes Dead",
        "state_topic":  f"{prefix}/all_nodes_dead",
        "payload_on":   "ON",
        "payload_off":  "OFF",
        "device_class": "problem",
        "icon":         "mdi:server-network-off",
        "device":       device,
    })

    # ── switch: DIRECT ────────────────────────────────────────────────────────
    await _enq("switch", "mihomo_direct", {
        "unique_id":     "mihomo_direct",
        "name":          "Direct (bypass proxy)",
        "state_topic":   f"{prefix}/switch/direct/state",
        "command_topic": f"{prefix}/switch/direct/set",
        "payload_on":    "ON",
        "payload_off":   "OFF",
        "icon":          "mdi:transit-skip",
        "device":        device,
    })

    # ── select: manual node ───────────────────────────────────────────────────
    if nodes:
        await _enq("select", "mihomo_node_select", {
            "unique_id":     "mihomo_node_select",
            "name":          "Active Node (manual)",
            "state_topic":   f"{prefix}/select/node/state",
            "command_topic": f"{prefix}/select/node/set",
            "options":       nodes,
            "icon":          "mdi:server-network",
            "device":        device,
        })

    # ── select: auto mode ─────────────────────────────────────────────────────
    await _enq("select", "mihomo_auto_mode", {
        "unique_id":     "mihomo_auto_mode",
        "name":          "Auto Mode",
        "state_topic":   f"{prefix}/select/auto/state",
        "command_topic": f"{prefix}/select/auto/set",
        "options":       ["off", "quick", "deep", "any"],
        "icon":          "mdi:auto-mode",
        "device":        device,
    })

    # ── test control buttons ──────────────────────────────────────────────────
    _test_buttons = [
        ("mihomo_test_quick", "Start Quick Test", "test/quick", "mdi:lightning-bolt"),
        ("mihomo_test_deep",  "Start Deep Test",  "test/deep",  "mdi:speedometer"),
        ("mihomo_test_stop",  "Stop Test",        "test/stop",  "mdi:stop-circle"),
    ]
    for uid, name, cmd, icon in _test_buttons:
        await _enq("button", uid, {
            "unique_id":     uid,
            "name":          name,
            "command_topic": f"{prefix}/{cmd}",
            "payload_press": "PRESS",
            "icon":          icon,
            "device":        device,
        })

    # ── test status sensors ───────────────────────────────────────────────────
    await _enq("sensor", "mihomo_test_status", {
        "unique_id":   "mihomo_test_status",
        "name":        "Test Status",
        "state_topic": f"{prefix}/test/status",
        "icon":        "mdi:progress-check",
        "device":      device,
    })
    await _enq("sensor", "mihomo_quick_progress", {
        "unique_id":   "mihomo_quick_progress",
        "name":        "Quick Test Progress",
        "state_topic": f"{prefix}/test/quick_progress",
        "icon":        "mdi:lightning-bolt",
        "device":      device,
    })
    await _enq("sensor", "mihomo_deep_progress", {
        "unique_id":   "mihomo_deep_progress",
        "name":        "Deep Test Progress",
        "state_topic": f"{prefix}/test/deep_progress",
        "icon":        "mdi:speedometer",
        "device":      device,
    })

    # ── system buttons ────────────────────────────────────────────────────────
    _buttons = [
        ("mihomo_flush_fakeip",  "Clear Fake-IP",    "fake_ip",   "mdi:ip-network-outline"),
        ("mihomo_flush_dns",     "Clear DNS Cache",  "dns_cache", "mdi:dns"),
        ("mihomo_restart",       "Restart Core",     "restart",   "mdi:restart"),
        ("mihomo_update_geo",    "Update GEO",       "geo",       "mdi:earth"),
    ]
    for uid, name, cmd, icon in _buttons:
        await _enq("button", uid, {
            "unique_id":     uid,
            "name":          name,
            "command_topic": f"{prefix}/button/{cmd}",
            "payload_press": "PRESS",
            "icon":          icon,
            "device":        device,
        })

    log.info("[mqtt] queued HA discovery (%d nodes, top-%d sensors)", len(nodes), top_n)


# ── command handler ────────────────────────────────────────────────────────────

async def _handle_command(cfg: dict, topic: str, payload: str) -> None:
    """React to an incoming MQTT command."""
    import app.mihomo as _mih
    import app.monitor as _monitor

    prefix = cfg.get("mqtt_topic_prefix", "node-tester").rstrip("/")
    host   = cfg.get("mihomo_host", "")
    port   = int(cfg.get("mihomo_port", 9090))
    secret = cfg.get("mihomo_secret", "")
    group  = cfg.get("proxy_group", "")

    rel = topic[len(prefix) + 1:] if topic.startswith(prefix + "/") else topic

    try:
        if rel == "switch/direct/set":
            if payload.upper() == "ON":
                await _mih.set_proxy(host, port, secret, group, "DIRECT")
                await publish_active_node("DIRECT")
                log.info("[mqtt] cmd → DIRECT ON")
            else:
                # Switch back to best alive node
                cache = _monitor.get_cache()
                alive = cache.get("alive") or []
                if alive:
                    from app.monitor import _best_node  # type: ignore[attr-defined]
                    best = _best_node(alive, cfg, "any")
                    if best:
                        await _mih.set_proxy(host, port, secret, group, best)
                        await publish_active_node(best)
                        log.info("[mqtt] cmd → DIRECT OFF, switched to %s", best)

        elif rel == "select/node/set":
            node = payload.strip()
            if node:
                await _mih.set_proxy(host, port, secret, group, node)
                await publish_active_node(node)
                log.info("[mqtt] cmd → node select: %s", node)

        elif rel == "select/auto/set":
            mode = payload.strip().lower()
            if mode in ("off", "quick", "deep", "any"):
                config.save({"auto_node_mode": mode})
                await _queue.put({
                    "topic":   f"{prefix}/select/auto/state",
                    "payload": mode,
                    "retain":  True,
                })
                log.info("[mqtt] cmd → auto mode: %s", mode)

        elif rel == "test/quick":
            from app.test_runner import run_test as _run_test
            asyncio.create_task(_run_test("quick"))
            log.info("[mqtt] cmd → start quick test")

        elif rel == "test/deep":
            from app.test_runner import run_test as _run_test
            asyncio.create_task(_run_test("deep"))
            log.info("[mqtt] cmd → start deep test")

        elif rel == "test/stop":
            from app.test_runner import stop_test as _stop_test
            _stop_test()
            log.info("[mqtt] cmd → stop test")

        elif rel == "button/fake_ip":
            await _mih.flush_fakeip(host, port, secret)
            log.info("[mqtt] cmd → flush fake-ip")

        elif rel == "button/dns_cache":
            await _mih.flush_dns(host, port, secret)
            log.info("[mqtt] cmd → flush DNS cache")

        elif rel == "button/restart":
            await _mih.restart_core(host, port, secret)
            log.info("[mqtt] cmd → restart core")

        elif rel == "button/geo":
            await _mih.update_geo(host, port, secret)
            log.info("[mqtt] cmd → update GEO")

    except Exception as e:
        log.warning("[mqtt] command error (%s): %s", rel, e)


# ── pub loop ──────────────────────────────────────────────────────────────────

async def _pub_loop(client: Any, cfg_host: str) -> None:
    while True:
        try:
            msg = await asyncio.wait_for(_queue.get(), timeout=30.0)
        except asyncio.TimeoutError:
            if config.load().get("mqtt_host") != cfg_host:
                break
            continue
        try:
            await client.publish(
                msg["topic"], msg["payload"],
                retain=msg.get("retain", True), qos=1,
            )
            log.debug("[mqtt] published %s", msg["topic"])
        except Exception as e:
            log.warning("[mqtt] publish error: %s", e)
            # put back in queue so it's not lost
            await _queue.put(msg)
            break


# ── background publisher task ─────────────────────────────────────────────────

async def run_publisher() -> None:
    """Maintains persistent MQTT connection; publishes queue + handles commands."""
    try:
        import aiomqtt
    except ImportError:
        log.warning("[mqtt] aiomqtt not installed — MQTT disabled")
        return

    await asyncio.sleep(15)

    while True:
        cfg = config.load()
        if not cfg.get("mqtt_enabled") or not cfg.get("mqtt_host"):
            await asyncio.sleep(30)
            continue

        kwargs: dict[str, Any] = {
            "hostname": cfg["mqtt_host"],
            "port":     int(cfg.get("mqtt_port", 1883)),
            "timeout":  10.0,
        }
        if cfg.get("mqtt_user"):
            kwargs["username"] = cfg["mqtt_user"]
        if cfg.get("mqtt_pass"):
            kwargs["password"] = cfg["mqtt_pass"]

        try:
            async with aiomqtt.Client(**kwargs) as client:
                log.info("[mqtt] connected to %s:%s",
                         cfg["mqtt_host"], cfg.get("mqtt_port", 1883))

                prefix = cfg.get("mqtt_topic_prefix", "node-tester").rstrip("/")

                # Subscribe to command topics
                await client.subscribe(f"{prefix}/switch/direct/set")
                await client.subscribe(f"{prefix}/select/node/set")
                await client.subscribe(f"{prefix}/select/auto/set")
                await client.subscribe(f"{prefix}/button/+")
                await client.subscribe(f"{prefix}/test/+")

                # Publish initial state: HA discovery + current data
                from app.monitor import get_cache
                import app.mihomo as _mih
                cache = get_cache()
                alive     = cache.get("alive")     or []
                dead      = cache.get("dead")      or []
                uncertain = cache.get("uncertain") or []
                all_nodes = alive + dead + uncertain

                await _publish_ha_discovery(all_nodes)
                await publish_state(alive, dead, uncertain)

                # Current active node
                try:
                    current = await _mih.get_active_leaf_node(
                        cfg["mihomo_host"], cfg["mihomo_port"],
                        cfg["mihomo_secret"], cfg.get("proxy_group", ""),
                    )
                    if current:
                        await publish_active_node(current)
                except Exception:
                    pass

                # Current auto mode
                auto_mode = cfg.get("auto_node_mode", "off")
                await _queue.put({
                    "topic":   f"{prefix}/select/auto/state",
                    "payload": auto_mode,
                    "retain":  True,
                })

                # Initial test status (idle on connect)
                await _queue.put({"topic": f"{prefix}/test/status",         "payload": "idle",  "retain": True})
                await _queue.put({"topic": f"{prefix}/test/quick_progress",  "payload": "0/0",   "retain": True})
                await _queue.put({"topic": f"{prefix}/test/deep_progress",   "payload": "0/0",   "retain": True})

                # Start publish loop in background
                pub_task = asyncio.create_task(_pub_loop(client, cfg["mqtt_host"]))
                try:
                    async for message in client.messages:
                        payload = message.payload.decode(errors="replace") if message.payload else ""
                        await _handle_command(cfg, str(message.topic), payload)
                finally:
                    pub_task.cancel()
                    try:
                        await pub_task
                    except asyncio.CancelledError:
                        pass

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("[mqtt] error: %s — retry in 30s", e)
            await asyncio.sleep(30)
