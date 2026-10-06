"""Background monitor: periodically reads URLTest state from Mihomo (no triggering)."""
import asyncio
import logging
from datetime import datetime, timezone as dt_timezone

import app.config as config
import app.mihomo as mihomo
import app.store as store
from app.core import scoring

log = logging.getLogger("node_tester.monitor")

# Deadlock / runaway protection
_POLL_TIMEOUT_SEC = 90  # max wall-time for one poll_once() run

# In-memory cache updated by background task
_cache: dict = {
    "alive":       [],
    "dead":        [],
    "uncertain":   [],
    "updated_at":  None,
    "error":       None,
    "last_switch": None,  # {"from": str, "to": str, "reason": str, "at": str}
    # Ladder state: "ladder_expected_node" is what the poll loop itself last
    # set the active node to; if the REAL active node ever differs from this
    # on the next poll, something else (Mihomo's own UI, or a manual switch
    # through node-tester) changed it -- that pauses the "climb to something
    # better" behaviour (manual_override=True) until either the node dies
    # (rescue always stays active) or a new manual/automatic switch happens.
    "ladder_expected_node": None,
    "manual_override":      False,
}

# Restore across restarts -- otherwise manual_override resets to False and the
# poll loop immediately climbs away from a manually-pinned node (see poll_once()).
_saved_state = store.load_monitor_state()
_cache["ladder_expected_node"] = _saved_state["ladder_expected_node"]
_cache["manual_override"] = _saved_state["manual_override"]

# Skip climb-to-best on the very first poll after a process start, even in
# plain auto mode (no manual pin): whatever node was active when the
# container stopped stays active through the restart. Normal auto-climbing
# resumes from the second poll onward. Without this, every addon/HA restart
# reset the ladder to "whatever scores best right now", which felt like the
# pin/last-used node was being ignored even though nothing was ever pinned.
_startup_grace = True


def get_cache() -> dict:
    return _cache


def _persist_ladder_state() -> None:
    store.save_monitor_state(_cache.get("ladder_expected_node"), bool(_cache.get("manual_override", False)))


def mark_manual_override(node: str) -> None:
    """Call this from anywhere a node is switched by explicit user action
    (Settings manual switch, MQTT select/node/set, forcing Direct/Reserve ON)
    -- makes the pause take effect immediately instead of waiting for the
    poll loop to notice."""
    _cache["ladder_expected_node"] = node
    _cache["manual_override"] = True
    _persist_ladder_state()


def resume_auto(node: str) -> None:
    """Call this when explicitly returning to automatic selection (Direct/
    Reserve switched OFF) -- syncs the tracker and lifts the pause."""
    _cache["ladder_expected_node"] = node
    _cache["manual_override"] = False
    _persist_ladder_state()


def _node_grade(node: str, results: dict) -> str | None:
    """Best available grade for a node: deep takes precedence over quick."""
    r = results.get(node, {})
    if r.get("deep") and r["deep"].get("grade"):
        return r["deep"]["grade"]
    if r.get("quick") and r["quick"].get("grade"):
        return r["quick"]["grade"]
    return None


def _ranked_nodes(nodes: list[str], cfg: dict, mode: str) -> list[str]:
    """Return the MAIN-eligible nodes from `nodes`, best first, ranked by `mode` (deep/quick/any).

    Eligibility: tagged "main" (backup/reserve/excluded never qualify here --
    backup is quarantine-only, reserve has its own selection path via
    _best_reserve_node) AND graded at/above min_grade_for_switch (untested
    nodes with no grade yet are not penalised)."""
    if not nodes:
        return []
    results   = store.get_node_results(nodes)
    groups    = cfg.get("node_groups") or {}
    threshold = cfg.get("min_grade_for_switch", "C")

    def _eligible(n: str) -> bool:
        if groups.get(n, "main") != "main":
            return False
        g = _node_grade(n, results)
        return g is None or scoring.grade_meets(g, threshold)

    candidates = [n for n in nodes if _eligible(n)]
    if not candidates:
        return []

    def score(n: str) -> tuple:
        r     = results.get(n, {})
        deep  = r["deep"]["score"]  if r.get("deep")  else -1
        quick = r["quick"]["score"] if r.get("quick") else -1
        if mode == "deep":
            return (deep, quick)
        if mode == "quick":
            return (quick, deep)
        return (max(deep, quick), deep, quick)

    return sorted(candidates, key=score, reverse=True)


def _best_node(nodes: list[str], cfg: dict, mode: str) -> str | None:
    """Best MAIN-eligible node from `nodes` (see _ranked_nodes)."""
    ranked = _ranked_nodes(nodes, cfg, mode)
    return ranked[0] if ranked else None


def _best_reserve_node(nodes: list[str], cfg: dict) -> str | None:
    """Best alive node tagged 'reserve' -- the fallback used only once every
    main node is dead/ineligible. If several are tagged reserve, rank them
    the same way as main nodes; if none are alive, caller falls to DIRECT."""
    groups   = cfg.get("node_groups") or {}
    reserves = [n for n in nodes if groups.get(n, "main") == "reserve"]
    if not reserves:
        return None
    results = store.get_node_results(reserves)

    def score(n: str) -> tuple:
        r     = results.get(n, {})
        deep  = r["deep"]["score"]  if r.get("deep")  else -1
        quick = r["quick"]["score"] if r.get("quick") else -1
        return (max(deep, quick), deep, quick)

    return max(reserves, key=score)


async def poll_once() -> None:
    """Read URLTest state from Mihomo; handle DIRECT fallback and dead-node switch."""
    cfg = config.load()
    if not config.is_configured() or not cfg.get("proxy_group"):
        return
    try:
        all_nodes = await mihomo.get_nodes_in_group(
            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
        )
        groups = cfg.get("node_groups") or {}
        all_nodes = [n for n in all_nodes if groups.get(n, "main") != "excluded"]

        confirmed_active, confirmed_dead, uncertain = await mihomo.classify_nodes(
            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], all_nodes
        )
        _cache["alive"]      = confirmed_active
        _cache["dead"]       = confirmed_dead
        _cache["uncertain"]  = uncertain
        _cache["updated_at"] = datetime.utcnow().isoformat(timespec="seconds")
        _cache["error"]      = None
        log.debug(
            "[monitor] poll ok: alive=%d dead=%d uncertain=%d",
            len(confirmed_active), len(confirmed_dead), len(uncertain),
        )
        import app.mqtt as mqtt
        asyncio.create_task(mqtt.publish_state(confirmed_active, confirmed_dead, uncertain))

        # Always track and publish the real active leaf node so MQTT stays in sync
        # even when the user switches nodes directly in the Mihomo UI.
        try:
            current = await mihomo.get_active_leaf_node(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )
        except Exception:
            current = None
        if current and current != _cache.get("current_node"):
            _cache["current_node"] = current
            asyncio.create_task(mqtt.publish_active_node(current))
            log.debug("[monitor] active node updated → %s", current)

        want_direct  = cfg.get("auto_direct_fallback", True)
        want_reserve = cfg.get("auto_reserve_fallback", True)
        want_switch  = cfg.get("auto_switch_dead", True)
        if not want_direct and not want_switch or not current:
            return

        now_str = datetime.utcnow().isoformat(timespec="seconds")

        # ── Unified recovery ladder ─────────────────────────────────────────────
        # want_switch is now the master gate: if off, this function never
        # proactively moves traffic (matches its old role for dead-node
        # protection; DIRECT-fallback used to be gated independently by
        # want_direct alone, but the two concerns are one cascade now).
        if not want_switch:
            return

        groups = cfg.get("node_groups") or {}

        # Mihomo's alive/history data is passive URLTest history -- but a
        # manually-selected node in a "select"-type group never gets that
        # periodic probing the way "urltest"/"fallback" group members do, so
        # it can easily show alive=False with no recent history and land
        # straight in confirmed_dead despite being perfectly reachable. Do
        # one active ping double-check for BOTH "uncertain" and
        # "confirmed_dead" before concluding current is actually down --
        # otherwise a manually-pinned-but-never-URLTested node gets
        # incorrectly "rescued" (and its manual pin cleared) on the very
        # next poll.
        effectively_alive = set(confirmed_active)
        if current in uncertain or current in confirmed_dead:
            try:
                ping_active, _, _ = await mihomo.ping_filter_nodes(
                    cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], [current]
                )
            except Exception as e:
                ping_active = []
                log.debug("[monitor] ping re-check failed for %s: %s", current, e)
            if current in ping_active:
                effectively_alive.add(current)
                log.debug("[monitor] %s uncertain but ping confirms alive", current)

        # Detect a change we didn't make ourselves (Mihomo's own UI, or a
        # manual switch through node-tester) since the last poll -- pauses
        # the "climb to something better" behaviour. Rescue-from-dead below
        # always stays active regardless of this pause.
        expected = _cache.get("ladder_expected_node")
        override = bool(_cache.get("manual_override", False))
        if expected is not None and current != expected:
            if not override:
                log.info("[monitor] external change detected (%s -> %s) -- pausing ladder climb", expected, current)
            override = True
            _cache["manual_override"] = True
        _cache["ladder_expected_node"] = current
        _persist_ladder_state()

        # DIRECT is never a real leaf in the proxy group's node list, so it can
        # never appear in effectively_alive -- treat it as "alive" here too, or
        # a manually-forced DIRECT (bypass proxy on purpose) would get instantly
        # rescued away from on the very next poll.
        current_is_alive = current in effectively_alive or current == "DIRECT"
        best_main = _best_node(list(effectively_alive), cfg, "any")

        if current_is_alive:
            global _startup_grace
            if _startup_grace:
                _startup_grace = False
                return  # first poll after a process start -- keep whatever was active, don't optimize yet
            if override:
                return  # respect the manual pick as long as it keeps working
            if not best_main or current == best_main:
                return  # already at the top, or nothing eligible to climb to
            target, reason = best_main, "climb_to_best_main"
        else:
            # Current is genuinely dead -- rescue always applies and clears
            # any prior manual pause (the manual pick is gone either way).
            if best_main:
                target, reason = best_main, "climb_to_best_main"
            else:
                reserve = _best_reserve_node(list(effectively_alive), cfg) if want_reserve else None
                if reserve:
                    target, reason = reserve, "reserve"
                elif want_direct:
                    target, reason = "DIRECT", "all_main_dead"
                else:
                    return  # nothing eligible and DIRECT fallback disabled — stay put
            _cache["manual_override"] = False
            _persist_ladder_state()

        if target == current:
            return

        await mihomo.set_proxy(
            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
            cfg["proxy_group"], target,
        )
        _cache["ladder_expected_node"] = target
        _persist_ladder_state()
        _cache["last_switch"] = {
            "from": current, "to": target,
            "reason": reason,
            "at": now_str,
        }
        log.info("[monitor] %s: %s -> %s", reason, current, target)
        asyncio.create_task(mqtt.publish_active_node(target))

    except Exception as e:
        _cache["error"] = str(e)
        log.warning("[monitor] poll failed: %s", e)


async def apply_best_node(after_test: str, tested_nodes: list[str] | None = None) -> None:
    """Switch to best node after a test completes.

    after_test:    "quick" or "deep"
    tested_nodes:  nodes actually tested (use these as candidates, not monitor cache).
                   Falls back to _cache["alive"] if not provided.

    auto_node_mode controls WHICH test triggers the switch:
      "quick" → only after Quick test  (rank by quick scores)
      "deep"  → only after Deep test   (rank by deep scores)
      "any"   → after either test      (rank by scores of the test that just ran)
      "off"   → never
    """
    cfg = config.load()
    mode = cfg.get("auto_node_mode", "off")
    if mode == "off":
        return
    if mode == "quick" and after_test != "quick":
        return
    if mode == "deep" and after_test != "deep":
        return

    # Use explicitly tested nodes if provided; fall back to monitor cache.
    # This is important: monitor cache only contains confirmed_active (no uncertain nodes),
    # but the test may have run on uncertain nodes that passed ping — those must be candidates too.
    candidates = list(tested_nodes) if tested_nodes else list(_cache.get("alive") or [])
    if not candidates:
        return

    if not config.is_configured() or not cfg.get("proxy_group"):
        return

    try:
        current = await mihomo.get_active_leaf_node(
            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
        )
    except Exception:
        return

    if not current or current == "DIRECT":
        return

    # A manual pin (Settings, MQTT select/node, Mihomo's own UI) pauses the
    # ladder's automatic climbing -- a completed test applying its result is
    # just another form of that same "climb to best" behaviour, so it must
    # respect the same pause as long as the pinned node is still alive.
    # Rescue-if-actually-dead still applies below (current not in candidates
    # falls through to the normal best-node switch).
    if _cache.get("manual_override") and current in candidates:
        log.info("[monitor] after-%s: %s was manually pinned and is still alive -- "
                 "skipping auto-switch", after_test, current)
        return

    best = _best_node(candidates, cfg, after_test)
    if not best or best == current:
        return

    try:
        await mihomo.set_proxy(
            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
            cfg["proxy_group"], best,
        )
        _cache["last_switch"] = {
            "from": current, "to": best,
            "reason": f"after_{after_test}",
            "at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        # Running a test and auto-applying its result is itself an explicit,
        # intentional action -- treat it the same as an automatic ladder
        # switch: sync the tracker and resume normal poll-loop behaviour.
        _cache["ladder_expected_node"] = best
        _cache["manual_override"] = False
        log.info("[monitor] after-%s: %s → %s (from %d candidates)",
                 after_test, current, best, len(candidates))
        import app.mqtt as mqtt
        asyncio.create_task(mqtt.publish_active_node(best))
        asyncio.create_task(sync_standby(cfg, best))
    except Exception as e:
        log.warning("[monitor] apply_best_node failed: %s", e)


_FAST_RETRY_SEC = 20  # on a failed poll, retry soon instead of waiting the full interval


async def run_monitor() -> None:
    """Loops forever; interval is re-read from config on every tick.

    A failed poll (e.g. Mihomo's API being briefly unreachable — OpenClash
    often restarts its core right when the WAN changes, which is exactly the
    moment we most need to react) retries quickly instead of waiting the full
    configured interval. Waiting up to monitor_interval_min (default 5 min)
    after one unlucky failed poll meant the recovery ladder could sit idle
    for minutes with no visibility into whether a switch was even needed."""
    await asyncio.sleep(5)
    while True:
        cfg = config.load()
        interval = int(cfg.get("monitor_interval_min", 5))
        if interval > 0:
            ok = True
            try:
                await asyncio.wait_for(poll_once(), timeout=_POLL_TIMEOUT_SEC)
                try:
                    await asyncio.wait_for(sync_standby(), timeout=_POLL_TIMEOUT_SEC)
                except Exception as e:
                    log.debug("[standby] sync failed: %s", e)
            except asyncio.TimeoutError:
                ok = False
                _cache["error"] = f"poll timed out after {_POLL_TIMEOUT_SEC}s"
                log.warning("[monitor] poll_once timed out after %ds", _POLL_TIMEOUT_SEC)
            except Exception as e:
                ok = False
                _cache["error"] = str(e) or repr(e)
                log.warning("[monitor] unexpected error in run_monitor: %s", repr(e))
            await asyncio.sleep(interval * 60 if ok else _FAST_RETRY_SEC)
        else:
            await asyncio.sleep(60)


# ── Standby selectors + fast guard ───────────────────────────────────────────
_STBY_BATCH       = 8   # candidates pinged in parallel
_STBY_MAX_BATCHES = 3   # give up after this many batches (24 nodes) without a hit
_GUARD_PING_MS = 2500  # per-URL timeout for guard probes / candidate pings
_guard_fail: dict = {"node": None, "count": 0}
_rescue_lock = asyncio.Lock()


async def _ranked_candidates(cfg: dict, exclude: set[str]) -> list[str]:
    """Main-tier nodes of the proxy group, best score first, minus `exclude`."""
    all_nodes = await mihomo.get_nodes_in_group(
        cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
    )
    groups = cfg.get("node_groups") or {}
    nodes = [n for n in all_nodes if groups.get(n, "main") != "excluded" and n not in exclude]
    return _ranked_nodes(nodes, cfg, "any")


async def _pick_alive_ranked(cfg: dict, ranked: list[str], need: int) -> list[str]:
    """First `need` nodes of `ranked` (rank order kept) that answer an active ping."""
    picked: list[str] = []
    limit = _STBY_BATCH * _STBY_MAX_BATCHES
    for i in range(0, min(len(ranked), limit), _STBY_BATCH):
        batch = ranked[i:i + _STBY_BATCH]
        active, _, _ = await mihomo.ping_filter_nodes(
            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], batch,
            timeout_ms=_GUARD_PING_MS,
        )
        alive = set(active)
        picked += [n for n in batch if n in alive]
        if len(picked) >= need:
            break
    return picked[:need]


async def sync_standby(cfg: dict | None = None, current: str | None = None) -> None:
    """Point each standby select group at the next-best alive node (rank 2, 3, ...).
    The outer fallback group fails over to these before 4G / DIRECT."""
    cfg = cfg or config.load()
    stby = [g for g in (cfg.get("standby_groups") or []) if g]
    if not stby or not config.is_configured() or not cfg.get("proxy_group"):
        return
    host, port, secret = cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"]
    if current is None:
        current = await mihomo.get_active_leaf_node(host, port, secret, cfg["proxy_group"])
    ranked = await _ranked_candidates(cfg, {current} if current else set())
    alive_cache = set(_cache.get("alive") or [])
    chosen = [n for n in ranked if n in alive_cache][:len(stby)]
    if len(chosen) < len(stby):
        rest = [n for n in ranked if n not in chosen]
        chosen += await _pick_alive_ranked(cfg, rest, len(stby) - len(chosen))
    for g, node in zip(stby, chosen):
        try:
            if await mihomo.get_selector_now(host, port, secret, g) != node:
                await mihomo.set_proxy(host, port, secret, g, node)
                log.info("[standby] %s -> %s", g, node)
        except Exception as e:
            log.debug("[standby] %s: %s", g, e)


async def _guard_rescue(cfg: dict, current: str) -> None:
    """Active node is dead: move proxy_group to the best-ranked node that answers."""
    async with _rescue_lock:
        ranked = await _ranked_candidates(cfg, {current})
        picked = await _pick_alive_ranked(cfg, ranked, 1)
        if not picked:
            log.warning("[guard] %s is down and no ranked node answers -- leaving it to the fallback group", current)
            return
        target = picked[0]
        await mihomo.set_proxy(
            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"], target
        )
        _cache["ladder_expected_node"] = target
        _cache["manual_override"] = False
        _persist_ladder_state()
        _cache["last_switch"] = {
            "from": current, "to": target, "reason": "guard_rescue",
            "at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        _cache["current_node"] = target
        log.info("[guard] rescue: %s -> %s", current, target)
        import app.mqtt as mqtt
        asyncio.create_task(mqtt.publish_active_node(target))
        # Force Mihomo to re-test the proxy group right away: otherwise its stale
        # "dead" flag keeps the outer fallback group on a standby node until its next
        # scheduled health check (and the standby shift below would move traffic again).
        try:
            await mihomo.ping_filter_nodes(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], [cfg["proxy_group"]]
            )
            if cfg.get("safe_group"):
                # ...and re-check the outer fallback group so it returns to the proxy group now
                await mihomo.force_group_check(
                    cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["safe_group"]
                )
        except Exception as e:
            log.debug("[guard] forced group check failed: %s", e)
    try:
        await sync_standby(cfg, target)
    except Exception as e:
        log.debug("[standby] sync after rescue failed: %s", e)


async def guard_tick() -> None:
    cfg = config.load()
    if not config.is_configured() or not cfg.get("proxy_group"):
        return
    if not cfg.get("auto_switch_dead", True):
        return  # rescue is part of the "switch dead node" feature
    host, port, secret = cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"]
    current = await mihomo.get_active_leaf_node(host, port, secret, cfg["proxy_group"])
    if not current or current == "DIRECT":
        _guard_fail.update(node=None, count=0)
        return
    # Mihomo's own outer fallback already left the proxy group -> it noticed first.
    left_group = False
    safe = cfg.get("safe_group")
    if safe:
        try:
            sn = await mihomo.get_selector_now(host, port, secret, safe)
            left_group = bool(sn) and sn != cfg["proxy_group"]
        except Exception:
            pass
    active, _, _ = await mihomo.ping_filter_nodes(host, port, secret, [current], timeout_ms=_GUARD_PING_MS)
    if active:
        _guard_fail.update(node=current, count=0)
        return
    if _guard_fail["node"] != current:
        _guard_fail.update(node=current, count=0)
    _guard_fail["count"] += 1
    need = max(1, int(cfg.get("guard_failures", 2)))
    if left_group or _guard_fail["count"] >= need:
        log.warning("[guard] active node %s is not answering (%d miss%s%s)",
                    current, _guard_fail["count"], "es" if _guard_fail["count"] != 1 else "",
                    ", fallback group already switched" if left_group else "")
        _guard_fail.update(node=None, count=0)
        await _guard_rescue(cfg, current)


async def run_guard() -> None:
    """Fast loop: detects a dead ACTIVE node within seconds (the slow monitor
    poll only runs every few minutes)."""
    await asyncio.sleep(15)
    while True:
        cfg = config.load()
        interval = int(cfg.get("guard_interval_sec", 10))
        if interval > 0:
            t0 = asyncio.get_running_loop().time()
            try:
                await asyncio.wait_for(guard_tick(), timeout=60)
            except asyncio.TimeoutError:
                log.warning("[guard] tick timed out")
            except Exception as e:
                log.debug("[guard] tick failed: %s", e)
            # fixed rate: the interval counts from the START of the tick, so a slow
            # probe of a dead node does not stretch the cycle
            await asyncio.sleep(max(1.0, max(3, interval) - (asyncio.get_running_loop().time() - t0)))
        else:
            await asyncio.sleep(30)
