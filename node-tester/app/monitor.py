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
}


def get_cache() -> dict:
    return _cache


def _node_grade(node: str, results: dict) -> str | None:
    """Best available grade for a node: deep takes precedence over quick."""
    r = results.get(node, {})
    if r.get("deep") and r["deep"].get("grade"):
        return r["deep"]["grade"]
    if r.get("quick") and r["quick"].get("grade"):
        return r["quick"]["grade"]
    return None


def _best_node(nodes: list[str], cfg: dict, mode: str) -> str | None:
    """Return the best MAIN-eligible node from `nodes`, ranked by `mode` (deep/quick/any).

    Eligibility: tagged "main" (backup/reserve/excluded never qualify here --
    backup is quarantine-only, reserve has its own selection path via
    _best_reserve_node) AND graded at/above min_grade_for_switch (untested
    nodes with no grade yet are not penalised)."""
    if not nodes:
        return None
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
        return None

    def score(n: str) -> tuple:
        r     = results.get(n, {})
        deep  = r["deep"]["score"]  if r.get("deep")  else -1
        quick = r["quick"]["score"] if r.get("quick") else -1
        if mode == "deep":
            return (deep, quick)
        if mode == "quick":
            return (quick, deep)
        return (max(deep, quick), deep, quick)

    return max(candidates, key=score)


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

        # If current only missed a single passive URLTest beat (classified
        # "uncertain", not confirmed dead), do one active ping double-check
        # before treating it as not-best — a lone dropped packet shouldn't
        # cause a switch away from an otherwise perfectly reachable node.
        effectively_alive = set(confirmed_active)
        if current in uncertain:
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

        best_main = _best_node(list(effectively_alive), cfg, "any")

        # Already sitting on the actual top-ranked eligible main node —
        # nothing to do. Ordinary main<->main reshuffling otherwise stays
        # test-driven via apply_best_node, not this poll loop. This is the
        # "ladder" exit condition: while current != best_main we keep
        # climbing every poll, whether current is DIRECT, reserve, or just a
        # lower-ranked (but alive) main node.
        if best_main and current == best_main:
            return

        if best_main:
            target, reason = best_main, "climb_to_best_main"
        else:
            reserve = _best_reserve_node(list(effectively_alive), cfg)
            if reserve:
                target, reason = reserve, "reserve"
            elif want_direct:
                target, reason = "DIRECT", "all_main_dead"
            else:
                return  # nothing eligible and DIRECT fallback disabled — stay put

        if target == current:
            return

        await mihomo.set_proxy(
            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
            cfg["proxy_group"], target,
        )
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
        log.info("[monitor] after-%s: %s → %s (from %d candidates)",
                 after_test, current, best, len(candidates))
        import app.mqtt as mqtt
        asyncio.create_task(mqtt.publish_active_node(best))
    except Exception as e:
        log.warning("[monitor] apply_best_node failed: %s", e)


async def run_monitor() -> None:
    """Loops forever; interval is re-read from config on every tick."""
    await asyncio.sleep(5)
    while True:
        cfg = config.load()
        interval = int(cfg.get("monitor_interval_min", 5))
        if interval > 0:
            try:
                await asyncio.wait_for(poll_once(), timeout=_POLL_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                _cache["error"] = f"poll timed out after {_POLL_TIMEOUT_SEC}s"
                log.warning("[monitor] poll_once timed out after %ds", _POLL_TIMEOUT_SEC)
            except Exception as e:
                _cache["error"] = str(e)
                log.warning("[monitor] unexpected error in run_monitor: %s", e)
            await asyncio.sleep(interval * 60)
        else:
            await asyncio.sleep(60)
