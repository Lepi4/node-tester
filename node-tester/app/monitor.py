"""Background monitor: periodically reads URLTest state from Mihomo (no triggering)."""
import asyncio
import logging
from datetime import datetime, timezone as dt_timezone

import app.config as config
import app.mihomo as mihomo
import app.store as store

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


def _best_node(nodes: list[str], cfg: dict, mode: str) -> str | None:
    """Return the best node from `nodes` ranked by `mode` (deep/quick/any).
    Main group always ranks above backup regardless of score."""
    if not nodes:
        return None
    results = store.get_node_results(nodes)
    groups  = cfg.get("node_groups") or {}

    def _gp(n: str) -> int:
        """Group priority: 0=main (wins), -1=backup."""
        return 0 if groups.get(n, "main") != "backup" else -1

    def score(n: str) -> tuple:
        r     = results.get(n, {})
        deep  = r["deep"]["score"]  if r.get("deep")  else -1
        quick = r["quick"]["score"] if r.get("quick") else -1
        gp    = _gp(n)
        if mode == "deep":
            return (gp, deep, quick)
        if mode == "quick":
            return (gp, quick, deep)
        return (gp, max(deep, quick), deep, quick)

    return max(nodes, key=score)


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

        want_direct  = cfg.get("auto_direct_fallback", True)
        want_switch  = cfg.get("auto_switch_dead", True)
        if not want_direct and not want_switch:
            return

        try:
            current = await mihomo.get_active_leaf_node(
                cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"], cfg["proxy_group"]
            )
        except Exception:
            return
        if not current:
            return

        import app.mqtt as mqtt
        now_str = datetime.utcnow().isoformat(timespec="seconds")

        # ── DIRECT fallback ────────────────────────────────────────────────────
        if want_direct:
            if not confirmed_active and current != "DIRECT":
                # All nodes dead → switch to DIRECT
                await mihomo.set_proxy(
                    cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                    cfg["proxy_group"], "DIRECT",
                )
                _cache["last_switch"] = {
                    "from": current, "to": "DIRECT",
                    "reason": "all_nodes_dead",
                    "at": now_str,
                }
                log.info("[monitor] all nodes dead → DIRECT (was %s)", current)
                asyncio.create_task(mqtt.publish_active_node("DIRECT"))
                return

            if current == "DIRECT" and confirmed_active:
                # At least one node recovered → switch to best
                best = _best_node(confirmed_active, cfg, "any")
                if best:
                    await mihomo.set_proxy(
                        cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
                        cfg["proxy_group"], best,
                    )
                    _cache["last_switch"] = {
                        "from": "DIRECT", "to": best,
                        "reason": "recovered_from_direct",
                        "at": now_str,
                    }
                    log.info("[monitor] recovered from DIRECT → %s", best)
                    asyncio.create_task(mqtt.publish_active_node(best))
                return  # nothing more to do after recovery

            if current == "DIRECT":
                return  # still in DIRECT, still no alive nodes — keep waiting

        # ── Dead-node protection ───────────────────────────────────────────────
        if not want_switch or not confirmed_active:
            return
        if current in set(confirmed_active):
            return  # node is alive — auto best-node selection is done in apply_best_node

        # Current node is dead → switch to top alive node
        best = _best_node(confirmed_active, cfg, "any")
        if not best or best == current:
            return

        await mihomo.set_proxy(
            cfg["mihomo_host"], cfg["mihomo_port"], cfg["mihomo_secret"],
            cfg["proxy_group"], best,
        )
        _cache["last_switch"] = {
            "from": current, "to": best,
            "reason": "dead",
            "at": now_str,
        }
        log.info("[monitor] dead-node switch: %s → %s", current, best)
        asyncio.create_task(mqtt.publish_active_node(best))

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
