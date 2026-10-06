import json
import os
from pathlib import Path

CONFIG_PATH = Path(os.environ.get("DATA_DIR", "/app/data")) / "config.json"

DEFAULTS: dict = {
    "mihomo_host": "",
    "mihomo_port": 9090,
    "mihomo_secret": "",
    "proxy_group": "",
    # Dedicated test slot: a Mihomo listener (slot_port) hard-wired to a select
    # group (slot_group). Tests switch/ride THAT group, never the production
    # proxy_group. Both empty/0 = legacy behaviour (tests switch proxy_group).
    "slot_port": 0,
    # Optional outer fallback group (e.g. SAFE = PROXY -> 4G -> DIRECT) that clients
    # actually use; only used to show the real traffic path on the dashboard.
    "safe_group": "",
    # Guard: fast dead-node detection for the ACTIVE node (needs auto_switch_dead).
    # Every guard_interval_sec it pings the active node; guard_failures misses in a
    # row (or the outer fallback group having left proxy_group) trigger an immediate
    # rescue to the best-ranked node that answers a ping. 0 = guard off.
    "guard_interval_sec": 10,
    "guard_failures": 2,
    # Select groups kept pointed at the next-best alive nodes (rank 2, 3, ...) so the
    # outer fallback group has good nodes to fail over to before 4G / DIRECT.
    "standby_groups": [],
    "slot_group": "",
    # Mihomo groups (e.g. a Fallback of 4G nodes) shown/switched as ONE node
    "group_nodes": [],
    "mixed_port": 7893,
    "proxy_user": "",
    "proxy_pass": "",
    # Deep Score weights (must sum to 100)
    "weight_quick":   40,
    "weight_browser": 30,
    "weight_speed":   20,
    "weight_ws":      10,
    # Browser component split: httpx BBC/TMDB vs Playwright video (sums to 100)
    "weight_browser_httpx": 60,   # video gets 100 - this
    # Logging
    "log_level": "WARNING",  # DEBUG / INFO / WARNING / ERROR
    # Timezone for local time display (IANA name)
    "timezone": "Europe/Moscow",
    # Background monitor: how often to re-read URLTest state from Mihomo (0 = off)
    "monitor_interval_min": 5,
    # Node groups: node_name → "main" | "backup" | "reserve" | "excluded"
    # main     — the normal table; ranked/auto-switched among themselves
    # backup   — quarantine: tested and tracked, but NEVER eligible for connection
    # reserve  — last resort before DIRECT: used only when every main node is
    #            dead/ineligible; if the reserve is also dead, falls to DIRECT
    # excluded — hidden from all tests and the dashboard table
    "node_groups": {},
    # A node graded below this is treated as dead for selection purposes (still
    # tested/tracked, just never chosen) — same effect as being in "backup".
    "min_grade_for_switch": "C",
    # Auto node selection: off / deep / quick / any
    "auto_node_mode": "off",
    # Switch to any alive node when current node dies (independent of auto_node_mode)
    "auto_switch_dead": True,
    # Switch to DIRECT when all nodes are dead; switch back when any node recovers
    "auto_direct_fallback": True,
    # Use a "reserve"-tagged node when all main nodes are dead/ineligible
    # (tried before falling further to DIRECT). Off = skip straight to DIRECT.
    "auto_reserve_fallback": True,
    # MQTT
    "mqtt_enabled":       False,
    "mqtt_host":          "",
    "mqtt_port":          1883,
    "mqtt_user":          "",
    "mqtt_pass":          "",
    "mqtt_topic_prefix":  "node-tester",
    "mqtt_ha_discovery":  True,
    "mqtt_top_nodes":     10,   # how many "top N" sensors to publish
    # Telegram media channels for testing (primary + 2 fallbacks)
    "tg_video_channels":  ["", "", ""],   # public channel names for video test
    "tg_image_channels":  ["", "", ""],   # public channel names for image test
    # Scheduled tests (quick and deep have independent schedules)
    "schedule_quick_enabled":  False,
    "schedule_quick_mode":     "interval", # "interval" | "daily" | "weekly"
    "schedule_quick_interval": 8,          # hours between runs
    "schedule_quick_hour":     2,          # hour to run (daily/weekly, 0-23)
    "schedule_quick_minute":   0,          # minute to run (daily/weekly, 0-59)
    "schedule_quick_days":     [0,1,2,3,4,5,6],
    "schedule_deep_enabled":   False,
    "schedule_deep_mode":      "interval",
    "schedule_deep_interval":  24,
    "schedule_deep_hour":      3,
    "schedule_deep_minute":    0,
    "schedule_deep_days":      [0,1,2,3,4,5,6],
}


def load() -> dict:
    if CONFIG_PATH.exists():
        try:
            data = json.loads(CONFIG_PATH.read_text())
            # Migrate legacy excluded_nodes → node_groups
            if "excluded_nodes" in data and not data.get("node_groups"):
                groups: dict = {}
                for n in (data.get("excluded_nodes") or []):
                    groups[n] = "excluded"
                data["node_groups"] = groups
            data.pop("excluded_nodes", None)
            return {**DEFAULTS, **data}
        except Exception:
            pass
    return DEFAULTS.copy()


def save(data: dict) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged = {**load(), **data}
    CONFIG_PATH.write_text(json.dumps(merged, indent=2))


def is_configured() -> bool:
    return bool(load().get("mihomo_host"))


def slot_enabled(cfg: dict) -> bool:
    return bool(cfg.get("slot_port")) and bool(cfg.get("slot_group"))


def test_group(cfg: dict) -> str:
    """Group whose selection the tests switch (the slot, else the real group)."""
    return cfg["slot_group"] if slot_enabled(cfg) else cfg["proxy_group"]


def test_port(cfg: dict) -> int:
    """Mihomo inbound port the tests send traffic through."""
    return int(cfg["slot_port"]) if slot_enabled(cfg) else cfg["mixed_port"]
