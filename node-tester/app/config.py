import json
import os
from pathlib import Path

CONFIG_PATH = Path(os.environ.get("DATA_DIR", "/app/data")) / "config.json"

DEFAULTS: dict = {
    "mihomo_host": "",
    "mihomo_port": 9090,
    "mihomo_secret": "",
    "proxy_group": "",
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
    # Node groups: node_name → "main" | "backup" | "excluded"
    # main   — ranked first, preferred in auto-switch
    # backup — ranked after main, used only when all main nodes dead
    # excluded — hidden from all tests and the dashboard table
    "node_groups": {},
    # Auto node selection: off / deep / quick / any
    "auto_node_mode": "off",
    # Switch to any alive node when current node dies (independent of auto_node_mode)
    "auto_switch_dead": True,
    # Switch to DIRECT when all nodes are dead; switch back when any node recovers
    "auto_direct_fallback": True,
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
