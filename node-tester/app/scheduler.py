"""Scheduled test runner: independent schedules for quick and deep tests."""
import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import app.config as config

log = logging.getLogger("node_tester.scheduler")


async def _run_test(test_type: str) -> None:
    from app.test_runner import run_test
    await run_test(test_type)


def _should_fire(test_type: str, cfg: dict, tz: ZoneInfo,
                 now_utc: datetime, now_loc: datetime,
                 state: dict) -> bool:
    prefix = f"schedule_{test_type}_"
    mode   = cfg.get(prefix + "mode", "interval")

    if mode == "interval":
        hours = max(1, int(cfg.get(prefix + "interval", 8)))
        last  = state["last_run_utc"]
        return last is None or (now_utc - last) >= timedelta(hours=hours)

    elif mode == "daily":
        hour     = int(cfg.get(prefix + "hour", 2))
        minute   = int(cfg.get(prefix + "minute", 0))
        slot_key = now_loc.strftime("%Y-%m-%d-") + f"{hour:02d}:{minute:02d}"
        if now_loc.hour == hour and now_loc.minute >= minute and state["last_slot"] != slot_key:
            state["last_slot"] = slot_key
            return True
        return False

    elif mode == "weekly":
        hour     = int(cfg.get(prefix + "hour", 2))
        minute   = int(cfg.get(prefix + "minute", 0))
        days     = cfg.get(prefix + "days", list(range(7)))
        slot_key = now_loc.strftime("%Y-%m-%d-") + f"{hour:02d}:{minute:02d}"
        if now_loc.weekday() in days and now_loc.hour == hour and now_loc.minute >= minute and state["last_slot"] != slot_key:
            state["last_slot"] = slot_key
            return True
        return False

    return False


async def run_scheduler() -> None:
    await asyncio.sleep(20)

    state = {
        "quick": {"last_run_utc": None, "last_slot": None, "running": False},
        "deep":  {"last_run_utc": None, "last_slot": None, "running": False},
    }

    while True:
        await asyncio.sleep(60)
        cfg = config.load()
        tz      = ZoneInfo(cfg.get("timezone", "Europe/Moscow"))
        now_utc = datetime.utcnow()
        now_loc = datetime.now(tz)

        for test_type in ("quick", "deep"):
            st = state[test_type]
            if not cfg.get(f"schedule_{test_type}_enabled"):
                continue
            if st["running"]:
                continue
            if not _should_fire(test_type, cfg, tz, now_utc, now_loc, st):
                continue

            st["last_run_utc"] = now_utc
            log.info("[scheduler] firing %s test", test_type)

            async def _task(tt=test_type, s=st):
                s["running"] = True
                try:
                    await _run_test(tt)
                finally:
                    s["running"] = False

            asyncio.create_task(_task())
