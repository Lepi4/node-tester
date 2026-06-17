"""Public API for triggering and stopping tests (used by scheduler and MQTT)."""
import asyncio
import logging

log = logging.getLogger("node_tester.test_runner")

_running: dict[str, bool] = {"quick": False, "deep": False}


class _FakeRequest:
    async def is_disconnected(self) -> bool:
        return False


async def run_test(test_type: str) -> None:
    """Run quick or deep test in the background. No-op if already running."""
    if _running.get(test_type):
        log.info("[test_runner] %s test already running, skipping", test_type)
        return
    from app.web.router import run_quick_stream, run_deep_stream
    fake = _FakeRequest()
    _running[test_type] = True
    try:
        log.info("[test_runner] starting %s test", test_type)
        resp = await (run_quick_stream(fake) if test_type == "quick" else run_deep_stream(fake))
        async for _ in resp.body_iterator:
            pass
        log.info("[test_runner] %s test finished", test_type)
    except Exception as e:
        log.warning("[test_runner] %s test error: %s", test_type, e)
    finally:
        _running[test_type] = False


def stop_test(test_type: str | None = None) -> None:
    """Stop a running test. If test_type is None, stops all running tests."""
    from app.web.router import _stop_events, _abort_callbacks  # type: ignore[attr-defined]
    types = [test_type] if test_type else list(_stop_events.keys())
    for t in types:
        ev = _stop_events.get(t)
        if ev:
            ev.set()
        cb = _abort_callbacks.get(t)
        if cb:
            cb()
        log.info("[test_runner] stop signaled: %s", t)


def is_running(test_type: str | None = None) -> bool:
    if test_type:
        return bool(_running.get(test_type))
    return any(_running.values())
