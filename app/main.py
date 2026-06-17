import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from app.web.router import router
import app.config as config
from app.monitor import run_monitor
from app.scheduler import run_scheduler
from app.mqtt import run_publisher

_log = logging.getLogger("node_tester.main")


def apply_log_level(level_str: str) -> None:
    level = getattr(logging, level_str.upper(), logging.WARNING)
    logging.getLogger("node_tester").setLevel(level)
    # Uvicorn access log не трогаем, только наши логгеры
    for name in ("node_tester.router", "node_tester.video", "node_tester.mihomo"):
        logging.getLogger(name).setLevel(level)


def _setup_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    ))
    root = logging.getLogger("node_tester")
    if not root.handlers:
        root.addHandler(handler)
    root.propagate = False
    apply_log_level(config.load().get("log_level", "WARNING"))


_setup_logging()


async def _monitor_supervisor() -> None:
    """Restart run_monitor if it crashes unexpectedly (acts as supervisor)."""
    while True:
        try:
            await run_monitor()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _log.error("[monitor] crashed, restarting in 30s: %s", e)
            await asyncio.sleep(30)


async def _scheduler_supervisor() -> None:
    while True:
        try:
            await run_scheduler()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _log.error("[scheduler] crashed, restarting in 30s: %s", e)
            await asyncio.sleep(30)


async def _mqtt_supervisor() -> None:
    while True:
        try:
            await run_publisher()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _log.error("[mqtt] crashed, restarting in 30s: %s", e)
            await asyncio.sleep(30)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    tasks = [
        asyncio.create_task(_monitor_supervisor()),
        asyncio.create_task(_scheduler_supervisor()),
        asyncio.create_task(_mqtt_supervisor()),
    ]
    yield
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


app = FastAPI(title="Node Tester", lifespan=_lifespan, docs_url=None, redoc_url=None)
app.include_router(router)
