"""SQLite history store for test results (time-series, used by charts)."""
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

_DB_PATH = Path("/app/data/history.db")


def _conn() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(_DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init() -> None:
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS history (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                module  TEXT    NOT NULL,
                node    TEXT    NOT NULL,
                score   REAL    NOT NULL,
                grade   TEXT,
                extra   TEXT,           -- JSON with module-specific fields
                ts      TEXT    NOT NULL -- ISO-8601 UTC
            )
        """)
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_mod_node_ts ON history(module, node, ts)"
        )


def insert(module: str, node: str, score: float, grade: str,
           ts: str, extra: dict) -> None:
    with _conn() as con:
        con.execute(
            "INSERT INTO history (module, node, score, grade, extra, ts) VALUES (?,?,?,?,?,?)",
            (module, node, score, grade, json.dumps(extra, ensure_ascii=False), ts),
        )


def get_history(module: str, days: int = 30) -> dict[str, list]:
    """Return {node: [{score, grade, ts, ...extra}]} for the given module and period."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat(timespec="seconds")
    with _conn() as con:
        rows = con.execute(
            "SELECT node, score, grade, extra, ts FROM history "
            "WHERE module=? AND ts>=? ORDER BY ts",
            (module, cutoff),
        ).fetchall()

    out: dict[str, list] = {}
    for row in rows:
        extra = {}
        try:
            extra = json.loads(row["extra"] or "{}")
        except Exception:
            pass
        entry = {"score": row["score"], "grade": row["grade"],
                 "timestamp": row["ts"], **extra}
        out.setdefault(row["node"], []).append(entry)
    return out


# Initialise schema on import
init()
