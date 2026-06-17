import json
import os
from datetime import datetime, timezone as dt_timezone
from pathlib import Path
from zoneinfo import ZoneInfo


_DATA = Path(os.environ.get("DATA_DIR", "/app/data")) / "results.json"


def _load() -> dict:
    if not _DATA.exists():
        return {"nodes": {}}
    try:
        return json.loads(_DATA.read_text(encoding="utf-8"))
    except Exception:
        return {"nodes": {}}


def _save(data: dict) -> None:
    _DATA.parent.mkdir(parents=True, exist_ok=True)
    _DATA.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def save_quick(name: str, result: dict) -> None:
    import app.db as db
    data = _load()
    ts = datetime.utcnow().isoformat(timespec="seconds")
    entry = {
        "score":        result["total_score"],
        "grade":        result["grade"],
        "avg_ms":       result.get("avg_ms"),
        "jitter_ms":    result.get("jitter_ms"),
        "loss_pct":     result.get("loss_pct"),
        "lat_contrib":  result.get("lat_contrib"),
        "stab_contrib": result.get("stab_contrib"),
        "issues":       [r["name"] for r in result.get("url_results", []) if r["ms"] is None],
        "timestamp":    ts,
    }
    data.setdefault("nodes", {}).setdefault(name, {})["quick"] = entry
    _save(data)
    db.insert("quick", name, entry["score"], entry["grade"], ts,
              {"avg_ms": entry["avg_ms"], "jitter_ms": entry["jitter_ms"]})


def save_speed(name: str, result: dict) -> None:
    import app.db as db
    data = _load()
    ts = datetime.utcnow().isoformat(timespec="seconds")
    entry = {"dl_mbps": result["dl_mbps"], "score": result["score"],
             "grade": result["grade"], "timestamp": ts}
    data.setdefault("nodes", {}).setdefault(name, {})["speed"] = entry
    _save(data)
    db.insert("speed", name, entry["score"], entry["grade"], ts,
              {"dl_mbps": entry["dl_mbps"]})


def save_deep(name: str, result: dict) -> None:
    import app.db as db
    data = _load()
    ts = datetime.utcnow().isoformat(timespec="seconds")
    entry = {"score": result["score"], "grade": result["grade"], "timestamp": ts}
    data.setdefault("nodes", {}).setdefault(name, {})["deep"] = entry
    _save(data)
    db.insert("deep", name, entry["score"], entry["grade"], ts, {})


def save_browser(name: str, result: dict) -> None:
    import app.db as db
    data = _load()
    ts = datetime.utcnow().isoformat(timespec="seconds")
    entry = {
        "ttfb_avg":      result["ttfb_avg"],
        "media_ok_pct":  result.get("media_ok_pct"),
        "total_res_kb":  result.get("total_res_kb"),
        "score":         result["score"],
        "grade":         result["grade"],
        "tg_img_ok":     result.get("tg_img_ok", 0),
        "tg_img_total":  result.get("tg_img_total", 0),
        "tg_ttfb_ms":    result.get("tg_ttfb_ms"),
        "sample_scores": [s["score"] for s in result.get("samples", [])],
        "timestamp":     ts,
    }
    data.setdefault("nodes", {}).setdefault(name, {})["browser"] = entry
    _save(data)
    db.insert("browser", name, entry["score"], entry["grade"], ts,
              {"ttfb_avg": entry["ttfb_avg"]})


def save_video(name: str, result: dict) -> None:
    import app.db as db
    data = _load()
    ts = datetime.utcnow().isoformat(timespec="seconds")
    entry = {
        "ttb_avg":         result.get("ttb_avg"),
        "buf_secs":        result.get("buf_secs"),
        "videos_buffered": result.get("videos_buffered"),
        "total":           result.get("total"),
        "tg_ok":           result.get("tg_ok", 0),
        "tg_total":        result.get("tg_total", 0),
        "tg_ttb_avg":      result.get("tg_ttb_avg"),
        "score":           result["score"],
        "grade":           result["grade"],
        "sample_scores":   [s["score"] for s in result.get("samples", [])],
        "timestamp":       ts,
    }
    data.setdefault("nodes", {}).setdefault(name, {})["video"] = entry
    _save(data)
    db.insert("video", name, entry["score"], entry["grade"], ts, {})


def save_ws(name: str, result: dict) -> None:
    import app.db as db
    data = _load()
    ts = datetime.utcnow().isoformat(timespec="seconds")
    entry = {
        "rtt_avg":    result["rtt_avg"],
        "rtt_jitter": result.get("rtt_jitter"),
        "tput_mbps":  result.get("tput_mbps"),
        "score":      result["score"],
        "grade":      result["grade"],
        "timestamp":  ts,
    }
    data.setdefault("nodes", {}).setdefault(name, {})["ws"] = entry
    _save(data)
    db.insert("ws", name, entry["score"], entry["grade"], ts,
              {"rtt_avg": entry["rtt_avg"], "tput_mbps": entry.get("tput_mbps")})


def save_dpi(name: str, result: dict) -> None:
    import app.db as db
    data = _load()
    ts = datetime.utcnow().isoformat(timespec="seconds")
    entry = {
        "alive_count":  result.get("alive_count", 0),
        "passed_count": result.get("passed_count", 0),
        "total_hosts":  result.get("total_hosts", 0),
        "score":        result["score"],
        "grade":        result["grade"],
        "timestamp":    ts,
    }
    data.setdefault("nodes", {}).setdefault(name, {})["dpi"] = entry
    _save(data)
    db.insert("dpi", name, entry["score"], entry["grade"], ts,
              {"passed": entry["passed_count"], "alive": entry["alive_count"]})


def get_node_results(nodes: list[str]) -> dict[str, dict]:
    """Return {name: {quick, deep, speed, ws, browser, video, dpi}} — no TTL, results persist."""
    data = _load()
    out  = {}
    for name in nodes:
        nd = data.get("nodes", {}).get(name, {})
        out[name] = {
            "quick":   nd.get("quick"),
            "deep":    nd.get("deep"),
            "speed":   nd.get("speed"),
            "ws":      nd.get("ws"),
            "browser": nd.get("browser"),
            "video":   nd.get("video"),
            "dpi":     nd.get("dpi"),
        }
    return out


def time_ago(ts: str, tz_name: str = "Europe/Moscow") -> str:
    try:
        delta = datetime.utcnow() - datetime.fromisoformat(ts)
        s = int(delta.total_seconds())
        if s < 120:   return f"{s}s ago"
        if s < 3600:  return f"{s // 60}m ago"
        if s < 86400: return f"{s // 3600}h ago"
        try:
            z = ZoneInfo(tz_name)
            local_dt = datetime.fromisoformat(ts).replace(tzinfo=dt_timezone.utc).astimezone(z)
            return local_dt.strftime("%-d %b %H:%M")
        except Exception:
            return f"{s // 86400}d ago"
    except Exception:
        return "?"
