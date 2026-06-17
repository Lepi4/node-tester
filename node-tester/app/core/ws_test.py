import asyncio
import base64
import logging
import os
import ssl
import struct
import time

import app.mihomo as mihomo
from app.core import scoring

log = logging.getLogger("node_tester.ws")

WS_SERVERS = [
    ("echo.websocket.org",    443, "/", True),
    ("echo.websocket.events", 443, "/", True),
]
PING_COUNT       = 10
PING_PAYLOAD_PFX = "nt-ping#"
PING_GAP_S       = 0.2
THROUGHPUT_BYTES = 50_000   # 50 KB round-trip
BETWEEN_RUNS_S   = 3.0


def _proxy_parts(cfg: dict) -> tuple[str, int, str, str]:
    host = cfg["mihomo_host"].removeprefix("https://").removeprefix("http://").strip("/")
    return host, cfg["mixed_port"], cfg.get("proxy_user", ""), cfg.get("proxy_pass", "")


# ─── Frame codec ─────────────────────────────────────────────────────────────

def _frame(opcode: int, payload: bytes | str) -> bytes:
    """Build a masked client→server WebSocket frame."""
    if isinstance(payload, str):
        payload = payload.encode()
    mask   = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    n      = len(payload)
    hdr    = bytes([0x80 | opcode])
    if n < 126:
        hdr += bytes([0x80 | n])
    elif n < 65536:
        hdr += struct.pack(">BH", 0x80 | 126, n)
    else:
        hdr += struct.pack(">BQ", 0x80 | 127, n)
    return hdr + mask + masked


async def _read_frame(reader: asyncio.StreamReader, timeout: float = 10.0) -> tuple[int, bytes]:
    hdr       = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
    opcode    = hdr[0] & 0x0F
    is_masked = bool(hdr[1] & 0x80)
    n         = hdr[1] & 0x7F
    if n == 126:
        n = struct.unpack(">H", await asyncio.wait_for(reader.readexactly(2), timeout=timeout))[0]
    elif n == 127:
        n = struct.unpack(">Q", await asyncio.wait_for(reader.readexactly(8), timeout=timeout))[0]
    mask_key = b""
    if is_masked:
        mask_key = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
    data = await asyncio.wait_for(reader.readexactly(n), timeout=timeout)
    if is_masked:
        data = bytes(b ^ mask_key[i % 4] for i, b in enumerate(data))
    return opcode, data


# ─── Connection ───────────────────────────────────────────────────────────────

async def _connect_ws(
    proxy_host: str, proxy_port: int, proxy_user: str, proxy_pass: str,
    target_host: str, target_port: int, path: str, use_ssl: bool,
):
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(proxy_host, proxy_port), timeout=15
    )
    auth_hdr = ""
    if proxy_user:
        creds = base64.b64encode(f"{proxy_user}:{proxy_pass}".encode()).decode()
        auth_hdr = f"Proxy-Authorization: Basic {creds}\r\n"

    writer.write(
        f"CONNECT {target_host}:{target_port} HTTP/1.1\r\n"
        f"Host: {target_host}:{target_port}\r\n"
        f"{auth_hdr}\r\n".encode()
    )
    await writer.drain()
    resp = b""
    while b"\r\n\r\n" not in resp:
        resp += await asyncio.wait_for(reader.read(4096), timeout=10)
    if b"200" not in resp.split(b"\r\n")[0]:
        writer.close()
        raise ConnectionError(f"CONNECT rejected: {resp[:80].decode(errors='replace')}")

    if use_ssl:
        ssl_ctx = ssl.create_default_context()
        await writer.start_tls(ssl_ctx, server_hostname=target_host)

    key = base64.b64encode(os.urandom(16)).decode()
    writer.write(
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {target_host}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"\r\n".encode()
    )
    await writer.drain()
    resp2 = b""
    while b"\r\n\r\n" not in resp2:
        resp2 += await asyncio.wait_for(reader.read(4096), timeout=10)
    if b"101" not in resp2.split(b"\r\n")[0]:
        writer.close()
        raise ConnectionError(f"WS upgrade failed: {resp2[:80].decode(errors='replace')}")

    return reader, writer


# ─── Single run ──────────────────────────────────────────────────────────────

async def _run_once(
    proxy_host: str, proxy_port: int, proxy_user: str, proxy_pass: str,
    emit, node: str, run_idx: int,
) -> dict:
    reader = writer = None
    connect_ms   = None
    server_label = None

    for target_host, target_port, path, use_ssl in WS_SERVERS:
        try:
            t0 = time.monotonic()
            log.debug("[ws] node=%s run=%d connecting %s:%d", node, run_idx, target_host, target_port)
            reader, writer = await _connect_ws(
                proxy_host, proxy_port, proxy_user, proxy_pass,
                target_host, target_port, path, use_ssl,
            )
            connect_ms   = round((time.monotonic() - t0) * 1000)
            server_label = target_host
            log.debug("[ws] node=%s run=%d connected in %dms", node, run_idx, connect_ms)
            await emit({"type": "step", "phase": "ws_connected", "node": node, "run": run_idx,
                        "server": server_label, "connect_ms": connect_ms})
            break
        except Exception as e:
            log.debug("[ws] node=%s run=%d connect failed (%.1fs): %s",
                      node, run_idx, time.monotonic() - t0, e)
            await emit({"type": "step", "phase": "ws_conn_fail", "node": node, "run": run_idx,
                        "server": target_host, "error": str(e)[:80]})

    if reader is None:
        return {"rtt_avg": None, "rtt_jitter": None, "success_pct": 0.0,
                "tput_mbps": None, "connect_ms": None, "rtts": [], "server": None}

    # ── Latency phase (10 ping-pong) ─────────────────────────────────────────
    rtts  = []
    fails = 0

    for i in range(PING_COUNT):
        token = f"{PING_PAYLOAD_PFX}{i}"
        try:
            t0 = time.monotonic()
            writer.write(_frame(0x01, token))
            await writer.drain()
            deadline = time.monotonic() + 5.0
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise asyncio.TimeoutError
                opcode, payload = await _read_frame(reader, timeout=remaining)
                if opcode == 0x08:
                    raise ConnectionError("server closed WS")
                if opcode == 0x09:            # server ping → reply pong
                    writer.write(_frame(0x0A, payload))
                    await writer.drain()
                    continue
                if opcode in (0x01, 0x02) and token.encode() in payload:
                    break
            rtt = (time.monotonic() - t0) * 1000
            rtts.append(rtt)
            await emit({"type": "step", "phase": "ws_ping", "node": node, "run": run_idx,
                        "i": i + 1, "total": PING_COUNT, "rtt_ms": round(rtt, 1)})
        except Exception as e:
            fails += 1
            await emit({"type": "step", "phase": "ws_ping", "node": node, "run": run_idx,
                        "i": i + 1, "total": PING_COUNT, "rtt_ms": None, "error": str(e)[:50]})
        await asyncio.sleep(PING_GAP_S)

    # ── Throughput phase (50 KB round-trip) ──────────────────────────────────
    tput_mbps = None
    try:
        data = os.urandom(THROUGHPUT_BYTES)
        t0 = time.monotonic()
        writer.write(_frame(0x02, data))
        await writer.drain()
        received = 0
        while received < THROUGHPUT_BYTES:
            opcode, payload = await _read_frame(reader, timeout=20.0)
            if opcode in (0x01, 0x02):
                received += len(payload)
            elif opcode == 0x08:
                break
        elapsed = time.monotonic() - t0
        if elapsed > 0 and received >= THROUGHPUT_BYTES * 0.9:
            tput_mbps = round(THROUGHPUT_BYTES * 2 * 8 / elapsed / 1_000_000, 2)
        await emit({"type": "step", "phase": "ws_throughput", "node": node, "run": run_idx,
                    "mbps": tput_mbps, "received": received})
    except Exception as e:
        await emit({"type": "step", "phase": "ws_throughput", "node": node, "run": run_idx,
                    "mbps": None, "error": str(e)[:50]})

    # ── Close ─────────────────────────────────────────────────────────────────
    try:
        writer.write(_frame(0x08, b"\x03\xe8"))
        await writer.drain()
        writer.close()
    except Exception:
        pass

    rtt_avg    = round(sum(rtts) / len(rtts), 1) if rtts else None
    rtt_jitter = round(max(rtts) - min(rtts), 1) if len(rtts) >= 2 else None

    return {
        "rtt_avg":    rtt_avg,
        "rtt_jitter": rtt_jitter,
        "success_pct": round((PING_COUNT - fails) / PING_COUNT * 100, 1),
        "tput_mbps":  tput_mbps,
        "connect_ms": connect_ms,
        "rtts":       [round(r, 1) for r in rtts],
        "server":     server_label,
    }


# ─── Public API ──────────────────────────────────────────────────────────────

async def test_node(node: str, cfg: dict, log_q=None) -> dict:
    """Test WebSocket quality through proxy. Runs twice, takes best result."""
    proxy_host, proxy_port, proxy_user, proxy_pass = _proxy_parts(cfg)
    host, port, secret, group = (
        cfg["mihomo_host"], cfg["mihomo_port"],
        cfg["mihomo_secret"], cfg["proxy_group"],
    )

    async def emit(ev: dict):
        if log_q:
            await log_q.put(ev)

    original = await mihomo.get_selector_now(host, port, secret, group)
    log.debug("[ws] node=%s original=%s", node, original)
    samples: list[dict] = []

    async def _do_run(run_num: int) -> dict:
        t_run = time.monotonic()
        log.debug("[ws] node=%s run=%d start", node, run_num)
        r  = await _run_once(proxy_host, proxy_port, proxy_user, proxy_pass, emit, node, run_num)
        sc = scoring.ws_score(r["rtt_avg"], r["success_pct"], r["tput_mbps"])
        gr = scoring.ws_grade(sc)
        log.debug("[ws] node=%s run=%d done (%.1fs) rtt=%s tput=%s score=%s",
                  node, run_num, time.monotonic() - t_run, r["rtt_avg"], r["tput_mbps"], sc)
        await emit({"type": "step", "phase": "ws_run_done", "node": node, "run": run_num,
                    "rtt_avg": r["rtt_avg"], "tput_mbps": r["tput_mbps"],
                    "success_pct": r["success_pct"], "score": sc, "grade": gr})
        return {**r, "score": sc, "grade": gr}

    try:
        await mihomo.set_proxy(host, port, secret, group, node)
        await asyncio.sleep(0.5)
        await emit({"type": "step", "phase": "ws_start", "node": node})

        samples.append(await _do_run(1))
        await asyncio.sleep(BETWEEN_RUNS_S)
        samples.append(await _do_run(2))

        # 3rd run only if EXACTLY one of the two failed
        failed_count = sum(1 for s in samples if s["score"] == 0)
        if failed_count == 1:
            await asyncio.sleep(BETWEEN_RUNS_S)
            log.debug("[ws] node=%s run=3 (retry: one of two runs failed)", node)
            samples.append(await _do_run(3))
        elif failed_count == 2:
            log.debug("[ws] node=%s both runs failed — no 3rd run", node)
    finally:
        if original:
            try:
                await mihomo.set_proxy(host, port, secret, group, original)
            except Exception:
                pass

    # Build final pair: if 3rd run was done, use [successful, run3]; else both runs
    if len(samples) == 3:
        good = [s for s in samples[:2] if s["score"] > 0]
        final_pair = good + [samples[2]]
    else:
        final_pair = samples

    def _avg_nn(vals: list) -> float | None:
        v = [x for x in vals if x is not None]
        return round(sum(v) / len(v), 1) if v else None

    n = len(final_pair) or 1
    avg_score   = round(sum(s["score"] for s in final_pair) / n, 1)
    grade       = scoring.ws_grade(round(avg_score))
    rtt_avg     = _avg_nn([s["rtt_avg"]    for s in final_pair])
    rtt_jitter  = _avg_nn([s["rtt_jitter"] for s in final_pair])
    connect_ms  = _avg_nn([s["connect_ms"] for s in final_pair])
    tput_mbps   = _avg_nn([s["tput_mbps"]  for s in final_pair])
    success_pct = round(sum(s["success_pct"] for s in final_pair) / n, 1)
    server      = next((s["server"] for s in final_pair if s["server"]), None)

    return {
        "name":        node,
        "rtt_avg":     rtt_avg,
        "rtt_jitter":  rtt_jitter,
        "connect_ms":  connect_ms,
        "success_pct": success_pct,
        "tput_mbps":   tput_mbps,
        "score":       avg_score,
        "grade":       grade,
        "server":      server,
        "samples":     [
            {"rtt_avg": s["rtt_avg"], "tput_mbps": s["tput_mbps"],
             "success_pct": s["success_pct"], "score": s["score"]}
            for s in samples
        ],
    }
