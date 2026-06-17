import asyncio
from urllib.parse import quote

import httpx

BUILT_IN    = {"DIRECT", "REJECT", "REJECT-DROP", "PASS", "COMPATIBLE"}
GROUP_TYPES = {"Selector", "URLTest", "Fallback", "LoadBalance", "Relay"}

_PING_URLS        = ["https://gstatic.com/generate_204", "https://www.google.com"]
_PING_TIMEOUT_MS  = 4000
_PING_HTTP_TIMEOUT = 7.0
_PING_SEMAPHORE   = 8


def _url(host: str, port: int) -> str:
    host = host.removeprefix("https://").removeprefix("http://").strip("/")
    return f"http://{host}:{port}"


def _headers(secret: str) -> dict:
    return {"Authorization": f"Bearer {secret}"} if secret else {}


async def get_version(host: str, port: int, secret: str) -> dict:
    async with httpx.AsyncClient(timeout=5.0) as c:
        r = await c.get(f"{_url(host, port)}/version", headers=_headers(secret))
        r.raise_for_status()
        return r.json()


async def get_selector_groups(host: str, port: int, secret: str) -> list[str]:
    async with httpx.AsyncClient(timeout=5.0) as c:
        r = await c.get(f"{_url(host, port)}/proxies", headers=_headers(secret))
        r.raise_for_status()
        proxies = r.json().get("proxies", {})
    return sorted(name for name, info in proxies.items() if info.get("type") == "Selector")


async def get_nodes_in_group(host: str, port: int, secret: str, group: str) -> list[str]:
    async with httpx.AsyncClient(timeout=8.0) as c:
        r = await c.get(f"{_url(host, port)}/proxies/{group}", headers=_headers(secret))
        r.raise_for_status()
        data = r.json()
        rp = await c.get(f"{_url(host, port)}/proxies", headers=_headers(secret))
        rp.raise_for_status()
        all_proxies = rp.json().get("proxies", {})

    return [
        n for n in data.get("all", [])
        if n not in BUILT_IN
        and all_proxies.get(n, {}).get("type") not in GROUP_TYPES
    ]


async def classify_nodes(
    host: str, port: int, secret: str, nodes: list[str]
) -> tuple[list[str], list[str], list[str]]:
    """Classify nodes into (confirmed_active, confirmed_dead, uncertain).

    confirmed_active  — Mihomo URLTest says alive=True  → include, no ping needed
    confirmed_dead    — Mihomo URLTest says alive=False AND no recent history
    uncertain         — no URLTest data (alive=None) or alive=False but had history
                        → need a ping to decide
    """
    async with httpx.AsyncClient(timeout=8.0) as c:
        r = await c.get(f"{_url(host, port)}/proxies", headers=_headers(secret))
        r.raise_for_status()
        all_proxies = r.json().get("proxies", {})

    confirmed_active: list[str] = []
    confirmed_dead:   list[str] = []
    uncertain:        list[str] = []

    for node in nodes:
        info    = all_proxies.get(node, {})
        alive   = info.get("alive")           # True / False / None
        history = info.get("history", [])
        last_ok = any(h.get("delay", 0) > 0 for h in history[-3:]) if history else False

        if alive is True:
            confirmed_active.append(node)
        elif alive is False and not last_ok:
            confirmed_dead.append(node)
        else:
            # alive=False but had recent success, or alive=None (no URLTest) → ping
            uncertain.append(node)

    return confirmed_active, confirmed_dead, uncertain


async def ping_filter_nodes(
    host: str, port: int, secret: str, nodes: list[str]
) -> tuple[list[str], list[str], dict[str, str]]:
    """Ping-based check for uncertain nodes: try 2 URLs, active if either responds."""
    if not nodes:
        return [], [], {}

    base = _url(host, port)
    hdrs = _headers(secret)
    sem  = asyncio.Semaphore(_PING_SEMAPHORE)

    async def ping_one(node: str, client: httpx.AsyncClient) -> tuple[str, bool, str]:
        async with sem:
            endpoint = f"{base}/proxies/{quote(node, safe='')}/delay"
            errors = []
            for url in _PING_URLS:
                try:
                    r = await client.get(
                        endpoint, headers=hdrs,
                        params={"timeout": _PING_TIMEOUT_MS, "url": url},
                    )
                    if r.status_code == 200:
                        delay = r.json().get("delay", 0)
                        if delay > 0:
                            return node, True, f"{delay}ms"
                        errors.append(f"{url} → delay=0")
                    else:
                        errors.append(f"{url} → HTTP {r.status_code}")
                except httpx.TimeoutException:
                    errors.append(f"{url} → timeout")
                except Exception as e:
                    errors.append(f"{url} → {e}")
            return node, False, "; ".join(errors)

    async with httpx.AsyncClient(timeout=_PING_HTTP_TIMEOUT) as c:
        results = await asyncio.gather(*[ping_one(n, c) for n in nodes])

    active  = [n for n, ok, _ in results if ok]
    dead    = [n for n, ok, _ in results if not ok]
    reasons = {n: r for n, ok, r in results if not ok}
    return active, dead, reasons


async def get_selector_now(host: str, port: int, secret: str, group: str) -> str | None:
    """Return the direct 'now' selection of a group (may be a sub-group, not a leaf)."""
    async with httpx.AsyncClient(timeout=5.0) as c:
        r = await c.get(f"{_url(host, port)}/proxies/{group}", headers=_headers(secret))
        r.raise_for_status()
        return r.json().get("now")


async def set_proxy(host: str, port: int, secret: str, group: str, node: str) -> None:
    """Switch a Selector group to a specific node."""
    async with httpx.AsyncClient(timeout=5.0) as c:
        r = await c.put(
            f"{_url(host, port)}/proxies/{group}",
            headers=_headers(secret),
            json={"name": node},
        )
        r.raise_for_status()


async def get_active_leaf_node(host: str, port: int, secret: str, group: str) -> str | None:
    """Resolve the actually-used node by following nested groups (Selector→Fallback→node)."""
    async with httpx.AsyncClient(timeout=5.0) as c:
        r = await c.get(f"{_url(host, port)}/proxies", headers=_headers(secret))
        r.raise_for_status()
        all_proxies = r.json().get("proxies", {})

    current = all_proxies.get(group, {}).get("now")
    for _ in range(8):
        if not current:
            return None
        info = all_proxies.get(current, {})
        if info.get("type") not in GROUP_TYPES:
            return current          # real proxy node
        nxt = info.get("now")
        if not nxt or nxt == current:
            return current          # can't go deeper
        current = nxt
    return current


async def flush_fakeip(host: str, port: int, secret: str) -> None:
    async with httpx.AsyncClient(timeout=5.0) as c:
        r = await c.delete(f"{_url(host, port)}/cache/fakeip", headers=_headers(secret))
        r.raise_for_status()


async def flush_dns(host: str, port: int, secret: str) -> None:
    async with httpx.AsyncClient(timeout=5.0) as c:
        r = await c.delete(f"{_url(host, port)}/dns/cache", headers=_headers(secret))
        r.raise_for_status()


async def restart_core(host: str, port: int, secret: str) -> None:
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.post(f"{_url(host, port)}/restart", headers=_headers(secret))
        r.raise_for_status()


async def update_geo(host: str, port: int, secret: str) -> None:
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(f"{_url(host, port)}/upgrade/geo", headers=_headers(secret))
        r.raise_for_status()


async def split_active_nodes(
    host: str, port: int, secret: str, group: str, nodes: list[str]
) -> tuple[list[str], list[str]]:
    """For dashboard display only — uses Mihomo URLTest state, no pings."""
    confirmed_active, confirmed_dead, uncertain = await classify_nodes(
        host, port, secret, nodes
    )
    # For display: uncertain nodes are shown as active (benefit of the doubt)
    return confirmed_active + uncertain, confirmed_dead
