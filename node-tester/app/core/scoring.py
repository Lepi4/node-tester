def latency_score(ms: int | None) -> int:
    """Returns 0-10 raw score. Multiplied by URL weight in latency.py."""
    if ms is None: return 0
    if ms < 50:    return 10
    if ms < 75:    return 9
    if ms < 100:   return 8
    if ms < 150:   return 7
    if ms < 250:   return 6
    if ms < 350:   return 5
    if ms < 500:   return 4
    if ms < 750:   return 3
    if ms < 1000:  return 2
    return 1


def jitter_score(ms: int | None) -> int:
    """True jitter (5+ sequential pings, trimmed). Returns 0-5.
    Jitter matters less than loss for TCP — capped at 5pts."""
    if ms is None: return 0
    if ms < 10:  return 5
    if ms < 20:  return 4
    if ms < 40:  return 3
    if ms < 80:  return 2
    if ms < 150: return 1
    return 0


def packet_loss_score(pct: float) -> int:
    """Stability pings loss. Returns 0-15.
    Packet loss is more damaging to TCP than jitter — weighted higher."""
    if pct >= 100: return 0
    if pct == 0:   return 15
    if pct < 5:    return 12   # near-perfect
    if pct < 15:   return 7    # 1/7 pings — minor blip
    if pct < 25:   return 3    # 2/7 pings — occasional drops
    if pct < 40:   return 1    # 3/7 pings — unreliable
    return 0


def speed_score(mbps: float | None) -> int:
    """Download speed score. Returns 0-15."""
    if mbps is None: return 0
    if mbps >= 50:  return 15
    if mbps >= 35:  return 12
    if mbps >= 25:  return 9
    if mbps >= 12:  return 6
    if mbps >= 1:   return 3
    return 0


def speed_grade(mbps: float | None) -> str:
    if mbps is None:  return "F"
    if mbps >= 50:    return "S"
    if mbps >= 35:    return "A"
    if mbps >= 25:    return "B"
    if mbps >= 12:    return "C"
    if mbps >= 1:     return "D"
    return "F"


def ws_score(rtt_avg: float | None, success_pct: float, tput_mbps: float | None = None) -> int:
    """WebSocket combined score 0-10: latency 0-5, stability 0-3, throughput 0-2."""
    if rtt_avg is None and success_pct == 0:
        return 0
    if rtt_avg is None:   lat = 0
    elif rtt_avg < 50:    lat = 5
    elif rtt_avg < 100:   lat = 4
    elif rtt_avg < 200:   lat = 3
    elif rtt_avg < 400:   lat = 2
    elif rtt_avg < 800:   lat = 1
    else:                 lat = 0
    if success_pct >= 100:  stab = 3
    elif success_pct >= 90: stab = 2
    elif success_pct >= 70: stab = 1
    else:                   stab = 0
    if tput_mbps is None:   tput = 0
    elif tput_mbps >= 20:   tput = 2
    elif tput_mbps >= 5:    tput = 1
    else:                   tput = 0
    return lat + stab + tput


def ws_grade(score: int) -> str:
    if score >= 9: return "S"
    if score >= 7: return "A"
    if score >= 5: return "B"
    if score >= 3: return "C"
    if score >= 1: return "D"
    return "F"


def browser_score(
    ttfb_avg: float | None,
    success_pct: float,
    media_ok_pct: float = 0,
    video_mbps: float | None = None,
    tg_ttfb_ms: float | None = None,
) -> int:
    """Browser score 0-10: TTFB 0-4, page success 0-1, media 0-2, video 0-3, TG speed 0-1."""
    if ttfb_avg is None and success_pct == 0:
        return 0
    # TTFB — page response speed
    if ttfb_avg is None:      tfb = 0
    elif ttfb_avg < 150:      tfb = 4
    elif ttfb_avg < 280:      tfb = 3
    elif ttfb_avg < 450:      tfb = 2
    elif ttfb_avg < 800:      tfb = 1
    else:                     tfb = 0
    # Page HTML success
    page = 1 if success_pct >= 50 else 0
    # Media + image loading
    if media_ok_pct >= 100:   media = 2
    elif media_ok_pct >= 50:  media = 1
    else:                     media = 0
    # Video streaming quality (YouTube CDN)
    if video_mbps is None:    video = 0
    elif video_mbps >= 8:     video = 3   # 1080p60+
    elif video_mbps >= 5:     video = 2   # 1080p
    elif video_mbps >= 2.5:   video = 1   # 480p usable
    else:                     video = 0   # buffering
    # TG CDN speed bonus (0-1): only awarded when TG images actually loaded
    if tg_ttfb_ms is None:    tg = 0
    elif tg_ttfb_ms < 600:    tg = 1   # fast TG CDN routing
    else:                     tg = 0
    return min(10, tfb + page + media + video + tg)


def video_quality_label(mbps: float | None) -> str:
    """Human-readable YouTube quality tier."""
    if mbps is None:       return "—"
    if mbps >= 25:         return "4K"
    if mbps >= 8:          return "1080p60"
    if mbps >= 5:          return "1080p"
    if mbps >= 2.5:        return "480p"
    if mbps >= 1:          return "360p"
    return "buffering"


def browser_grade(score: int) -> str:
    if score >= 9: return "S"
    if score >= 7: return "A"
    if score >= 5: return "B"
    if score >= 3: return "C"
    if score >= 1: return "D"
    return "F"


def video_score(
    ttb_ms: float | None,
    videos_buffered: int,
    total: int,
    tg_ttb_ms: float | None = None,
    tg_ok: int = 0,
    tg_total: int = 0,
    buf_secs: float = 0,
) -> int:
    """Video streaming score 0-10.

    No TG (tg_total=0): buf_secs 0-7 + DASH TTB 0-3 = max 10
    TG configured but all failed: capped at 4 (hard penalty)
    TG ok: DASH 0-5 + TG 0-5 = max 10; granular TTB thresholds ensure spread
    """
    buf_frac = videos_buffered / total if total else 0

    # ── TG configured but all downloads failed: hard cap ──────────────────────
    if tg_total > 0 and tg_ok == 0:
        if buf_secs >= 30 and ttb_ms is not None and ttb_ms < 5_000:
            return 4
        elif buf_secs >= 30:
            return 3
        elif buf_secs >= 20:
            return 2
        else:
            return 1 if buf_frac >= 1.0 else 0

    # ── With TG ok: DASH (0-5) + TG (0-5) ────────────────────────────────────
    if tg_total > 0 and tg_ok > 0:
        # DASH component (0-5): buf_secs + TTB combined
        if buf_secs >= 30:
            if ttb_ms is None:          dash = 2
            elif ttb_ms < 2_500:        dash = 5   # mieru: 2008ms
            elif ttb_ms < 4_000:        dash = 4   # most nodes: ~3000ms
            elif ttb_ms < 6_000:        dash = 3   # shadowtls: 4030ms
            else:                       dash = 2
        elif buf_secs >= 20:
            dash = 3
        elif buf_frac >= 1.0:
            dash = 1
        else:
            dash = 0

        # TG component (0-5): granular TTB thresholds
        tg_frac = tg_ok / tg_total
        if tg_frac >= 1.0:
            if tg_ttb_ms is None:              tg_comp = 2
            elif tg_ttb_ms < 600:              tg_comp = 5   # ≤600ms: excellent
            elif tg_ttb_ms < 800:              tg_comp = 4
            elif tg_ttb_ms < 1_500:            tg_comp = 3
            elif tg_ttb_ms < 3_000:            tg_comp = 2
            else:                              tg_comp = 1
        elif tg_frac >= 0.5:
            tg_comp = 1
        else:
            tg_comp = 0

        return min(10, dash + tg_comp)

    # ── No TG configured: DASH-only scoring ───────────────────────────────────
    if buf_secs >= 30:     buf2 = 7
    elif buf_secs >= 20:   buf2 = 6
    elif buf_secs >= 12:   buf2 = 5
    elif buf_secs >= 6:    buf2 = 3
    elif buf_frac >= 1.0:  buf2 = 1
    else:                  buf2 = 0
    if ttb_ms is None:      spd2 = 0
    elif ttb_ms < 1_500:    spd2 = 3
    elif ttb_ms < 3_000:    spd2 = 2
    elif ttb_ms < 5_000:    spd2 = 1
    else:                   spd2 = 0
    return min(10, buf2 + spd2)


def video_grade(score: int) -> str:
    if score >= 9: return "S"
    if score >= 7: return "A"
    if score >= 5: return "B"
    if score >= 3: return "C"
    if score >= 1: return "D"
    return "F"


def dpi_score(passed: int, alive: int) -> int:
    """DPI bypass score 0-10: fraction of alive hosts that passed 64 KB POST."""
    if alive == 0:
        return 0
    pct = passed / alive
    if pct >= 1.0:  return 10
    if pct >= 0.9:  return 9
    if pct >= 0.8:  return 8
    if pct >= 0.7:  return 7
    if pct >= 0.6:  return 6
    if pct >= 0.5:  return 5
    if pct >= 0.4:  return 4
    if pct >= 0.3:  return 3
    if pct >= 0.1:  return 1
    return 0


def dpi_grade(score: int) -> str:
    if score >= 9: return "S"
    if score >= 7: return "A"
    if score >= 5: return "B"
    if score >= 3: return "C"
    if score >= 1: return "D"
    return "F"


def combined_browser_score(browser_s: int, video_s: int, httpx_pct: int = 60) -> int:
    """Merge httpx browser + Playwright video into 0-10 using configurable split.
    httpx_pct = percentage for httpx test; video gets (100 - httpx_pct)%."""
    video_pct = 100 - httpx_pct
    return min(10, round(browser_s * httpx_pct / 100 + video_s * video_pct / 100))


def deep_score(
    quick_s: int, browser_s: int, speed_s: int, ws_s: int, dpi_s: int = 0,
    w_quick: int = 30, w_browser: int = 20, w_speed: int = 15, w_ws: int = 10, w_dpi: int = 25,
) -> int:
    """Combine module scores into 0-100 deep score with configurable weights.
    Each score is normalised to the module's max before applying its weight:
      quick max=100, browser max=10, speed max=15, ws max=10, dpi max=10."""
    q = (quick_s   or 0) * (w_quick   / 100)
    b = (browser_s or 0) * (w_browser / 10)
    s = (speed_s   or 0) * (w_speed   / 15)
    w = (ws_s      or 0) * (w_ws      / 10)
    d = (dpi_s     or 0) * (w_dpi     / 10)
    return min(100, round(q + b + s + w + d))


def deep_grade(score: int) -> str:
    if score >= 90: return "S"
    if score >= 75: return "A"
    if score >= 60: return "B"
    if score >= 45: return "C"
    if score >= 30: return "D"
    return "F"


def grade(score: int) -> str:
    if score >= 90: return "S"
    if score >= 75: return "A"
    if score >= 60: return "B"
    if score >= 45: return "C"
    if score >= 30: return "D"
    return "F"
