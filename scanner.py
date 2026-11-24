#!/usr/bin/env python3
"""PUMP / DUMP scanner for OKX and Gate.io USDT perpetual contracts.

Detects strong, confirmed moves after a preceding consolidation/distribution range.
No false-breakout, support/resistance, order-block or BOS logic is included.
"""
from __future__ import annotations

import json
import math
import os
import struct
import time
import zlib
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter

# ============================== CONFIG ==============================
OKX = "https://www.okx.com"
GATE = "https://api.gateio.ws/api/v4"
STATE_FILE = Path("pump_dump_state.json")

TOP_UNIVERSE = int(os.getenv("TOP_UNIVERSE", "500"))
UNIVERSE_CANDIDATES = int(os.getenv("UNIVERSE_CANDIDATES", "1000"))
MAX_MONITORED = int(os.getenv("MAX_MONITORED", "500"))
UNIVERSE_LOOKBACK_5M = 36
SCAN_WORKERS = max(1, min(int(os.getenv("SCAN_WORKERS", "5")), 10))
HTTP_RETRIES = 3
HTTP_TIMEOUT = 25
REQUEST_INTERVAL = 0.06

# Required move from the prior range boundary to the latest confirmed close.
PUMP_DUMP_THRESHOLDS = {"4H": 25.0, "1D": 50.0}
# Consolidation search windows. The base must be immediately before the impulse.
BASE_WINDOWS = {"4H": range(12, 85, 4), "1D": range(5, 26, 2)}
MAX_IMPULSE_BARS = {"4H": 24, "1D": 4}
MAX_BASE_RANGE_PCT = {"4H": 22.0, "1D": 35.0}
CANDLE_LIMIT = {"4H": 180, "1D": 120}
CHART_VISIBLE = {"4H": 72, "1D": 90}
EXCLUDE_BASES = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "GBP", "AEUR", "BFUSD", "USDE", "USD0", "PYUSD"}

ACTIVE_EXCHANGE = "okx"
_thread_local = threading.local()
_request_lock = threading.Lock()
_last_request_time = 0.0

# ============================== HTTP ===============================
def get_session():
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": "PUMP-DUMP-SCANNER/1.0", "Accept": "application/json"})
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=0)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _thread_local.session = session
    return session


def get_json(url: str, params: dict | None = None, timeout: int = HTTP_TIMEOUT):
    global _last_request_time
    last_error = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            with _request_lock:
                wait = REQUEST_INTERVAL - (time.monotonic() - _last_request_time)
                if wait > 0:
                    time.sleep(wait)
                _last_request_time = time.monotonic()
            response = get_session().get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < HTTP_RETRIES:
                time.sleep(min(attempt, 3))
    raise RuntimeError(f"HTTP failed after {HTTP_RETRIES} attempts: {last_error}")

# ============================== SYMBOLS ============================
def base_of(symbol: str) -> str:
    return symbol[:-4] if symbol.endswith("USDT") else symbol


def okx_symbol(inst_id: str):
    return inst_id[:-10] + "USDT" if inst_id.endswith("-USDT-SWAP") else None


def gate_symbol(name: str):
    return name.replace("_", "") if name.endswith("_USDT") else None


def okx_universe():
    instruments = get_json(OKX + "/api/v5/public/instruments", {"instType": "SWAP"})
    if instruments.get("code") != "0":
        raise RuntimeError("OKX instruments: " + str(instruments.get("msg")))
    supported = {}
    for item in instruments.get("data", []):
        inst = item.get("instId", "")
        sym = okx_symbol(inst)
        if item.get("state") == "live" and item.get("settleCcy") == "USDT" and sym and base_of(sym) not in EXCLUDE_BASES:
            supported[sym] = inst
    tickers = get_json(OKX + "/api/v5/market/tickers", {"instType": "SWAP"})
    if tickers.get("code") != "0":
        raise RuntimeError("OKX tickers: " + str(tickers.get("msg")))
    items = []
    for ticker in tickers.get("data", []):
        sym = okx_symbol(ticker.get("instId", ""))
        if not sym or sym not in supported:
            continue
        try:
            last = float(ticker.get("last") or 0)
            turnover = float(ticker.get("volCcy24h") or 0) * last
            open_24h = float(ticker.get("open24h") or 0)
            change = (last / open_24h - 1) * 100 if last > 0 and open_24h > 0 else 0.0
        except (TypeError, ValueError):
            continue
        if last > 0 and turnover > 0:
            items.append((sym, turnover, change))
    items.sort(key=lambda x: x[1], reverse=True)
    return items[:UNIVERSE_CANDIDATES]


def gate_universe():
    contracts = get_json(GATE + "/futures/usdt/contracts")
    supported = {gate_symbol(x.get("name", "")) for x in contracts if x.get("in_delisting") is False}
    tickers = get_json(GATE + "/futures/usdt/tickers")
    items = []
    for ticker in tickers:
        sym = gate_symbol(ticker.get("contract", ""))
        if not sym or sym not in supported or base_of(sym) in EXCLUDE_BASES:
            continue
        try:
            turnover = float(ticker.get("volume_24h_quote") or 0)
            change = float(ticker.get("change_percentage") or 0)
        except (TypeError, ValueError):
            continue
        if turnover > 0:
            items.append((sym, turnover, change))
    items.sort(key=lambda x: x[1], reverse=True)
    return items[:UNIVERSE_CANDIDATES]

# ============================== CANDLES =============================
def okx_rows(inst_id: str, bar: str, limit: int):
    rows_by_ts = {}
    cursor = None
    while len(rows_by_ts) < limit:
        count = min(300, limit - len(rows_by_ts))
        params = {"instId": inst_id, "bar": bar, "limit": str(count)}
        if cursor is not None:
            params["after"] = cursor
        data = get_json(OKX + "/api/v5/market/history-candles", params)
        if data.get("code") != "0":
            raise RuntimeError(f"OKX candles {inst_id}: {data.get('msg')}")
        batch = data.get("data", [])
        if not batch:
            break
        for row in batch:
            rows_by_ts[int(row[0])] = row
        oldest = min(int(row[0]) for row in batch)
        if cursor == str(oldest) or len(batch) < 2:
            break
        cursor = str(oldest)
        if len(rows_by_ts) >= limit:
            break
    return [rows_by_ts[k] for k in sorted(rows_by_ts)][-limit:]


def gate_rows(contract: str, interval: str, limit: int):
    rows = get_json(GATE + "/futures/usdt/candlesticks", {"contract": contract, "interval": interval, "limit": min(limit, 2000)})
    return rows[-limit:]


def normalise_rows(rows, source):
    out = []
    for row in rows:
        if source == "okx":
            ts, op, hi, lo, cl, vol = row[:6]
            ts = int(ts) // 1000
        else:
            ts, op, hi, lo, cl, vol = row["t"], row["o"], row["h"], row["l"], row["c"], row.get("sum", row.get("v", 0))
            ts = int(ts)
        out.append([ts, float(op), float(hi), float(lo), float(cl), float(vol)])
    return sorted(out, key=lambda x: x[0])


def get_bars(exchange, symbol, timeframe, limit):
    if exchange == "okx":
        inst = symbol.replace("USDT", "-USDT-SWAP")
        bar = {"5m": "5m", "4H": "4H", "1D": "1D"}[timeframe]
        return normalise_rows(okx_rows(inst, bar, limit), "okx")
    contract = base_of(symbol) + "_USDT"
    interval = {"5m": "5m", "4H": "4h", "1D": "1d"}[timeframe]
    return normalise_rows(gate_rows(contract, interval, limit), "gate")

# ============================== UNIVERSE RANK ======================
def activity_score(item):
    sym, turnover, change_24h = item
    try:
        rows = get_bars(ACTIVE_EXCHANGE, sym, "5m", UNIVERSE_LOOKBACK_5M)
        if len(rows) >= 12:
            first, last = rows[0][1], rows[-1][4]
            highs = [r[2] for r in rows]
            lows = [r[3] for r in rows]
            move = abs(last / first - 1) * 100 if first > 0 else 0
            rng = (max(highs) - min(lows)) / last * 100 if last > 0 else 0
            volatility = sum(abs(rows[i][4] / rows[i-1][4] - 1) * 100 for i in range(1, len(rows)) if rows[i-1][4] > 0)
        else:
            move = rng = volatility = 0
    except Exception as exc:
        print(f"[UNIVERSE] {sym}: 5m ranking unavailable: {type(exc).__name__}", flush=True)
        move = rng = volatility = 0
    score = abs(change_24h) * .20 + volatility * .35 + rng * .25 + move * .20
    return sym, turnover, score


def build_universe():
    global ACTIVE_EXCHANGE
    try:
        candidates = okx_universe()
        ACTIVE_EXCHANGE = "okx"
    except Exception as exc:
        print(f"[WARN] OKX unavailable, switching to Gate.io: {exc}", flush=True)
        candidates = gate_universe()
        ACTIVE_EXCHANGE = "gate"
    if not candidates:
        return []
    print(f"[UNIVERSE] {len(candidates)} liquid candidates on {ACTIVE_EXCHANGE}", flush=True)
    scored = []
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        futures = [pool.submit(activity_score, item) for item in candidates]
        for future in as_completed(futures):
            try:
                scored.append(future.result())
            except Exception as exc:
                print(f"[UNIVERSE] ranking failed: {exc}", flush=True)
    scored.sort(key=lambda x: (x[2], x[1]), reverse=True)
    selected = scored[:min(TOP_UNIVERSE, MAX_MONITORED)]
    print(f"[UNIVERSE] Selected {len(selected)} contracts", flush=True)
    return selected

# ============================== SIGNAL ENGINE ======================
def closed_bars(rows, timeframe, now=None):
    seconds = 14400 if timeframe == "4H" else 86400
    now = int(now or time.time())
    # Do not evaluate a candle until its full timeframe has elapsed.
    return [r for r in rows if int(r[0]) + seconds <= now]


def detect_impulse(rows, timeframe):
    """Return the strongest current base-breakout impulse, or None.

    The base must be a compact range. The latest confirmed close must remain beyond
    its boundary by the timeframe threshold. Volume is descriptive, not a filter.
    """
    bars = closed_bars(rows, timeframe)
    threshold = PUMP_DUMP_THRESHOLDS[timeframe]
    max_impulse = MAX_IMPULSE_BARS[timeframe]
    max_base_range = MAX_BASE_RANGE_PCT[timeframe]
    best = None
    if len(bars) < 30:
        return None

    for impulse_len in range(1, min(max_impulse, len(bars) - 12) + 1):
        impulse = bars[-impulse_len:]
        impulse_start = len(bars) - impulse_len
        last_close = bars[-1][4]
        if last_close <= 0:
            continue
        for base_len in BASE_WINDOWS[timeframe]:
            if impulse_start - base_len < 0:
                continue
            base = bars[impulse_start-base_len:impulse_start]
            base_high = max(x[2] for x in base)
            base_low = min(x[3] for x in base)
            base_mid = (base_high + base_low) / 2
            if base_mid <= 0:
                continue
            base_range_pct = (base_high - base_low) / base_mid * 100
            if base_range_pct > max_base_range:
                continue

            peak_high = max(x[2] for x in impulse)
            peak_low = min(x[3] for x in impulse)
            pump_pct = (last_close / base_high - 1) * 100 if base_high > 0 else 0
            dump_pct = ((base_low - last_close) / base_low) * 100 if base_low > 0 else 0

            direction = None
            move_pct = 0.0
            if last_close > base_high and pump_pct >= threshold:
                direction, move_pct = "PUMP", pump_pct
            elif last_close < base_low and dump_pct >= threshold:
                direction, move_pct = "DUMP", dump_pct
            if not direction:
                continue

            # Ensure the impulse itself actually travelled beyond the base boundary.
            if direction == "PUMP" and peak_high <= base_high:
                continue
            if direction == "DUMP" and peak_low >= base_low:
                continue

            # A longer, tighter base is preferred when the move magnitude is similar.
            quality = move_pct + min(base_len, 48) * 0.08 - base_range_pct * 0.12
            candidate = {
                "direction": direction,
                "timeframe": timeframe,
                "move_pct": move_pct,
                "base_high": base_high,
                "base_low": base_low,
                "base_range_pct": base_range_pct,
                "base_start": base[0][0],
                "base_end": base[-1][0],
                "impulse_start": impulse[0][0],
                "signal_time": bars[-1][0],
                "price": last_close,
                "volume": sum(x[5] for x in impulse),
                "base_volume": sum(x[5] for x in base) / len(base),
                "impulse_bars": impulse_len,
                "base_bars": base_len,
                "quality": quality,
                "bars": bars,
            }
            if best is None or candidate["quality"] > best["quality"]:
                best = candidate
    return best

# ============================== PNG CHART ==========================
def png_chunk(kind, data):
    body = kind + data
    return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body) & 0xffffffff)


def render_signal_chart(signal, symbol):
    """Draw candles, highlighted base range, breakout boundary and volume bars."""
    bars = signal["bars"][-CHART_VISIBLE[signal["timeframe"]]:]
    width, height = 1200, 760
    left, right, top, price_bottom = 78, 1168, 62, 565
    vol_top, vol_bottom = 600, 700
    bg, grid = (13, 19, 31), (40, 52, 70)
    up, down = (28, 205, 145), (238, 75, 91)
    px = bytearray(bg * (width * height))

    def pixel(x, y, color):
        if 0 <= x < width and 0 <= y < height:
            p = (y * width + x) * 3
            px[p:p+3] = bytes(color)

    def line(x1, y1, x2, y2, color, thick=1):
        dx, sx = abs(x2-x1), 1 if x1 < x2 else -1
        dy, sy = -abs(y2-y1), 1 if y1 < y2 else -1
        err = dx + dy
        while True:
            for ox in range(-(thick//2), thick//2+1):
                for oy in range(-(thick//2), thick//2+1):
                    pixel(x1+ox, y1+oy, color)
            if x1 == x2 and y1 == y2:
                break
            e2 = 2 * err
            if e2 >= dy:
                err += dy; x1 += sx
            if e2 <= dx:
                err += dx; y1 += sy

    def rect(x1, y1, x2, y2, color):
        xa, xb = max(0, min(x1, x2)), min(width-1, max(x1, x2))
        ya, yb = max(0, min(y1, y2)), min(height-1, max(y1, y2))
        if xa > xb or ya > yb:
            return
        row = bytes(color) * (xb-xa+1)
        for yy in range(ya, yb+1):
            pos = (yy*width+xa)*3
            px[pos:pos+len(row)] = row

    def translucent(x1, y1, x2, y2, color, alpha):
        xa, xb = max(0, min(x1, x2)), min(width-1, max(x1, x2))
        ya, yb = max(0, min(y1, y2)), min(height-1, max(y1, y2))
        for yy in range(ya, yb+1):
            for xx in range(xa, xb+1):
                p = (yy*width+xx)*3
                for c in range(3):
                    px[p+c] = int(px[p+c]*(1-alpha) + color[c]*alpha)

    lows = [b[3] for b in bars]
    highs = [b[2] for b in bars]
    lo, hi = min(lows), max(highs)
    pad = (hi-lo)*0.08 if hi > lo else abs(hi)*0.02
    lo -= pad; hi += pad
    span = hi-lo or 1
    def yp(v): return int(price_bottom - (v-lo)/span*(price_bottom-top))
    step = (right-left)/len(bars)
    candle_w = max(3, min(12, int(step*.58)))

    for i in range(6):
        y = top + (price_bottom-top)*i//5
        line(left, y, right, y, grid)
    for i in range(9):
        x = left + (right-left)*i//8
        line(x, top, x, price_bottom, grid)

    # Highlight only the portion of the base visible in this chart window.
    start_ts, end_ts = signal["base_start"], signal["base_end"]
    base_indices = [i for i, b in enumerate(bars) if start_ts <= b[0] <= end_ts]
    if base_indices:
        x1 = int(left + min(base_indices)*step)
        x2 = int(left + (max(base_indices)+1)*step)
        zone_color = (42, 115, 160) if signal["direction"] == "PUMP" else (145, 93, 45)
        translucent(x1, yp(signal["base_high"]), x2, yp(signal["base_low"]), zone_color, .30)

    boundary = signal["base_high"] if signal["direction"] == "PUMP" else signal["base_low"]
    boundary_color = (40, 200, 145) if signal["direction"] == "PUMP" else (238, 85, 95)
    line(left, yp(boundary), right, yp(boundary), boundary_color, 2)

    max_vol = max((b[5] for b in bars), default=1) or 1
    for i, b in enumerate(bars):
        x = int(left + (i+.5)*step)
        color = up if b[4] >= b[1] else down
        line(x, yp(b[2]), x, yp(b[3]), color, 2)
        y1, y2 = sorted((yp(b[1]), yp(b[4])))
        if y2-y1 < 2: y2 = y1+2
        rect(x-candle_w//2, y1, x+candle_w//2, y2, color)
        vh = int((b[5]/max_vol)*(vol_bottom-vol_top))
        rect(x-candle_w//2, vol_bottom-vh, x+candle_w//2, vol_bottom, color)

    signal_idx = next((i for i, b in enumerate(bars) if b[0] == signal["signal_time"]), len(bars)-1)
    sx = int(left + (signal_idx+.5)*step)
    sy = yp(signal["price"])
    line(sx, top, sx, price_bottom, (245, 195, 75), 1)
    # Cross marker at the signal candle.
    line(sx-9, sy-9, sx+9, sy+9, (255, 220, 90), 3)
    line(sx-9, sy+9, sx+9, sy-9, (255, 220, 90), 3)

    raw = bytearray()
    stride = width*3
    for y in range(height):
        raw.append(0); raw.extend(px[y*stride:(y+1)*stride])
    png = b"\x89PNG\r\n\x1a\n"
    png += png_chunk(b"IHDR", struct.pack(">2I5B", width, height, 8, 2, 0, 0, 0))
    png += png_chunk(b"IDAT", zlib.compress(bytes(raw), 6))
    png += png_chunk(b"IEND", b"")
    return png

# ============================== TELEGRAM ===========================
def send_telegram_signal(symbol, signal):
    token, chat_id = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("[ERROR] Telegram credentials missing", flush=True)
        return False
    direction = signal["direction"]
    emoji = "🚀" if direction == "PUMP" else "🔻"
    tf = signal["timeframe"]
    move_sign = "+" if direction == "PUMP" else "−"
    caption = (
        f"{emoji} <b>{direction} ALERT — {tf}</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"Монета: <b>{symbol}</b>\n"
        f"Движение от границы диапазона: <b>{move_sign}{signal['move_pct']:.2f}%</b>\n"
        f"Текущая цена: <code>{signal['price']:.12g}</code>\n"
        f"Граница диапазона: <code>{(signal['base_high'] if direction == 'PUMP' else signal['base_low']):.12g}</code>\n"
        f"Диапазон накопления: <b>{signal['base_range_pct']:.2f}%</b>\n"
        f"Свечей импульса: <b>{signal['impulse_bars']}</b>\n"
        f"Объём импульса: <b>{signal['volume']:.6g}</b>\n"
        f"Биржа: <b>{ACTIVE_EXCHANGE.upper()}</b>\n"
        "━━━━━━━━━━━━━━\n"
        "Сигнал после закрытия подтверждённой свечи. Объём не является обязательным фильтром."
    )
    try:
        png = render_signal_chart(signal, symbol)
        response = requests.post(
            f"https://api.telegram.org/bot{token}/sendPhoto",
            data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
            files={"photo": (f"{symbol}_{tf}_{direction}.png", png, "image/png")},
            timeout=60,
        )
        response.raise_for_status()
        result = response.json()
        if result.get("ok") is True:
            print(f"[TELEGRAM] Sent {direction} {symbol} {tf} {signal['move_pct']:.2f}%", flush=True)
            return True
        print("[TELEGRAM] Rejected: " + str(result.get("description")), flush=True)
    except Exception as exc:
        print(f"[TELEGRAM] Delivery failed {symbol} {tf}: {type(exc).__name__}: {exc}", flush=True)
    return False


def send_test():
    token, chat_id = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": "🟢 <b>PUMP / DUMP SCANNER — тест связи</b>\nБот подключён. Это не торговый сигнал.", "parse_mode": "HTML"}, timeout=20)
        r.raise_for_status()
        return r.json().get("ok") is True
    except Exception as exc:
        print(f"[TELEGRAM TEST] {exc}", flush=True)
        return False

# ============================== STATE / SCAN =======================
def load_state():
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("seen", []), list):
                return data
        except Exception as exc:
            print(f"[STATE] Cannot read state: {exc}", flush=True)
    return {"seen": []}


def process_symbol(exchange, symbol):
    found = []
    errors = []
    for timeframe in ("4H", "1D"):
        try:
            rows = get_bars(exchange, symbol, timeframe, CANDLE_LIMIT[timeframe])
            signal = detect_impulse(rows, timeframe)
            if signal:
                found.append((symbol, signal))
        except Exception as exc:
            errors.append(f"{timeframe}: {type(exc).__name__}: {exc}")
    return symbol, found, errors


def signal_key(symbol, signal):
    return "|".join((symbol, signal["timeframe"], signal["direction"], str(signal["impulse_start"])))


def main():
    global ACTIVE_EXCHANGE
    started = time.time()
    print("PUMP / DUMP SCANNER: start", flush=True)
    print(f"[CONFIG] thresholds: 4H={PUMP_DUMP_THRESHOLDS['4H']}%; 1D={PUMP_DUMP_THRESHOLDS['1D']}%; universe={TOP_UNIVERSE}", flush=True)

    if os.getenv("TELEGRAM_TEST") == "1" and not send_test():
        raise RuntimeError("Telegram test failed")

    universe = build_universe()
    exchange = ACTIVE_EXCHANGE
    if not universe:
        raise RuntimeError("No USDT perpetual contracts found")

    state = load_state()
    seen = set(state.get("seen", []))
    discovered = []
    errors_count = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        futures = {pool.submit(process_symbol, exchange, sym): sym for sym, _, _ in universe}
        for future in as_completed(futures):
            completed += 1
            sym = futures[future]
            try:
                _, signals, errors = future.result()
                errors_count += len(errors)
                for item in signals:
                    key = signal_key(*item)
                    if key not in seen:
                        discovered.append((item[0], item[1], key))
                if errors:
                    print(f"[WARN] {sym}: {'; '.join(errors)}", flush=True)
            except Exception as exc:
                errors_count += 1
                print(f"[WARN] {sym}: {type(exc).__name__}: {exc}", flush=True)
            if completed % 25 == 0 or completed == len(universe):
                print(f"Progress: {completed}/{len(universe)}; errors={errors_count}", flush=True)

    discovered.sort(key=lambda x: x[1]["signal_time"])
    sent = failed = 0
    print(f"[INFO] New PUMP/DUMP candidates: {len(discovered)}", flush=True)
    for symbol, signal, key in discovered:
        print(f"SIGNAL | {symbol} | {signal['timeframe']} | {signal['direction']} | {signal['move_pct']:.2f}% | price={signal['price']}", flush=True)
        if send_telegram_signal(symbol, signal):
            seen.add(key)
            sent += 1
        else:
            failed += 1

    state["seen"] = sorted(seen)[-20000:]
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"PUMP / DUMP SCANNER: finished; symbols={len(universe)}; errors={errors_count}; candidates={len(discovered)}; sent={sent}; failed={failed}; exchange={exchange}; elapsed={time.time()-started:.1f}s", flush=True)


if __name__ == "__main__":
    main()
