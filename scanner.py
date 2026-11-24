#!/usr/bin/env python3
"""FALSE BREAKOUT SCANNER scheduled backend. Dynamic OKX/Gate.io scanner."""

from __future__ import annotations

import json
import html
import math
import os
import time
import threading
import struct
import zlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter


# ============================================================
# CONFIGURATION
# ============================================================

OKX = "https://www.okx.com"
GATE = "https://api.gateio.ws/api/v4"

STATE_FILE = Path("state.json")

# Daily candles cache
DAILY_CACHE_FILE = Path("daily_cache.json")
DAILY_CACHE_VERSION = 1

_daily_cache = {
    "version": DAILY_CACHE_VERSION,
    "exchange": None,
    "symbols": {}
}
_daily_cache_lock = threading.Lock()

# Independent 4H candles cache. This file belongs only to this scanner project.
FOUR_HOUR_CACHE_FILE = Path("four_hour_cache.json")
FOUR_HOUR_CACHE_VERSION = 1
_four_hour_cache = {
    "version": FOUR_HOUR_CACHE_VERSION,
    "exchange": None,
    "symbols": {}
}
_four_hour_cache_lock = threading.Lock()

# False-breakout parameters
FB = {
    "swing": 1,
    "minSwingAtr": 1,
    "mergeAtr": 0.5,
    "leaveAtr": 0.5
}

TOP_UNIVERSE = 500
# Liquidity preselection pool. Activity ranking is then calculated inside it.
UNIVERSE_CANDIDATES = 1000
# Strict impulse filters for the dynamic universe.
DAILY_IMPULSE_MIN_PCT = 50.0
FOUR_HOUR_IMPULSE_MIN_PCT = 25.0
# Measure a recent impulse from the first candle's open to the furthest
# high/low reached inside each rolling window.
DAILY_IMPULSE_LOOKBACK = 5       # last 5 daily candles (5 days)
FOUR_HOUR_IMPULSE_LOOKBACK = 2   # last 2 four-hour candles (8 hours)
IMPULSE_REPEAT_STEP_PCT = 20.0   # resend after an additional 20 percentage points
DAILY_CHART_CANDLES = 90
FOUR_HOUR_CHART_CANDLES = 72
MAX_MONITORED = int(os.getenv("MAX_MONITORED", "500"))

SCAN_WORKERS = max(
    1,
    min(int(os.getenv("SCAN_WORKERS", "5")), 10)
)

HTTP_RETRIES = 3
HTTP_TIMEOUT = 25
REQUEST_INTERVAL = 0.06

EXCLUDE_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "GBP",
    "AEUR", "BFUSD", "USDE", "USD0", "PYUSD"
}

ACTIVE_EXCHANGE = "okx"

_thread_local = threading.local()
_request_lock = threading.Lock()
_last_request_time = 0.0


# ============================================================
# HTTP
# ============================================================

def get_session():
    session = getattr(_thread_local, "session", None)

    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "GOLD-SCANNER/1.0",
            "Accept": "application/json"
        })

        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=0
        )

        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _thread_local.session = session

    return session


def request_spacing():
    global _last_request_time

    with _request_lock:
        now = time.monotonic()
        wait = REQUEST_INTERVAL - (now - _last_request_time)

        if wait > 0:
            time.sleep(wait)

        _last_request_time = time.monotonic()


def get_json(url: str, params: dict | None = None, timeout: int = HTTP_TIMEOUT):
    last_error = None

    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            request_spacing()

            response = get_session().get(
                url,
                params=params,
                timeout=timeout
            )

            response.raise_for_status()
            return response.json()

        except (requests.RequestException, ValueError) as exc:
            last_error = exc

            if attempt < HTTP_RETRIES:
                time.sleep(min(1.0 * attempt, 3.0))

    raise RuntimeError(
        f"HTTP request failed after {HTTP_RETRIES} attempts: "
        f"{type(last_error).__name__}: {last_error}"
    )


# ============================================================
# EXCHANGE CANDLES
# ============================================================

def _okx_rows(inst_id: str, bar: str, limit: int):
    rows_by_ts = {}
    cursor = None

    while len(rows_by_ts) < limit:
        count = min(300, limit - len(rows_by_ts))

        params = {
            "instId": inst_id,
            "bar": bar,
            "limit": str(count)
        }

        if cursor is not None:
            params["after"] = cursor

        data = get_json(
            OKX + "/api/v5/market/history-candles",
            params
        )

        if data.get("code") != "0":
            raise RuntimeError(
                f"OKX candles {inst_id}: {data.get('msg')}"
            )

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

        time.sleep(0.04)

    return [
        rows_by_ts[k]
        for k in sorted(rows_by_ts)
    ][-limit:]


def _gate_rows(contract: str, interval: str, limit: int):
    params = {
        "contract": contract,
        "interval": interval,
        "limit": min(limit, 2000)
    }

    rows = get_json(
        GATE + "/futures/usdt/candlesticks",
        params
    )

    return rows[-limit:]


# ============================================================
# SYMBOL NORMALIZATION
# ============================================================

def _canonical_from_okx(inst_id: str):
    return (
        inst_id[:-10] + "USDT"
        if inst_id.endswith("-USDT-SWAP")
        else None
    )


def _canonical_from_gate(name: str):
    return (
        name.replace("_", "")
        if name.endswith("_USDT")
        else None
    )


def _base(symbol: str):
    return (
        symbol[:-4]
        if symbol.endswith("USDT")
        else symbol
    )


# ============================================================
# MARKET UNIVERSE
# ============================================================

def _okx_universe():
    instruments = get_json(
        OKX + "/api/v5/public/instruments",
        {"instType": "SWAP"}
    )

    if instruments.get("code") != "0":
        raise RuntimeError(
            "OKX instruments: " + str(instruments.get("msg"))
        )

    supported = {}

    for item in instruments.get("data", []):
        inst_id = item.get("instId", "")

        if (
            item.get("state") == "live"
            and item.get("settleCcy") == "USDT"
            and inst_id.endswith("-USDT-SWAP")
        ):
            sym = _canonical_from_okx(inst_id)

            if sym and _base(sym) not in EXCLUDE_BASES:
                supported[sym] = inst_id

    tickers = get_json(
        OKX + "/api/v5/market/tickers",
        {"instType": "SWAP"}
    )

    if tickers.get("code") != "0":
        raise RuntimeError(
            "OKX tickers: " + str(tickers.get("msg"))
        )

    items = []

    for ticker in tickers.get("data", []):
        inst_id = ticker.get("instId", "")
        sym = _canonical_from_okx(inst_id)

        if not sym or sym not in supported:
            continue

        try:
            last = float(ticker.get("last") or 0)
            turnover = float(ticker.get("volCcy24h") or 0) * last
            open_24h = float(ticker.get("open24h") or 0)
            change_24h = (
                (last / open_24h - 1.0) * 100.0
                if last > 0 and open_24h > 0
                else 0.0
            )
        except (TypeError, ValueError, ZeroDivisionError):
            continue

        if turnover > 0 and last > 0:
            items.append((sym, turnover, change_24h))

    items.sort(key=lambda x: x[1], reverse=True)
    return items[:UNIVERSE_CANDIDATES]


def _gate_universe():
    contracts = get_json(
        GATE + "/futures/usdt/contracts"
    )

    supported = {
        _canonical_from_gate(x.get("name", ""))
        for x in contracts
        if x.get("in_delisting") is False
    }

    tickers = get_json(
        GATE + "/futures/usdt/tickers"
    )

    items = []

    for ticker in tickers:
        sym = _canonical_from_gate(
            ticker.get("contract", "")
        )

        if (
            not sym
            or sym not in supported
            or _base(sym) in EXCLUDE_BASES
        ):
            continue

        try:
            turnover = float(ticker.get("volume_24h_quote") or 0)
            change_24h = float(ticker.get("change_percentage") or 0)
        except (TypeError, ValueError):
            continue

        if turnover > 0:
            items.append((sym, turnover, change_24h))

    items.sort(key=lambda x: x[1], reverse=True)
    return items[:UNIVERSE_CANDIDATES]


def _impulse_pct(exchange, symbol, interval, lookback):
    """Largest directional excursion from the first candle's open."""
    rows = get_bars(exchange, symbol, interval, lookback)
    if len(rows) < lookback:
        return None

    window = rows[-lookback:]
    start_price = float(window[0][1])
    if start_price <= 0:
        return None

    highest = max(float(row[2]) for row in window)
    lowest = min(float(row[3]) for row in window)
    up_pct = (highest / start_price - 1.0) * 100.0
    down_pct = (1.0 - lowest / start_price) * 100.0
    return max(0.0, up_pct, down_pct)


def _impulse_details(bars, lookback):
    """Return the strongest signed excursion from the first candle open."""
    if len(bars) < lookback:
        return None

    window = bars[-lookback:]
    first = window[0]
    start_price = float(first["open"] if isinstance(first, dict) else first[1])
    if start_price <= 0:
        return None

    highest = max(float(x["high"] if isinstance(x, dict) else x[2]) for x in window)
    lowest = min(float(x["low"] if isinstance(x, dict) else x[3]) for x in window)
    up_pct = (highest / start_price - 1.0) * 100.0
    down_pct = (1.0 - lowest / start_price) * 100.0

    if up_pct >= down_pct:
        direction = "PUMP"
        value = up_pct
    else:
        direction = "DUMP"
        value = down_pct

    return {
        "pct": round(max(0.0, value), 2),
        "direction": direction,
        "start_time": int(first["time"] if isinstance(first, dict) else first[0]),
        "start_price": start_price,
        "extreme_price": highest if direction == "PUMP" else lowest
    }


def _measure_symbol_impulses(exchange, symbol):
    """Measure 5D and 8H(2x4H) impulses independently."""
    daily = None
    four_hour = None

    try:
        daily = _impulse_pct(
            exchange, symbol, "1d", DAILY_IMPULSE_LOOKBACK
        )
    except Exception as exc:
        print(
            f"[UNIVERSE] {symbol}: daily impulse unavailable: "
            f"{type(exc).__name__}: {exc}",
            flush=True
        )

    try:
        four_hour = _impulse_pct(
            exchange, symbol, "4h", FOUR_HOUR_IMPULSE_LOOKBACK
        )
    except Exception as exc:
        print(
            f"[UNIVERSE] {symbol}: 4H impulse unavailable: "
            f"{type(exc).__name__}: {exc}",
            flush=True
        )

    return daily, four_hour


def build_stable_universe():
    """Select only liquid USDT perpetuals with a strong recent 1D or 4H impulse."""
    global ACTIVE_EXCHANGE

    try:
        candidates = _okx_universe()
        ACTIVE_EXCHANGE = "okx"

    except Exception as exc:
        print(
            f"[WARN] OKX universe unavailable; "
            f"using Gate.io: {type(exc).__name__}: {exc}",
            flush=True
        )
        candidates = _gate_universe()
        ACTIVE_EXCHANGE = "gate"

    if not candidates:
        return []

    exchange = ACTIVE_EXCHANGE
    measured = []
    print(
        f"[UNIVERSE] Liquidity preselection: {len(candidates)} contracts; "
        f"checking {DAILY_IMPULSE_LOOKBACK} daily candles (5 days) and "
        f"{FOUR_HOUR_IMPULSE_LOOKBACK} 4H candles (8 hours)...",
        flush=True
    )

    with ThreadPoolExecutor(max_workers=min(SCAN_WORKERS, 10)) as executor:
        futures = {
            executor.submit(_measure_symbol_impulses, exchange, item[0]): item
            for item in candidates
        }
        for future in as_completed(futures):
            sym, turnover, change_24h = futures[future]
            try:
                daily_impulse, four_hour_impulse = future.result()
            except Exception:
                daily_impulse, four_hour_impulse = None, None

            # A symbol must meet at least one explicit impulse threshold.
            daily_ok = (
                daily_impulse is not None
                and daily_impulse >= DAILY_IMPULSE_MIN_PCT
            )
            four_hour_ok = (
                four_hour_impulse is not None
                and four_hour_impulse >= FOUR_HOUR_IMPULSE_MIN_PCT
            )
            if not (daily_ok or four_hour_ok):
                continue

            # Rank by strength relative to the applicable threshold.
            # This lets a very strong 4H pump/dump compete with a 1D impulse.
            score_1d = (
                daily_impulse / DAILY_IMPULSE_MIN_PCT
                if daily_impulse is not None else 0.0
            )
            score_4h = (
                four_hour_impulse / FOUR_HOUR_IMPULSE_MIN_PCT
                if four_hour_impulse is not None else 0.0
            )
            score = max(score_1d, score_4h)
            measured.append((
                sym, turnover, change_24h,
                daily_impulse, four_hour_impulse, score
            ))

    measured.sort(key=lambda x: (x[5], x[1]), reverse=True)
    limit = min(TOP_UNIVERSE, MAX_MONITORED)
    selected = measured[:limit]

    daily_count = sum(
        1 for x in measured
        if x[3] is not None and x[3] >= DAILY_IMPULSE_MIN_PCT
    )
    four_hour_count = sum(
        1 for x in measured
        if x[4] is not None and x[4] >= FOUR_HOUR_IMPULSE_MIN_PCT
    )

    print(
        f"[UNIVERSE] Qualified={len(measured)}; selected={len(selected)}; "
        f"1D >= {DAILY_IMPULSE_MIN_PCT:.0f}%: {daily_count}; "
        f"4H >= {FOUR_HOUR_IMPULSE_MIN_PCT:.0f}%: {four_hour_count}; "
        f"exchange={ACTIVE_EXCHANGE}",
        flush=True
    )

    if selected:
        preview = ", ".join(
            f"{x[0]}(1D={'n/a' if x[3] is None else f'{x[3]:.1f}%'}, "
            f"4H={'n/a' if x[4] is None else f'{x[4]:.1f}%'})"
            for x in selected[:12]
        )
        print(f"[UNIVERSE] Strongest impulses: {preview}", flush=True)
    else:
        print(
            "[UNIVERSE] No contracts met the configured impulse thresholds; "
            "scanner will not substitute weaker movers.",
            flush=True
        )

    return [
        (sym, turnover, None)
        for sym, turnover, _, _, _, _ in selected
    ]


# ============================================================
# INDICATOR HELPERS
# ============================================================

def daily_returns(klines):
    out = {}

    for i in range(1, max(1, len(klines) - 1)):
        prev = float(klines[i - 1][4])
        close = float(klines[i][4])

        if prev > 0:
            out[int(klines[i][0])] = close / prev - 1

    return out


def stdev(values):
    if not values:
        return 0.0

    m = sum(values) / len(values)

    return math.sqrt(
        sum((x - m) ** 2 for x in values) / len(values)
    )


def corr(a, b):
    if not a or len(a) != len(b):
        return 0.0

    ma = sum(a) / len(a)
    mb = sum(b) / len(b)

    num = sum(
        (x - ma) * (y - mb)
        for x, y in zip(a, b)
    )

    da = sum((x - ma) ** 2 for x in a)
    db = sum((y - mb) ** 2 for y in b)

    return (
        num / math.sqrt(da * db)
        if da and db
        else 0.0
    )


def wilder_atr(highs, lows, closes, period=14):
    trs = []

    for i in range(1, len(highs)):
        trs.append(
            max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1])
            )
        )

    if len(trs) < period:
        return None

    atr = sum(trs[:period]) / period

    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period

    return atr


# ============================================================
# CANDLE DATA
# ============================================================

def _normalise_rows(rows, source):
    out = []

    for row in rows:
        if source == "okx":
            ts, op, hi, lo, cl, vol = row[:6]
        else:
            ts, op, hi, lo, cl, vol = (
                row["t"],
                row["o"],
                row["h"],
                row["l"],
                row["c"],
                row.get("sum", row.get("v", 0))
            )

        out.append([
            int(ts) if source == "gate" else int(ts) // 1000,
            float(op),
            float(hi),
            float(lo),
            float(cl),
            float(vol)
        ])

    return out


def get_bars(exchange, symbol, interval, limit):
    if exchange == "okx":
        inst_id = symbol.replace(
            "USDT",
            "-USDT-SWAP"
        )

        bar = {
            "5m": "5m",
            "1h": "1H",
            "4h": "4H",
            "1d": "1D"
        }.get(interval, "1D")

        rows = _okx_rows(
            inst_id,
            bar,
            limit
        )

        return _normalise_rows(rows, "okx")

    contract = _base(symbol) + "_USDT"
    gate_interval = {
        "5m": "5m",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d"
    }.get(interval, "1d")

    rows = _gate_rows(
        contract,
        gate_interval,
        limit
    )

    return _normalise_rows(rows, "gate")


# ============================================================
# DAILY CANDLES CACHE
# ============================================================

def load_daily_cache(exchange):
    global _daily_cache

    if not DAILY_CACHE_FILE.exists():
        print(
            "[CACHE] daily_cache.json not found; "
            "full daily history will be downloaded.",
            flush=True
        )
        _daily_cache = {
            "version": DAILY_CACHE_VERSION,
            "exchange": exchange,
            "symbols": {}
        }
        return

    try:
        data = json.loads(
            DAILY_CACHE_FILE.read_text(encoding="utf-8")
        )

        if (
            data.get("version") != DAILY_CACHE_VERSION
            or data.get("exchange") != exchange
            or not isinstance(data.get("symbols"), dict)
        ):
            print(
                "[CACHE] Cache version or exchange mismatch; "
                "starting with an empty cache.",
                flush=True
            )
            _daily_cache = {
                "version": DAILY_CACHE_VERSION,
                "exchange": exchange,
                "symbols": {}
            }
            return

        valid_symbols = {}

        for symbol, rows in data["symbols"].items():
            if not isinstance(rows, list):
                continue

            valid_rows = []

            for row in rows:
                try:
                    if len(row) < 6:
                        continue

                    valid_rows.append([
                        int(row[0]),
                        float(row[1]),
                        float(row[2]),
                        float(row[3]),
                        float(row[4]),
                        float(row[5])
                    ])
                except (TypeError, ValueError):
                    continue

            valid_rows.sort(key=lambda x: x[0])

            if valid_rows:
                valid_symbols[symbol] = valid_rows[-700:]

        _daily_cache = {
            "version": DAILY_CACHE_VERSION,
            "exchange": exchange,
            "symbols": valid_symbols
        }

        print(
            f"[CACHE] Loaded daily history for "
            f"{len(valid_symbols)} symbols.",
            flush=True
        )

    except Exception as exc:
        print(
            f"[CACHE] Cannot read daily cache: "
            f"{type(exc).__name__}: {exc}",
            flush=True
        )

        _daily_cache = {
            "version": DAILY_CACHE_VERSION,
            "exchange": exchange,
            "symbols": {}
        }


def get_cached_daily_bars(exchange, symbol):
    with _daily_cache_lock:
        cached = _daily_cache["symbols"].get(symbol, [])
        cached = [row[:] for row in cached]

    download_limit = 30 if cached else 700

    try:
        fresh = get_bars(
            exchange,
            symbol,
            "1d",
            download_limit
        )

        merged = {
            int(row[0]): row
            for row in cached
        }

        for row in fresh:
            merged[int(row[0])] = row

        result = [
            merged[ts]
            for ts in sorted(merged)
        ][-700:]

        with _daily_cache_lock:
            _daily_cache["symbols"][symbol] = result

        return result

    except Exception as exc:
        if len(cached) >= 60:
            print(
                f"[CACHE] {symbol}: daily refresh failed; "
                f"using cached history. "
                f"{type(exc).__name__}: {exc}",
                flush=True
            )
            return cached

        raise


def save_daily_cache():
    with _daily_cache_lock:
        data = {
            "version": DAILY_CACHE_VERSION,
            "exchange": _daily_cache["exchange"],
            "symbols": _daily_cache["symbols"]
        }

        serialized = json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":")
        )

    temp_file = DAILY_CACHE_FILE.with_suffix(".tmp")

    temp_file.write_text(
        serialized,
        encoding="utf-8"
    )

    temp_file.replace(DAILY_CACHE_FILE)

    print(
        f"[CACHE] Saved daily history for "
        f"{len(data['symbols'])} symbols.",
        flush=True
    )


# ============================================================
# 4H CANDLES CACHE
# ============================================================
def load_four_hour_cache(exchange):
    global _four_hour_cache

    if not FOUR_HOUR_CACHE_FILE.exists():
        print(
            "[CACHE] four_hour_cache.json not found; "
            "full 4H history will be downloaded.",
            flush=True
        )
        _four_hour_cache = {
            "version": FOUR_HOUR_CACHE_VERSION,
            "exchange": exchange,
            "symbols": {}
        }
        return

    try:
        data = json.loads(
            FOUR_HOUR_CACHE_FILE.read_text(encoding="utf-8")
        )

        if (
            data.get("version") != FOUR_HOUR_CACHE_VERSION
            or data.get("exchange") != exchange
            or not isinstance(data.get("symbols"), dict)
        ):
            print(
                "[CACHE] 4H cache version or exchange mismatch; "
                "starting with an empty cache.",
                flush=True
            )
            _four_hour_cache = {
                "version": FOUR_HOUR_CACHE_VERSION,
                "exchange": exchange,
                "symbols": {}
            }
            return

        valid_symbols = {}
        for symbol, rows in data["symbols"].items():
            if not isinstance(rows, list):
                continue
            valid_rows = []
            for row in rows:
                try:
                    if len(row) < 6:
                        continue
                    valid_rows.append([
                        int(row[0]), float(row[1]), float(row[2]),
                        float(row[3]), float(row[4]), float(row[5])
                    ])
                except (TypeError, ValueError):
                    continue
            valid_rows.sort(key=lambda x: x[0])
            if valid_rows:
                valid_symbols[symbol] = valid_rows[-700:]

        _four_hour_cache = {
            "version": FOUR_HOUR_CACHE_VERSION,
            "exchange": exchange,
            "symbols": valid_symbols
        }
        print(
            f"[CACHE] Loaded 4H history for {len(valid_symbols)} symbols.",
            flush=True
        )

    except Exception as exc:
        print(
            f"[CACHE] Cannot read 4H cache: {type(exc).__name__}: {exc}",
            flush=True
        )
        _four_hour_cache = {
            "version": FOUR_HOUR_CACHE_VERSION,
            "exchange": exchange,
            "symbols": {}
        }


def get_cached_four_hour_bars(exchange, symbol):
    with _four_hour_cache_lock:
        cached = [row[:] for row in _four_hour_cache["symbols"].get(symbol, [])]

    download_limit = 30 if cached else 700
    try:
        fresh = get_bars(exchange, symbol, "4h", download_limit)
        merged = {int(row[0]): row for row in cached}
        for row in fresh:
            merged[int(row[0])] = row
        result = [merged[ts] for ts in sorted(merged)][-700:]
        with _four_hour_cache_lock:
            _four_hour_cache["symbols"][symbol] = result
        return result
    except Exception as exc:
        if len(cached) >= 60:
            print(
                f"[CACHE] {symbol}: 4H refresh failed; using cached history. "
                f"{type(exc).__name__}: {exc}",
                flush=True
            )
            return cached
        raise


def save_four_hour_cache():
    with _four_hour_cache_lock:
        data = {
            "version": FOUR_HOUR_CACHE_VERSION,
            "exchange": _four_hour_cache["exchange"],
            "symbols": _four_hour_cache["symbols"]
        }
        serialized = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

    temp_file = FOUR_HOUR_CACHE_FILE.with_suffix(".tmp")
    temp_file.write_text(serialized, encoding="utf-8")
    temp_file.replace(FOUR_HOUR_CACHE_FILE)
    print(
        f"[CACHE] Saved 4H history for {len(data['symbols'])} symbols.",
        flush=True
    )


def _bars_to_dict(rows):
    return [
        {
            "time": int(k[0]),
            "open": k[1],
            "high": k[2],
            "low": k[3],
            "close": k[4],
            "volume": k[5]
        }
        for k in rows
    ]


# ============================================================
# FALSE BREAKOUT ENGINE
# ============================================================

def atr_series(days, period=14):
    out = [None] * len(days)
    prev = 0.0
    total = 0.0

    for i, d in enumerate(days):
        tr = (
            d["high"] - d["low"]
            if i == 0
            else max(
                d["high"] - d["low"],
                abs(d["high"] - days[i - 1]["close"]),
                abs(d["low"] - days[i - 1]["close"])
            )
        )

        if i < period:
            total += tr

            if i == period - 1:
                prev = total / period
                out[i] = prev
        else:
            prev = (
                prev * (period - 1) + tr
            ) / period

            out[i] = prev

    return out


def fb_events(days):
    n = len(days)
    L = FB["swing"]

    atr = atr_series(days, 14)
    ev = [None] * n

    for p in range(L, n - L - 1):
        c = p + L
        i = c + 1

        hp = days[p]["high"]
        lp = days[p]["low"]
        a = atr[p]

        if a is None:
            continue

        is_h = True
        is_l = True

        sw_lo = float("inf")
        sw_hi = float("-inf")

        max_a = float("-inf")
        min_a = float("inf")

        for k in range(p - L, c + 1):
            b = days[k]

            sw_lo = min(sw_lo, b["low"])
            sw_hi = max(sw_hi, b["high"])

            if k < p:
                if b["high"] >= hp:
                    is_h = False

                if b["low"] <= lp:
                    is_l = False

            elif k > p:
                if b["high"] > hp:
                    is_h = False

                if b["low"] < lp:
                    is_l = False

                max_a = max(max_a, b["high"])
                min_a = min(min_a, b["low"])

        strong_h = (
            is_h
            and hp - sw_lo >= a * FB["minSwingAtr"]
            and max_a < hp
        )

        strong_l = (
            is_l
            and sw_hi - lp >= a * FB["minSwingAtr"]
            and min_a > lp
        )

        if not strong_h and not strong_l:
            continue

        if ev[i] is None:
            ev[i] = {
                "atr": atr[c],
                "h": [],
                "l": []
            }

        if strong_h:
            ev[i]["h"].append({
                "price": hp,
                "time": days[p]["time"]
            })

        if strong_l:
            ev[i]["l"].append({
                "price": lp,
                "time": days[p]["time"]
            })

    return ev, atr


def fb_compute(days, m5, now_ms=None):
    n = len(days)
    L = FB["swing"]

    if n < L * 2 + 20:
        return {
            "levels": [],
            "events": []
        }

    ev, atr = fb_events(days)

    H = []
    Lo = []

    def add(arr, side, it, a):
        if any(
            abs(x["price"] - it["price"])
            <= a * FB["mergeAtr"]
            for x in arr
        ):
            return

        arr.append({
            "side": side,
            "price": it["price"],
            "time": it["time"],
            "st": 0,
            "touch": 0
        })

        if len(arr) > 240:
            arr.pop(0)

    def add_day(i):
        e = ev[i]

        if e:
            for x in e["h"]:
                add(H, "H", x, e["atr"] or 0)

            for x in e["l"]:
                add(Lo, "L", x, e["atr"] or 0)

    if m5:
        ds = (
            (m5[0]["time"] + 86399) // 86400
        ) * 86400

        k = next(
            (
                i for i, d in enumerate(days)
                if d["time"] >= ds
            ),
            -1
        )

        i_start = k if k >= 0 else n
    else:
        i_start = n

    if i_start > n - 1:
        i_start = n - 1

    for i in range(i_start):
        add_day(i)

        H = [
            x for x in H
            if days[i]["high"] < x["price"]
        ]

        Lo = [
            x for x in Lo
            if days[i]["low"] > x["price"]
        ]

    events = []
    cur = i_start

    add_day(cur)

    now_sec = (
        (now_ms or int(time.time() * 1000)) / 1000
    )

    for bi, b in enumerate(m5):
        if b["time"] < days[i_start]["time"]:
            continue

        while (
            cur + 1 < n
            and b["time"] >= days[cur + 1]["time"]
        ):
            cur += 1
            add_day(cur)

        closed = (
            bi < len(m5) - 1
            or b["time"] + 300 <= now_sec
        )

        D = FB["leaveAtr"] * (
            atr[cur - 1] or atr[cur] or 0
        )

        gone = False

        for x in H:
            if closed and b["close"] > x["price"]:
                x["st"] = 2
                gone = True
                continue

            if closed and b["high"] >= x["price"]:
                x["st"] = 1
                x["touch"] = b["time"]

                events.append({
                    "side": "H",
                    "price": x["price"],
                    "time": x["time"],
                    "touch": b["time"]
                })

                continue

            if (
                x["st"] == 1
                and x["touch"] < b["time"]
                and (
                    b["high"] >= x["price"] + D
                    or b["low"] <= x["price"] - D
                )
            ):
                x["st"] = 2
                gone = True

        for x in Lo:
            if closed and b["close"] < x["price"]:
                x["st"] = 2
                gone = True
                continue

            if closed and b["low"] <= x["price"]:
                x["st"] = 1
                x["touch"] = b["time"]

                events.append({
                    "side": "L",
                    "price": x["price"],
                    "time": x["time"],
                    "touch": b["time"]
                })

                continue

            if (
                x["st"] == 1
                and x["touch"] < b["time"]
                and (
                    b["low"] <= x["price"] - D
                    or b["high"] >= x["price"] + D
                )
            ):
                x["st"] = 2
                gone = True

        if gone:
            H = [
                x for x in H
                if x["st"] != 2
            ]

            Lo = [
                x for x in Lo
                if x["st"] != 2
            ]

    return {
        "levels": H + Lo,
        "events": events
    }



def fb_compute_4h(bars4h, m5, now_ms=None):
    n = len(bars4h)
    L = FB["swing"]

    if n < L * 2 + 20:
        return {
            "levels": [],
            "events": []
        }

    ev, atr = fb_events(bars4h)

    H = []
    Lo = []

    def add(arr, side, it, a):
        if any(
            abs(x["price"] - it["price"])
            <= a * FB["mergeAtr"]
            for x in arr
        ):
            return

        arr.append({
            "side": side,
            "price": it["price"],
            "time": it["time"],
            "st": 0,
            "touch": 0
        })

        if len(arr) > 240:
            arr.pop(0)

    def add_day(i):
        e = ev[i]

        if e:
            for x in e["h"]:
                add(H, "H", x, e["atr"] or 0)

            for x in e["l"]:
                add(Lo, "L", x, e["atr"] or 0)

    if m5:
        ds = (
            (m5[0]["time"] + 14399) // 14400
        ) * 14400

        k = next(
            (
                i for i, d in enumerate(bars4h)
                if d["time"] >= ds
            ),
            -1
        )

        i_start = k if k >= 0 else n
    else:
        i_start = n

    if i_start > n - 1:
        i_start = n - 1

    for i in range(i_start):
        add_day(i)

        H = [
            x for x in H
            if bars4h[i]["high"] < x["price"]
        ]

        Lo = [
            x for x in Lo
            if bars4h[i]["low"] > x["price"]
        ]

    events = []
    cur = i_start

    add_day(cur)

    now_sec = (
        (now_ms or int(time.time() * 1000)) / 1000
    )

    for bi, b in enumerate(m5):
        if b["time"] < bars4h[i_start]["time"]:
            continue

        while (
            cur + 1 < n
            and b["time"] >= bars4h[cur + 1]["time"]
        ):
            cur += 1
            add_day(cur)

        closed = (
            bi < len(m5) - 1
            or b["time"] + 300 <= now_sec
        )

        D = FB["leaveAtr"] * (
            atr[cur - 1] or atr[cur] or 0
        )

        gone = False

        for x in H:
            if closed and b["close"] > x["price"]:
                x["st"] = 2
                gone = True
                continue

            if closed and b["high"] >= x["price"]:
                x["st"] = 1
                x["touch"] = b["time"]

                events.append({
                    "side": "H",
                    "price": x["price"],
                    "time": x["time"],
                    "touch": b["time"],
                    "timeframe": "4H"
                })

                continue

            if (
                x["st"] == 1
                and x["touch"] < b["time"]
                and (
                    b["high"] >= x["price"] + D
                    or b["low"] <= x["price"] - D
                )
            ):
                x["st"] = 2
                gone = True

        for x in Lo:
            if closed and b["close"] < x["price"]:
                x["st"] = 2
                gone = True
                continue

            if closed and b["low"] <= x["price"]:
                x["st"] = 1
                x["touch"] = b["time"]

                events.append({
                    "side": "L",
                    "price": x["price"],
                    "time": x["time"],
                    "touch": b["time"],
                    "timeframe": "4H"
                })

                continue

            if (
                x["st"] == 1
                and x["touch"] < b["time"]
                and (
                    b["low"] <= x["price"] - D
                    or b["high"] >= x["price"] + D
                )
            ):
                x["st"] = 2
                gone = True

        if gone:
            H = [
                x for x in H
                if x["st"] != 2
            ]

            Lo = [
                x for x in Lo
                if x["st"] != 2
            ]

    return {
        "levels": H + Lo,
        "events": events
    }


# ============================================================
# STATE
# ============================================================

def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(
                STATE_FILE.read_text()
            )
        except Exception as exc:
            print(
                f"[WARN] Cannot read state.json: {exc}",
                flush=True
            )

    return {
        "primed": False,
        "seen": [],
        "seeded_symbols": [],
        "impulse_alerts": {}
    }


# ============================================================
# TELEGRAM
# ============================================================

def telegram_post(message, timeout=20):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print(
            "[ERROR] Telegram credentials are missing",
            flush=True
        )
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    try:
        response = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            },
            timeout=timeout
        )

        response.raise_for_status()
        result = response.json()

        if result.get("ok") is True:
            return True

        print(
            "[ERROR] Telegram API rejected message: "
            + str(result.get("description", "unknown error")),
            flush=True
        )

        return False

    except Exception as exc:
        print(
            f"[ERROR] Telegram delivery failed: "
            f"{type(exc).__name__}: {exc}",
            flush=True
        )

        return False


def _png_chunk(chunk_type, data):
    body = chunk_type + data
    return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body) & 0xffffffff)



# 5x7 bitmap font used for chart headers (standard library only).
_FONT_5X7 = {
"A":["01110","10001","10001","11111","10001","10001","10001"],
"B":["11110","10001","10001","11110","10001","10001","11110"],
"C":["01111","10000","10000","10000","10000","10000","01111"],
"D":["11110","10001","10001","10001","10001","10001","11110"],
"E":["11111","10000","10000","11110","10000","10000","11111"],
"F":["11111","10000","10000","11110","10000","10000","10000"],
"G":["01111","10000","10000","10111","10001","10001","01111"],
"H":["10001","10001","10001","11111","10001","10001","10001"],
"I":["11111","00100","00100","00100","00100","00100","11111"],
"J":["00111","00010","00010","00010","10010","10010","01100"],
"K":["10001","10010","10100","11000","10100","10010","10001"],
"L":["10000","10000","10000","10000","10000","10000","11111"],
"M":["10001","11011","10101","10101","10001","10001","10001"],
"N":["10001","11001","10101","10011","10001","10001","10001"],
"O":["01110","10001","10001","10001","10001","10001","01110"],
"P":["11110","10001","10001","11110","10000","10000","10000"],
"Q":["01110","10001","10001","10001","10101","10010","01101"],
"R":["11110","10001","10001","11110","10100","10010","10001"],
"S":["01111","10000","10000","01110","00001","00001","11110"],
"T":["11111","00100","00100","00100","00100","00100","00100"],
"U":["10001","10001","10001","10001","10001","10001","01110"],
"V":["10001","10001","10001","10001","10001","01010","00100"],
"W":["10001","10001","10001","10101","10101","10101","01010"],
"X":["10001","10001","01010","00100","01010","10001","10001"],
"Y":["10001","10001","01010","00100","00100","00100","00100"],
"Z":["11111","00001","00010","00100","01000","10000","11111"],
"0":["01110","10001","10011","10101","11001","10001","01110"],
"1":["00100","01100","00100","00100","00100","00100","01110"],
"2":["01110","10001","00001","00010","00100","01000","11111"],
"3":["11110","00001","00001","01110","00001","00001","11110"],
"4":["00010","00110","01010","10010","11111","00010","00010"],
"5":["11111","10000","10000","11110","00001","00001","11110"],
"6":["01110","10000","10000","11110","10001","10001","01110"],
"7":["11111","00001","00010","00100","01000","01000","01000"],
"8":["01110","10001","10001","01110","10001","10001","01110"],
"9":["01110","10001","10001","01111","00001","00001","01110"],
"/":["00001","00010","00100","01000","10000","00000","00000"],
"-":["00000","00000","00000","11111","00000","00000","00000"],
".":["00000","00000","00000","00000","00000","00110","00110"],
":":["00000","00110","00110","00000","00110","00110","00000"],
"|":["00100","00100","00100","00100","00100","00100","00100"],
" ":["00000"]*7
}


def _order_block_zones(bars, input_range=25):
    """
    Port of the supplied Pine Order Blocks core:
    prior-range BOS, last opposite candle's tracked high/low as block bounds,
    and removal when candle close breaks the far edge.
    Returns active bullish/bearish zones with their creation timestamps.
    """
    zones = []
    last_down_index = None
    last_down_high = 0.0
    last_low = 0.0
    last_up_index = None
    last_up_low = 0.0
    last_high = 0.0
    last_long_index = -1

    for i, candle in enumerate(bars):
        op = float(candle["open"]); cl = float(candle["close"])
        hi = float(candle["high"]); lo = float(candle["low"])
        if i >= input_range + 1:
            prior = bars[i-input_range:i]
            structure_low = min(float(x["low"]) for x in prior)
            structure_high = max(float(x["high"]) for x in prior)
            prev_c = float(bars[i-1]["close"])
            prev_prior = bars[i-input_range-1:i-1]
            prev_structure_low = min(float(x["low"]) for x in prev_prior) if len(prev_prior) == input_range else structure_low
            prev_structure_high = max(float(x["high"]) for x in prev_prior) if len(prev_prior) == input_range else structure_high

            bearish_bos = cl < structure_low and prev_c >= prev_structure_low
            bullish_bos = cl > structure_high and prev_c <= prev_structure_high

            if bearish_bos and last_up_index is not None and i-last_up_index < 1000:
                top = max(last_high, last_up_low)
                bottom = min(last_high, last_up_low)
                if top > bottom > 0:
                    zones.append({"kind":"BEARISH", "top":top, "bottom":bottom,
                                  "start":int(bars[last_up_index]["time"]), "active":True})
            if bullish_bos and last_down_index is not None and i-last_down_index < 1000 and i > last_long_index:
                top = max(last_down_high, last_low)
                bottom = min(last_down_high, last_low)
                if top > bottom > 0:
                    zones.append({"kind":"BULLISH", "top":top, "bottom":bottom,
                                  "start":int(bars[last_down_index]["time"]), "active":True})
                    last_long_index = i

            # Apply the Pine block invalidation rules to all existing zones.
            for z in zones:
                if not z["active"]:
                    continue
                if z["kind"] == "BULLISH" and cl < z["bottom"]:
                    z["active"] = False
                elif z["kind"] == "BEARISH" and cl > z["top"]:
                    z["active"] = False

        # Pine's candle tracking: reset on each opposite candle, then keep
        # running extreme until the next candle of that same direction.
        if cl < op:
            last_down_high = hi
            last_down_index = i
            last_low = lo
        elif cl > op:
            last_up_index = i
            last_up_low = lo
            last_high = hi

        if last_down_index is not None:
            last_low = min(lo, last_low)
        if last_up_index is not None:
            last_high = max(hi, last_high)

    return [z for z in zones if z["active"]]



def render_candlestick_png(bars, title, max_candles=90, width=1100, height=620, zones=None):
    """Render candles, Pine-derived OB zones and a visible symbol/timeframe header."""
    data = bars[-max_candles:]
    if len(data) < 2:
        raise ValueError("Not enough candles to render chart")

    bg = (15, 23, 42)
    grid = (43, 55, 75)
    up = (22, 190, 130)
    down = (239, 76, 86)
    text_color = (235, 241, 250)
    pixels = bytearray(bg * (width * height))

    def pixel(x, y, color):
        if 0 <= x < width and 0 <= y < height:
            i = (y * width + x) * 3
            pixels[i:i+3] = bytes(color)

    def line(x1, y1, x2, y2, color, thickness=1):
        dx = abs(x2-x1); sx = 1 if x1 < x2 else -1
        dy = -abs(y2-y1); sy = 1 if y1 < y2 else -1
        err = dx + dy
        while True:
            for ox in range(-(thickness//2), thickness//2+1):
                for oy in range(-(thickness//2), thickness//2+1):
                    pixel(x1+ox, y1+oy, color)
            if x1 == x2 and y1 == y2:
                break
            e2 = 2 * err
            if e2 >= dy:
                err += dy; x1 += sx
            if e2 <= dx:
                err += dx; y1 += sy

    def rect(x1, y1, x2, y2, color):
        for yy in range(max(0, y1), min(height, y2+1)):
            for xx in range(max(0, x1), min(width, x2+1)):
                pixel(xx, yy, color)

    def blend_rect(x1, y1, x2, y2, color, alpha=52):
        for yy in range(max(0, y1), min(height, y2+1)):
            for xx in range(max(0, x1), min(width, x2+1)):
                idx = (yy * width + xx) * 3
                for c in range(3):
                    pixels[idx+c] = (pixels[idx+c]*(255-alpha) + color[c]*alpha)//255

    def draw_text(x, y, text, color=text_color, scale=2):
        cursor = x
        for ch in str(text).upper():
            glyph = _FONT_5X7.get(ch, _FONT_5X7[" "])
            for gy, row in enumerate(glyph):
                for gx, bit in enumerate(row):
                    if bit == "1":
                        rect(cursor+gx*scale, y+gy*scale,
                             cursor+(gx+1)*scale-1, y+(gy+1)*scale-1, color)
            cursor += 6*scale
        return cursor

    left, right, top, bottom = 65, width-28, 92, height-48
    highs = [float(x["high"]) for x in data]
    lows = [float(x["low"]) for x in data]
    visible_times = [int(x["time"]) for x in data]
    lo, hi = min(lows), max(highs)
    relevant_zones = [
        z for z in (zones or [])
        if z["top"] >= lo*0.75 and z["bottom"] <= hi*1.25
    ]
    if relevant_zones:
        lo = min(lo, min(z["bottom"] for z in relevant_zones))
        hi = max(hi, max(z["top"] for z in relevant_zones))
    pad = (hi-lo)*0.06 if hi > lo else max(abs(hi)*0.02, 0.01)
    lo -= pad; hi += pad
    span = hi-lo or 1.0

    def yprice(value):
        return int(bottom - (value-lo)/span*(bottom-top))

    # Header background and title.
    rect(0, 0, width-1, 69, (20, 31, 51))
    draw_text(28, 22, title, text_color, 3)
    line(0, 70, width-1, 70, (67, 86, 115), 1)

    for i in range(6):
        y = top + (bottom-top)*i//5
        line(left, y, right, y, grid)
    for i in range(9):
        x = left + (right-left)*i//8
        line(x, top, x, bottom, grid)

    step = (right-left)/len(data)

    # Draw active OB zones before candles, extending to the right edge.
    for z in relevant_zones:
        start_ts = int(z["start"])
        try:
            start_idx = next(i for i, ts in enumerate(visible_times) if ts >= start_ts)
            x1 = int(left + start_idx*step)
        except StopIteration:
            x1 = left
        y1, y2 = sorted((yprice(z["top"]), yprice(z["bottom"])))
        if z["kind"] == "BULLISH":
            zone_color = (80, 190, 130)
            border_color = (65, 210, 145)
        else:
            zone_color = (210, 160, 65)
            border_color = (235, 180, 75)
        blend_rect(x1, y1, right, y2, zone_color, 48)
        line(x1, y1, right, y1, border_color, 2)
        line(x1, y2, right, y2, border_color, 2)

    candle_w = max(3, min(10, int(step*0.58)))
    for i, candle in enumerate(data):
        x = int(left + (i+0.5)*step)
        op = float(candle["open"]); cl = float(candle["close"])
        high = float(candle["high"]); low = float(candle["low"])
        color = up if cl >= op else down
        line(x, yprice(high), x, yprice(low), color, 2)
        y1, y2 = sorted((yprice(op), yprice(cl)))
        if y2-y1 < 2:
            y2 = y1+2
        rect(x-candle_w//2, y1, x+candle_w//2, y2, color)

    last_price = float(data[-1]["close"])
    ylast = yprice(last_price)
    line(left, ylast, right, ylast, (235, 190, 70), 1)

    raw = bytearray()
    stride = width*3
    for y in range(height):
        raw.append(0)
        raw.extend(pixels[y*stride:(y+1)*stride])
    png = b"\x89PNG\r\n\x1a\n"
    png += _png_chunk(b"IHDR", struct.pack(">2I5B", width, height, 8, 2, 0, 0, 0))
    png += _png_chunk(b"IDAT", zlib.compress(bytes(raw), 6))
    png += _png_chunk(b"IEND", b"")
    return png


def send_impulse_telegram(sym, impulse_by_tf):
    """Send one Telegram media group with one or two timeframe charts."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("[ERROR] Telegram credentials are missing", flush=True)
        return False

    captions = ["🚀 <b>ОБНАРУЖЕН МОЩНЫЙ ИМПУЛЬС</b>", f"Монета: <b>{html.escape(sym)}</b>"]
    media = []
    files = {}
    chart_specs = []
    if "1D" in impulse_by_tf:
        chart_specs.append(("1D", impulse_by_tf["1D"], DAILY_CHART_CANDLES))
    if "4H" in impulse_by_tf:
        chart_specs.append(("4H", impulse_by_tf["4H"], FOUR_HOUR_CHART_CANDLES))
    for tf, detail, _count in chart_specs:
        sign = "+" if detail["direction"] == "PUMP" else "-"
        period_label = "5 ДНЕЙ" if tf == "1D" else "8 ЧАСОВ"
        captions.append(
            f"{tf} ({period_label}): <b>{detail['direction']}</b> "
            f"{sign}{detail['pct']:.1f}%"
        )

    try:
        for index, (tf, detail, count) in enumerate(chart_specs):
            interval = "1d" if tf == "1D" else "4h"
            # Load extended history so the Order Blocks engine can detect
            # BOS events and keep zones that originated before the visible chart.
            history_rows = get_bars(ACTIVE_EXCHANGE, sym, interval, max(500, count))
            history_bars = _bars_to_dict(history_rows)
            zones = _order_block_zones(history_bars, input_range=25)
            display_bars = history_bars[-count:]
            chart_symbol = sym[:-4] + "/USDT" if sym.endswith("USDT") else sym
            chart_title = f"{chart_symbol} | {tf} | ORDER BLOCKS"
            image_bytes = render_candlestick_png(
                history_bars, chart_title, count, zones=zones
            )
            filename = f"chart_{index}.png"
            files[filename] = (filename, image_bytes, "image/png")
            media_item = {
                "type": "photo",
                "media": f"attach://{filename}"
            }
            if index == 0:
                media_item["caption"] = "\n".join(captions)
                media_item["parse_mode"] = "HTML"
            media.append(media_item)

        if len(media) == 1:
            filename, file_tuple = next(iter(files.items()))
            url = f"https://api.telegram.org/bot{token}/sendPhoto"
            response = requests.post(
                url,
                data={"chat_id": chat_id, "caption": "\n".join(captions), "parse_mode": "HTML"},
                files={"photo": file_tuple},
                timeout=45
            )
        else:
            url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
            response = requests.post(
                url,
                data={"chat_id": chat_id, "media": json.dumps(media, ensure_ascii=False)},
                files=files,
                timeout=45
            )
        response.raise_for_status()
        result = response.json()
        if result.get("ok") is True:
            print(f"[TELEGRAM] Impulse alert sent: {sym} ({','.join(impulse_by_tf)})", flush=True)
            return True
        print("[ERROR] Telegram media group rejected: " + str(result.get("description", "unknown error")), flush=True)
        return False
    except Exception as exc:
        print(f"[ERROR] Impulse chart delivery failed for {sym}: {type(exc).__name__}: {exc}", flush=True)
        return False


def send_telegram(sym, event, atr_pct):
    side = (
        "ЛОЖНЫЙ ПРОБОЙ ВВЕРХ 🔴"
        if event["side"] == "H"
        else "ЛОЖНЫЙ ПРОБОЙ ВНИЗ 🟢"
    )

    dt = time.strftime(
        "%d.%m.%Y %H:%M UTC",
        time.gmtime(event["touch"])
    )

    message = (
        "🟡 <b>FALSE BREAKOUT SCANNER</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"Монета: <b>{html.escape(sym)}</b>\n"
        f"Сигнал: <b>{side}</b>\n"
        f"Таймфрейм уровня: <b>{html.escape(event.get('timeframe', '1D'))}</b>\n"
        f"Уровень: <code>{event['price']:.12g}</code>\n"
        f"Время свечи: {dt}\n"
        f"ATR: {atr_pct:.2f}%\n"
        f"Биржа: {ACTIVE_EXCHANGE.upper()}\n"
        "━━━━━━━━━━━━━━"
    )

    if telegram_post(message):
        print(
            f"[TELEGRAM] Sent: {sym}",
            flush=True
        )
        return True

    return False


def send_telegram_test():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print(
            "[TELEGRAM TEST] Missing bot token or chat ID",
            flush=True
        )
        return False

    message = (
        "🟡 <b>FALSE BREAKOUT SCANNER — ТЕСТ УВЕДОМЛЕНИЯ</b>\n\n"
        "Telegram успешно подключён!\n"
        "Бот может отправлять уведомления.\n\n"
        "Это проверочное сообщение, не торговый сигнал."
    )

    if telegram_post(message, timeout=15):
        print(
            "[TELEGRAM TEST] Message sent successfully",
            flush=True
        )
        return True

    print(
        "[TELEGRAM TEST] Message was not delivered",
        flush=True
    )

    return False


# ============================================================
# PER-SYMBOL PROCESSING
# ============================================================

def process_symbol(exchange, sym, is_new_symbol, seen_snapshot):
    """
    Process one symbol independently.

    Daily levels are invalidated by daily candles.
    4H levels are invalidated by 4H candles.
    Both level sets are checked against the same last 288 five-minute candles.
    """
    try:
        days_raw = get_cached_daily_bars(exchange, sym)
        four_hour_raw = get_cached_four_hour_bars(exchange, sym)
        m5_raw = get_bars(exchange, sym, "5m", 288)

        days = _bars_to_dict(days_raw)
        bars4h = _bars_to_dict(four_hour_raw)
        m5 = _bars_to_dict(m5_raw)

        if len(days) < 60 or len(bars4h) < 60 or len(m5) < 30:
            return (
                sym, [], [], False,
                f"insufficient candle history "
                f"(daily={len(days)}, 4h={len(bars4h)}, 5m={len(m5)})"
            )

        # Keep the original daily ATR calculation for daily-level alerts.
        highs = [d["high"] for d in days[:-1]]
        lows = [d["low"] for d in days[:-1]]
        closes = [d["close"] for d in days[:-1]]
        atr_value = wilder_atr(highs, lows, closes, 14)
        last_close = days[-1]["close"]
        daily_atr_pct = (
            atr_value / last_close * 100
            if atr_value and last_close > 0
            else 0.0
        )

        # 4H ATR is displayed for 4H-level alerts, so the metric matches the level TF.
        highs4 = [d["high"] for d in bars4h[:-1]]
        lows4 = [d["low"] for d in bars4h[:-1]]
        closes4 = [d["close"] for d in bars4h[:-1]]
        atr4_value = wilder_atr(highs4, lows4, closes4, 14)
        last_close4 = bars4h[-1]["close"]
        atr4_pct = (
            atr4_value / last_close4 * 100
            if atr4_value and last_close4 > 0
            else 0.0
        )

        daily_result = fb_compute(days, m5)
        four_hour_result = fb_compute_4h(bars4h, m5)

        impulse_data = {}
        daily_impulse = _impulse_details(days, DAILY_IMPULSE_LOOKBACK)
        four_hour_impulse = _impulse_details(bars4h, FOUR_HOUR_IMPULSE_LOOKBACK)
        if daily_impulse and daily_impulse["pct"] >= DAILY_IMPULSE_MIN_PCT:
            impulse_data["1D"] = daily_impulse
        if four_hour_impulse and four_hour_impulse["pct"] >= FOUR_HOUR_IMPULSE_MIN_PCT:
            impulse_data["4H"] = four_hour_impulse

        discovered = []
        seed_keys = []

        combined = (
            [(event, daily_atr_pct) for event in daily_result["events"]]
            + [(event, atr4_pct) for event in four_hour_result["events"]]
        )

        for event, atr_pct in combined:
            # Mark the original daily engine's events without changing its logic.
            if "timeframe" not in event:
                event["timeframe"] = "1D"

            key = (
                f"{sym}|{event['timeframe']}|{event['side']}|"
                f"{event['time']}|{event['price']:.12g}|{event['touch']}"
            )

            if is_new_symbol:
                seed_keys.append(key)
            elif key not in seen_snapshot:
                discovered.append((sym, event, atr_pct, key))

        return sym, discovered, seed_keys, impulse_data, True, None

    except Exception as exc:
        return sym, [], [], {}, False, f"{type(exc).__name__}: {exc}"


# ============================================================
# MAIN
# ============================================================

def main():
    global ACTIVE_EXCHANGE

    started_at = time.time()

    print(
        "FALSE BREAKOUT SCANNER: start",
        flush=True
    )

    print(
        f"[CONFIG] swing={FB['swing']}; "
        f"minSwingAtr={FB['minSwingAtr']}; "
        f"max_monitored={MAX_MONITORED}; "
        f"workers={SCAN_WORKERS}; "
        f"http_retries={HTTP_RETRIES}",
        flush=True
    )

    if os.getenv("TELEGRAM_TEST") == "1":
        test_ok = send_telegram_test()

        if not test_ok:
            raise RuntimeError(
                "Telegram test message was not delivered"
            )

        print(
            "[TELEGRAM TEST] Test completed; continuing scanner.",
            flush=True
        )

    symbols = build_stable_universe()
    exchange = ACTIVE_EXCHANGE

    # Load independent daily and 4H candle caches for this scanner project.
    load_daily_cache(exchange)
    load_four_hour_cache(exchange)

    print(
        f"Universe selected: {len(symbols)} symbols; "
        f"monitor cap={MAX_MONITORED}; "
        f"exchange={exchange}",
        flush=True
    )

    if not symbols:
        raise RuntimeError(
            "No symbols available in the selected universe"
        )

    state = load_state()

    seen = set(
        state.get("seen", [])
    )

    seeded = set(
        state.get("seeded_symbols", [])
    )

    seen_snapshot = frozenset(seen)

    discovered = []
    seed_keys = []
    impulse_candidates = {}

    successful_symbols = 0
    failed_symbols = 0

    total = len(symbols)
    completed = 0

    print(
        f"[INFO] Starting scan of {total} symbols "
        f"with {SCAN_WORKERS} workers",
        flush=True
    )

    with ThreadPoolExecutor(
        max_workers=SCAN_WORKERS
    ) as executor:

        futures = {}

        for sym, quote_volume, _ in symbols:
            is_new_symbol = sym not in seeded

            future = executor.submit(
                process_symbol,
                exchange,
                sym,
                is_new_symbol,
                seen_snapshot
            )

            futures[future] = sym

        for future in as_completed(futures):
            completed += 1
            sym = futures[future]

            try:
                (
                    result_sym,
                    symbol_events,
                    symbol_seed_keys,
                    symbol_impulses,
                    success,
                    error
                ) = future.result()

                if success:
                    successful_symbols += 1

                    discovered.extend(
                        symbol_events
                    )

                    seed_keys.extend(
                        symbol_seed_keys
                    )
                    if symbol_impulses:
                        impulse_candidates[result_sym] = symbol_impulses

                    seeded.add(result_sym)

                else:
                    failed_symbols += 1

                    print(
                        f"[WARN] {sym}: {error}",
                        flush=True
                    )

            except Exception as exc:
                failed_symbols += 1

                print(
                    f"[WARN] {sym}: worker failed: "
                    f"{type(exc).__name__}: {exc}",
                    flush=True
                )

            if (
                completed % 10 == 0
                or completed == total
            ):
                print(
                    f"Progress: {completed}/{total}; "
                    f"successful={successful_symbols}; "
                    f"failed={failed_symbols}",
                    flush=True
                )

    # Save updated daily and 4H caches after all workers have finished.
    save_daily_cache()
    save_four_hour_cache()

    if not state.get("primed"):
        state["primed"] = True

        print(
            "Initial run: historical signals seeded; "
            "no old alerts sent.",
            flush=True
        )

    seen.update(seed_keys)

    sent_count = 0
    failed_count = 0

    print(
        f"[INFO] New signal candidates: {len(discovered)}",
        flush=True
    )

    discovered.sort(
        key=lambda item: item[1]["touch"]
    )

    for sym, event, atr_pct, key in discovered:
        side = (
            "ЛОЖНЫЙ ПРОБОЙ ВВЕРХ"
            if event["side"] == "H"
            else "ЛОЖНЫЙ ПРОБОЙ ВНИЗ"
        )

        dt = time.strftime(
            "%Y-%m-%d %H:%M UTC",
            time.gmtime(event["touch"])
        )

        print(
            f"SIGNAL | {sym} | {side} | "
            f"level={event['price']} | "
            f"candle={dt} | "
            f"ATR%={atr_pct:.2f}",
            flush=True
        )

        if send_telegram(sym, event, atr_pct):
            seen.add(key)
            sent_count += 1

        else:
            failed_count += 1

    # Impulse alerts are independent from false-breakout alerts.
    # First qualifying appearance is sent; same-direction continuation is resent
    # only after another +20 percentage points. Direction change starts a new alert.
    impulse_alerts = state.get("impulse_alerts", {})
    if not isinstance(impulse_alerts, dict):
        impulse_alerts = {}

    impulse_sent_count = 0
    impulse_failed_count = 0
    now_ts = int(time.time())

    for sym, details in sorted(impulse_candidates.items()):
        previous = impulse_alerts.get(sym, {})
        if not isinstance(previous, dict):
            previous = {}

        notify = {}
        for tf, detail in details.items():
            old = previous.get(tf, {})
            old_pct = float(old.get("pct", 0.0)) if isinstance(old, dict) else 0.0
            old_direction = old.get("direction") if isinstance(old, dict) else None
            old_time = int(old.get("sent_at", 0)) if isinstance(old, dict) else 0
            stale = old_time and (now_ts - old_time > 7 * 86400)
            is_new = not old or stale
            direction_changed = old_direction and old_direction != detail["direction"]
            strengthened = detail["pct"] >= old_pct + IMPULSE_REPEAT_STEP_PCT
            if is_new or direction_changed or strengthened:
                notify[tf] = detail

        if not notify:
            continue

        if send_impulse_telegram(sym, notify):
            for tf, detail in notify.items():
                previous[tf] = {
                    "pct": detail["pct"],
                    "direction": detail["direction"],
                    "sent_at": now_ts
                }
            impulse_alerts[sym] = previous
            impulse_sent_count += 1
        else:
            impulse_failed_count += 1

    state["impulse_alerts"] = impulse_alerts
    state["seeded_symbols"] = sorted(seeded)
    state["seen"] = sorted(seen)[-10000:]

    STATE_FILE.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2
        ) + "\n"
    )

    elapsed = time.time() - started_at

    print(
        f"FALSE BREAKOUT SCANNER: finished; "
        f"scanned={successful_symbols}; "
        f"symbol errors={failed_symbols}; "
        f"new candidates={len(discovered)}; "
        f"telegram sent={sent_count}; "
        f"telegram failed={failed_count}; "
        f"impulse alerts sent={impulse_sent_count}; "
        f"impulse alerts failed={impulse_failed_count}; "
        f"exchange={exchange}; "
        f"elapsed={elapsed:.1f}s",
        flush=True
    )


if __name__ == "__main__":
    main()
