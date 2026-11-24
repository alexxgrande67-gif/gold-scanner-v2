#!/usr/bin/env python3
"""GOLD SCANNER scheduled backend. Optimized OKX/Gate.io scanner."""

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

# Daily candles cache (independent to this scanner project).
DAILY_CACHE_FILE = Path("daily_cache.json")
DAILY_CACHE_VERSION = 1
DAILY_CACHE_LIMIT = 700

_daily_cache = {
    "version": DAILY_CACHE_VERSION,
    "exchange": None,
    "symbols": {}
}
_daily_cache_lock = threading.Lock()

# Separate 4H cache. It never shares files with the first scanner project.
FOUR_HOUR_CACHE_FILE = Path("four_hour_cache.json")
FOUR_HOUR_CACHE_VERSION = 1
FOUR_HOUR_CACHE_LIMIT = 240

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

# Order Blocks: defaults copied from the supplied Pine Script v6 indicator.
OB_INPUT_RANGE = 25
OB_SHOW_BEARISH_BOS = True
OB_SHOW_BULLISH_BOS = True
OB_USE_MITIGATED_BLOCKS = False
OB_MAX_DRAWN_BOXES = 50  # TradingView's default object limit when max_boxes_count is omitted.
CHART_DAILY_CANDLES = 90
CHART_4H_CANDLES = 72
CHART_OB_HISTORY_CANDLES = 2000  # common maximum supported by OKX/Gate chart history retrieval

TOP_UNIVERSE = 500
# First keep liquid contracts, then rank them by current activity.
UNIVERSE_CANDIDATES = 1000
UNIVERSE_LOOKBACK_5M = 36  # 3 hours of recent activity
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
            last_price = float(ticker.get("last") or 0)
            turnover = (
                float(ticker.get("volCcy24h") or 0)
                * last_price
            )
            open_24h = float(ticker.get("open24h") or 0)
            change_24h_pct = (
                (last_price / open_24h - 1.0) * 100.0
                if open_24h > 0 and last_price > 0
                else 0.0
            )
        except (TypeError, ValueError):
            continue

        if turnover > 0 and last_price > 0:
            items.append((sym, turnover, change_24h_pct))

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
            turnover = float(
                ticker.get("volume_24h_quote") or 0
            )
            change_24h_pct = float(
                ticker.get("change_percentage") or 0
            )
        except (TypeError, ValueError):
            continue

        if turnover > 0:
            items.append((sym, turnover, change_24h_pct))

    items.sort(key=lambda x: x[1], reverse=True)
    return items[:UNIVERSE_CANDIDATES]


def _activity_score_candidate(item):
    """Score both upward and downward movers, emphasizing the last 3 hours."""
    sym, turnover, change_24h_pct = item
    recent_volatility_pct = 0.0
    recent_range_pct = 0.0
    recent_move_pct = 0.0

    try:
        rows = get_bars(
            ACTIVE_EXCHANGE,
            sym,
            "5m",
            UNIVERSE_LOOKBACK_5M
        )

        if len(rows) >= 12:
            first_open = float(rows[0][1])
            last_close = float(rows[-1][4])
            highs = [float(row[2]) for row in rows]
            lows = [float(row[3]) for row in rows]

            if first_open > 0 and last_close > 0:
                recent_move_pct = abs(
                    (last_close / first_open - 1.0) * 100.0
                )
                recent_range_pct = (
                    (max(highs) - min(lows)) / last_close * 100.0
                )

            absolute_returns = []
            previous = None
            for row in rows:
                close = float(row[4])
                if previous and previous > 0 and close > 0:
                    absolute_returns.append(
                        abs(close / previous - 1.0) * 100.0
                    )
                previous = close

            recent_volatility_pct = sum(absolute_returns)

    except Exception as exc:
        print(
            f"[UNIVERSE] {sym}: recent candles unavailable; "
            f"ranking by 24h move only ({type(exc).__name__})",
            flush=True
        )

    # Absolute values intentionally include both pumpers and dumpers.
    # Recent 3h activity has 80% of the weight so stale 24h movers fall back.
    score = (
        abs(change_24h_pct) * 0.20
        + recent_volatility_pct * 0.35
        + recent_range_pct * 0.25
        + recent_move_pct * 0.20
    )

    return (sym, turnover, score, change_24h_pct,
            recent_volatility_pct, recent_range_pct, recent_move_pct)


def build_stable_universe():
    """Build a fresh, activity-ranked USDT perpetual universe every run."""
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

    print(
        f"[UNIVERSE] Liquid candidates={len(candidates)}; "
        f"ranking by 24h move + last 3h volatility/movement; "
        f"exchange={ACTIVE_EXCHANGE}",
        flush=True
    )

    scored = []
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as executor:
        futures = [
            executor.submit(_activity_score_candidate, item)
            for item in candidates
        ]
        for future in as_completed(futures):
            try:
                scored.append(future.result())
            except Exception as exc:
                print(
                    f"[UNIVERSE] candidate ranking failed: "
                    f"{type(exc).__name__}: {exc}",
                    flush=True
                )

    scored.sort(key=lambda x: (x[2], x[1]), reverse=True)
    selected = scored[:min(TOP_UNIVERSE, MAX_MONITORED)]

    print(
        f"[UNIVERSE] Selected={len(selected)} of {len(candidates)}; "
        f"top activity examples="
        + ", ".join(
            f"{x[0]}({x[2]:.2f})" for x in selected[:10]
        ),
        flush=True
    )

    return [
        (sym, turnover, score)
        for sym, turnover, score, *_ in selected
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
            "4h": "4H",
            "1d": "1D"
        }.get(interval)
        if bar is None:
            raise ValueError(f"Unsupported OKX interval: {interval}")

        rows = _okx_rows(
            inst_id,
            bar,
            limit
        )

        return _normalise_rows(rows, "okx")

    contract = _base(symbol) + "_USDT"
    gate_interval = {
        "5m": "5m",
        "4h": "4h",
        "1d": "1d"
    }.get(interval)
    if gate_interval is None:
        raise ValueError(f"Unsupported Gate.io interval: {interval}")

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
                valid_symbols[symbol] = valid_rows[-DAILY_CACHE_LIMIT:]

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

    download_limit = 30 if cached else DAILY_CACHE_LIMIT

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
        ][-DAILY_CACHE_LIMIT:]

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
# FOUR-HOUR CANDLES CACHE
# ============================================================

def load_four_hour_cache(exchange):
    global _four_hour_cache

    if not FOUR_HOUR_CACHE_FILE.exists():
        print(
            "[CACHE] four_hour_cache.json not found; "
            "4H history will be downloaded.",
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
                "[CACHE] 4H cache version/exchange mismatch; "
                "starting empty.",
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
                valid_symbols[symbol] = valid_rows[-FOUR_HOUR_CACHE_LIMIT:]

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

    download_limit = 30 if cached else FOUR_HOUR_CACHE_LIMIT
    try:
        fresh = get_bars(exchange, symbol, "4h", download_limit)
        merged = {int(row[0]): row for row in cached}
        for row in fresh:
            merged[int(row[0])] = row
        result = [merged[ts] for ts in sorted(merged)][-FOUR_HOUR_CACHE_LIMIT:]
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


def fb_compute(days, m5, now_ms=None, timeframe="1D", candle_seconds=86400):
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
        if candle_seconds == 86400:
            # Preserve the original daily alignment exactly.
            ds = (
                (m5[0]["time"] + candle_seconds - 1) // candle_seconds
            ) * candle_seconds
        else:
            # For 4H, include the candle containing the first 5m bar;
            # otherwise up to almost four hours of the 288-bar window is skipped.
            ds = (m5[0]["time"] // candle_seconds) * candle_seconds

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
                    "touch": b["time"],
                    "timeframe": timeframe
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
                    "timeframe": timeframe
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
        "seeded_symbols": []
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


# ============================================================
# TRADINGVIEW-STYLE ORDER BLOCK ENGINE AND PNG RENDERER
# ============================================================

def _compute_tv_order_blocks(bars, input_range=OB_INPUT_RANGE,
                              use_mitigated_blocks=OB_USE_MITIGATED_BLOCKS):
    """Port the supplied Pine v6 Order Blocks state machine bar-for-bar.

    Pine defaults are retained: close as both breaker sources, 25-bar structure,
    mitigated blocks disabled, and BOS lines enabled. Returned coordinates are
    candle indices in the full input history.
    """
    if len(bars) <= input_range + 1:
        return {"bullish": [], "bearish": [], "bos": []}

    long_boxes = []
    short_boxes = []
    bos_lines = []
    box_sequence = 0

    candle_colour_mode = 0
    bos_candle = False
    bullish_alert = False
    bearish_alert = False
    short_retest = False
    long_retest = False

    last_down_index = 0
    last_down = 0.0
    last_low = 0.0
    last_up_index = 0
    last_up = 0.0
    last_up_low = 0.0
    last_up_open = 0.0
    last_high = 0.0
    last_long_index = 0

    previous_structure_low = None
    previous_structure_high = None
    previous_close = None

    for i, candle in enumerate(bars):
        bos_candle = False
        bullish_alert = False
        bearish_alert = False
        short_retest = False
        long_retest = False

        if i < input_range:
            previous_close = float(candle["close"])
            continue

        # Pine: ta.lowest(low, inputRange)[1] / ta.highest(high, inputRange)[1]
        # and getStructure*Index() both inspect bars [i-inputRange, ..., i-1].
        lows_window = [float(bars[j]["low"]) for j in range(i-input_range, i)]
        highs_window = [float(bars[j]["high"]) for j in range(i-input_range, i)]
        structure_low = min(lows_window)
        structure_high = max(highs_window)
        structure_low_idx = next(
            j for j in range(i-input_range, i)
            if float(bars[j]["low"]) == structure_low
        )
        structure_high_idx = next(
            j for j in range(i-input_range, i)
            if float(bars[j]["high"]) == structure_high
        )

        close = float(candle["close"])
        high = float(candle["high"])
        low = float(candle["low"])
        op = float(candle["open"])

        # ta.crossunder(close, structureLow) / ta.crossover(close, structureHigh).
        crossed_down = (
            previous_close is not None
            and previous_structure_low is not None
            and close < structure_low
            and previous_close >= previous_structure_low
        )
        crossed_up = (
            previous_close is not None
            and previous_structure_high is not None
            and close > structure_high
            and previous_close <= previous_structure_high
        )

        # Bearish Break of Structure: box(left=lastUpIndex, top=lastHigh, bottom=lastUpLow).
        if crossed_down and (i - last_up_index) < 1000:
            short_boxes.append({
                "left": last_up_index,
                "start": i,
                "top": last_high,
                "bottom": last_up_low,
                "state": 0,
                "direction": "bearish",
                "sequence": box_sequence
            })
            box_sequence += 1
            if OB_SHOW_BEARISH_BOS:
                bos_lines.append({
                    "left": structure_low_idx,
                    "right": i,
                    "price": structure_low,
                    "direction": "bearish"
                })
            bos_candle = True
            candle_colour_mode = 0
            bearish_alert = True

        # Bullish Break of Structure: box(left=lastDownIndex, top=lastDown, bottom=lastLow).
        if crossed_up and (i - last_down_index) < 1000 and i > last_long_index:
            long_boxes.append({
                "left": last_down_index,
                "start": i,
                "top": last_down,
                "bottom": last_low,
                "state": 0,
                "direction": "bullish",
                "sequence": box_sequence
            })
            box_sequence += 1
            if OB_SHOW_BULLISH_BOS:
                bos_lines.append({
                    "left": structure_high_idx,
                    "right": i,
                    "price": structure_high,
                    "direction": "bullish"
                })
            bos_candle = True
            candle_colour_mode = 1
            last_long_index = i
            bullish_alert = True

        # Update short/bearish blocks in reverse insertion order, as Pine does.
        for j in range(len(short_boxes) - 1, -1, -1):
            block = short_boxes[j]
            b_top = block["top"]
            b_bottom = block["bottom"]
            if (high > b_bottom and low < b_bottom and i > block["start"]
                    and block["state"] == 0 and use_mitigated_blocks):
                block["state"] = 1
                short_retest = True
            if close > b_top:
                short_boxes.pop(j)

        # Update long/bullish blocks in reverse insertion order, as Pine does.
        for j in range(len(long_boxes) - 1, -1, -1):
            block = long_boxes[j]
            b_top = block["top"]
            b_bottom = block["bottom"]
            if (low < b_top and high > b_top and i > block["start"]
                    and block["state"] == 0 and use_mitigated_blocks):
                block["state"] = 1
                long_retest = True
            if close < b_bottom:
                long_boxes.pop(j)

        # Candle Tracking is intentionally after BOS and block updates, matching Pine.
        if close < op:
            last_down = high
            last_down_index = i
            last_low = low

        if close > op:
            last_up = close
            last_up_index = i
            last_up_open = op
            last_up_low = low
            last_high = high

        last_high = max(high, last_high)
        last_low = min(low, last_low)

        previous_close = close
        previous_structure_low = structure_low
        previous_structure_high = structure_high

    # TradingView's default max_boxes_count is 50 in total, not 50 per direction.
    active_boxes = sorted(long_boxes + short_boxes, key=lambda item: item["sequence"])
    active_boxes = active_boxes[-OB_MAX_DRAWN_BOXES:]
    return {
        "bullish": [item for item in active_boxes if item["direction"] == "bullish"],
        "bearish": [item for item in active_boxes if item["direction"] == "bearish"],
        "bos": bos_lines[-50:]
    }


def _png_chunk(chunk_type, data):
    body = chunk_type + data
    return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body) & 0xffffffff)


def render_orderblock_chart(bars, title, max_candles=90, width=1200, height=720):
    """Render candles, active OB zones and BOS lines using standard-library PNG."""
    if len(bars) < 2:
        raise ValueError("Not enough candles to render chart")

    ob = _compute_tv_order_blocks(bars)
    visible_start = max(0, len(bars) - max_candles)
    data = bars[visible_start:]
    if len(data) < 2:
        raise ValueError("Not enough visible candles to render chart")

    bg = (13, 19, 31)
    grid = (38, 49, 67)
    up = (20, 190, 130)
    down = (235, 76, 88)
    text_color = (215, 225, 238)
    pixels = bytearray(bg * (width * height))

    def pixel(x, y, color):
        if 0 <= x < width and 0 <= y < height:
            pos = (y * width + x) * 3
            pixels[pos:pos+3] = bytes(color)

    def line(x1, y1, x2, y2, color, thickness=1):
        dx = abs(x2-x1); sx = 1 if x1 < x2 else -1
        dy = -abs(y2-y1); sy = 1 if y1 < y2 else -1
        err = dx + dy
        while True:
            half = max(0, thickness // 2)
            for ox in range(-half, half+1):
                for oy in range(-half, half+1):
                    pixel(x1+ox, y1+oy, color)
            if x1 == x2 and y1 == y2:
                break
            e2 = 2 * err
            if e2 >= dy:
                err += dy; x1 += sx
            if e2 <= dx:
                err += dx; y1 += sy

    def rect(x1, y1, x2, y2, color):
        xa, xb = sorted((max(0, x1), min(width-1, x2)))
        ya, yb = sorted((max(0, y1), min(height-1, y2)))
        if xa > xb or ya > yb:
            return
        row = bytes(color) * (xb-xa+1)
        for yy in range(ya, yb+1):
            pos = (yy*width+xa)*3
            pixels[pos:pos+len(row)] = row

    def blend_rect(x1, y1, x2, y2, color, opacity):
        xa, xb = max(0, min(x1, x2)), min(width-1, max(x1, x2))
        ya, yb = max(0, min(y1, y2)), min(height-1, max(y1, y2))
        if xa > xb or ya > yb:
            return
        for yy in range(ya, yb+1):
            for xx in range(xa, xb+1):
                pos = (yy*width+xx)*3
                pixels[pos] = int(pixels[pos]*(1-opacity) + color[0]*opacity)
                pixels[pos+1] = int(pixels[pos+1]*(1-opacity) + color[1]*opacity)
                pixels[pos+2] = int(pixels[pos+2]*(1-opacity) + color[2]*opacity)

    left, right, top, bottom = 76, width-28, 62, height-55
    highs = [float(x["high"]) for x in data]
    lows = [float(x["low"]) for x in data]
    # Include OB prices in the visible scale so active zones are not clipped vertically.
    visible_blocks = []
    for direction in ("bullish", "bearish"):
        for block in ob[direction]:
            if block["left"] < len(bars) and block["top"] > 0 and block["bottom"] > 0:
                visible_blocks.append(block)
                if block["left"] < len(bars) and block["left"] >= visible_start:
                    highs.append(float(block["top"]))
                    lows.append(float(block["bottom"]))
                else:
                    highs.append(float(block["top"]))
                    lows.append(float(block["bottom"]))
    lo, hi = min(lows), max(highs)
    pad = (hi-lo)*0.07 if hi > lo else max(abs(hi)*0.02, 0.01)
    lo -= pad; hi += pad
    span = hi-lo or 1.0

    def yprice(value):
        return int(bottom - (value-lo)/span*(bottom-top))

    step = (right-left)/len(data)
    candle_w = max(3, min(11, int(step*0.58)))

    # Grid first, then translucent OB rectangles, then BOS and candles.
    for i in range(6):
        y = top + (bottom-top)*i//5
        line(left, y, right, y, grid)
    for i in range(9):
        x = left + (right-left)*i//8
        line(x, top, x, bottom, grid)

    bullish_count = 0
    bearish_count = 0
    for block in visible_blocks:
        block_left = max(block["left"], visible_start)
        x1 = int(left + (block_left-visible_start)*step)
        # Pine box.new(..., extend=extend.right): zone reaches the right edge.
        x2 = right
        y1 = yprice(block["top"])
        y2 = yprice(block["bottom"])
        if block["direction"] == "bullish":
            bullish_count += 1
            colour = (207, 203, 202) if block["state"] == 1 else (192, 230, 174)
            opacity = 0.20 if block["state"] == 1 else 0.40
        else:
            bearish_count += 1
            colour = (207, 203, 202) if block["state"] == 1 else (219, 166, 50)
            opacity = 0.20
        blend_rect(x1, min(y1, y2), x2, max(y1, y2), colour, opacity)

    bos_count = 0
    for bos in ob["bos"]:
        if bos["right"] < visible_start:
            continue
        x1 = int(left + (max(bos["left"], visible_start)-visible_start)*step)
        x2 = int(left + (bos["right"]-visible_start+0.5)*step)
        y = yprice(bos["price"])
        colour = (230, 74, 82) if bos["direction"] == "bearish" else (45, 190, 115)
        line(x1, y, x2, y, colour, 2)
        bos_count += 1

    for i, candle in enumerate(data):
        x = int(left + (i+0.5)*step)
        op = float(candle["open"]); cl = float(candle["close"])
        high = float(candle["high"]); low = float(candle["low"])
        colour = up if cl >= op else down
        line(x, yprice(high), x, yprice(low), colour, 2)
        y1, y2 = sorted((yprice(op), yprice(cl)))
        if y2-y1 < 2:
            y2 = y1+2
        rect(x-candle_w//2, y1, x+candle_w//2, y2, colour)

    last_price = float(data[-1]["close"])
    ylast = yprice(last_price)
    line(left, ylast, right, ylast, (235, 190, 70), 1)

    # PNG encoding; no external imaging package is required on GitHub Actions.
    raw = bytearray()
    stride = width*3
    for y in range(height):
        raw.append(0)
        raw.extend(pixels[y*stride:(y+1)*stride])
    png = b"\x89PNG\r\n\x1a\n"
    png += _png_chunk(b"IHDR", struct.pack(">2I5B", width, height, 8, 2, 0, 0, 0))
    png += _png_chunk(b"IDAT", zlib.compress(bytes(raw), 6))
    png += _png_chunk(b"IEND", b"")
    return png, {
        "bullish": bullish_count,
        "bearish": bearish_count,
        "bos_lines": bos_count
    }


def send_telegram(sym, event, atr_pct):
    """Send a false-breakout alert with its timeframe chart and Pine-equivalent OBs."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("[ERROR] Telegram credentials are missing", flush=True)
        return False

    side = (
        "ЛОЖНЫЙ ПРОБОЙ ВВЕРХ 🔴"
        if event["side"] == "H"
        else "ЛОЖНЫЙ ПРОБОЙ ВНИЗ 🟢"
    )
    timeframe = event.get("timeframe", "1D")
    timeframe_label = "1D — Дневной" if timeframe == "1D" else "4H — Четырёхчасовой"
    dt = time.strftime("%d.%m.%Y %H:%M UTC", time.gmtime(event["touch"]))
    caption = (
        "🟡 <b>FALSE BREAKOUT SCANNER</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"Монета: <b>{html.escape(sym)}</b>\n"
        f"Сигнал: <b>{side}</b>\n"
        f"Таймфрейм уровня: <b>{html.escape(timeframe_label)}</b>\n"
        f"Уровень: <code>{event['price']:.12g}</code>\n"
        f"Время свечи: {dt}\n"
        f"ATR: {atr_pct:.2f}%\n"
        f"Биржа: {ACTIVE_EXCHANGE.upper()}\n"
        "━━━━━━━━━━━━━━\n"
        "На графике: ордерблоки и BOS по алгоритму Pine Script v6"
    )

    try:
        if timeframe == "1D":
            with _daily_cache_lock:
                raw = [row[:] for row in _daily_cache["symbols"].get(sym, [])]
            interval = "1d"
            visible_count = CHART_DAILY_CANDLES
        else:
            with _four_hour_cache_lock:
                raw = [row[:] for row in _four_hour_cache["symbols"].get(sym, [])]
            interval = "4h"
            visible_count = CHART_4H_CANDLES

        # Fetch a deeper chart-only history for the OB state machine. The scanner's
        # signal caches are intentionally shorter; using only those would omit older
        # still-active TradingView-style blocks. Gate.io supports up to 2000 bars and
        # OKX is paginated by get_bars(). If the chart fetch fails, fall back to cache.
        try:
            chart_raw = get_bars(
                ACTIVE_EXCHANGE, sym, interval, CHART_OB_HISTORY_CANDLES
            )
            if len(chart_raw) >= OB_INPUT_RANGE + 2:
                raw = chart_raw
        except Exception as chart_exc:
            print(
                f"[CHART] {sym} {timeframe}: deep history unavailable; "
                f"using cache ({type(chart_exc).__name__}: {chart_exc})",
                flush=True
            )

        if len(raw) < OB_INPUT_RANGE + 2:
            raise RuntimeError(
                f"not enough candles to calculate order blocks: {len(raw)}"
            )

        bars = _bars_to_dicts(raw)
        png_bytes, ob_stats = render_orderblock_chart(
            bars,
            title=f"{sym} {timeframe}",
            max_candles=visible_count
        )

        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        response = requests.post(
            url,
            data={
                "chat_id": chat_id,
                "caption": caption,
                "parse_mode": "HTML"
            },
            files={"photo": (f"{sym}_{timeframe}_orderblocks.png", png_bytes, "image/png")},
            timeout=60
        )
        response.raise_for_status()
        result = response.json()
        if result.get("ok") is True:
            print(
                f"[TELEGRAM] Sent chart+signal: {sym} {timeframe}; "
                f"bullish_OB={ob_stats['bullish']}; bearish_OB={ob_stats['bearish']}; "
                f"BOS={ob_stats['bos_lines']}",
                flush=True
            )
            return True
        print("[ERROR] Telegram API rejected chart: " + str(result.get("description", "unknown error")), flush=True)
        return False

    except Exception as exc:
        print(
            f"[ERROR] Chart/Telegram delivery failed for {sym} {timeframe}: "
            f"{type(exc).__name__}: {exc}",
            flush=True
        )
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

def _bars_to_dicts(raw):
    return [
        {
            "time": int(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5])
        }
        for k in raw
    ]


def _atr_percent(bars):
    highs = [d["high"] for d in bars[:-1]]
    lows = [d["low"] for d in bars[:-1]]
    closes = [d["close"] for d in bars[:-1]]
    atr_value = wilder_atr(highs, lows, closes, 14)
    last_close = bars[-1]["close"] if bars else 0.0
    return (
        atr_value / last_close * 100.0
        if atr_value and last_close > 0
        else 0.0
    )


def process_symbol(exchange, sym, is_new_symbol, seen_snapshot):
    """Process daily and 4H levels; evaluate both against the same 288 5m bars."""
    try:
        days_raw = get_cached_daily_bars(exchange, sym)
        try:
            four_hours_raw = get_cached_four_hour_bars(exchange, sym)
        except Exception as exc:
            print(
                f"[WARN] {sym}: 4H candles unavailable; daily scan continues. "
                f"{type(exc).__name__}: {exc}",
                flush=True
            )
            four_hours_raw = []
        m5_raw = get_bars(exchange, sym, "5m", 288)

        days = _bars_to_dicts(days_raw)
        four_hours = _bars_to_dicts(four_hours_raw)
        m5 = _bars_to_dicts(m5_raw)

        if len(days) < 60 or len(m5) < 30:
            return (
                sym, [], [], False,
                f"insufficient history (daily={len(days)}, 4H={len(four_hours)}, 5m={len(m5)})"
            )

        daily_result = fb_compute(
            days, m5, timeframe="1D", candle_seconds=86400
        )

        four_hour_result = {"levels": [], "events": []}
        if len(four_hours) >= 60:
            four_hour_result = fb_compute(
                four_hours, m5, timeframe="4H", candle_seconds=14400
            )
        else:
            print(
                f"[WARN] {sym}: not enough 4H history ({len(four_hours)} candles); "
                "4H levels skipped for this run.",
                flush=True
            )

        discovered = []
        seed_keys = []
        all_events = [
            (event, _atr_percent(days))
            for event in daily_result["events"]
        ]
        if four_hour_result["events"]:
            atr_4h_pct = _atr_percent(four_hours)
            all_events.extend(
                (event, atr_4h_pct)
                for event in four_hour_result["events"]
            )

        for event, atr_pct in all_events:
            key = (
                f"{sym}|{event.get('timeframe', '1D')}|{event['side']}|"
                f"{event['time']}|{event['price']:.12g}|{event['touch']}"
            )

            if is_new_symbol:
                seed_keys.append(key)
            elif key not in seen_snapshot:
                discovered.append((sym, event, atr_pct, key))

        return sym, discovered, seed_keys, True, None

    except Exception as exc:
        return (
            sym, [], [], False,
            f"{type(exc).__name__}: {exc}"
        )


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

    # Load independent daily and 4H caches for this scanner project.
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

    # Save both timeframe caches after all workers have finished.
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
            f"SIGNAL | {sym} | {event.get('timeframe', '1D')} | {side} | "
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
        f"exchange={exchange}; "
        f"elapsed={elapsed:.1f}s",
        flush=True
    )


if __name__ == "__main__":
    main()
