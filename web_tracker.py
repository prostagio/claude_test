#!/usr/bin/env python3
"""
Polymarket BTC UP/DOWN Web Tracker
Run: python3 web_tracker.py  →  open http://localhost:8080
"""

import asyncio
import json
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# ── Dependency bootstrap ──────────────────────────────────────────────────────
try:
    import aiohttp
    import websockets
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse
    import uvicorn
except ImportError:
    import subprocess, os
    print("Installing: aiohttp, websockets, fastapi, uvicorn …")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q",
         "aiohttp>=3.9", "websockets>=12", "fastapi>=0.109", "uvicorn>=0.27"],
        check=True,
    )
    os.execv(sys.executable, [sys.executable] + sys.argv)

# ── Constants ─────────────────────────────────────────────────────────────────
EXCHANGES = ["Binance", "Coinbase", "Kraken", "OKX", "Bybit"]
GAMMA     = "https://gamma-api.polymarket.com"
CLOB      = "https://clob.polymarket.com"
WINDOW    = 300  # 5-minute window in seconds

# ── Shared state ──────────────────────────────────────────────────────────────
btc:    Dict[str, Optional[float]] = {e: None for e in EXCHANGES}
btc_ts: Dict[str, Optional[float]] = {e: None for e in EXCHANGES}

markets_state: List[Dict[str, Any]] = []
pm_status:  str = "Searching…"
pm_last_ok: Optional[float] = None

candle_opens:     Dict[str, float] = {}          # slug → btc avg snapshot when window opened
candle_snap:      Dict[str, Optional[float]] = {e: None for e in EXCHANGES}  # exchange → price at window open
candle_snap_slug: str = ""                       # which window the snap belongs to
candle_kline:     Dict[str, Optional[float]] = {}  # slug → Binance 5m kline open (immediate fallback)
candle_ref_cache: Dict[str, float] = {}          # slug → Polymarket Chainlink ref price (cached once found)
_server_start:    float = time.time()
ws_clients: Set[WebSocket] = set()


# ── Helpers ───────────────────────────────────────────────────────────────────

def current_window_ts() -> int:
    return (int(time.time()) // WINDOW) * WINDOW


def window_slug(ts: int) -> str:
    return f"btc-updown-5m-{ts}"


def btc_avg() -> Optional[float]:
    vals = [v for v in btc.values() if v is not None]
    return sum(vals) / len(vals) if vals else None


# ── Polymarket data ───────────────────────────────────────────────────────────

async def fetch_event(session: aiohttp.ClientSession, ts: int) -> Optional[Dict]:
    try:
        async with session.get(
            f"{GAMMA}/events",
            params={"slug": window_slug(ts)},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()
        events = data if isinstance(data, list) else [data]
        return events[0] if events else None
    except Exception:
        return None


async def fetch_candle_ref_live(
    session: aiohttp.ClientSession,
    event_start_iso: str,
    event_end_iso: str,
) -> Optional[float]:
    """Fetch price-to-beat live from Polymarket's official past-results API (no caching).
    API returns resolved markets with endTime < currentEventStartTime.
    The price-to-beat = closePrice of the window ending at event_start_iso (Chainlink BTC/USD).
    Tries multiple currentEventStartTime offsets since Polymarket resolves markets asynchronously."""
    end_dt = datetime.fromisoformat(event_end_iso.replace("Z", "+00:00"))

    # Try increasing offsets: event_end, event_end+5m, event_end+10m
    for extra in (0, WINDOW, WINDOW * 2):
        future_iso = datetime.fromtimestamp(
            end_dt.timestamp() + extra, tz=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            async with session.get(
                "https://polymarket.com/api/past-results",
                params={"symbol": "BTC", "variant": "fiveminute", "assetType": "crypto",
                        "currentEventStartTime": future_iso},
                timeout=aiohttp.ClientTimeout(total=6),
            ) as r:
                d = await r.json()
            results = (d.get("data") or {}).get("results") or []
            for entry in reversed(results):
                entry_end = entry.get("endTime", "").replace(".000Z", "Z")
                if entry_end.startswith(event_start_iso[:16]):
                    return float(entry["closePrice"])
        except Exception:
            pass
    return None


async def fetch_binance_window_open(
    session: aiohttp.ClientSession,
    window_start_iso: str,
) -> Optional[float]:
    """Fetch the Binance 5m kline open price for the exact window start time.
    This is available immediately at T+0 and is a good proxy for Chainlink price."""
    try:
        st_ms = int(
            datetime.fromisoformat(window_start_iso.replace("Z", "+00:00")).timestamp() * 1000
        )
        async with session.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "5m", "startTime": st_ms, "limit": 1},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            data = await r.json()
        if data and len(data[0]) > 1:
            return float(data[0][1])  # index 1 = open price
    except Exception:
        pass
    return None


def parse_event(event: Dict, window_ts: int) -> Dict[str, Any]:
    market = (event.get("markets") or [{}])[0]

    try:
        tids = json.loads(market.get("clobTokenIds") or "[]")
    except Exception:
        tids = []

    try:
        op = json.loads(market.get("outcomePrices") or "[]")
    except Exception:
        op = []

    # Derive start/end from window_ts if API doesn't return them
    fallback_start = datetime.fromtimestamp(window_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    fallback_end   = datetime.fromtimestamp(window_ts + WINDOW, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "slug":        window_slug(window_ts),
        "title":       event.get("title") or market.get("question") or "",
        "start_time":  market.get("eventStartTime") or event.get("startDate") or fallback_start,
        "end_time":    market.get("endDate") or event.get("endDate") or fallback_end,
        "up_token":    tids[0] if len(tids) > 0 else None,
        "dn_token":    tids[1] if len(tids) > 1 else None,
        "up_price":    float(op[0]) if op else None,
        "dn_price":    float(op[1]) if len(op) > 1 else None,
        "up_bid":      market.get("bestBid"),
        "up_ask":      market.get("bestAsk"),
        "dn_bid":      None,
        "dn_ask":      None,
        "candle_open":      None,
        "candle_ref_price": None,
        "active":           event.get("active", False),
    }


async def enrich_with_clob(session: aiohttp.ClientSession, mkt: Dict) -> None:
    """Overwrite Gamma prices with live CLOB midpoint + bid/ask."""
    t = aiohttp.ClientTimeout(total=5)

    async def mid(tid: str) -> Optional[float]:
        try:
            async with session.get(
                f"{CLOB}/midpoint", params={"token_id": tid}, timeout=t
            ) as r:
                d = await r.json()
                v = d.get("mid")
                return float(v) if v is not None else None
        except Exception:
            return None

    async def best(tid: str, side: str) -> Optional[float]:
        try:
            async with session.get(
                f"{CLOB}/price", params={"token_id": tid, "side": side}, timeout=t
            ) as r:
                d = await r.json()
                v = d.get("price")
                return float(v) if v is not None else None
        except Exception:
            return None

    up, dn = mkt.get("up_token"), mkt.get("dn_token")
    coros, keys = [], []
    if up:
        coros += [mid(up), best(up, "SELL"), best(up, "BUY")]
        keys  += ["up_price", "up_bid", "up_ask"]
    if dn:
        coros += [mid(dn), best(dn, "SELL"), best(dn, "BUY")]
        keys  += ["dn_price", "dn_bid", "dn_ask"]

    if not coros:
        return

    results = await asyncio.gather(*coros)
    for k, v in zip(keys, results):
        if v is not None:
            mkt[k] = v


async def pm_loop(session: aiohttp.ClientSession) -> None:
    global markets_state, pm_status, pm_last_ok, candle_snap, candle_snap_slug

    while True:
        now_ts = current_window_ts()
        timestamps = [now_ts]

        raw = await asyncio.gather(
            *[fetch_event(session, ts) for ts in timestamps],
            return_exceptions=True,
        )

        new_markets: List[Dict] = []
        now_utc = datetime.now(timezone.utc)

        for ts, event in zip(timestamps, raw):
            if isinstance(event, Exception) or event is None:
                continue
            mkt = parse_event(event, ts)
            slug = mkt["slug"]

            # Snapshot per-exchange prices at window open
            if mkt.get("start_time"):
                try:
                    st = datetime.fromisoformat(
                        mkt["start_time"].replace("Z", "+00:00")
                    )
                    secs_in = (now_utc - st).total_seconds()
                    if st <= now_utc:
                        if slug != candle_snap_slug:
                            # New window: reset snap
                            candle_snap = {e: btc.get(e) for e in EXCHANGES}
                            candle_snap_slug = slug
                        # Fill in missing entries: within 30s of window open OR 30s of server start
                        fresh = (time.time() - _server_start) <= 30
                        if any(candle_snap[e] is None for e in EXCHANGES) and (secs_in <= 30 or fresh):
                            for e in EXCHANGES:
                                if candle_snap[e] is None and btc.get(e) is not None:
                                    candle_snap[e] = btc[e]
                        if slug not in candle_opens:
                            avg = btc_avg()
                            if avg:
                                candle_opens[slug] = avg
                except Exception:
                    pass

            mkt["candle_open"]     = candle_opens.get(slug)
            mkt["candle_ref_price"] = None  # filled below: Polymarket Chainlink (live)
            mkt["candle_kline"]     = candle_kline.get(slug)  # Binance 5m open (immediate fallback)
            new_markets.append(mkt)

        if new_markets:
            cur  = new_markets[0]
            slug = cur["slug"]
            try:
                st = datetime.fromisoformat(
                    (cur.get("start_time") or "").replace("Z", "+00:00")
                )
                if st <= now_utc and cur.get("start_time") and cur.get("end_time"):
                    # Fetch Binance 5m kline open — retry every cycle until we get a real value
                    if candle_kline.get(slug) is None:
                        kline = await fetch_binance_window_open(session, cur["start_time"])
                        if kline is not None:
                            candle_kline[slug] = kline
                    cur["candle_kline"]     = candle_kline.get(slug)
                    cur["candle_ref_price"] = candle_ref_cache.get(slug)

                    # Publish early so UI shows kline price-to-beat without waiting for slow ref fetch
                    markets_state = new_markets

                    # Fetch Polymarket Chainlink ref — retry every cycle until found
                    if candle_ref_cache.get(slug) is None:
                        ref = await fetch_candle_ref_live(session, cur["start_time"], cur["end_time"])
                        if ref is not None:
                            candle_ref_cache[slug] = ref
                            cur["candle_ref_price"] = ref
            except Exception:
                pass

            # Enrich current window with live CLOB prices
            await enrich_with_clob(session, cur)
            markets_state = new_markets
            pm_status  = f"Live – {len(new_markets)} window(s) tracked"
            pm_last_ok = time.time()
        else:
            pm_status = "No markets found – retrying…"

        await asyncio.sleep(2)


# ── Exchange WebSocket / REST feeds ───────────────────────────────────────────

async def binance_feed() -> None:
    uri = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20) as ws:
                async for raw in ws:
                    d = json.loads(raw)
                    btc["Binance"] = float(d["p"])
                    btc_ts["Binance"] = time.time()
        except Exception:
            await asyncio.sleep(3)


async def coinbase_feed(session: aiohttp.ClientSession) -> None:
    while True:
        try:
            async with session.get(
                "https://api.coinbase.com/v2/prices/BTC-USD/spot",
                timeout=aiohttp.ClientTimeout(total=6),
            ) as r:
                d = await r.json()
                btc["Coinbase"] = float(d["data"]["amount"])
                btc_ts["Coinbase"] = time.time()
        except Exception:
            pass
        await asyncio.sleep(3)


async def kraken_feed() -> None:
    uri = "wss://ws.kraken.com"
    sub = json.dumps(
        {"event": "subscribe", "pair": ["XBT/USD"], "subscription": {"name": "trade"}}
    )
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20) as ws:
                await ws.send(sub)
                async for raw in ws:
                    d = json.loads(raw)
                    if isinstance(d, list) and len(d) == 4 and d[2] == "trade":
                        tr = d[1]
                        if tr:
                            btc["Kraken"] = float(tr[-1][0])
                            btc_ts["Kraken"] = time.time()
        except Exception:
            await asyncio.sleep(3)


async def okx_feed() -> None:
    uri = "wss://ws.okx.com:8443/ws/v5/public"
    sub = json.dumps(
        {"op": "subscribe", "args": [{"channel": "trades", "instId": "BTC-USDT"}]}
    )
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20) as ws:
                await ws.send(sub)
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("arg", {}).get("channel") == "trades":
                        td = d.get("data", [])
                        if td:
                            btc["OKX"] = float(td[-1]["px"])
                            btc_ts["OKX"] = time.time()
        except Exception:
            await asyncio.sleep(3)


async def bybit_feed() -> None:
    uri = "wss://stream.bybit.com/v5/public/spot"
    sub = json.dumps({"op": "subscribe", "args": ["publicTrade.BTCUSDT"]})
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20) as ws:
                await ws.send(sub)
                async for raw in ws:
                    d = json.loads(raw)
                    if d.get("topic") == "publicTrade.BTCUSDT":
                        td = d.get("data", [])
                        if td:
                            btc["Bybit"] = float(td[-1]["p"])
                            btc_ts["Bybit"] = time.time()
        except Exception:
            await asyncio.sleep(3)


# ── Broadcast loop ────────────────────────────────────────────────────────────

async def broadcast_loop() -> None:
    while True:
        if ws_clients:
            payload = json.dumps({
                "btc":         dict(btc),
                "btc_ts":      dict(btc_ts),
                "candle_snap": dict(candle_snap),
                "markets":     markets_state,
                "status":      pm_status,
                "last_ok":     pm_last_ok,
                "ts":          time.time(),
            }, default=str)
            dead: Set[WebSocket] = set()
            for client in ws_clients:
                try:
                    await client.send_text(payload)
                except Exception:
                    dead.add(client)
            ws_clients -= dead
        await asyncio.sleep(0.2)


# ── FastAPI app ───────────────────────────────────────────────────────────────

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=20),
        headers={"User-Agent": UA},
    ) as session:
        tasks = [
            asyncio.create_task(broadcast_loop()),
            asyncio.create_task(pm_loop(session)),
            asyncio.create_task(binance_feed()),
            asyncio.create_task(coinbase_feed(session)),
            asyncio.create_task(kraken_feed()),
            asyncio.create_task(okx_feed()),
            asyncio.create_task(bybit_feed()),
        ]
        yield
        for t in tasks:
            t.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse((Path(__file__).parent / "index.html").read_text())


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except Exception:
        pass
    finally:
        ws_clients.discard(websocket)


@app.get("/api/state")
async def api_state():
    now = time.time()
    return {
        "btc":         dict(btc),
        "btc_ts":      dict(btc_ts),
        "candle_snap": dict(candle_snap),
        "markets":     markets_state,
        "status":      pm_status,
        "last_ok":     pm_last_ok,
        "ts":          now,
    }


@app.get("/debug")
async def debug():
    now = time.time()
    m0 = markets_state[0] if markets_state else None
    return {
        "btc": {k: round(v, 2) if v else None for k, v in btc.items()},
        "pm_status": pm_status,
        "pm_last_ok_secs": round(now - pm_last_ok, 1) if pm_last_ok else None,
        "markets": len(markets_state),
        "current_market": m0,
        "candle_kline": {k: round(v, 2) if v else None for k, v in candle_kline.items()},
        "candle_ref_cache": {k: round(v, 2) if v else None for k, v in candle_ref_cache.items()},
        "slug": m0["slug"] if m0 else None,
        "start_time": m0["start_time"] if m0 else None,
        "end_time": m0["end_time"] if m0 else None,
    }


if __name__ == "__main__":
    print("\n  BTC Tracker starting…")
    print("  Open: http://localhost:8080\n")
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="warning")
