# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the trackers

**Terminal UI (Rich TUI):**
```bash
python3 btc_tracker.py
```
Auto-installs `aiohttp`, `websockets`, `rich` on first run. Quit with Ctrl+C.

**Web tracker:**
```bash
python3 web_tracker.py
# then open http://localhost:8080
```
Auto-installs `aiohttp`, `websockets`, `fastapi`, `uvicorn` on first run.

**Install deps manually:**
```bash
pip install -r requirements.txt
```

**Debug endpoint (web tracker only):**
```
http://localhost:8080/debug
http://localhost:8080/api/state
```

## Architecture

There are two separate, self-contained applications that share the same conceptual data model but no code:

### `btc_tracker.py` — Terminal UI

Runs a single `asyncio.gather()` with these concurrent coroutines:
- `binance_feed()` / `kraken_feed()` / `okx_feed()` / `bybit_feed()` — WebSocket trade streams writing into global `btc` / `btc_ts` dicts
- `coinbase_feed(session)` — REST poll every 3s
- `pm_loop(session)` — discovers active Polymarket 5-min BTC markets (via Gamma API) then fetches CLOB prices every 2s; re-discovers every 30s
- `render_loop(live)` — calls `build_ui()` every 0.5s and updates the Rich `Live` display

Market state is held in a single global `pm` dict. The UI is built in `build_ui()` which assembles a Rich `Layout` with a left panel (exchange prices table) and right panel (Polymarket UP/DOWN data).

### `web_tracker.py` + `index.html` — Web UI

FastAPI app launched via `uvicorn`. Background tasks run inside the `lifespan` context manager. Same exchange feed coroutines as the terminal version plus:
- `pm_loop(session)` — more complex than the terminal version: tracks markets by **5-minute window slug** (`btc-updown-5m-{unix_ts_rounded_to_300s}`), fetches via Gamma's `/events?slug=…` endpoint
- `broadcast_loop()` — pushes full state JSON to connected WebSocket clients every 200ms

**Price-to-beat hierarchy** (applied both server-side and in frontend):
1. `candle_ref_price` — Polymarket's official Chainlink BTC/USD closing price fetched from `/api/past-results` (authoritative but only available after resolution; retried every cycle)
2. `candle_kline` — Binance 5m kline open price for the window start (available immediately at T+0; fetched via Binance REST klines API)
3. `candle_open` — live BTC average snapshot taken at window open (immediate fallback)

**Frontend data flow** (`index.html`):
- Main data comes from HTTP polling `/api/state` every 400ms (the `poll()` function)
- A fast 100ms `setInterval` (`fastUpdate()`) updates only the delta and candle direction bar in-place without full DOM rebuild
- The WebSocket endpoint (`/ws`) exists server-side but the frontend does **not** currently use it for data — the `ws_clients` broadcast is dead code in the current frontend

## Key external APIs

| API | Purpose |
|-----|---------|
| `gamma-api.polymarket.com/events?slug=…` | Fetch current 5-min BTC window by slug |
| `clob.polymarket.com/midpoint` | CLOB midpoint price for a token |
| `clob.polymarket.com/price?side=SELL/BUY` | Best bid/ask |
| `polymarket.com/api/past-results` | Chainlink ref price (authoritative close price) |
| `api.binance.com/api/v3/klines` | 5m kline open for window start time |
| Exchange WebSocket URIs | Binance, Kraken, OKX, Bybit trade streams |
| `api.coinbase.com/v2/prices/BTC-USD/spot` | Coinbase REST spot price |

## Important conventions

- All times displayed in **GET (Asia/Tbilisi, UTC+4)** — hardcoded as `TZ = 'Asia/Tbilisi'` in the frontend.
- Window slugs follow the pattern `btc-updown-5m-{unix_ts}` where `unix_ts = (int(time.time()) // 300) * 300`.
- Prices from Polymarket CLOB are **0–1 probabilities**, not dollar amounts. UP + DOWN ≈ 1.00.
- Exchange feeds reconnect automatically on any exception with a 3s sleep.
- The `candle_snap` dict captures per-exchange prices at window open; `candle_snap_slug` tracks which window the snapshot belongs to so it resets correctly on each new 5-minute window.
