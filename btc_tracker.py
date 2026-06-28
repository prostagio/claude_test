#!/usr/bin/env python3
"""
Polymarket 5-Min BTC Tracker
Real-time BTC prices from multiple exchanges + Polymarket UP/DOWN prediction market.
"""

import asyncio
import json
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# ── Dependency bootstrap ──────────────────────────────────────────────────────
try:
    import aiohttp
    import websockets
    from rich import box
    from rich.console import Console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
except ImportError:
    import os
    import subprocess
    print("Installing dependencies (aiohttp, websockets, rich)...")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q",
         "aiohttp>=3.9", "websockets>=12", "rich>=13"],
        check=True,
    )
    print("Done. Restarting...\n")
    os.execv(sys.executable, [sys.executable] + sys.argv)


# ── Shared state ──────────────────────────────────────────────────────────────

EXCHANGES = ["Binance", "Coinbase", "Kraken", "OKX", "Bybit"]

# BTC spot prices per exchange
btc: Dict[str, Optional[float]] = {e: None for e in EXCHANGES}
btc_ts: Dict[str, Optional[float]] = {e: None for e in EXCHANGES}

# Polymarket market state
pm: Dict[str, Any] = {
    "status":      "Searching...",
    "question":    None,
    "target":      None,
    "end_time":    None,
    "up_price":    None,   # YES/UP midpoint (0–1)
    "up_bid":      None,
    "up_ask":      None,
    "dn_price":    None,   # NO/DOWN midpoint (0–1)
    "dn_bid":      None,
    "dn_ask":      None,
    "up_token":    None,   # Polymarket token_id for YES
    "dn_token":    None,   # Polymarket token_id for NO
    "n_markets":   0,
    "last_ok":     None,
}

console = Console()


# ── Polymarket discovery ──────────────────────────────────────────────────────

async def pm_discover(session: aiohttp.ClientSession) -> None:
    """Find active 5-minute BTC markets on Polymarket."""
    gamma = "https://gamma-api.polymarket.com"
    candidates = []

    search_sets = [
        {"active": "true", "closed": "false", "tag": "bitcoin", "limit": "100"},
        {"active": "true", "closed": "false", "tag": "crypto",  "limit": "100"},
        {"active": "true", "closed": "false",                   "limit": "100"},
    ]

    for params in search_sets:
        try:
            async with session.get(
                f"{gamma}/markets",
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status != 200:
                    continue
                raw = await r.json()

            markets = raw if isinstance(raw, list) else raw.get("markets", [])

            for m in markets:
                if not isinstance(m, dict):
                    continue
                q    = (m.get("question") or m.get("title") or "").lower()
                slug = (m.get("slug") or "").lower()

                is_btc = "btc" in q or "bitcoin" in q or "btc" in slug
                is_5m  = (
                    "5 min" in q or "5-min" in q or "5min" in q
                    or "5 minute" in q or "5-minute" in q
                    or ("5" in slug and "min" in slug)
                )

                if is_btc and is_5m:
                    candidates.append(m)

        except Exception as e:
            pm["status"] = f"Discover error: {e}"

    pm["n_markets"] = len(candidates)

    if not candidates:
        pm["status"] = "No active 5-min BTC markets found"
        return

    # Pick the market expiring soonest but still in the future
    now = datetime.now(timezone.utc)
    best = None
    best_end = None

    for m in candidates:
        end_raw = m.get("endDate") or m.get("end_date_iso") or m.get("end_date")
        if end_raw:
            try:
                end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
                if end_dt > now:
                    if best is None or end_dt < best_end:
                        best = m
                        best_end = end_dt
            except Exception:
                pass

    if best is None:
        best = candidates[0]

    pm["question"] = best.get("question") or best.get("title")
    pm["end_time"] = best.get("endDate") or best.get("end_date_iso") or best.get("end_date")
    pm["up_token"] = None
    pm["dn_token"] = None

    for tok in best.get("tokens") or []:
        outcome = (tok.get("outcome") or "").upper().strip()
        tid = tok.get("token_id") or tok.get("tokenId")
        if outcome in ("YES", "UP"):
            pm["up_token"] = tid
        elif outcome in ("NO", "DOWN"):
            pm["dn_token"] = tid

    # Extract strike price from question text
    if pm["question"]:
        m_price = re.search(r"\$\s*([\d,]+(?:\.\d+)?)", pm["question"])
        if m_price:
            pm["target"] = float(m_price.group(1).replace(",", ""))

    pm["status"] = f"{len(candidates)} market(s) found"


# ── Polymarket price fetching ─────────────────────────────────────────────────

async def pm_fetch_prices(session: aiohttp.ClientSession) -> None:
    """Fetch midpoint and best bid/ask for UP and DOWN tokens."""
    clob = "https://clob.polymarket.com"
    t = aiohttp.ClientTimeout(total=5)

    async def mid(token_id: str) -> Optional[float]:
        try:
            async with session.get(
                f"{clob}/midpoint", params={"token_id": token_id}, timeout=t
            ) as r:
                d = await r.json()
                v = d.get("mid")
                return float(v) if v is not None else None
        except Exception:
            return None

    async def best_price(token_id: str, side: str) -> Optional[float]:
        # side="SELL" → best bid; side="BUY" → best ask
        try:
            async with session.get(
                f"{clob}/price",
                params={"token_id": token_id, "side": side},
                timeout=t,
            ) as r:
                d = await r.json()
                v = d.get("price")
                return float(v) if v is not None else None
        except Exception:
            return None

    up = pm["up_token"]
    dn = pm["dn_token"]
    if not up and not dn:
        return

    coros, keys = [], []
    if up:
        coros += [mid(up), best_price(up, "SELL"), best_price(up, "BUY")]
        keys  += ["up_price", "up_bid", "up_ask"]
    if dn:
        coros += [mid(dn), best_price(dn, "SELL"), best_price(dn, "BUY")]
        keys  += ["dn_price", "dn_bid", "dn_ask"]

    results = await asyncio.gather(*coros)
    for k, v in zip(keys, results):
        pm[k] = v

    pm["last_ok"] = time.time()


async def pm_loop(session: aiohttp.ClientSession) -> None:
    discover_at = 0.0
    while True:
        if time.time() >= discover_at:
            await pm_discover(session)
            discover_at = time.time() + 30

        if pm["up_token"] or pm["dn_token"]:
            await pm_fetch_prices(session)

        await asyncio.sleep(2)


# ── Exchange feeds ────────────────────────────────────────────────────────────

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
                        trades = d[1]
                        if trades:
                            btc["Kraken"] = float(trades[-1][0])
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


# ── Rendering ─────────────────────────────────────────────────────────────────

def fmt_time_left(end_iso: Optional[str]) -> str:
    if not end_iso:
        return "--"
    try:
        end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        secs = int((end - datetime.now(timezone.utc)).total_seconds())
        if secs < 0:
            return "[red]EXPIRED[/red]"
        m, s = divmod(secs, 60)
        h, m = divmod(m, 60)
        if h:
            return f"{h}h {m:02d}m {s:02d}s"
        if m:
            return f"[yellow]{m}m {s:02d}s[/yellow]"
        return f"[bold red]{s}s !!![/bold red]"
    except Exception:
        return "?"


def build_ui() -> Layout:
    now = time.time()

    # ── BTC prices table ──────────────────────────────────────────────────────
    price_tbl = Table(
        title="[bold cyan]BTC / USD – Live Prices[/bold cyan]",
        box=box.ROUNDED,
        border_style="cyan",
        expand=True,
    )
    price_tbl.add_column("Exchange", style="bold", no_wrap=True)
    price_tbl.add_column("Price (USD)", justify="right")
    price_tbl.add_column("Age", justify="right", style="dim")
    price_tbl.add_column("", justify="center", width=2)

    valid_prices = []
    for ex in EXCHANGES:
        p  = btc[ex]
        ts = btc_ts[ex]
        if p is None:
            price_tbl.add_row(ex, "[dim]connecting…[/dim]", "--", "[dim]○[/dim]")
        else:
            valid_prices.append(p)
            age = now - ts
            col = "green" if age < 3 else ("yellow" if age < 10 else "red")
            price_tbl.add_row(
                ex,
                f"[{col}]${p:,.2f}[/{col}]",
                f"{age:.1f}s",
                f"[{col}]●[/{col}]",
            )

    if valid_prices:
        avg    = sum(valid_prices) / len(valid_prices)
        spread = max(valid_prices) - min(valid_prices)
        price_tbl.add_section()
        price_tbl.add_row("[bold]Average[/bold]",  f"[bold]${avg:,.2f}[/bold]", "", "")
        price_tbl.add_row("[dim]Ex-spread[/dim]",  f"[dim]${spread:,.2f}[/dim]", "", "")

    # ── Polymarket panel ──────────────────────────────────────────────────────
    text = Text()

    # Status
    text.append("Status: ", style="dim")
    text.append(f"{pm['status']}\n\n", style="bold white")

    # Question (truncated)
    if pm["question"]:
        q = pm["question"]
        if len(q) > 64:
            q = q[:61] + "…"
        text.append("Question:\n", style="dim")
        text.append(f"  {q}\n\n", style="italic white")

    # Strike price vs current BTC
    if pm["target"] is not None:
        text.append("Strike Price: ", style="dim")
        text.append(f"${pm['target']:,.2f}", style="bold yellow")
        if valid_prices:
            avg  = sum(valid_prices) / len(valid_prices)
            diff = avg - pm["target"]
            col  = "green" if diff > 0 else "red"
            arrow = "▲" if diff > 0 else "▼"
            label = "ABOVE" if diff > 0 else "BELOW"
            text.append(
                f"   {arrow} ${abs(diff):,.0f} [{col}]{label}[/{col}] target\n"
            )
        else:
            text.append("\n")

    # Countdown
    text.append("Time Left:    ", style="dim")
    text.append(fmt_time_left(pm["end_time"]) + "\n")

    # Divider
    text.append("\n" + "─" * 40 + "\n\n", style="dim")

    # UP price row
    up_p = pm.get("up_price")
    dn_p = pm.get("dn_price")

    text.append("  ⬆  UP  (YES): ", style="bold green")
    if up_p is not None:
        text.append(f"{up_p:.4f}  ({up_p * 100:.1f}%)", style="bold bright_green")
        ub, ua = pm.get("up_bid"), pm.get("up_ask")
        if ub and ua:
            text.append(f"\n      bid {ub:.4f}  /  ask {ua:.4f}", style="dim green")
    else:
        text.append("[dim]-- (waiting)[/dim]")
    text.append("\n\n")

    # DOWN price row
    text.append("  ⬇  DOWN (NO):  ", style="bold red")
    if dn_p is not None:
        text.append(f"{dn_p:.4f}  ({dn_p * 100:.1f}%)", style="bold bright_red")
        db, da = pm.get("dn_bid"), pm.get("dn_ask")
        if db and da:
            text.append(f"\n      bid {db:.4f}  /  ask {da:.4f}", style="dim red")
    else:
        text.append("[dim]-- (waiting)[/dim]")
    text.append("\n\n")

    # Market direction signal
    text.append("─" * 40 + "\n", style="dim")
    if up_p is not None and dn_p is not None:
        total = up_p + dn_p
        text.append(f"  Sum: {total:.4f}   ", style="dim")
        if up_p > dn_p:
            text.append("Market → ", style="dim")
            text.append("BULLISH  ▲", style="bold bright_green")
        else:
            text.append("Market → ", style="dim")
            text.append("BEARISH  ▼", style="bold bright_red")
        edge = abs(up_p - dn_p) * 100
        text.append(f"   Edge: {edge:.1f}%\n", style="dim")

    # Last polled
    if pm["last_ok"]:
        age = now - pm["last_ok"]
        text.append(f"\n[dim]Polymarket polled {age:.1f}s ago[/dim]")

    pm_panel = Panel(
        text,
        title="[bold magenta]Polymarket 5-Min BTC[/bold magenta]",
        border_style="magenta",
        expand=True,
    )

    # ── Assemble layout ───────────────────────────────────────────────────────
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=3),
    )
    layout["body"].split_row(
        Layout(price_tbl, name="left"),
        Layout(pm_panel,  name="right"),
    )

    ts = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    layout["header"].update(Panel(
        f"[bold white]Polymarket 5-Min BTC Tracker[/bold white]"
        f"  [dim]│  {ts}  │  Ctrl+C to quit[/dim]",
        style="on dark_blue",
    ))
    layout["footer"].update(Panel(
        "[dim]Price sources: Binance WS · Coinbase REST · Kraken WS · OKX WS · Bybit WS\n"
        "Market data:  Polymarket Gamma API + CLOB (polled every 2s)\n"
        "Prices are 0–1 (probability). UP + DOWN ≈ 1.00[/dim]",
        border_style="dim",
    ))

    return layout


async def render_loop(live: Live) -> None:
    while True:
        live.update(build_ui())
        await asyncio.sleep(0.5)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=20)
    ) as session:
        with Live(
            build_ui(),
            refresh_per_second=4,
            screen=True,
            console=console,
        ) as live:
            await asyncio.gather(
                render_loop(live),
                pm_loop(session),
                binance_feed(),
                coinbase_feed(session),
                kraken_feed(),
                okx_feed(),
                bybit_feed(),
            )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Tracker stopped.[/dim]")
