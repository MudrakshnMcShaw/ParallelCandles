#!/usr/bin/env python3
import asyncio
import signal
import urllib.parse
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import aiohttp
import pytz
import redis
from motor.motor_asyncio import AsyncIOMotorClient
import logging

# ── CONFIG ────────────────────────────────────────────────────────────────────
from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")

# API config
BASE_URL = config.get("accelpix", "base_url")
API_TOKEN = config.get("accelpix", "api_key")

# Redis config
REDIS_HOST = config.get("database", "REDIS_HOST")
REDIS_PORT = config.getint("database", "REDIS_PORT")
REDIS_DB = config.getint("database", "REDIS_DB")
REDIS_PASSWORD = config.get("database", "REDIS_PASSWORD")

MONGO_URI = config.get("database", "MONGO_URI")
REDIS_PATTERN = "dv1:*"
MONGO_DB = "Accelpix_Candle_data"
MONGO_COLL = "OHLC_MINUTE_1"

MAX_CONCURRENCY = 100
REQUEST_TIMEOUT = 8

IST = pytz.timezone("Asia/Kolkata")

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(message)s")
log = logging.getLogger("poller")

SHUTDOWN = False

# ── TIME/URL HELPERS ──────────────────────────────────────────────────────────
def ist_now() -> datetime:
    return datetime.now(IST)

def prev_minute_window() -> tuple[datetime, datetime]:
    start = ist_now().replace(second=0, microsecond=0) - timedelta(minutes=1)
    end = start.replace(second=59, microsecond=0)
    return start, end

def next_minute_sleep_seconds(offset: float = 0.2) -> float:
    now = ist_now()
    nxt = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    return max(0.0, (nxt - now).total_seconds() + offset)

def url_for(provider_tkr: str, start_dt: datetime, end_dt: datetime) -> str:
    start_str = urllib.parse.quote(start_dt.strftime("%Y%m%d %H:%M:%S"), safe="")
    end_str = urllib.parse.quote(end_dt.strftime("%Y%m%d %H:%M:%S"), safe="")
    p = urllib.parse.quote(provider_tkr, safe="")
    u = f"{BASE_URL}/{p}/{start_str}/{end_str}/1"
    if API_TOKEN:
        u += f"?api_token={urllib.parse.quote(API_TOKEN, safe='')}"
    return u

def parse_td_ist_to_epoch(td: str) -> int:
    # td format: "YYYY-MM-DD HH:MM:SS" (IST)
    dt = IST.localize(datetime.strptime(td, "%Y-%m-%d %H:%M:%S"))
    return int(dt.timestamp())

# ── REDIS ─────────────────────────────────────────────────────────────────────
def load_tickers_from_redis() -> List[Dict]:
    r = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
        password=REDIS_PASSWORD, decode_responses=True
    )
    try:
        r.ping()
    except Exception as e:
        log.error("Redis connect failed: %s", e)
        return []

    out: List[Dict] = []
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=REDIS_PATTERN, count=1000)
        for k in keys:
            if not k.startswith("dv1:"):
                continue
            provider_tkr = k.split("dv1:", 1)[1]
            xts_symbol = r.get(k)  # may be None
            out.append({"provider_tkr": provider_tkr, "xts_symbol": xts_symbol})
        if cursor == 0:
            break

    # Ensure index tickers exist
    required = ['NIFTY 50', 'NIFTY BANK', 'NIFTY FIN SERVICE', 'SENSEX-BSE']
    have = {d["provider_tkr"] for d in out}
    for t in required:
        if t not in have:
            out.append({"provider_tkr": t, "xts_symbol": 'SENSEX' if t == 'SENSEX-BSE' else t})
    return out

# ── HTTP ──────────────────────────────────────────────────────────────────────
async def fetch_bars(session: aiohttp.ClientSession, provider_tkr: str,
                     start_dt: datetime, end_dt: datetime) -> Optional[list]:
    url = url_for(provider_tkr, start_dt, end_dt)
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data if isinstance(data, list) else None
    except Exception as e:
        log.warning("[%s] fetch failed: %s", provider_tkr, e)
        return None

# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
async def poll_loop(tickers: List[Dict]):
    mongo = AsyncIOMotorClient(MONGO_URI)
    coll = mongo[MONGO_DB][MONGO_COLL]

    # Create our OWN compound unique index on (Symbol, LastTradeTime)
    await coll.create_index([("Symbol", 1), ("LastTradeTime", 1)],
                            unique=True, name="idx_symbol_lut")

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY, limit_per_host=MAX_CONCURRENCY)
    providers = [t["provider_tkr"] for t in tickers]
    p2sym = {t["provider_tkr"]: t.get("xts_symbol") for t in tickers}
    INDEX_TICKERS = {"NIFTY 50", "NIFTY BANK", "NIFTY FIN SERVICE", "SENSEX-BSE"}

    async with aiohttp.ClientSession(connector=connector) as session:
        log.info("Polling %d tickers (concurrency=%d)", len(providers), MAX_CONCURRENCY)

        while not SHUTDOWN:
            await asyncio.sleep(next_minute_sleep_seconds())
            start_dt, end_dt = prev_minute_window()

            # gather in parallel
            tasks = [fetch_bars(session, p, start_dt, end_dt) for p in providers]
            results = await asyncio.gather(*tasks)

            # upsert each bar with (Symbol, LastTradeTime) as unique key
            ops = []
            for provider_tkr, bars in zip(providers, results):
                if not bars:
                    continue
                symbol = p2sym.get(provider_tkr)
                for bar in bars:
                    td = bar.get("td")
                    if not td:
                        continue
                    try:
                        ts_unix = parse_td_ist_to_epoch(td)
                    except Exception:
                        continue

                    vol_val = 0 if provider_tkr in INDEX_TICKERS else (bar.get("vol") or 0)

                    doc = {
                        "Symbol": symbol,
                        "provider_tkr": provider_tkr,
                        "LastTradeTime": ts_unix,
                        "ts": td,
                        "Open": bar.get("op"),
                        "High": bar.get("hp"),
                        "Low": bar.get("lp"),
                        "Close": bar.get("cp"),
                        "Volume": vol_val,
                        "OpenInterest": bar.get("oi"),
                        "fetched_at": ist_now(),
                    }

                    # Upsert on (Symbol, LastTradeTime)
                    ops.append((
                        {"Symbol": symbol, "LastTradeTime": ts_unix},
                        {"$set": doc}
                    ))

            # Execute upserts in small batches
            from pymongo import UpdateOne
            BATCH = 500
            for i in range(0, len(ops), BATCH):
                batch = [UpdateOne(f, u, upsert=True) for f, u in ops[i:i+BATCH]]
                if not batch:
                    continue
                try:
                    res = await coll.bulk_write(batch, ordered=False)
                    log.info("Upserted batch: matched=%s modified=%s upserted=%s",
                             res.matched_count, res.modified_count,
                             len(getattr(res, "upserted_ids", {}) or {}))
                except Exception as e:
                    # With unique index, races may cause dup errors; unordered bulk minimizes impact
                    log.warning("bulk_write error (some ops may still succeed): %s", e)

    mongo.close()

# ── ENTRYPOINT ────────────────────────────────────────────────────────────────
def _ask_shutdown():
    global SHUTDOWN
    SHUTDOWN = True
    log.info("Shutdown requested...")

def _setup_signals(loop: asyncio.AbstractEventLoop):
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, _ask_shutdown)

async def main():
    tickers = load_tickers_from_redis()
    if not tickers:
        log.error("No tickers found")
        return
    log.info("Loaded %d tickers", len(tickers))
    await poll_loop(tickers)

if __name__ == "__main__":
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except Exception:
        pass

    loop = asyncio.new_event_loop()
    _setup_signals(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
        log.info("Shutdown complete")
