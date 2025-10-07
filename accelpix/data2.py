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
config.read("/root/ParallelCandles/config.ini")

# API config
BASE_URL = config.get("accelpix", "base_url")
API_TOKEN = config.get("accelpix", "api_key")

# Redis config
REDIS_HOST = config.get("local_db", "REDIS_HOST")
REDIS_PORT = config.getint("local_db", "REDIS_PORT")
REDIS_DB = config.getint("local_db", "REDIS_DB")
REDIS_PASSWORD = config.get("local_db", "REDIS_PASSWORD")
REDIS_PATTERN = "dv1:*"

MONGO_URI = config.get("local_db", "MONGO_URI")
MONGO_DB = "Accelpix_Candle_Data"
MONGO_COLL = "OHLC_MINUTE_1"

MAX_CONCURRENCY = 100
REQUEST_TIMEOUT = 8

IST = pytz.timezone("Asia/Kolkata")

import os
import logging
from logging.handlers import RotatingFileHandler

# ── LOGGING ────────────────────────────────────────────────────────────────
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "accelpix.log")

# Create a rotating file handler — 10 MB per file, keep 5 backups
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=10_000_000, backupCount=5)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s %(message)s"))

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s %(message)s"))

# Configure root logger
logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])

log = logging.getLogger("poller")
log.info("Logging initialized. Writing to %s", LOG_FILE)


SHUTDOWN = False

# ── TIME/URL HELPERS ──────────────────────────────────────────────────────────
def ist_now() -> datetime:
    return datetime.now(IST)

def prev_minute_window() -> tuple:
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
    while not SHUTDOWN:
        now = ist_now()
        weekday = now.weekday()  # Monday=0, Sunday=6
        start_time = now.replace(hour=9, minute=15, second=30, microsecond=0)
        end_time = now.replace(hour=23, minute=31, second=30, microsecond=0)

        # Weekend check (Saturday=5, Sunday=6)
        if weekday >= 5:
            log.info("Weekend (%s). Sleeping until Monday 09:15:30...", now.strftime("%A"))
            # compute next Monday 9:15:30
            days_ahead = (7 - weekday)
            next_start = start_time + timedelta(days=days_ahead)
            sleep_seconds = (next_start - now).total_seconds()
            await asyncio.sleep(max(0, sleep_seconds))
            continue

        # Before market open
        if now < start_time:
            sleep_seconds = (start_time - now).total_seconds()
            log.info("Market not open yet (%.1fs to go)...", sleep_seconds)
            await asyncio.sleep(sleep_seconds)
            continue

        # After market close
        if now > end_time:
            log.info("Market closed. Sleeping until next open...")
            next_start = start_time + timedelta(days=1)
            # skip weekends automatically
            while next_start.weekday() >= 5:
                next_start += timedelta(days=1)
            sleep_seconds = (next_start - now).total_seconds()
            await asyncio.sleep(sleep_seconds)
            continue

        #Inside allowed window
        tickers = load_tickers_from_redis()
        if not tickers:
            log.error("No tickers found.")
            await asyncio.sleep(60)  # retry every minute
            continue

        log.info("Market open — starting poll loop for %d tickers.", len(tickers))
        await poll_loop(tickers)

        # when poll_loop exits (e.g., SHUTDOWN=True), check again
        log.info("Poll loop exited. Checking schedule again...")


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
