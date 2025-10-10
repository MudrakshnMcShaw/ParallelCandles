import asyncio
import os
import time
import signal
import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any

import pytz
from configparser import ConfigParser
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
import logging

# ---- configuration ----
CONFIG_PATH = "/root/ParallelCandles/config.ini"
config = ConfigParser()
config.read(CONFIG_PATH)

IST = pytz.timezone("Asia/Kolkata")

MONGO_DB3_URI = config.get("live_db", "MONGO_URI")
DB3_NAME = "CandleData"
CANDLE_COLL = "OHLC_MINUTE_1"
METRICS_COLL = "ParallelCandleMetrics"
OUTPUT_COLL = "complete_candle"

INSTRUMENT_CSV = os.getenv("INSTRUMENT_CSV", "instrument_list_ohlc.csv")
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "200"))

# market hours
MARKET_START_HOUR = 9
MARKET_START_MIN = 15
MARKET_END_HOUR = 23
MARKET_END_MIN = 30

# logging setup
Path("./logs").mkdir(parents=True, exist_ok=True)
logFileName = f'./logs/complete_candle_{datetime.now().strftime("%Y%m%dT%H%M%S")}.log'
logging.basicConfig(level=logging.INFO,
                    format="[%(levelname)s] %(asctime)s %(message)s",
                    handlers=[logging.FileHandler(logFileName), logging.StreamHandler()])
logger = logging.getLogger("CompleteCandleService")

SHUTDOWN = False

# ---------------- helper functions ----------------
def now_ist() -> datetime:
    return datetime.now(IST)

def minute_epoch_for_prev_minute(ts: datetime) -> int:
    prev = ts.astimezone(IST) - timedelta(minutes=1)
    prev = prev.replace(second=0, microsecond=0)
    return int(prev.timestamp())

def load_symbols_from_csv(path: str = INSTRUMENT_CSV) -> List[str]:
    p = Path(path)
    if not p.exists():
        logger.warning(f"Instrument CSV not found at {path}; returning empty list")
        return []
    out: List[str] = []
    with p.open("r", encoding="utf-8", newline="") as fh:
        rdr = csv.DictReader(fh)
        if "symbol" not in (rdr.fieldnames or []):
            logger.warning(f"CSV must contain column 'symbol'. Found: {rdr.fieldnames}")
            return []
        for r in rdr:
            s = (r.get("symbol") or "").strip()
            if s:
                out.append(s)
    required = ['NIFTY 50', 'NIFTY BANK', 'NIFTY FIN SERVICE', 'SENSEX']
    for sym in required:
        if sym not in out:
            out.append(sym)
    logger.info(f"Loaded {len(out)} symbols from {path}")
    return out

# ---------------- DB helpers ----------------
async def ensure_indexes(db):
    out_coll = db[OUTPUT_COLL]
    metrics_coll = db[METRICS_COLL]
    try:
        await out_coll.create_index([("Symbol", 1), ("LastTradeTime", 1)], unique=True,
                                    background=True, name="idx_symbol_lasttrade")
        logger.info("Ensured index on output (Symbol, LastTradeTime) unique")
    except Exception as e:
        logger.warning(f"Failed to ensure output index: {e}")
    try:
        await metrics_coll.create_index([("Symbol", 1), ("LastUpdateTime", 1)], background=True,
                                        name="idx_metrics_symbol_lastup")
        logger.info("Ensured index on metrics (Symbol, LastUpdateTime)")
    except Exception as e:
        logger.warning(f"Failed to ensure metrics index: {e}")

async def find_candle(db, symbol: str, minute_epoch: int) -> Optional[Dict[str, Any]]:
    coll = db[CANDLE_COLL]
    try:
        doc = await coll.find_one({"Symbol": symbol, "LastTradeTime": int(minute_epoch)})
        if doc:
            return doc
        doc = await coll.find_one({"Symbol": symbol, "LastTradeTime": {"$gte": int(minute_epoch), "$lte": int(minute_epoch + 59)}})
        return doc
    except Exception as e:
        logger.exception(f"find_candle error for {symbol}: {e}")
        return None

async def find_metric(db, symbol: str, minute_epoch: int) -> Optional[Dict[str, Any]]:
    coll = db[METRICS_COLL]
    try:
        doc = await coll.find_one({
            "Symbol": symbol,
            "LastUpdateTime": {"$gte": minute_epoch, "$lt": minute_epoch + 60}
        })
        logger.info(f"metric doc for {symbol} at {minute_epoch}: {doc}")
        return doc
    except Exception as e:
        logger.exception(f"find_metric error for {symbol}: {e}")
        return None

async def build_and_store_for_symbol(symbol: str, minute_epoch: int, db, out_coll, sem: asyncio.Semaphore):
    async with sem:
        try:
            candle = await find_candle(db, symbol, minute_epoch)
            if not candle:
                logger.info(f"[SKIP] {symbol}: no candle for minute {datetime.fromtimestamp(minute_epoch, IST).strftime('%Y-%m-%d %H:%M:%S')}")
                return {"status": "skipped", "symbol": symbol, "reason": "no_candle"}
            metric = await find_metric(db, symbol, minute_epoch)
            combined = dict(candle)
            combined["LastTradeTime"] = minute_epoch
            if metric:
                for k, v in metric.items():
                    if k in ("_id", "Symbol"):
                        continue
                    combined[k] = v
                combined["_MetricDoc"] = {k: v for k, v in metric.items() if k != "_id"}
            else:
                combined["_MetricDoc"] = None
            combined["CollectedAt"] = int(time.time())
            combined["SourceCandleCollection"] = CANDLE_COLL
            combined["MetricCollection"] = METRICS_COLL
            try:
                await out_coll.insert_one(combined)
                logger.info(f"[WRITE] {symbol} minute {datetime.fromtimestamp(minute_epoch, IST).strftime('%Y-%m-%d %H:%M:%S')} inserted")
                return {"status": "ok", "symbol": symbol}
            except DuplicateKeyError:
                logger.info(f"[DUPLICATE] {symbol} minute {datetime.fromtimestamp(minute_epoch, IST).strftime('%Y-%m-%d %H:%M:%S')} already exists; skipping")
                return {"status": "duplicate", "symbol": symbol}
            except Exception as e:
                logger.exception(f"[ERROR] insert failed for {symbol}: {e}")
                return {"status": "error", "symbol": symbol, "error": str(e)}
        except Exception as e:
            logger.exception(f"[ERROR] unexpected error for {symbol}: {e}")
            return {"status": "error", "symbol": symbol, "error": str(e)}

# ---------------- backfill ----------------
async def backfill_today(db, out_coll):
    now = now_ist()
    market_start = now.replace(hour=MARKET_START_HOUR, minute=MARKET_START_MIN, second=0, microsecond=0)
    minute = market_start
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    while minute < now.replace(second=0, microsecond=0):
        minute_epoch = int(minute.timestamp())
        existing = await out_coll.find_one({"LastTradeTime": minute_epoch})
        if existing:
            logger.debug(f"[BACKFILL] Minute {minute.strftime('%H:%M')} already exists, skipping")
        else:
            symbols = load_symbols_from_csv()
            tasks = [build_and_store_for_symbol(sym, minute_epoch, db, out_coll, sem) for sym in symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            ok = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "ok")
            logger.info(f"[BACKFILL] Minute {minute.strftime('%H:%M')} done, ok={ok}")
        minute += timedelta(minutes=1)

# ---------------- scheduler loop ----------------
async def minute_scheduler_loop():
    global SHUTDOWN
    client = AsyncIOMotorClient(MONGO_DB3_URI)
    db = client[DB3_NAME]
    out_coll = db[OUTPUT_COLL]
    await ensure_indexes(db)
    await backfill_today(db, out_coll)
    logger.info("Backfill complete, starting live minute scheduler")
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    while not SHUTDOWN:
        now = now_ist()
        next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        target_time = next_minute + timedelta(seconds=3)
        wait = (target_time - now).total_seconds()
        if wait > 0:
            await asyncio.sleep(wait)
        minute_epoch = minute_epoch_for_prev_minute(now_ist())
        logger.info(f"Starting build for minute {datetime.fromtimestamp(minute_epoch, IST).strftime('%Y-%m-%d %H:%M:%S')}")
        symbols = load_symbols_from_csv()
        if not symbols:
            logger.info("No symbols loaded; sleeping until next minute")
            continue
        tasks = [build_and_store_for_symbol(sym, minute_epoch, db, out_coll, sem) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        ok = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "ok")
        dup = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "duplicate")
        skip = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "skipped")
        err = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "error")
        logger.info(f"Minute {datetime.fromtimestamp(minute_epoch, IST).strftime('%Y-%m-%d %H:%M:%S')} done: ok={ok}, dup={dup}, skipped={skip}, err={err}")
    await client.close()

# ---------------- signal handling ----------------
def ask_shutdown(signum=None, frame=None):
    global SHUTDOWN
    SHUTDOWN = True
    logger.info("Shutdown requested")

# ---------------- main ----------------
if __name__ == "__main__":
    signal.signal(signal.SIGINT, ask_shutdown)
    signal.signal(signal.SIGTERM, ask_shutdown)
    loop = asyncio.get_event_loop()
    try:
        while not SHUTDOWN:
            now = now_ist()
            start_time = now.replace(hour=MARKET_START_HOUR, minute=MARKET_START_MIN, second=30, microsecond=0)
            end_time = now.replace(hour=MARKET_END_HOUR, minute=MARKET_END_MIN, second=30, microsecond=0)
            if now.weekday() >= 5:
                logger.info("Weekend detected — sleeping for 1 hour.")
                time.sleep(3600)
                continue
            if start_time <= now <= end_time:
                logger.info("Market open — starting minute scheduler.")
                try:
                    loop.run_until_complete(minute_scheduler_loop())
                except Exception as e:
                    logger.exception(f"Scheduler loop error: {e}")
                finally:
                    logger.info("Minute scheduler exited — waiting briefly before retry")
                    time.sleep(2)
            else:
                logger.info("Market closed — sleeping until next check.")
                time.sleep(60)
    finally:
        loop.close()
        logger.info("CompleteCandleService exiting")
