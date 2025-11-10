#!/usr/bin/env python3
import asyncio
from datetime import datetime, timedelta
import os
import signal
import time
from typing import List, Dict, Optional
from pathlib import Path
import csv

import pytz
import redis
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
import logging
from datetime import datetime, timezone, timedelta

from configparser import ConfigParser

config = ConfigParser()
config.read("/root/ParallelCandles/config.ini")

# -------------------- CONFIG --------------------
IST = pytz.timezone("Asia/Kolkata")

# Primary Mongo for DB1/DB2 (can be local or one URI)
MONGO_URI = config.get("local_db", "MONGO_URI")

# DB1 (TrueData)
DB1_NAME = "True_Data_Candle_Data"
DB1_COLL = "OHLC_MINUTE_1"

# DB2 (Accelpix)
DB2_NAME = "Accelpix_Candle_Data"
DB2_COLL = "OHLC_MINUTE_1"

# DB3 (consolidated)
MONGO_DB3_URI = config.get("live_db", "MONGO_URI")
DB3_NAME = "CandleData"
DB3_COLL = "OHLC_MINUTE_1"

# Redis for instrument list (CSV preferred, but kept)
REDIS_HOST = config.get("live_db", "REDIS_HOST")
REDIS_PORT = config.get("live_db", "REDIS_PORT")
REDIS_DB = config.get("live_db", "REDIS_DB")
REDIS_PASSWORD = config.get("live_db", "REDIS_PASSWORD")
REDIS_PATTERN = "dv1:*"

# Redis endpoints where we set DONE and ping keys
DONE_REDIS_HOST = config.get("live_db", "REDIS_HOST")
DONE_REDIS_PORT = config.get("live_db", "REDIS_PORT")
DONE_REDIS_DB = config.get("live_db", "REDIS_DB")
DONE_REDIS_PASSWORD = config.get("live_db", "REDIS_PASSWORD")

PING_REDIS_HOST = config.get("live_db", "REDIS_HOST")
PING_REDIS_PORT = config.get("live_db", "REDIS_PORT")
PING_REDIS_DB = config.get("live_db", "REDIS_DB")
PING_REDIS_PASSWORD = config.get("live_db", "REDIS_PASSWORD")

# TTL for DONE flag (seconds)
DONE_KEY_TTL = int(os.getenv("DONE_KEY_TTL", "10"))

# concurrency
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "200"))
MAX_AGE_SECONDS = int(os.getenv("MAX_AGE_SECONDS", "300"))

# CSV instrument list
INSTRUMENT_CSV = os.getenv("INSTRUMENT_CSV", "instrument_list_ohlc.csv")

# Index name used across all DBs
IDX_NAME = "idx_symbol_lut"

# ------------------------------------------------
SHUTDOWN = False

# -------------------- LOGGER --------------------
Path("./logs").mkdir(parents=True, exist_ok=True)
logFileName = f'./logs/test{datetime.now().strftime("%Y%m%dT%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s %(message)s",
    handlers=[
        logging.FileHandler(logFileName),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Consolidator")
# ------------------------------------------------


def now_ist() -> datetime:
    return datetime.now(IST)


def minute_epoch_for_prev_minute(ts: datetime) -> int:
    prev = ts.astimezone(IST) - timedelta(minutes=1)
    prev = prev.replace(second=0, microsecond=0)
    return int(prev.timestamp())


def normalize_epoch_s(ts: int) -> int:
    """Ensure unix seconds (not ms)."""
    ts = int(ts)
    if ts > 1_000_000_000_000:  # ms
        ts //= 1000
    return ts


def parse_possible_ts_from_doc(doc: Dict) -> Optional[int]:
    if not doc:
        return None
    if "LastTradeTime" in doc and isinstance(doc["LastTradeTime"], (int, float)):
        return normalize_epoch_s(doc["LastTradeTime"])
    for k in ("ts", "td", "time", "timestamp"):
        val = doc.get(k)
        if isinstance(val, (int, float)):
            return normalize_epoch_s(val)
        if isinstance(val, str):
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    # assume IST if naive
                    return int(datetime.strptime(val, fmt).replace(tzinfo=IST).timestamp())
                except Exception:
                    pass
    return None


async def ensure_idx_symbol_lut(coll):
    """Create (Symbol, LastTradeTime) index with a fixed name, idempotent."""
    try:
        await coll.create_index(
            [("Symbol", 1), ("LastTradeTime", 1)],
            name=IDX_NAME,
            unique=True,
            background=True,
        )
    except Exception as e:
        logger.warning(f"Index ensure failed on {coll.full_name if hasattr(coll,'full_name') else coll}: {e}")


async def find_candle_in_db(coll, symbol: str, minute_epoch: int) -> Optional[Dict]:
    """
    Index-first lookup for candle:
      1) exact match on Symbol + LastTradeTime
      2) bounded range on LastTradeTime for that Symbol (minute window)
    Returns the document or None.
    """
    try:
        minute_epoch = int(minute_epoch)
    except Exception:
        logger.info(f"minute_epoch not int: {minute_epoch}")
        return None

    proj = {"_id": 1, "Symbol": 1, "LastTradeTime": 1, "Open": 1, "High": 1, "Low": 1, "Close": 1, "Volume": 1, "OpenInterest": 1}

    queries = [
        {"Symbol": symbol, "LastTradeTime": minute_epoch},
        {"Symbol": symbol, "LastTradeTime": {"$gte": minute_epoch, "$lte": minute_epoch + 59}},
    ]

    for q in queries:
        try:
            # Hint by the shared index name
            doc = await coll.find_one(q, projection=proj, hint=IDX_NAME)
            if doc:
                logger.info(f"Index-hit for {symbol} query={q}")
                return doc
        except Exception as e:
            logger.info(f"find_one with hint failed for {q}: {e}; retrying without hint")
            try:
                doc = await coll.find_one(q, projection=proj)
                if doc:
                    logger.info(f"Index-hit (no hint) for {symbol} query={q}")
                    return doc
            except Exception as e2:
                logger.info(f"find_one retry failed for {q}: {e2}")

    return None


async def push_to_db3(coll3, doc: Dict, source_db: str,
                      redis_done_client: Optional[redis.Redis], redis_ping_client: Optional[redis.Redis]) -> None:
    """
    Upsert into DB3 and set redis DONE + ping. All operations are best-effort.
    """
    if not doc:
        return
    ts = parse_possible_ts_from_doc(doc)
    symbol = doc.get("Symbol") or doc.get("provider_tkr") or doc.get("symbol") or doc.get("sym")
    if not symbol or ts is None:
        logger.warning(f"Skipping push_to_db3: missing symbol or ts in doc: {doc}")
        return

    doc_copy = dict(doc)

    try:
        await coll3.update_one(
            {"Symbol": symbol, "LastTradeTime": ts},
            {"$set": doc_copy},
            upsert=True
        )
        logger.info(f"PUSHED -> {symbol} @ {datetime.fromtimestamp(ts, IST).strftime('%Y-%m-%d %H:%M:%S')} from {source_db}")
    except Exception as e:
        logger.info(f"Failed to upsert {symbol} ts={ts} into DB3: {e}")

    # Redis DONE
    if redis_done_client:
        try:
            redis_done_client.set(symbol, "DONE", ex=DONE_KEY_TTL)
            logger.info(f"Set DONE key for {symbol} (ttl={DONE_KEY_TTL}s)")
        except Exception as e:
            logger.info(f"Failed to set DONE key for {symbol} in Redis: {e}")

    # Redis PING
    if redis_ping_client:
        try:
            redis_ping_client.set(f"{symbol}_1", ts)
            logger.info(f"Set ping key {symbol}_1 -> {ts}")
        except Exception as e:
            logger.info(f"Failed to set ping key for {symbol} in Redis: {e}")


async def check_and_consolidate_for_symbol(symbol: str, minute_epoch: int,
                                           db1_coll, db2_coll) -> Optional[Dict]:
    """
    Try db1 then db2, return {"doc": doc, "source": "db1|db2"} or None.
    """
    doc = await find_candle_in_db(db1_coll, symbol, minute_epoch)
    if doc:
        return {"doc": doc, "source": "db1"}

    doc = await find_candle_in_db(db2_coll, symbol, minute_epoch)
    if doc:
        return {"doc": doc, "source": "db2"}

    return None

async def run_cycle(symbols: List[str], minute_epoch: int,
                    db1_coll, db2_coll, db3_coll,
                    redis_done_client: Optional[redis.Redis], redis_ping_client: Optional[redis.Redis]):
    from pymongo.errors import DuplicateKeyError

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    loop = asyncio.get_running_loop()

    candidate_symbols = list(symbols)
    symbols_to_check = candidate_symbols[:]

    # containers for classification decisions
    classified_db1: Dict[str, int] = {}   # symbol -> consolidation_time (first-check or recheck)
    upsert_symbols_with_ts: List[tuple] = []  # (symbol, ts) for those we upserted -> db2
    missing_after_checks: List[str] = []

    # metrics collection (in DB3 database)
    metrics_coll = db3_coll.database["ParallelCandleMetrics"]

    # Ensure an index to avoid accidental duplicate minute records (Symbol + LastUpdateTime unique)
    # This is idempotent and fast if index already exists.
    try:
        await metrics_coll.create_index([("Symbol", 1), ("LastUpdateTime", 1)], name="idx_symbol_lastup", unique=True, background=True)
    except Exception as e:
        logger.info(f"[METRIX] ensure index failed or already exists: {e}")

    # 1) FIRST Redis DONE batch-check — if DONE present now => classify as db1 at this time
    if redis_done_client:
        try:
            first_check_time = int(time.time())
            def mget_keys(client, keys):
                return client.mget(keys)
            done_vals = await loop.run_in_executor(None, mget_keys, redis_done_client, symbols_to_check)

            remaining = []
            for s, v in zip(symbols_to_check, done_vals):
                if v is not None:
                    # classify immediately as db1 (ConsolidationTime = first_check_time)
                    classified_db1[s] = first_check_time
                else:
                    remaining.append(s)
            symbols_to_check = remaining
            logger.info(f"Initial DONE mget: classified {len(classified_db1)} symbols as db1; will check {len(symbols_to_check)} remaining")
        except Exception as e:
            logger.info(f"Initial Redis DONE batch-check failed: {e}")
            symbols_to_check = candidate_symbols[:]
    else:
        logger.info("No DONE Redis client available; will check DBs for all symbols")

    # 2) Check DB1/DB2 concurrently for symbols not DONE
    results_store = []  # (symbol, doc, source)
    missing: List[str] = []

    async def _worker_check(sym: str):
        async with sem:
            try:
                found = await check_and_consolidate_for_symbol(sym, minute_epoch, db1_coll, db2_coll)
                if not found:
                    missing.append(sym)
                else:
                    results_store.append((sym, found["doc"], found["source"]))
            except Exception as e:
                logger.info(f"Error during DB checks for {sym}: {e}")
                missing.append(sym)

    if symbols_to_check:
        await asyncio.gather(*(_worker_check(s) for s in symbols_to_check))
    logger.info(f"DB checks: found {len(results_store)} docs, missing {len(missing)}")

    # 3) Retry missing once shortly after minute boundary (existing behavior)
    if missing:
        curr_minute_start = now_ist().replace(second=0, microsecond=0)
        target_retry_time = curr_minute_start + timedelta(seconds=1)
        wait = (target_retry_time - now_ist()).total_seconds()
        if wait > 0:
            await asyncio.sleep(wait)

        retry_missing = []
        async def _retry_worker(sym: str):
            async with sem:
                try:
                    found = await check_and_consolidate_for_symbol(sym, minute_epoch, db1_coll, db2_coll)
                    if not found:
                        retry_missing.append(sym)
                    else:
                        results_store.append((sym, found["doc"], found["source"]))
                except Exception as e:
                    logger.info(f"Retry error {sym}: {e}")
                    retry_missing.append(sym)

        await asyncio.gather(*(_retry_worker(s) for s in missing))
        missing = retry_missing
        logger.info(f"After retry: total found docs {len(results_store)}, still missing {len(missing)}")
    else:
        logger.info("No missing symbols to retry")

    # 4) RE-CHECK Redis DONE immediately before write:
    #    Any candidate that became DONE now -> classify as db1 at re-check time and remove from write candidates.
    candidates_map: Dict[str, Dict] = {sym: {"doc": doc, "source": source} for sym, doc, source in results_store}

    if redis_done_client and candidates_map:
        try:
            recheck_time = int(time.time())
            keys = list(candidates_map.keys())
            def mget_batch(client, keys):
                return client.mget(keys)
            done_vals = await loop.run_in_executor(None, mget_batch, redis_done_client, keys)
            to_remove = []
            for k, v in zip(keys, done_vals):
                if v is not None:
                    # became DONE -> classify as db1 at recheck time
                    classified_db1[k] = recheck_time
                    to_remove.append(k)
            for k in to_remove:
                candidates_map.pop(k, None)
            if to_remove:
                logger.info(f"Re-check DONE: classified {len(to_remove)} symbols as db1 and removed them from upsert candidates")
        except Exception as e:
            logger.info(f"Redis re-check failed: {e}")

    # 5) Anything left in candidates_map we will upsert into DB3.
    #    Per your rule: if we are inserting, that data is taken from DB2 => classify as db2 at insertion time
    bulk_ops = []
    docs_for_redis = []
    upsert_symbols_with_ts = []  # (symbol, ts)
    for sym, info in candidates_map.items():
        doc = info["doc"]
        ts = parse_possible_ts_from_doc(doc)
        symbol = doc.get("Symbol") or doc.get("provider_tkr") or doc.get("symbol") or doc.get("sym")
        if not symbol or ts is None:
            logger.warning(f"Skipping candidate (no symbol/ts) for {sym}: {doc}")
            continue
        filter_q = {"Symbol": symbol, "LastTradeTime": ts}
        update_q = {"$set": dict(doc)}
        bulk_ops.append(UpdateOne(filter_q, update_q, upsert=True))
        docs_for_redis.append((symbol, ts))
        upsert_symbols_with_ts.append((symbol, ts))

    # 6) Execute bulk write to DB3
    if bulk_ops:
        try:
            t_bulk_start = time.time()
            res = await db3_coll.bulk_write(bulk_ops, ordered=False)
            logger.info(f"Bulk upsert done (ops={len(bulk_ops)}). took {time.time() - t_bulk_start:.3f}s")
        except Exception as e:
            logger.info(f"DB3 bulk_write failed: {e}")
    else:
        logger.info("No bulk ops to perform for DB3")

    # 7) Set Redis DONE + ping for the symbols we attempted to upsert
    if (redis_done_client or redis_ping_client) and docs_for_redis:
        try:
            def do_redis_pipelines(done_client, ping_client, tuples):
                pd = done_client.pipeline() if done_client else None
                pp = ping_client.pipeline() if ping_client else None
                for symbol, ts in tuples:
                    if pd:
                        pd.set(symbol, "DONE", ex=DONE_KEY_TTL)
                    if pp:
                        pp.set(f"{symbol}_1", ts)
                if pd:
                    pd.execute()
                if pp:
                    pp.execute()
            await loop.run_in_executor(None, do_redis_pipelines, redis_done_client, redis_ping_client, docs_for_redis)
            logger.info("Redis DONE + ping set for upserted symbols")
        except Exception as e:
            logger.info(f"Redis pipeline error for newly-upserted symbols: {e}")

    # 8) INSERT METRICS DOCUMENTS (append-only per minute)
    # Use minute_epoch as LastUpdateTime (exact start of minute)
    last_update_time = int(minute_epoch)

    # a) insert for classified_db1 (initial mget or recheck)
    for sym, consolidation_time in classified_db1.items():
        doc = {
            "Symbol": sym,
            "ConsolidationSource": "db1",
            "ConsolidationTime": int(datetime.now(timezone(timedelta(hours=5, minutes=30))).timestamp() * 1000),
            "LastUpdateTime": last_update_time
        }
        try:
            await metrics_coll.insert_one(doc)
            logger.info(f"[METRIX] inserted db1 metric for {sym} @ ConsolidationTime={int(datetime.now(timezone(timedelta(hours=5, minutes=30))).timestamp() * 1000)}, LastUpdateTime={last_update_time}")
        except DuplicateKeyError:
            # If the same (Symbol, LastUpdateTime) doc already exists, ignore
            logger.info(f"[METRIX] duplicate metric exists for {sym} @ LastUpdateTime={last_update_time}; skipping insert")
        except Exception as e:
            logger.info(f"[METRIX] Failed to insert db1 metric for {sym}: {e}")

    # b) insert for all symbols we upserted -> db2 (use insertion time as ConsolidationTime)
    for sym, ts in upsert_symbols_with_ts:
        insert_time = int(datetime.now(timezone(timedelta(hours=5, minutes=30))).timestamp() * 1000)
        doc = {
            "Symbol": sym,
            "ConsolidationSource": "db2",
            "ConsolidationTime": insert_time,
            "LastUpdateTime": last_update_time
        }
        try:
            await metrics_coll.insert_one(doc)
            logger.info(f"[METRIX] inserted db2 metric for {sym} @ ConsolidationTime={insert_time}, LastUpdateTime={last_update_time}")
        except DuplicateKeyError:
            logger.info(f"[METRIX] duplicate metric exists for {sym} @ LastUpdateTime={last_update_time}; skipping insert")
        except Exception as e:
            logger.info(f"[METRIX] Failed to insert db2 metric for {sym}: {e}")

    # c) insert for any symbol still missing after retry => dbx
    if missing:
        decision_time = int(datetime.now(timezone(timedelta(hours=5, minutes=30))).timestamp() * 1000)
        for sym in missing:
            doc = {
                "Symbol": sym,
                "ConsolidationSource": "dbx",
                "ConsolidationTime": decision_time,
                "LastUpdateTime": last_update_time
            }
            try:
                await metrics_coll.insert_one(doc)
                logger.info(f"[METRIX] inserted dbx metric for {sym} @ ConsolidationTime={decision_time}, LastUpdateTime={last_update_time}")
            except DuplicateKeyError:
                logger.info(f"[METRIX] duplicate metric exists for {sym} @ LastUpdateTime={last_update_time}; skipping insert")
            except Exception as e:
                logger.info(f"[METRIX] Failed to insert dbx metric for {sym}: {e}")

    logger.info("run_cycle complete with append-only metrics recorded to ParallelCandleMetrics")

# ---------------------------------------------------------------------------

def load_symbols_from_csv() -> List[str]:
    path = INSTRUMENT_CSV
    p = Path(path)
    if not p.exists():
        logger.info(f"{path} not found")
        return []
    out: List[str] = []
    with p.open("r", encoding="utf-8", newline="") as fh:
        rdr = csv.DictReader(fh)
        if "symbol" not in (rdr.fieldnames or []):
            logger.info(f"CSV must contain column 'symbol'. Found columns: {rdr.fieldnames}")
            return []
        for r in rdr:
            s = (r.get("symbol") or "").strip()
            if s:
                out.append(s)
    logger.info(f"Loaded {len(out)} symbols from {path}")

    # add required index tickers if not already in list
    required = ['NIFTY 50', 'NIFTY BANK', 'NIFTY FIN SERVICE', 'SENSEX']
    for sym in required:
        if sym not in out:
            out.append(sym)

    return out

async def minute_scheduler_loop():
    global SHUTDOWN

    # clients
    client_main = AsyncIOMotorClient(MONGO_URI)
    db1_coll = client_main[DB1_NAME][DB1_COLL]
    db2_coll = client_main[DB2_NAME][DB2_COLL]

    # DB3 client/URI
    db3_uri = MONGO_DB3_URI
    client_db3 = AsyncIOMotorClient(db3_uri)
    db3_coll = client_db3[DB3_NAME][DB3_COLL]

    # Ensure shared index exists on all three (idempotent)
    await ensure_idx_symbol_lut(db1_coll)
    await ensure_idx_symbol_lut(db2_coll)
    await ensure_idx_symbol_lut(db3_coll)

    # Redis clients (optional) — cast ports/dbs to int to be safe
    try:
        redis_done_client = redis.Redis(
            host=DONE_REDIS_HOST,
            port=int(DONE_REDIS_PORT),
            db=6,
            password=DONE_REDIS_PASSWORD,
            decode_responses=True
        )
        redis_done_client.ping()
        logger.info(f"Connected to DONE Redis {DONE_REDIS_HOST}:{DONE_REDIS_PORT} db={DONE_REDIS_DB}")
    except Exception as e:
        logger.info(f"Cannot connect to DONE Redis: {e}")
        redis_done_client = None

    try:
        redis_ping_client = redis.Redis(
            host=PING_REDIS_HOST,
            port=int(PING_REDIS_PORT),
            db=int(PING_REDIS_DB),
            password=PING_REDIS_PASSWORD,
            decode_responses=True
        )
        redis_ping_client.ping()
        logger.info(f"Connected to PING Redis {PING_REDIS_HOST}:{PING_REDIS_PORT} db={PING_REDIS_DB}")
    except Exception as e:
        logger.info(f"Cannot connect to PING Redis: {e}")
        redis_ping_client = None

    # load symbols from CSV
    logger.info("Consolidator started. Loading symbols from CSV...")
    symbols = load_symbols_from_csv()
    if not symbols:
        logger.info("No symbols found. Exiting.")
        return

    logger.info(f"Loaded {len(symbols)} symbols (sample: {symbols[:10]})")

    while not SHUTDOWN:
        now = now_ist()
        next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        target_time = next_minute + timedelta(seconds=0.3)
        wait = (target_time - now).total_seconds()
        if wait > 0:
            await asyncio.sleep(wait)

        minute_epoch = minute_epoch_for_prev_minute(now_ist())
        logger.info(f"Running consolidation for {datetime.fromtimestamp(minute_epoch, IST).strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            await run_cycle(
                symbols, minute_epoch,
                db1_coll, db2_coll, db3_coll,
                redis_done_client, redis_ping_client
            )
        except Exception as e:
            logger.info(f"Error in run_cycle: {e}")

        # reload CSV each loop to pick up changes
        symbols = load_symbols_from_csv()

    client_main.close()
    client_db3.close()


def ask_shutdown(signum=None, frame=None):
    global SHUTDOWN
    SHUTDOWN = True
    logger.info("Shutdown requested...")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, ask_shutdown)
    signal.signal(signal.SIGTERM, ask_shutdown)

    loop = asyncio.get_event_loop()

    while not SHUTDOWN:
        now = now_ist()
        start_time = now.replace(hour=9, minute=15, second=30, microsecond=0)
        end_time = now.replace(hour=23, minute=30, second=30, microsecond=0)

        # Skip weekends
        if now.weekday() >= 5:
            logger.info("Weekend detected — sleeping for 1 hour.")
            time.sleep(3600)
            continue

        # If current time is within market hours → run the scheduler
        if start_time <= now <= end_time:
            logger.info("Market open — starting minute scheduler loop.")
            try:
                loop.run_until_complete(minute_scheduler_loop())
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
            finally:
                logger.info("Minute scheduler exited — waiting for next minute.")
                time.sleep(5)
        else:
            logger.info("Market closed — sleeping until next check.")
            time.sleep(60)  # check again after a minute

    loop.close()
    logger.info("Consolidator exiting.")
