

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
import logging

# -------------------- CONFIG --------------------
IST = pytz.timezone("Asia/Kolkata")

# Primary Mongo for DB1/DB2 (can be local or one URI)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

# DB1 (TrueData)
DB1_NAME = os.getenv("DB1_NAME", "True_Data_Candle_Data")
DB1_COLL = os.getenv("DB1_COLL", "OHLC_MINUTE_1")

# DB2 (Accelpix)
DB2_NAME = os.getenv("DB2_NAME", "Accelpix_Candle_Data")
DB2_COLL = os.getenv("DB2_COLL", "OHLC_MINUTE_1")

MONGO_DB3_URI = os.getenv("MONGO_DB3_URI", "mongodb://mudraksh:myUserAdmin@139.5.188.158:27017/?directConnection=true")
MONGO_DB3_HOST = os.getenv("MONGO_DB3_HOST", "139.5.188.158")
MONGO_DB3_PORT = os.getenv("MONGO_DB3_PORT", "27017")
MONGO_DB3_USER = os.getenv("MONGO_DB3_USER", "mudraksh")
MONGO_DB3_PASS = os.getenv("MONGO_DB3_PASS", "myUserAdmin")
DB3_NAME = os.getenv("DB3_NAME", "CandleData")
DB3_COLL = os.getenv("DB3_COLL", "OHLC_MINUTE_1")

# Redis for instrument list -- not used if CSV exists (we read CSV now)
REDIS_HOST = os.getenv("REDIS_HOST", "139.5.189.229")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "8"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "mudraksh_test")
REDIS_PATTERN = os.getenv("REDIS_PATTERN", "dv1:*")  # not used by CSV loader

# Redis endpoint(s) where we set DONE and ping keys (can be same as above or different)
DONE_REDIS_HOST = os.getenv("DONE_REDIS_HOST", "172.16.162.133")
DONE_REDIS_PORT = int(os.getenv("DONE_REDIS_PORT", 6379))
DONE_REDIS_DB = int(os.getenv("DONE_REDIS_DB", "6"))            # default DB for DONE writes
DONE_REDIS_PASSWORD = os.getenv("DONE_REDIS_PASSWORD", "mudraksh")

PING_REDIS_HOST = os.getenv("PING_REDIS_HOST", "172.16.162.133")
PING_REDIS_PORT = int(os.getenv("PING_REDIS_PORT", 6379))
PING_REDIS_DB = int(os.getenv("PING_REDIS_DB", "9"))            # default DB for ping timestamps
PING_REDIS_PASSWORD = os.getenv("PING_REDIS_PASSWORD", "mudraksh")

# TTL for DONE flag (seconds)
DONE_KEY_TTL = int(os.getenv("DONE_KEY_TTL", "10"))

# concurrency
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "200"))
MAX_AGE_SECONDS = int(os.getenv("MAX_AGE_SECONDS", "300"))

# CSV instrument list
INSTRUMENT_CSV = os.getenv("INSTRUMENT_CSV", "instrument_list_ohlc.csv")

# ------------------------------------------------
SHUTDOWN = False

# -------------------- LOGGER --------------------
Path("./logs").mkdir(parents=True, exist_ok=True)
logFileName = f'./logs/consolidator_{datetime.now().strftime("%Y%m%dT%H%M%S")}.log'

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


def parse_possible_ts_from_doc(doc: Dict) -> Optional[int]:
    if not doc:
        return None
    if "LastTradeTime" in doc and isinstance(doc["LastTradeTime"], (int, float)):
        return int(doc["LastTradeTime"])
    for k in ("ts", "td", "time", "timestamp"):
        val = doc.get(k)
        if isinstance(val, (int, float)):
            return int(val)
        if isinstance(val, str):
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    # assume IST if naive
                    return int(datetime.strptime(val, fmt).replace(tzinfo=IST).timestamp())
                except Exception:
                    pass
    return None

async def find_candle_in_db(coll, symbol: str, minute_epoch: int) -> Optional[Dict]:
    """
    Index-first lookup for candle:
      1) exact match on Symbol + LastTradeTime
      2) bounded range on LastTradeTime for that Symbol (minute window)
    Returns the document or None.
    """
    # Ensure types are int for LastTradeTime
    try:
        minute_epoch = int(minute_epoch)
    except Exception:
        logger.error(f"minute_epoch not int: {minute_epoch}")
        return None

    # Projection: only fields we need (tune as required)
    proj = {"_id": 1, "Symbol": 1, "LastTradeTime": 1, "Open": 1, "High": 1, "Low": 1, "Close": 1, "Volume": 1, "OpenInterest": 1}

    # 1) exact equality checks (fast)
    queries = [
        {"Symbol": symbol, "LastTradeTime": minute_epoch}
    ]

    # 2) bounded range (still indexable because Symbol is equality first, then range on LastTradeTime)
    range_queries = [
        {"Symbol": symbol, "LastTradeTime": {"$gte": minute_epoch, "$lte": minute_epoch + 59}}
    ]

    # Hint uses the compound index; make sure the index name/order matches your index.
    # You can pass either the index specification or the index name:
    index_hint = [("Symbol", 1), ("LastTradeTime", 1)]

    for q in queries + range_queries:
        try:
            # find_one supports 'hint' in motor/pymongo
            doc = await coll.find_one(q, projection=proj, hint=index_hint)
            if doc:
                logger.info(f"Index-hit for {symbol} query={q}")
                return doc
        except Exception as e:
            # If hint fails (index name mismatch) fallback gracefully to no-hint search once
            logger.info(f"find_one with hint failed for {q}: {e}; retrying without hint")
            try:
                doc = await coll.find_one(q, projection=proj)
                if doc:
                    logger.info(f"Index-hit (no hint) for {symbol} query={q}")
                    return doc
            except Exception as e2:
                logger.error(f"find_one retry failed for {q}: {e2}")

    # No doc found (important: no full-collection scan here)
    return None


async def push_to_db3(coll3, doc: Dict, source_db: str,
                      redis_done_client: redis.Redis, redis_ping_client: redis.Redis) -> None:
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
    # doc_copy["_consolidated_from"] = source_db
    # doc_copy["_consolidated_at"] = int(time.time())

    try:
        await coll3.update_one(
            {"Symbol": symbol, "LastTradeTime": ts},
            {"$set": doc_copy},
            upsert=True
        )
        logger.info(f"PUSHED -> {symbol} @ {datetime.fromtimestamp(ts, IST).strftime('%Y-%m-%d %H:%M:%S')} from {source_db}")
    except Exception as e:
        logger.error(f"Failed to upsert {symbol} ts={ts} into DB3: {e}")
        # still attempt redis updates below (best-effort)

    # Update DONE key in redis_done_client
    try:
        redis_done_client.set(symbol, "DONE", ex=DONE_KEY_TTL)
        logger.info(f"Set DONE key for {symbol} (ttl={DONE_KEY_TTL}s)")
    except Exception as e:
        logger.error(f"Failed to set DONE key for {symbol} in Redis: {e}")

    # Update ping timestamp in ping redis
    try:
        redis_ping_client.set(f"{symbol}_1", ts)
        logger.info(f"Set ping key {symbol}_1 -> {ts}")
    except Exception as e:
        logger.error(f"Failed to set ping key for {symbol} in Redis: {e}")

# === Replace check_and_consolidate_for_symbol with this version ===
async def check_and_consolidate_for_symbol(symbol: str, minute_epoch: int,
                                           db1_coll, db2_coll) -> Optional[Dict]:
    """
    Try db1 then db2, return tuple (doc, source_db) or None if not found.
    This function does NOT push to DB3 or Redis — it only finds and returns doc for batching.
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
                    redis_done_client: redis.Redis, redis_ping_client: redis.Redis):
    """
    Run primary pass + retry; collect found docs then bulk upsert into db3 and pipeline redis updates.
    """
    missing: List[str] = []
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    results_store = []  # list of (symbol, doc, source)

    async def _worker(sym: str):
        async with sem:
            try:
                found = await check_and_consolidate_for_symbol(sym, minute_epoch, db1_coll, db2_coll)
                if not found:
                    missing.append(sym)
                else:
                    # Keep original doc, but ensure we have integer LastTradeTime
                    doc = found["doc"]
                    source = found["source"]
                    results_store.append((sym, doc, source))
            except Exception as e:
                logger.error(f"Error processing {sym}: {e}")
                missing.append(sym)

    t0 = time.time()
    await asyncio.gather(*(_worker(s) for s in symbols))
    t_primary = time.time() - t0
    logger.info(f"Primary pass finished in {t_primary:.3f}s; found {len(results_store)} docs, {len(missing)} missing")

    # Retry missing once at 2-3s as before
    if missing:
        logger.info(f"{len(missing)} symbols missing after primary pass; will retry at second 3 for this minute")
        minute_start = datetime.fromtimestamp(minute_epoch, IST)
        target_retry_time = minute_start + timedelta(seconds=1)
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
                    logger.error(f"Retry error {sym}: {e}")
                    retry_missing.append(sym)

        t2 = time.time()
        await asyncio.gather(*(_retry_worker(s) for s in missing))
        t_retry = time.time() - t2
        logger.info(f"Retry pass finished in {t_retry:.3f}s; found additional {len(results_store) - 0} docs")
        if retry_missing:
            for s in retry_missing:
                logger.warning(f"NOT FOUND after retry: {s} @ {datetime.fromtimestamp(minute_epoch, IST).strftime('%Y-%m-%d %H:%M:%S')}")
    # Now we have results_store of docs to push — do bulk update + redis pipeline
    if not results_store:
        logger.info("No docs to push to DB3 this cycle")
        return

    # Build bulk ops for DB3 upserts
    from pymongo import UpdateOne
    bulk_ops = []
    for sym, doc, source in results_store:
        ts = parse_possible_ts_from_doc(doc)
        symbol = doc.get("Symbol") or doc.get("provider_tkr") or doc.get("symbol") or doc.get("sym")
        if not symbol or ts is None:
            logger.warning(f"Skipping (no symbol/ts) for {sym}: {doc}")
            continue
        # Optionally: strip large fields you don't want to store into DB3
        doc_copy = dict(doc)
        filter_q = {"Symbol": symbol, "LastTradeTime": ts}
        update_q = {"$set": doc_copy}
        bulk_ops.append(UpdateOne(filter_q, update_q, upsert=True))

    # Execute bulk write (unordered) and pipeline redis sets
    t_bulk_start = time.time()
    try:
        if bulk_ops:
            res = await db3_coll.bulk_write(bulk_ops, ordered=False)
            logger.info(f"Bulk upsert complete: matched={res.matched_count}, upserted={len(getattr(res, 'upserted_ids', []) or [])}, modified={res.modified_count}")
    except Exception as e:
        logger.error(f"DB3 bulk_write failed: {e}")

    # Redis pipelines: set DONE keys and ping keys in pipeline to reduce RTTs
    try:
        if redis_done_client:
            pipe_done = redis_done_client.pipeline()
            pipe_ping = redis_ping_client.pipeline() if redis_ping_client else None
            for sym, doc, source in results_store:
                symbol = doc.get("Symbol") or doc.get("provider_tkr") or doc.get("symbol") or doc.get("sym")
                ts = parse_possible_ts_from_doc(doc)
                if symbol:
                    pipe_done.set(symbol, "DONE", ex=DONE_KEY_TTL)
                    if pipe_ping is not None:
                        pipe_ping.set(f"{symbol}_1", ts)
            # execute both pipelines
            pipe_done.execute()
            if pipe_ping is not None:
                pipe_ping.execute()
            logger.info("Redis pipelines executed for DONE + PING")
    except Exception as e:
        logger.error(f"Redis pipeline error: {e}")

    t_bulk_total = time.time() - t_bulk_start
    logger.info(f"Bulk push + redis pipeline took {t_bulk_total:.3f}s for {len(bulk_ops)} ops")


def load_symbols_from_csv() -> List[str]:
    path = INSTRUMENT_CSV
    p = Path(path)
    if not p.exists():
        logger.error(f"{path} not found")
        return []
    out: List[str] = []
    with p.open("r", encoding="utf-8", newline="") as fh:
        rdr = csv.DictReader(fh)
        if "symbol" not in (rdr.fieldnames or []):
            logger.error(f"CSV must contain column 'symbol'. Found columns: {rdr.fieldnames}")
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


def build_db3_uri() -> str:
    """
    Build DB3 URI from either MONGO_DB3_URI (preferred) or individual parts.
    """
    if MONGO_DB3_URI:
        logger.info("Using provided MONGO_DB3_URI for DB3")
        return MONGO_DB3_URI

    if MONGO_DB3_HOST:
        userpass = ""
        if MONGO_DB3_USER:
            userpass = MONGO_DB3_USER
            if MONGO_DB3_PASS:
                userpass = f"{userpass}:{MONGO_DB3_PASS}"
            userpass = f"{userpass}@"
        port = f":{MONGO_DB3_PORT}" if MONGO_DB3_PORT else ""
        uri = f"mongodb://{userpass}{MONGO_DB3_HOST}{port}"
        logger.info(f"Constructed DB3 URI for host {MONGO_DB3_HOST}")
        return uri

    # fallback to main MONGO_URI
    logger.info("Falling back to MONGO_URI for DB3")
    return MONGO_URI


async def minute_scheduler_loop():
    global SHUTDOWN

    # clients
    client_main = AsyncIOMotorClient(MONGO_URI)
    db1_coll = client_main[DB1_NAME][DB1_COLL]
    db2_coll = client_main[DB2_NAME][DB2_COLL]

    # DB3 on separate client/URI
    db3_uri = build_db3_uri()
    client_db3 = AsyncIOMotorClient(db3_uri)
    db3_coll = client_db3[DB3_NAME][DB3_COLL]

    # Redis clients for DONE and PING updates (sync redis-py used for lightweight sets)
    try:
        redis_done_client = redis.Redis(host=DONE_REDIS_HOST, port=DONE_REDIS_PORT, db=DONE_REDIS_DB,
                                        password=DONE_REDIS_PASSWORD, decode_responses=True)
        redis_done_client.ping()
        logger.info(f"Connected to DONE Redis {DONE_REDIS_HOST}:{DONE_REDIS_PORT} db={DONE_REDIS_DB}")
    except Exception as e:
        logger.error(f"Cannot connect to DONE Redis: {e}")
        redis_done_client = None  # handled defensively

    try:
        redis_ping_client = redis.Redis(host=PING_REDIS_HOST, port=PING_REDIS_PORT, db=PING_REDIS_DB,
                                        password=PING_REDIS_PASSWORD, decode_responses=True)
        redis_ping_client.ping()
        logger.info(f"Connected to PING Redis {PING_REDIS_HOST}:{PING_REDIS_PORT} db={PING_REDIS_DB}")
    except Exception as e:
        logger.error(f"Cannot connect to PING Redis: {e}")
        redis_ping_client = None

    # load symbols from CSV (preferred)
    logger.info("Consolidator started. Loading symbols from CSV...")
    symbols = load_symbols_from_csv()
    if not symbols:
        logger.error("No symbols found. Exiting.")
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
            # pass redis clients, but functions expect redis.Redis; if None, handle gracefully
            await run_cycle(symbols, minute_epoch, db1_coll, db2_coll, db3_coll,
                            redis_done_client if redis_done_client else redis.Redis(decode_responses=True, socket_connect_timeout=1),
                            redis_ping_client if redis_ping_client else redis.Redis(decode_responses=True, socket_connect_timeout=1))
        except Exception as e:
            logger.error(f"Error in run_cycle: {e}")

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
    try:
        loop.run_until_complete(minute_scheduler_loop())
    finally:
        loop.close()
        logger.info("Consolidator exiting")
