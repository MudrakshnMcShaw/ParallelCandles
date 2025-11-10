#!/usr/bin/env python3
"""
Refined TrueData consumer with per-minute summary cycles:
- Cycle: starts at minute:00, lasts 3 seconds (until minute:03).
- During cycle we collect stats and at end emit a compact summary log.
- Queues are flushed at the end of each cycle.
- Reduced per-event console noise (summary logs at INFO).
"""

import os
import json
import time
import queue
import logging
import threading
import websocket
import datetime
from datetime import timezone, timedelta
from configparser import ConfigParser
from pymongo import MongoClient, UpdateOne
from redis import Redis

# -------------------- Globals --------------------
message_queue_raw = queue.Queue(maxsize=2000)
message_queue_parsed = queue.Queue(maxsize=2000)

ws = None
connected = False
should_reconnect = True
reconnect_delay = 5
max_reconnect_delay = 300

# config
cfg = ConfigParser()
cfg.read('/root/ParallelCandles/config.ini')

# DB / Redis handles (filled by setup_connections)
mongo_local_db = None
mongo_live_db = None
redis_sym_conn = None   # used for raw list push
redis_ping_conn = None  # used to persist id map and ping timestamps
done_redis_db = None

# runtime maps and params
sym_id_map = {}
td_to_orig = {}
expiry_map = {}
candle_cutoff_seconds = 62
redis_raw_list = 'trueDataQueue'

# subscription list that will be sent from on_open
last_subscription = []

# -------------------- Cycle statistics (thread-safe) --------------------
stats_lock = threading.Lock()
cycle_stats = {
    'expected_symbols': 0,
    'processed_symbols': set(),   # set of Symbol strings that were processed this cycle
    'done_symbols': set(),        # set of symbols for which DONE was set (redis)
    'local_insert_count': 0,
    'live_insert_count': 0,
    'last_symbol': None,
    'last_symbol_time_ms': None,
    'cycle_start_ts': None,
    'cycle_end_ts': None
}

# -------------------- Logging --------------------
# -------------------- Logging --------------------
os.makedirs("./APIResponseLogs", exist_ok=True)
logFileName = f'./APIResponseLogs/truedata_cycle_{datetime.datetime.now().strftime("%Y%m%dT%H%M%S")}.log'
console = logging.StreamHandler()
console.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(levelname)s] %(module)s %(asctime)s %(message)s",
    handlers=[logging.FileHandler(logFileName), console]
)

# <<< ADD THIS BLOCK IMMEDIATELY AFTER basicConfig >>>
import logging as _logging
_logging.getLogger('pymongo').setLevel(_logging.WARNING)
_logging.getLogger('urllib3').setLevel(_logging.WARNING)
# <<< END ADD >>>

logging.info("Logger configured. Logfile: %s", logFileName)

# -------------------- Helpers --------------------
def now_ms():
    return int(time.time() * 1000)

def parse_iso_as_ist(ts_str: str) -> int:
    """Parse 'YYYY-MM-DDTHH:MM:SS' as IST and return epoch seconds (defensive)."""
    try:
        dt = datetime.datetime.strptime(ts_str, '%Y-%m-%dT%H:%M:%S')
        ist = timezone(timedelta(hours=5, minutes=30))
        dt = dt.replace(tzinfo=ist)
        epoch = int(dt.timestamp())
        # logging.debug("Parsed timestamp %s -> %d", ts_str, epoch)
        return epoch
    except Exception as ex:
        logging.debug("parse_iso_as_ist fallback for %s: %s", ts_str, ex)
        try:
            return int(time.mktime(time.strptime(ts_str, '%Y-%m-%dT%H:%M:%S')))
        except Exception as e2:
            logging.error("Failed to parse timestamp %s: %s", ts_str, e2)
            raise

def load_symbol_mapping(csv_file="/root/ParallelCandles/truedata/sample_truedata_out.csv"):
    """Load mapping td_to_orig from CSV; keys are TrueData symbols."""
    global td_to_orig
    td_to_orig = {}
    try:
        import csv
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                td = (row.get("truedata_symbol") or "").strip()
                orig = (row.get("original_symbol") or "").strip()
                if td and orig:
                    td_to_orig[td] = orig
        logging.info("Loaded %d td->orig mappings from %s", len(td_to_orig), csv_file)
    except Exception as e:
        logging.warning("Could not load CSV mapping (%s): %s", csv_file, e)

def count_bar1min_entries(parsed_or_raw):
    """Count bar1min entries in raw or parsed message."""
    parsed = None
    if isinstance(parsed_or_raw, dict):
        parsed = parsed_or_raw
    else:
        try:
            parsed = json.loads(parsed_or_raw)
        except Exception:
            parsed = None

    if not parsed or 'bar1min' not in parsed:
        return 0

    bar = parsed['bar1min']
    if isinstance(bar, (list, tuple)) and len(bar) > 0 and isinstance(bar[0], (list, tuple)):
        return len(bar)
    return 1

def snapshot_queue(q, max_items=10):
    """Safe non-destructive snapshot of queue contents (up to max_items)."""
    items = []
    try:
        with q.mutex:
            for i, it in enumerate(q.queue):
                if i >= max_items:
                    break
                items.append(it)
    except Exception as e:
        logging.error("snapshot_queue error: %s", e)
    return items

def flush_queue(q):
    """Clear a queue safely using its internal mutex and return count cleared."""
    count = 0
    try:
        with q.mutex:
            count = len(q.queue)
            q.queue.clear()
        return count
    except Exception as e:
        logging.error("flush_queue error: %s", e)
        return 0

# -------------------- Symbol processing --------------------
def process_symbols_added(symbollist):
    """
    Defensive processing of the 'symbols added' payload.
    Accepts lists of [truedata_symbol, numeric_id] or dicts. Updates sym_id_map.
    """
    global sym_id_map, td_to_orig, expiry_map, redis_ping_conn
    logging.debug("process_symbols_added called; incoming sample len=%s", len(symbollist) if symbollist else 0)
    if not symbollist:
        logging.warning("process_symbols_added called with empty list")
        return

    for entry in symbollist:
        try:
            td_sym = None
            td_id = None
            if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                td_sym = entry[0]
                td_id = str(entry[1])
            elif isinstance(entry, dict):
                td_sym = entry.get("symbol") or entry.get("truedata_symbol") or entry.get("name")
                td_id = str(entry.get("id") or entry.get("token") or entry.get("numeric_id") or entry.get("instId", ""))
            else:
                logging.debug("Unknown symbol entry format: %r", entry)
                continue

            if not td_sym or not td_id:
                logging.debug("Incomplete symbol entry: %r", entry)
                continue

            for k, v in (expiry_map or {}).items():
                if k in td_sym:
                    td_sym = td_sym.replace(k, v)

            orig = td_to_orig.get(td_sym, td_sym)
            sym_id_map[td_id] = orig
        except Exception as e:
            logging.error("Error processing symbol entry %r: %s", entry, e)

    logging.info("Updated sym_id_map size=%d", len(sym_id_map))
    if redis_ping_conn is not None:
        try:
            redis_ping_conn.hset('trueDataIDMap', mapping=sym_id_map)
        except Exception as e:
            logging.error("Failed to persist trueDataIDMap to redis: %s", e)

# -------------------- WebSocket callbacks --------------------
def on_open(wsapp):
    global connected, reconnect_delay, last_subscription
    connected = True
    reconnect_delay = 5
    logging.info("WebSocket opened")
    try:
        if last_subscription:
            payload = {"method": "addsymbol", "symbols": last_subscription}
            wsapp.send(json.dumps(payload))
            logging.info("Subscription sent from on_open: %d symbols", len(last_subscription))
    except Exception as e:
        logging.error("Failed to send subscription from on_open: %s", e)

def on_close(wsapp, close_status_code, close_msg):
    global connected
    connected = False
    logging.warning("WebSocket closed: %s - %s", close_status_code, close_msg)

def on_error(wsapp, err):
    global connected
    connected = False
    logging.error("WebSocket error: %s", err)

def on_message(wsapp, message):
    """Quickly handle incoming messages; enqueue raw and parsed; process controls."""
    try:
        if not isinstance(message, str):
            # logging.debug("Received non-str message, ignoring (type=%s)", type(message))
            return

        bar_count = count_bar1min_entries(message)
        # logging.debug("message bar count=%d", bar_count)

        has_bar = bar_count > 0

        if has_bar:
            try:
                message_queue_raw.put_nowait(message)
            except queue.Full:
                logging.warning("message_queue_raw full; dropped raw message")

        parsed = None
        try:
            parsed = json.loads(message)
        except Exception:
            parsed = None

        if parsed and isinstance(parsed, dict):
            # control message handling
            if parsed.get('success') is not None and parsed.get('message'):
                possible_lists = [
                    parsed.get('symbollist'),
                    parsed.get('symbols'),
                    parsed.get('symbol_list'),
                    parsed.get('data'),
                    parsed.get('symList'),
                    parsed.get('symbolList')
                ]
                symbollist = next((x for x in possible_lists if isinstance(x, (list, tuple))), None)
                if symbollist:
                    process_symbols_added(symbollist)

            try:
                message_queue_parsed.put_nowait(parsed)
            except queue.Full:
                logging.warning("message_queue_parsed full; dropped parsed message")
    except Exception as e:
        logging.error("Unexpected error in on_message: %s", e)

# -------------------- Worker threads --------------------
def stream_handler_raw(redis_conn):
    logging.info("stream_handler_raw started")
    while True:
        buffer = []
        try:
            while not message_queue_raw.empty() and len(buffer) < 200:
                try:
                    msg = message_queue_raw.get_nowait()
                    buffer.append(msg)
                except queue.Empty:
                    break
        except Exception as e:
            logging.error("Error reading message_queue_raw: %s", e)

        if buffer:
            try:
                redis_conn.rpush(redis_raw_list, *buffer)
            except Exception as e:
                logging.error("Failed pushing raw messages to Redis: %s", e)
        time.sleep(0.001)

def stream_handler_parsed(mongo_coll_primary, mongo_coll_db3, ping_conn, done_redis_conn):
    logging.info("stream_handler_parsed started")
    while True:
        buffer = []
        try:
            while not message_queue_parsed.empty() and len(buffer) < 500:
                try:
                    parsed = message_queue_parsed.get_nowait()
                except queue.Empty:
                    break

                if not isinstance(parsed, dict):
                    continue

                if parsed.get('success') and parsed.get('message'):
                    possible_lists = [parsed.get('symbollist'), parsed.get('symbols'), parsed.get('data')]
                    symbollist = next((x for x in possible_lists if isinstance(x, (list, tuple))), None)
                    if symbollist:
                        process_symbols_added(symbollist)
                    continue

                if 'bar1min' in parsed:
                    try:
                        bar = parsed['bar1min']
                        buffer.append(bar)
                    except Exception as e:
                        logging.error("Malformed bar1min in parsed message: %s", e)
                        continue
        except Exception as e:
            logging.error("Error processing message_queue_parsed: %s", e)

        if buffer:
            buffer.reverse()
            try:
                handle_candle_buffer(buffer, mongo_coll_primary, mongo_coll_db3, ping_conn, done_redis_conn)
            except Exception as e:
                logging.error("handle_candle_buffer failed: %s", e)
        time.sleep(0.001)

def handle_candle_buffer(candle_list, coll_primary, coll_db3, ping_conn, done_redis_conn):
    """
    Convert list-of-bars into documents and write them.
    Update cycle_stats counters under lock when actions succeed.
    """
    now_time = time.time()
    docs = []
    # Keep mapping of symbols to docs to attribute counts correctly
    symbol_docs_map = {}

    for candle in candle_list:
        try:
            if not isinstance(candle, (list, tuple)) or len(candle) < 6:
                logging.debug("Unexpected candle format: %r", candle)
                continue

            td_id = str(candle[0])
            ts_str = str(candle[1])
            try:
                last_trade_time = parse_iso_as_ist(ts_str)
            except Exception as e:
                logging.error("Timestamp parse failed for %s: %s", ts_str, e)
                continue

            symbol = sym_id_map.get(td_id)
            if symbol is None:
                logging.debug("No mapping for TrueData id %s; skipping candle", td_id)
                continue

            if time.time() - last_trade_time > candle_cutoff_seconds:
                logging.debug("Stale candle for %s; skipping", symbol)
                continue

            doc = {
                'Symbol': symbol,
                'LastTradeTime': last_trade_time,
                'Open': float(candle[2]),
                'High': float(candle[3]),
                'Low': float(candle[4]),
                'Close': float(candle[5]),
                'Volume': int(candle[6]) if len(candle) > 6 and candle[6] is not None else 0,
                'OpenInterest': int(candle[7]) if len(candle) > 7 and candle[7] is not None else 0
            }
            docs.append(doc)
            symbol_docs_map.setdefault(symbol, []).append(doc)
        except Exception as e:
            logging.error("Error preparing candle doc: %s | data: %r", e, candle)

    if not docs:
        return

    # Insert into primary Mongo and update local_insert_count
    inserted_symbols = set()
    try:
        res = coll_primary.insert_many(docs)
        inserted_count = len(res.inserted_ids)
        logging.debug("Inserted %d docs into primary Mongo", inserted_count)
        # attribute inserted symbols
        for d in docs:
            inserted_symbols.add(d['Symbol'])
    except Exception as e:
        logging.error("Primary Mongo insert_many failed: %s", e)
        inserted_count = 0

    # Upsert into DB3 if configured
    live_upserted_symbols = set()
    if coll_db3 is not None:
        try:
            ops = []
            for d in docs:
                filt = {"Symbol": d["Symbol"], "LastTradeTime": d["LastTradeTime"]}
                ops.append(UpdateOne(filt, {"$set": d}, upsert=True))
            if ops:
                result = coll_db3.bulk_write(ops, ordered=False)
                # best-effort attribution: treat all docs as upserted/handled
                for d in docs:
                    live_upserted_symbols.add(d['Symbol'])
                logging.debug("DB3 bulk_write completed (docs=%d)", len(ops))
        except Exception as e:
            logging.error("DB3 bulk_write/upsert failed: %s", e)

    # Set DONE flags and pings, increment stats on success
    done_symbols_local = set()
    for sym in inserted_symbols:
        try:
            # logging.info("Setting DONE for %s", sym)
            done_redis_db.set(sym, 'DONE', ex=10)
            done_symbols_local.add(sym)
        except Exception as e:
            logging.error("Failed to set DONE for %s: %s", sym, e)

    # ping updates
    for sym in inserted_symbols:
        try:
            # For last trade time, pick the last doc for the symbol
            last_ts = symbol_docs_map[sym][-1]['LastTradeTime']
            redis_ping_conn.set(f"{sym}_1", last_ts)
        except Exception as e:
            logging.error("Failed to set ping for %s: %s", sym, e)

    # Update cycle stats under lock
    with stats_lock:
        for sym in inserted_symbols:
            cycle_stats['processed_symbols'].add(sym)
        for sym in done_symbols_local:
            cycle_stats['done_symbols'].add(sym)
        cycle_stats['local_insert_count'] += len(inserted_symbols)
        cycle_stats['live_insert_count'] += len(live_upserted_symbols)
        # update last symbol / timestamp with ms
        if inserted_symbols:
            # choose most recent symbol from docs based on last_trade_time
            latest_sym = None
            latest_ts = -1
            for sym, docs_for_sym in symbol_docs_map.items():
                t = docs_for_sym[-1]['LastTradeTime']
                if t > latest_ts:
                    latest_ts = t
                    latest_sym = sym
            cycle_stats['last_symbol'] = latest_sym
            cycle_stats['last_symbol_time_ms'] = now_ms()

# -------------------- WebSocket runner with reconnect --------------------
def start_websocket(url, on_msg, on_err, on_clo, on_op):
    global ws, should_reconnect, reconnect_delay
    logging.info("start_websocket launching background thread for %s", url)

    def run_forever():
        global ws, connected, reconnect_delay
        while should_reconnect:
            ws = websocket.WebSocketApp(url, on_message=on_msg, on_error=on_err, on_close=on_clo)
            ws.on_open = on_op
            try:
                logging.debug("WebSocketApp run_forever starting")
                ws.run_forever()
                connected = False
                if should_reconnect:
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
            except Exception as e:
                logging.error("WebSocket run_forever raised: %s", e)
                time.sleep(reconnect_delay)

    t = threading.Thread(target=run_forever, daemon=True)
    t.start()
    return t

# -------------------- Setup DB/Redis connections --------------------
def setup_connections():
    global mongo_local_db, mongo_live_db, redis_sym_conn, redis_ping_conn, done_redis_db, expiry_map

    logging.info("Setting up DB and Redis connections from config")
    # Mongo primary
    mongo_uri = cfg.get('local_db', 'MONGO_URI')
    client_primary = MongoClient(mongo_uri)
    mongo_local_db = client_primary['True_Data_Candle_Data']['OHLC_MINUTE_1']
    logging.info("Connected to primary Mongo")

    # Mongo DB3 optional
    mongo_uri_db3 = cfg.get('live_db', 'MONGO_URI', fallback=None)
    if mongo_uri_db3:
        client_db3 = MongoClient(mongo_uri_db3)
        mongo_live_db = client_db3['CandleData']['OHLC_MINUTE_1']
        logging.info("Connected to DB3 Mongo")
    else:
        mongo_live_db = None
        logging.info("DB3 not configured")

    # Redis connections
    redis_host = cfg.get('live_db', 'REDIS_HOST')
    redis_port = int(cfg.get('live_db', 'REDIS_PORT'))
    redis_pass = cfg.get('live_db', 'REDIS_PASSWORD', fallback=None)

    redis_sym_conn = Redis(db=0, host=redis_host, port=redis_port, password=redis_pass, decode_responses=True)
    redis_ping_conn = Redis(db=int(cfg.get('redisParams', 'pingdb')), host=redis_host, port=redis_port, password=redis_pass, decode_responses=True)
    done_redis_db = Redis(db=6, host=redis_host, port=redis_port, password=redis_pass, decode_responses=True)

    logging.info("Redis connections ready (host=%s port=%s)", redis_host, redis_port)

    expiry_map = {}
    globals().update({
        'mongo_local_db': mongo_local_db,
        'mongo_live_db': mongo_live_db,
        'redis_sym_conn': redis_sym_conn,
        'redis_ping_conn': redis_ping_conn,
        'done_redis_db': done_redis_db
    })

    return mongo_local_db, mongo_live_db, redis_sym_conn, redis_ping_conn, done_redis_db

# -------------------- Cycle manager --------------------
def cycle_manager():
    """
    Runs in background. At the start of every minute it opens a cycle window
    that lasts 3 seconds (00.000 -> 03.000). During that window we collect stats.
    At the end we log a single summary and flush queues.
    """
    logging.info("Cycle manager started (per-minute cycles, 3 seconds each)")
    while True:
        # wait until top of next minute
        now = datetime.datetime.now()
        next_minute = (now + datetime.timedelta(minutes=1)).replace(second=0, microsecond=0)
        sleep_for = (next_minute - now).total_seconds()
        time.sleep(sleep_for)

        # start cycle
        start_ts = time.time()
        with stats_lock:
            cycle_stats['cycle_start_ts'] = start_ts
            cycle_stats['cycle_end_ts'] = None
            cycle_stats['processed_symbols'].clear()
            cycle_stats['done_symbols'].clear()
            cycle_stats['local_insert_count'] = 0
            cycle_stats['live_insert_count'] = 0
            cycle_stats['last_symbol'] = None
            cycle_stats['last_symbol_time_ms'] = None
            cycle_stats['expected_symbols'] = len(last_subscription) if last_subscription else 0

        # keep window open for 3 seconds
        window_end = start_ts + 3.0
        logging.debug("Cycle started at %s (will run until %s)", start_ts, window_end)
        while time.time() < window_end:
            # just sleep in short bursts; workers update stats concurrently
            time.sleep(0.05)

        # close cycle and compute summary
        end_ts = time.time()
        with stats_lock:
            cycle_stats['cycle_end_ts'] = end_ts
            expected = cycle_stats['expected_symbols']
            processed = set(cycle_stats['processed_symbols'])
            done = set(cycle_stats['done_symbols'])
            local_count = cycle_stats['local_insert_count']
            live_count = cycle_stats['live_insert_count']
            last_sym = cycle_stats['last_symbol']
            last_sym_ts_ms = cycle_stats['last_symbol_time_ms']

        logging.info(f"processed {processed} done {done}")

        missing = []
        # if last_subscription:
        #     # last_subscription contains expected TrueData symbol strings (or original names as you prepared)
        #     for s in last_subscription:
        #         if s not in processed:
        #             missing.append(s)

        # # Trim large lists for logging
        # missing_preview = missing

        # # Summary log (single compact message)
        # logging.info(
        #     "CYCLE SUMMARY minute=%s processed=%d missing=%d done=%d local_inserts=%d live_inserts=%d last_symbol=%s last_symbol_ts_ms=%s",
        #     next_minute.strftime("%Y-%m-%dT%H:%M"),
        #     len(processed),
        #     len(missing),
        #     len(done),
        #     local_count,
        #     live_count,
        #     last_sym or "-",
        #     last_sym_ts_ms or "-"
        # )
        # if missing:
        #     logging.info("Missing sample (%d): %s", len(missing), ", ".join(missing_preview))

        # compute expected originals (same namespace as processed set)
        expected_originals = []
        if last_subscription:
            for td_sym in last_subscription:
                # map TrueData symbol -> original symbol if mapping available
                expected_originals.append(td_to_orig.get(td_sym, td_sym))

        # processed is a set of original-symbol strings (as created in handle_candle_buffer)
        processed = set(cycle_stats['processed_symbols'])

        # compute missing using the same symbol form
        missing = [s for s in expected_originals if s not in processed]

        # prepare safe preview (stringify and trim)
        missing_preview = [str(x) for x in missing]
        missing_preview_str = ", ".join(missing_preview) if missing_preview else ""

        logging.info(
            "CYCLE SUMMARY minute=%s processed=%d missing=%d done=%d local_inserts=%d live_inserts=%d last_symbol=%s last_symbol_ts_ms=%s",
            next_minute.strftime("%Y-%m-%dT%H:%M"),
            len(processed),
            len(missing),
            len(done),
            local_count,
            live_count,
            last_sym or "-",
            last_sym_ts_ms or "-"
        )

        if missing:
            logging.info("Missing sample (%d): %s", len(missing), missing_preview_str)
        else:
            logging.debug("No missing symbols this cycle")


        # Flush queues at cycle end and log counts
        raw_cleared = flush_queue(message_queue_raw)
        parsed_cleared = flush_queue(message_queue_parsed)
        logging.info("Flushed queues at cycle end: raw_cleared=%d parsed_cleared=%d", raw_cleared, parsed_cleared)

# -------------------- Debug printers (light) --------------------
def start_debug_printers():
    def debug_sym_map(interval=30):
        while True:
            try:
                logging.debug("sym_id_map size=%d sample=%s", len(sym_id_map), dict(list(sym_id_map.items())[:10]))
            except Exception:
                pass
            time.sleep(interval)
    threading.Thread(target=debug_sym_map, daemon=True).start()

# -------------------- Main runner --------------------
def run_main_loop():
    global last_subscription

    logging.info("Run main loop starting")
    load_symbol_mapping()

    # build subscription list using TrueData symbols (keys of td_to_orig)
    subscription_list = ['NIFTY 50', 'NIFTY BANK', 'NIFTY FIN SERVICE', 'SENSEX', 'INDIA VIX']
    if td_to_orig:
        subscription_list += list(td_to_orig.keys())
    seen = set()
    subscription_list = [s for s in subscription_list if not (s in seen or seen.add(s))]
    logging.info("Prepared subscription list with %d symbols", len(subscription_list))
    last_subscription = subscription_list  # set global for on_open

    # Setup DB / Redis
    coll_primary, coll_db3, sym_conn, ping_conn, done_conn = setup_connections()

    # Start cycle manager and debug printers
    threading.Thread(target=cycle_manager, daemon=True).start()
    start_debug_printers()

    # Start worker threads
    threading.Thread(target=stream_handler_raw, args=(sym_conn,), daemon=True).start()
    threading.Thread(target=stream_handler_parsed, args=(coll_primary, coll_db3, ping_conn, done_conn), daemon=True).start()
    logging.info("Worker threads started")

    # Start websocket
    username = cfg.get('truedata', 'userID')
    password = cfg.get('truedata', 'password')
    port = cfg.get('truedata', 'port')
    ws_url = f"wss://push.truedata.in:{port}?user={username}&password={password}"
    start_websocket(ws_url, on_message, on_error, on_close, on_open)

    # Wait briefly for connection; subscription will be sent in on_open as well
    wait = 0
    while not connected and wait < 10:
        time.sleep(1)
        wait += 1

    # If sym_id_map empty after connection, try re-send once
    check_wait = 0
    while check_wait < 10:
        if sym_id_map:
            break
        time.sleep(1)
        check_wait += 1

    if not sym_id_map:
        try:
            if ws is not None and last_subscription:
                ws.send(json.dumps({"method": "addsymbol", "symbols": last_subscription}))
        except Exception as e:
            logging.error("Failed to re-send subscription: %s", e)

    logging.info("run_main_loop completed setup; pipeline running")

# -------------------- Entry point with market-hours gating --------------------
if __name__ == "__main__":
    import pytz
    IST = pytz.timezone("Asia/Kolkata")

    def ist_now():
        return datetime.datetime.now(IST)

    def next_market_start(now):
        next_start = now.replace(hour=9, minute=15, second=30, microsecond=0)
        if now >= next_start:
            next_start += timedelta(days=1)
        while next_start.weekday() >= 5:
            next_start += timedelta(days=1)
        return next_start

    logging.info("Starting main market-hours runner")
    while True:
        now = ist_now()
        weekday = now.weekday()
        start_time = now.replace(hour=9, minute=15, second=30, microsecond=0)
        end_time = now.replace(hour=23, minute=30, second=30, microsecond=0)

        if weekday >= 5:
            next_start = next_market_start(now)
            time.sleep((next_start - now).total_seconds())
            continue

        if now < start_time:
            sleep_seconds = (start_time - now).total_seconds()
            time.sleep(sleep_seconds)
            continue

        if now > end_time:
            next_start = next_market_start(now)
            time.sleep((next_start - now).total_seconds())
            continue

        # Inside market hours: start pipeline
        try:
            run_main_loop()
        except KeyboardInterrupt:
            should_reconnect = False
            try:
                if ws:
                    ws.close()
            except Exception:
                pass
            break
        except Exception as e:
            logging.error("Unexpected error in run_main_loop: %s", e)

        # Keep process alive until market end
        while True:
            now = ist_now()
            if now > end_time:
                should_reconnect = False
                try:
                    if ws:
                        ws.close()
                except Exception:
                    pass
                break
            time.sleep(30)

        # Sleep until next market open
        next_start = next_market_start(ist_now())
        time.sleep((next_start - ist_now()).total_seconds())
