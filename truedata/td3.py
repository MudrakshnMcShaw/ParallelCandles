
import websocket
import threading
import time
import json
import queue
import urllib.request as urllib
import logging
import datetime
import csv
from pymongo import MongoClient
from redis import Redis
from configparser import ConfigParser

# ---------------------------
# Context creation & helpers
# ---------------------------

def create_context(config_path: str = "config.ini") -> dict:
    """Create and return a context dict to hold state instead of using a class."""
    config = ConfigParser()
    config.read(config_path)

    # Mongo connection (fix original trailing-comma bug)
    mongo_host = config.get('mongoParams', 'ip')
    mongo_port = int(config.get('mongoParams', 'port'))
    conn = MongoClient(host=mongo_host, port=mongo_port)
    db_cloud = conn['True_Data']['OHLC_MINUTE_1']

    # Basic fields and queues
    ctx = {
        'config': config,
        'db_cloud': db_cloud,
        'messageQueue1': queue.Queue(maxsize=2000),
        'messageQueue2': queue.Queue(maxsize=2000),
        'redisQueue': 'trueDataQueue',
        'toggle': False,
        'ws': None,
        'connected': False,
        'should_reconnect': True,
        'reconnect_delay': 5,
        'max_reconnect_delay': 300,
        'candleCuttOffTime': 62,
        'symIdMap': {},
        'td_to_orig': {},
        'expiryMap': json.loads(config.get('expiryDetails','expiryMap')),
        'subsricptionList': ['NIFTY 50', 'NIFTY BANK', 'NIFTY FIN SERVICE', 'SENSEX'],
        # Redis connections will be set after reading redis config
        'redisConn_cloud': None,
        'symConn': None,
        'pingConn': None,
        # API login fields
        'username': config.get('loginDetails', 'userID'),
        'password': config.get('loginDetails', 'password'),
        'port': config.get('loginDetails', 'port'),
        # logging
        'logFileName': f'./APIResponseLogs/logfile_{datetime.datetime.now().strftime("%Y%m%dT%H%M%S")}.log'
    }

    # Redis connections
    redis_host = config.get('redisParams', 'ip')
    redis_port = int(config.get('redisParams', 'port'))
    redis_db = int(config.get('redisParams', 'db'))
    pingdb = int(config.get('redisParams', 'pingdb'))

    ctx['redisConn_cloud'] = Redis(db=redis_db, decode_responses=True,
                                  host=redis_host, port=redis_port,
                                  password="mudraksh_test")
    ctx['symConn'] = Redis(db=0, decode_responses=True,
                           host=redis_host, port=redis_port,
                           password="mudraksh_test")
    ctx['pingConn'] = Redis(db=pingdb, decode_responses=True,
                            host=redis_host, port=redis_port,
                            password="mudraksh_test")

    # Load any symbol mapping CSV
    converted_file = "sample_truedata_out.csv"
    try:
        with open(converted_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                td_sym = row.get("truedata_symbol")
                orig_sym = row.get("original_symbol")
                if td_sym and orig_sym:
                    td_sym, orig_sym = td_sym.strip(), orig_sym.strip()
                    ctx['subsricptionList'].append(td_sym)
                    ctx['td_to_orig'][td_sym] = orig_sym
    except Exception as e:
        logging.warning(f"Could not load converted symbols: {e}")

    # dedupe list preserving order
    seen = set()
    ctx['subsricptionList'] = [s for s in ctx['subsricptionList'] if not (s in seen or seen.add(s))]

    return ctx

# ---------------------------
# WebSocket callbacks
# ---------------------------

def on_error(ws, error, ctx):
    logging.error(f"WebSocket error: {error}")
    ctx['connected'] = False

def on_close(ws, close_status_code, close_msg, ctx):
    logging.error(f"WebSocket closed: {close_status_code} - {close_msg}")
    ctx['connected'] = False

def on_open(ws, ctx):
    print('Starting websocket connection with TrueData')
    logging.info('Starting WebSocket connection')
    ctx['connected'] = True
    ctx['reconnect_delay'] = 5  # reset backoff on success

def on_message(ws, message, ctx):
    """
    Put raw JSON into messageQueue1 (archival) and parsed dict into messageQueue2 (processing).
    """
    try:
        try:
            if 'bar1min' in message:
                try:
                    ctx['messageQueue1'].put_nowait(message)
                except queue.Full:
                    logging.warning("messageQueue1 is full; dropping raw message")
        except Exception:
            pass

        parsed = None
        if isinstance(message, str):
            try:
                parsed = json.loads(message)
            except Exception as e:
                logging.debug(f"Failed to parse incoming message as JSON: {e}")
                parsed = None

        if parsed and isinstance(parsed, dict):
            try:
                ctx['messageQueue2'].put_nowait(parsed)
            except queue.Full:
                logging.warning("messageQueue2 is full; dropping parsed message")
    except Exception as e:
        logging.error(f"Unexpected error in on_message: {e}")

# ---------------------------
# Start websocket & threads
# ---------------------------

def start_api(ctx):
    """Create WebSocketApp and start a background thread to run_forever with reconnect/backoff."""
    websocket.enableTrace(False)
    try:
        if ctx.get('ws') is not None:
            try:
                ctx['ws'].close()
            except Exception:
                pass

        url = f"wss://push.truedata.in:{ctx['port']}?user={ctx['username']}&password={ctx['password']}"
        # wrapper callbacks that inject ctx
        def _on_message(ws, msg): return on_message(ws, msg, ctx)
        def _on_error(ws, err): return on_error(ws, err, ctx)
        def _on_close(ws, code, reason): return on_close(ws, code, reason, ctx)
        def _on_open(ws): return on_open(ws, ctx)

        ws_app = websocket.WebSocketApp(
            url,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
            keep_running=False
        )
        ws_app.on_open = _on_open
        ctx['ws'] = ws_app

        def run_websocket_loop():
            while ctx['should_reconnect']:
                try:
                    ws_app.run_forever()
                    if ctx['should_reconnect']:
                        logging.info(f"Connection lost. Reconnecting in {ctx['reconnect_delay']} seconds...")
                        time.sleep(ctx['reconnect_delay'])
                        ctx['reconnect_delay'] = min(ctx['reconnect_delay'] * 2, ctx['max_reconnect_delay'])
                except Exception as e:
                    logging.error(f"WebSocket run_forever error: {e}")
                    if ctx['should_reconnect']:
                        time.sleep(ctx['reconnect_delay'])

        t = threading.Thread(target=run_websocket_loop, daemon=True)
        t.start()
    except Exception as e:
        logging.error(f"Error in start_api: {e}")
        ctx['connected'] = False

# ---------------------------
# Stream handlers (workers)
# ---------------------------

def stream_handler1(ctx):
    """
    Consume raw JSON strings from messageQueue1 and rpush them to Redis list self.redisQueue.
    """
    while True:
        buffer = []
        try:
            while not ctx['messageQueue1'].empty() and len(buffer) < 200:
                try:
                    msg = ctx['messageQueue1'].get_nowait()
                    buffer.append(msg)
                except queue.Empty:
                    break
        except Exception as e:
            logging.error(f"Error reading messageQueue1: {e}")

        if buffer:
            try:
                ctx['symConn'].rpush(ctx['redisQueue'], *buffer)
                logging.debug(f"Pushed {len(buffer)} raw messages to Redis list")
            except Exception as e:
                logging.error(f"Error pushing raw messages to redis ({ctx['redisQueue']}): {e}")
        time.sleep(0.001)

def stream_handler2(ctx):
    """
    Consume parsed dict messages from messageQueue2, extract bar1min lists and call on_candleData.
    """
    while True:
        buffer = []
        try:
            while not ctx['messageQueue2'].empty() and len(buffer) < 500:
                try:
                    parsed = ctx['messageQueue2'].get_nowait()
                except queue.Empty:
                    break

                if not isinstance(parsed, dict):
                    continue

                # If it's a control message, handle success/admin messages
                if parsed.get('success') and parsed.get('message'):
                    try:
                        on_success(parsed, ctx)
                    except Exception as e:
                        logging.error(f"on_success raised: {e}")
                    continue

                if 'bar1min' in parsed:
                    try:
                        bar = parsed['bar1min']
                        buffer.append(bar)
                    except Exception as e:
                        logging.error(f"Malformed bar1min in parsed message: {e}")
                        continue
        except Exception as e:
            logging.error(f"Error processing messageQueue2: {e}")

        if buffer:
            try:
                buffer.reverse()
                logging.debug(f"Calling on_candleData with {len(buffer)} candles")
                on_candleData(buffer, ctx)
            except Exception as e:
                logging.error(f"on_candleData crashed: {e}")

        time.sleep(0.001)

# ---------------------------
# Candle processing & handlers
# ---------------------------

def on_candleData(candleBuffer: list, ctx):
    """
    Convert bar1min lists into documents and write to Mongo + Redis.
    """
    logging.info(f"on_candleData called with {len(candleBuffer)} items")
    # Skip before market open (original logic had <= 9:15)
    try:
        if datetime.datetime.now().time() <= datetime.time(9, 15, 0):
            logging.debug("Before market open; skipping candle processing")
            return
    except Exception:
        pass

    insertBuffer = []
    for candleData in candleBuffer:
        try:
            if not isinstance(candleData, (list, tuple)) or len(candleData) < 6:
                logging.warning(f"Unexpected candleData format, skipping: {candleData}")
                continue

            td_id = str(candleData[0])
            ts_str = str(candleData[1])
            try:
                candleTime = int(time.mktime(time.strptime(ts_str, '%Y-%m-%dT%H:%M:%S')))
            except Exception as e:
                logging.error(f"Timestamp parse error for {ts_str}: {e}")
                continue

            symbol = ctx['symIdMap'].get(td_id) if isinstance(ctx['symIdMap'], dict) else None
            if symbol is None:
                logging.error(f"No mapping for TrueData id {td_id}; skipping candle")
                continue

            if time.time() - candleTime > ctx['candleCuttOffTime']:
                logging.error(f"Stale candle for {symbol} (id {td_id}) at {ts_str}; skipping")
                continue

            post = {
                '_id': f"{symbol}{candleTime}",
                'Open': float(candleData[2]),
                'High': float(candleData[3]),
                'Low': float(candleData[4]),
                'Close': float(candleData[5]),
                'LastTradeTime': candleTime,
                'Volume': int(candleData[6]) if len(candleData) > 6 and candleData[6] is not None else 0,
                'OpenInterest': int(candleData[7]) if len(candleData) > 7 and candleData[7] is not None else 0,
                'Symbol': symbol
            }

            insertBuffer.append(post)
            logging.info(post)
        except Exception as e:
            logging.error(f"Error preparing candle doc: {e} | data: {candleData}")

    if not insertBuffer:
        logging.debug("No valid candles to insert after processing")
        return

    # Bulk insert with try/except
    try:
        res = ctx['db_cloud'].insert_many(insertBuffer)
        logging.info(f"Inserted {len(res.inserted_ids)} candles to MongoDB")
    except Exception as e:
        logging.error(f"Mongo insert_many failed: {e}")

    # Set per-symbol DONE flag and ping timestamp in redis (best-effort)
    for doc in insertBuffer:
        sym = doc.get('Symbol')
        try:
            ctx['redisConn_cloud'].set(sym, 'DONE', ex=10)
            logging.info(sym)
        except Exception as e:
            logging.error(f"Failed to set DONE flag for {sym} in redisConn_cloud: {e}")
        try:
            ctx['pingConn'].set(f"{sym}_1", doc.get('LastTradeTime'))
        except Exception as e:
            logging.error(f"Failed to set ping timestamp for {sym} in pingConn: {e}")

# ---------------------------
# Control messages
# ---------------------------

def on_success(response: dict, ctx):
    if response.get('success') == True:
        msg = response.get('message')
        if msg == 'TrueData Real Time Data Service':
            ctx['connected'] = True
            logging.critical('Authentication successful!')
        elif msg == 'symbols added':
            logging.critical('Symbol subscription successful!')
            for symData in response.get('symbollist', []):
                if symData:
                    td_sym = symData[0]
                    td_id = symData[1]
                    # replace expiry codes
                    for expiry in ctx['expiryMap'].keys():
                        if expiry in td_sym:
                            td_sym = td_sym.replace(expiry, ctx['expiryMap'][expiry])
                    orig_sym = ctx['td_to_orig'].get(td_sym, td_sym)
                    ctx['symIdMap'][td_id] = orig_sym

            try:
                ctx['pingConn'].hset('trueDataIDMap', mapping=ctx['symIdMap'])
            except Exception as e:
                logging.error(f"Failed to write sym map to redis: {e}")
            logging.critical(f'Updated sym map (ID -> original_symbol): {ctx["symIdMap"]}')
    elif response.get('success') == False:
        logging.error(f'Failed Response! \n {response.get("message")}')

def on_trade(trade, ctx):
    try:
        sym = ctx['symIdMap'].get(trade[0], trade[0])
        print(f'Symbol-->{sym}, ltp---> {trade[2]}')
    except Exception:
        logging.exception("Error in on_trade")

def setCount(candle, ctx):
    symbol = candle['Symbol']
    logging.debug(f'Setting timestamp on redis for {symbol}')
    ctx['pingConn'].set(f'{symbol}_1', candle['LastTradeTime'])
    return True

# ---------------------------
# Subscription / logout
# ---------------------------

def subscribe_symbols(ctx):
    try:
        payload = {
            "method": "addsymbol",
            "symbols": ctx['subsricptionList']
        }
        if ctx.get('ws'):
            ctx['ws'].send(json.dumps(payload))
    except Exception as e:
        logging.error(f"Failed to send subscribe payload: {e}")

def logout(ctx):
    try:
        if ctx.get('ws'):
            ctx['ws'].send(json.dumps({"method": "logout"}))
    except Exception as e:
        logging.error(f"Error sending logout: {e}")

# ---------------------------
# Disconnection checker (runs in background)
# ---------------------------

def disconnection_checker(ctx):
    logging.critical('Started checking for data API disconnections....')
    while not ctx['connected']:
        print('Waiting for API to connect....')
        time.sleep(10)
    ctx['apiConnectionStatus'] = True

    lastTime = time.time()
    while True:
        if time.time() - lastTime >= 10:
            lastTime = time.time()
        if ctx['connected'] == False:
            ctx['apiConnectionStatus'] = False
            checkInternetStatus(ctx)
            try:
                logout(ctx)
            except Exception as e:
                logging.error(e)
            logging.critical('Trying to reconnect API')
            try:
                start_api(ctx)
                time.sleep(5)
                wait_count = 0
                while not ctx['connected'] and wait_count < 30:
                    print('Waiting for API to connect in order to subscribe symbols')
                    time.sleep(0.5)
                    wait_count += 1
                subscribe_symbols(ctx)
                ctx['apiConnectionStatus'] = True
            except Exception as e:
                logging.error(e)
                time.sleep(10)
                continue
            logging.critical(f'In apiStateChecker: reconnection Status: {"Connected!" if ctx["connected"] else "Disconnected!"}')

        time.sleep(2)

# ---------------------------
# Utilities
# ---------------------------

def checkInternetStatus(ctx):
    working = False
    while not working:
        try:
            urllib.urlopen('http://google.com')
            working = True
        except Exception as e:
            print(e)
            working = False
            time.sleep(1)
        print(f'Internet Status: {"Connected" if working else "Disconnected!"}')

def cleanup(ctx):
    """Properly clean up resources when shutting down"""
    ctx['should_reconnect'] = False
    if ctx.get('ws'):
        try:
            ctx['ws'].close()
        except Exception:
            pass

# ---------------------------
# Entrypoint
# ---------------------------

def main():
    import os
    try:
        os.mkdir("./APIResponseLogs")
    except Exception:
        pass

    ctx = create_context()
    # setup logging
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(module)s %(asctime)s %(message)s",
        handlers=[
            logging.FileHandler(ctx['logFileName']),
            console
        ]
    )

    # start ws
    start_api(ctx)

    # start worker threads
    threading.Thread(target=stream_handler1, args=(ctx,), daemon=True).start()
    threading.Thread(target=stream_handler2, args=(ctx,), daemon=True).start()

    # wait for initial connection attempts
    retry_count = 0
    while not ctx['connected'] and retry_count < 3:
        logging.info("Waiting for initial connection...")
        time.sleep(5)
        retry_count += 1

    if not ctx['connected']:
        logging.warning("Initial connection failed, will retry in background")

    logging.critical(f'Symbols list for day is: {ctx["subsricptionList"]}')
    # block until connected before subscribing (mimics class behavior)
    while not ctx['connected']:
        logging.critical('Waiting for API to connect in order to subscribe symbols')
        time.sleep(0.5)

    subscribe_symbols(ctx)

    # disconnection checker thread
    tdc = threading.Thread(target=disconnection_checker, args=(ctx,), daemon=True)
    tdc.start()

    # keep main alive; graceful shutdown handling
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        cleanup(ctx)
        logging.info("Shutting down gracefully...")

if __name__ == "__main__":
    main()
