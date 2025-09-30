import websocket
import threading
import time
import json
import queue
import urllib.request as urllib
import logging
import datetime
from pymongo import MongoClient
from redis import Redis
from configparser import ConfigParser

class TrueDataAPIManager():

    def __init__(self):
        configReader = ConfigParser()
        configReader.read('config.ini')
        configReaderPass = ConfigParser()
        configReaderPass.read('config.ini')
        mongoHost = configReader.get('mongoParams','ip'), 
        mongoPort = int(configReader.get('mongoParams','port'))
        connPostData = MongoClient(host=mongoHost, port=mongoPort)
        self.db_cloud = connPostData['True_Data_Candle_Data']['OHLC_MINUTE_1']
        websocket.enableTrace(True)
        self.username = configReader.get('loginDetails','userID')
        self.password = configReader.get('loginDetails','password')
        self.port = configReader.get('loginDetails','port')
        self.messageQueue1 = queue.Queue(maxsize=2000)
        self.messageQueue2 = queue.Queue(maxsize=2000)
        self.redisQueue = 'trueDataQueue'
        self.toggle = False
        self.ws = None
        self.connected = False
        self.should_reconnect = True
        self.reconnect_delay = 5  # Initial delay in seconds
        self.max_reconnect_delay = 300  # Maximum delay of 5 minutes
        
        logging.info('True data client started in thread')
        self.startAPI()
        logging.critical('Out of start API')
        
        threading.Thread(target=self.streamHandler1, args = (1,)).start()
        threading.Thread(target=self.streamHandler2, args = (1,)).start()
        
        # Wait for initial connection attempt
        retry_count = 0
        while not self.connected and retry_count < 3:
            logging.info("Waiting for initial connection...")
            time.sleep(5)
            retry_count += 1
            
        if not self.connected:
            logging.warning("Initial connection failed, will retry in background")
        
        self.candleCuttOffTime = 62
        self.symIdMap = {}
        redisHost = configReader.get('redisParams', 'ip')
        redisPort = int(configReader.get('redisParams', 'port'))

        self.redisConn_cloud = Redis(db=int(configReader.get('redisParams','db')), decode_responses=True,
                                    host=redisHost, port=redisPort,
                                    password= "mudraksh_test" )
        self.symConn = Redis(db=0, decode_responses=True,
                                    host=redisHost, port=redisPort,
                                    password= "mudraksh_test")
        self.pingConn = Redis(db=int(configReader.get('redisParams','pingdb')), decode_responses=True,
                                    host=redisHost, port=redisPort,
                                    password= "mudraksh_test")
        
        ## YYMMDD --->  DDMonthInShortYY FOR WEEKLY AND MONTHLY EXPIRY
        self.expiryMap = json.loads(configReader.get('expiryDetails','expiryMap'))
        
        self.subsricptionList = ['NIFTY 50', 'NIFTY BANK', 'NIFTY FIN SERVICE', 'SENSEX']

        import csv
        self.td_to_orig = {}
        converted_file = "sample_truedata_out.csv"

        try:
            with open(converted_file, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    td_sym = row.get("truedata_symbol")
                    orig_sym = row.get("original_symbol")
                    if td_sym and orig_sym:
                        td_sym, orig_sym = td_sym.strip(), orig_sym.strip()
                        self.subsricptionList.append(td_sym)
                        self.td_to_orig[td_sym] = orig_sym
        except Exception as e:
            print(f"Warning: Could not load converted symbols: {e}")


        seen = set()
        self.subsricptionList = [s for s in self.subsricptionList if not (s in seen or seen.add(s))]

        logging.critical(f'Symbols list for day is: {self.subsricptionList}')
        while not self.connected:
            logging.critical(
                'Waiting for API to connect in order to subscribe symbols')
            time.sleep(0.5)
        self.subsrcibeSymbols()

    def startAPI(self):
        try:
            if self.ws is not None:
                self.ws.close()
                
            self.ws = websocket.WebSocketApp(
                f"wss://push.truedata.in:{self.port}?user={self.username}&password={self.password}",
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                keep_running=False
            )
            self.ws.on_open = self.on_open
            
            def run_websocket():
                while self.should_reconnect:
                    try:
                        self.ws.run_forever()
                        if self.should_reconnect:
                            logging.info(f"Connection lost. Reconnecting in {self.reconnect_delay} seconds...")
                            time.sleep(self.reconnect_delay)
                            # Implement exponential backoff
                            self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
                    except Exception as e:
                        logging.error(f"WebSocket run_forever error: {e}")
                        if self.should_reconnect:
                            time.sleep(self.reconnect_delay)
            
            threading.Thread(target=run_websocket).start()
            
        except Exception as e:
            logging.error(f"Error in startAPI: {e}")
            self.connected = False

    def on_error(self,ws, error):
        logging.error(f"WebSocket error: {error}")
        self.connected = False

    def on_close(self, ws,close_status_code, close_msg):
        logging.error(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.connected = False

    def on_open(self, ws):
        print('Starting websocket connection with TrueData')
        logging.info('Starting WebSocket connection')
        self.connected = True
        self.reconnect_delay = 5  # Reset delay on successful connection

    # ------------------ REPLACE THESE METHODS IN YOUR CLASS ------------------

    def on_message(self, ws, message):
        """
        Put raw JSON into messageQueue1 (archival) and parsed dict into messageQueue2 (processing).
        """
        try:
            try:
                if 'bar1min' in message:
                    self.messageQueue1.put_nowait(message)
            except queue.Full:
                logging.warning("messageQueue1 is full; dropping raw message")

            # parse once and push parsed dict to queue2 for DB processing
            parsed = None
            if isinstance(message, str):
                try:
                    parsed = json.loads(message)
                except Exception as e:
                    # not JSON => nothing to process further
                    logging.debug(f"Failed to parse incoming message as JSON: {e}")
                    parsed = None

            if parsed and isinstance(parsed, dict):
                # push parsed message for streamHandler2
                try:
                    self.messageQueue2.put_nowait(parsed)
                except queue.Full:
                    logging.warning("messageQueue2 is full; dropping parsed message")
        except Exception as e:
            logging.error(f"Unexpected error in on_message: {e}")

    def streamHandler1(self, count):
        """
        Consume raw JSON strings from messageQueue1 and rpush them to Redis list self.redisQueue.
        """
        while True:
            buffer = []
            try:
                while not self.messageQueue1.empty() and len(buffer) < 200:
                    try:
                        msg = self.messageQueue1.get_nowait()
                        buffer.append(msg)
                    except queue.Empty:
                        break
            except Exception as e:
                logging.error(f"Error reading messageQueue1: {e}")

            if buffer:
                try:
                    # rpush raw strings
                    self.symConn.rpush(self.redisQueue, *buffer)
                    logging.debug(f"Pushed {len(buffer)} raw messages to Redis list")
                except Exception as e:
                    logging.error(f"Error pushing raw messages to redis ({self.redisQueue}): {e}")
            time.sleep(0.001)


    def streamHandler2(self, count):
        """
        Consume parsed dict messages from messageQueue2, extract bar1min lists and call on_candleData.
        """
        while True:
            buffer = []
            try:
                while not self.messageQueue2.empty() and len(buffer) < 500:
                    try:
                        parsed = self.messageQueue2.get_nowait()
                    except queue.Empty:
                        break

                    if not isinstance(parsed, dict):
                        continue

                    # If it's a control message, handle success/admin messages
                    if parsed.get('success') and parsed.get('message'):
                        # handle heartbeat/auth/symbols added messages
                        try:
                            self.on_success(parsed)
                        except Exception as e:
                            logging.error(f"on_success raised: {e}")
                        continue

                    # If this parsed message contains bar1min, append the payload list
                    if 'bar1min' in parsed:
                        try:
                            bar = parsed['bar1min']
                            # expected bar is list: [id, time, open, high, low, close, vol?, oi?]
                            buffer.append(bar)
                        except Exception as e:
                            logging.error(f"Malformed bar1min in parsed message: {e}")
                            continue
            except Exception as e:
                logging.error(f"Error processing messageQueue2: {e}")

            if buffer:
                # ensure order expected by your on_candleData; you used reverse earlier
                try:
                    buffer.reverse()
                    logging.debug(f"Calling on_candleData with {len(buffer)} candles")
                    self.on_candleData(buffer)
                except Exception as e:
                    logging.error(f"on_candleData crashed: {e}")

            time.sleep(0.001)


    def on_candleData(self, candleBuffer: list):
        """
        Convert bar1min lists into documents and write to Mongo + Redis.
        Uses safe lookups and guarded inserts so exceptions don't halt the pipeline.
        """
        logging.info(f"on_candleData called with {len(candleBuffer)} items")
        # Ensure we're only processing market hours (as you had)
        try:
            if datetime.datetime.now().time() <= datetime.time(9, 15, 0):
                logging.debug("Before market open; skipping candle processing")
                return
        except Exception:
            # if datetime weird, continue
            pass

        insertBuffer = []
        for candleData in candleBuffer:
            try:
                # defensive extraction
                if not isinstance(candleData, (list, tuple)) or len(candleData) < 6:
                    logging.warn(f"Unexpected candleData format, skipping: {candleData}")
                    continue

                td_id = str(candleData[0])
                ts_str = str(candleData[1])
                try:
                    candleTime = int(time.mktime(time.strptime(ts_str, '%Y-%m-%dT%H:%M:%S')))
                except Exception as e:
                    logging.error(f"Timestamp parse error for {ts_str}: {e}")
                    continue

                # safe symbol lookup
                symbol = self.symIdMap.get(td_id) if isinstance(self.symIdMap, dict) else None
                if symbol is None:
                    logging.error(f"No mapping for TrueData id {td_id}; skipping candle")
                    continue

                # check timeliness
                if time.time() - candleTime > self.candleCuttOffTime:
                    logging.error(f"Stale candle for {symbol} (id {td_id}) at {ts_str}; skipping")
                    continue

                post = {
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
            res = self.db_cloud.insert_many(insertBuffer)
            logging.info(f"Inserted {len(res.inserted_ids)} candles to MongoDB")
        except Exception as e:
            logging.error(f"Mongo insert_many failed: {e}")
            # do not return — still try to set Redis keys for best-effort downstream signaling

        # Set per-symbol DONE flag and ping timestamp in redis (best-effort)
        for doc in insertBuffer:
            sym = doc.get('Symbol')
            try:
                self.redisConn_cloud.set(sym, 'DONE', ex=10)
                logging.info(sym)
            except Exception as e:
                logging.error(f"Failed to set DONE flag for {sym} in redisConn_cloud: {e}")
            try:
                self.pingConn.set(f"{sym}_1", doc.get('LastTradeTime'))
            except Exception as e:
                logging.error(f"Failed to set ping timestamp for {sym} in pingConn: {e}")


    def on_success(self, response):
        if response['success'] == True:
            if response['message'] == 'TrueData Real Time Data Service':
                self.connected = True
                logging.critical(f'Authentication successful!')

            elif response['message'] == 'symbols added':
                logging.critical(f'Symbol subscription successful!')
                for symData in response['symbollist']:
                    if symData:
                        td_sym = symData[0]   # the truedata symbol
                        td_id = symData[1]    # the numeric ID

                        # replace expiry code as before
                        for expiry in self.expiryMap.keys():
                            if expiry in td_sym:
                                td_sym = td_sym.replace(expiry, self.expiryMap[expiry])

                        # map TrueData ID -> ORIGINAL symbol (via td_to_orig lookup)
                        orig_sym = self.td_to_orig.get(td_sym, td_sym)  # fallback to td_sym
                        self.symIdMap[td_id] = orig_sym

                self.pingConn.hset('trueDataIDMap', mapping=self.symIdMap)
                logging.critical(f'Updated sym map (ID -> original_symbol): {self.symIdMap}')


        elif response['success'] == False:
            logging.error(f'Failed Response! \n {response["message"]}')

    def on_unknownMessage(self, response):
        pass

    def subsrcibeSymbols(self):
        self.ws.send(json.dumps({
            "method": "addsymbol",
            "symbols": self.subsricptionList
        }))

    def logout(self):
        self.ws.send(json.dumps({
            "method": "logout"
        }))

    def disconnectionChecker(self):
        logging.critical(f'Started checking for data API disconnections....')
        while not self.connected:
            print('Waiting for API to connect....')
            time.sleep(10)
        self.apiConnectionStatus = True

        lastTime = time.time()
        while True:
            if time.time()-lastTime >= 10:
                lastTime = time.time()
            if self.connected == False:
                self.apiConnectionStatus = False
                self.checkInternetStatus()
                try:
                    self.logout()
                except Exception as e:
                    logging.error(e)
                logging.critical('Trying to reconnect API')
                try:
                    self.startAPI()
                    time.sleep(5)
                    while not self.connected:
                        print(
                            'Waiting for API to connect in order to subscribe symbols')
                        time.sleep(0.5)
                    self.subsrcibeSymbols()
                    self.apiConnectionStatus = True
                except Exception as e:
                    logging.error(e)
                    time.sleep(10)
                    continue
                logging.critical(
                    f'In apiStateChecker: reconnection Status: {"Connected!" if self.connected else "Disconnected!"}')

            time.sleep(2)

    def on_trade(self, trade):
        print(f'Symbol-->{self.symIdMap[trade[0]]}, ltp---> {trade[2]}')

    def setCount(self, candle):
        symbol = candle['Symbol']
        logging.debug(f'Setting timestamp on redis for {symbol}')
        self.pingConn.set(f'{symbol}_1',candle['LastTradeTime'])
        return True
    
    def checkInternetStatus(self):
        working = False
        while not working:
            try:
                urllib.urlopen('http://google.com')
                working = True
            except Exception as e:
                print(e)
                working = False
                time.sleep(1)
            print(
                f'Internet Status: {"Connected" if working else "Disconnected!"}')

    def cleanup(self):
        """Call this method to properly clean up resources when shutting down"""
        self.should_reconnect = False
        if self.ws:
            self.ws.close()

if __name__ == "__main__":
    import os

    try: 
        os.mkdir("./APIResponseLogs")
    except Exception as e:
        print(e)
    
    logFileName = f'./APIResponseLogs/logfile_{datetime.datetime.now().strftime("%Y%m%dT%H%M%S")}.log'

    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)

    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(module)s %(asctime)s %(message)s",
        handlers=[
            logging.FileHandler(logFileName),
            console
        ]
    )

    try:
        obj = TrueDataAPIManager()
        threading.Thread(target=obj.disconnectionChecker).start()
    except KeyboardInterrupt:
        obj.cleanup()
        logging.info("Shutting down gracefully...")