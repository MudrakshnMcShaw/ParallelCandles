from signalrcore_async.hub_connection_builder import HubConnectionBuilder
from signalrcore_async.protocol.msgpack import MessagePackHubProtocol
import logging
from urllib.parse import quote
import asyncio


class ApiData():
    """
    Provides methods and properties to connect, stream and get market data
    """

    def __init__(self):

        self.connection_started = None
        self.connection_stopped = None
        self.on_trade = None
        self.on_tradeSnapshot = None
        self.on_best = None
        self.on_refs = None
        self.on_srefs = None
        self.on_greeks = None
        self.on_greekSnapshot = None
        self.IsConnectionEstablished = False

    async def initialize(self, api_token, api_host):
        """
        initialize the api with token for authentication and authorization
        """
        api_token = quote(api_token)
        #print(api_token)
        protocol = "wss"
        host = api_host
        # host = "localhost:5011"
        hub_url = f"{protocol}://{host}/api/fda/apidata/stream?api_token={api_token}"

        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)

        global connection
        connection = HubConnectionBuilder()\
            .with_url(hub_url)\
            .build()
        #     .with_automatic_reconnect({
        #     "type": "interval",
        #     "keep_alive_interval": 10,
        #     "reconnect_interval": 5,
        #      "max_attempts": 5
        #     #"intervals": [1, 3, 5, 6, 7, 87, 3]
        # }).build()#.configure_logging(logging.DEBUG, handler=handler)\
        # .build()
        # .with_automatic_reconnect({"type": "interval","keep_alive_interval": 10,"intervals": [1, 3, 5, 6, 7, 87, 3]})\

        # .build()

        # .with_hub_protocol(MessagePackHubProtocol())\ # live data not working with msgpack

        connection.on_close(self.connection_stopped)
        connection.on_open(self.connection_started)

        try:
            await connection.start()
            print("connection estabilished")

            connection.on("_t", self.on_tradeSnapshot_handler)
            connection.on("t", self.on_trade_handler)
            connection.on("_b", self.on_best_handler)
            connection.on("b", self.on_best_handler)
            connection.on("_r", self.on_srefs_handler)
            connection.on("f", self.on_refs_handler)
            connection.on("_G", self.on_greekSnapshot_handler)
            connection.on("G", self.on_greeks_handler)

            # connection.on("Shutdown",self.on_shutdown_msg)
            self.IsConnectionEstablished = True
            return "ok"
        except Exception as err:
            print(err)
            #self.IsConnectionEstablished = False
            return "not_ok"

    # region Distonnect
    async def disconnect(self):
        if(not self.IsConnectionEstablished):
            return
        await connection.stop()
    # endregion

    # region connection events
    def on_connection_started(self, callback):
        self.connection_started = callback

    def on_connection_stopped(self, callback):
        self.connection_stopped = callback
    # endregion

    # region stream handlers
    def on_trade_handler(self, msg):
        if(self.on_trade):
            self.on_trade(msg)

    def on_tradeSnapshot_handler(self, msg):
        if(self.on_tradeSnapshot):
            self.on_tradeSnapshot(msg)

    def on_best_handler(self, msg):
        if(self.on_best):
            self.on_best(msg)

    def on_refs_handler(self, msg):
        if(self.on_refs):
            self.on_refs(msg)

    def on_srefs_handler(self, msg):
        if(self.on_srefs):
            self.on_srefs(msg)

    def on_greekSnapshot_handler(self, msg):
        if(self.on_greeks):
            self.on_greekSnapshot(msg)

    def on_greeks_handler(self, msg):
        if(self.on_greeks):
            self.on_greeks(msg)

    # def on_shutdown_msg(self,msg):
        # print(msg)
        # asyncio.run(self.disconnect())

    # endregion

    # region callbacks
    def on_trade_update(self, callback):
        """
        Tick data update.
        Returns apidata_models.Trade as json
        """
        self.on_trade = callback

    def on_tradeSnapshot_update(self, callback):
        self.on_tradeSnapshot = callback

    def on_best_update(self, callback):
        self.on_best = callback

    def on_refs_update(self, callback):
        self.on_refs = callback

    def on_srefs_update(self, callback):
        self.on_srefs = callback

    def on_greekSnapshot_update(self, callback):
        self.on_greekSnapshot = callback

    def on_greeks_update(self, callback):
        self.on_greeks = callback

    # endregion

    # region stream subscription methods
    async def subscribeAll(self, syms):
        await connection.invoke("SubscribeAll", [syms])
        #asyncio.create_task(connection.invoke("SubscribeAll", [syms]))

    async def subscribeTrade(self, syms):
        await connection.invoke("Ticks", [syms])

    async def subscribeBestAndRefs(self, syms):
        await connection.invoke("Others", [syms])

    async def unsubscribeAll(self, syms):
        await connection.invoke("UnsubscribeAll", [syms])

    async def subscribeGreeks(self, syms):
        await connection.invoke("SubscribeGreeks", [syms])

    async def subscribeSegments(self, needSnapshot=False):
        res = await connection.invoke("SubscribeSegments", [needSnapshot])
        return res

    async def subscribeOptionChainRange(self, spotName, expiryDate, numberOfStrikes=2):
        res = await connection.invoke("SubscribeChainRange", [spotName, expiryDate, numberOfStrikes])
        return res

    async def subscribeOptionChain(self, spotName, expiryDate):
        res = await connection.invoke("SubscribeChain", [spotName, expiryDate])
        return res

    async def unsubscribeOptionChain(self, spotName, expiryDate):
        await connection.invoke("UnsubscribeChain", [spotName, expiryDate])

    # endregion

    # region eod data

    async def get_eod(self, ticker, startDate, endDate):
        """
        @param : ticker , startDate , EndDate
        ticker : BANKNIFTY-1 , NIFTY-1 , TCS
        startDate and endDate : formate : yyyyMMdd eg:20201001

        eg : ['NIFTY-1','20200828', '20200901']
        """
        if((not ticker) or (startDate.isdigit() == False) or (len(startDate) != 8) or (len(endDate) != 8) or (endDate.isdigit() == False)):
            print("not enough arguments")
            return

        ed = await connection.invoke("Eod", [ticker, startDate, endDate])
        return ed

    async def get_eod_contract(self, underlyingTicker, startDate, endDate, contractExpiry):
        """
        @param : ticker , startDate , EndDate , contractExpiry
        underlyingTicker : BANKNIFTY , NIFTY
        startDate and endDate : formate : yyyyMMdd eg:20201001
        contractExpiry : formate : yyyyMMdd eg: 20201126

        eg: ['NIFTY', '20200828', '20200901', '20201029']

        """

        if((not underlyingTicker) or (startDate.isdigit() == False) or (len(startDate) != 8) or (endDate.isdigit() == False) or (len(endDate) != 8) or (contractExpiry.isdigit() == False) or (len(contractExpiry) != 8)):
            print("Arguments are not enough or invalid")
            return

        ed = await connection.invoke("EodContract", [underlyingTicker, startDate, endDate, contractExpiry])
        return ed

    async def get_intra_eod(self, ticker="", startDate="", endDate="", resolution="5"):
        """
        @param : ticker , startDate , EndDate , resolution
        ticker : BANKNIFTY-1 , NIFTY-1 , TCS
        startDate and endDate : formate : yyyyMMdd eg:20201001
        resolution : time resolution in minutes. default: 5 minutes

        eg : ['NIFTY-1', '20200828', '20200901', '5']
        """
        if((not ticker) or (startDate.isdigit() == False) or (len(startDate) != 8) or (endDate.isdigit() == False) or (len(endDate) != 8) or (not resolution)):
            print("Arguments are not enough or invalid")
            return

        ied = await connection.invoke("IEod", [ticker, startDate, endDate, resolution])
        return ied

    async def get_intra_eod_contract(self, underlyingTicker="", startDate="", endDate="", contractExpiry="", resolution="5"):
        """
        @param : ticker , startDate , EndDate , contractExpiry , resolution

        ticker : BANKNIFTY-1 , NIFTY-1 , TCS
        startDate and endDate : formate : yyyyMMdd eg:20201001
        contractExpiry : formate : yyyyMMdd eg: 20201126
        resolution : time resolution in minutes. default: 5 minutes

        eg:['NIFTY', '20200828', '20200901', '20201029', '5']
        """
        if((not underlyingTicker) or (startDate.isdigit() == False) or (len(startDate) != 8) or (endDate.isdigit() == False) or (len(endDate) != 8) or (not resolution)):
            print("Arguments are not enough or invalid")
            return

        ied = await connection.invoke("IEodContract", [underlyingTicker, startDate, endDate, contractExpiry, resolution])
        return ied

    async def get_back_ticks(self, ticker, lastDateTime):

        if((not ticker) or (not lastDateTime)):
            print("Arguments are not enough or invalid")
            return

        ied = await connection.invoke("TicksHistory", [ticker, lastDateTime])
        return ied

    # endregion
