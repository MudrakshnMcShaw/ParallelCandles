# modules
import json
import datetime


class Master:
    def __init__(self, json_msg):
        for i in json_msg:
            jsonStr = json.dumps(i)
            parse = json.loads(jsonStr)
        self.xid = parse["xid"]
        self.tkr = parse["tkr"]
        self.atkr = parse["atkr"]
        self.ctkr = parse["ctkr"]
        self.exp = parse["exp"]
        self.utkr = parse["utkr"]
        self.inst = parse["inst"]
        self.a3tkr = parse["a3tkr"]
        self.sp = parse["sp"]
        self.tk = parse["tk"]
        #self.time = parse["time"]


class Trade:
    def __init__(self, json_msg):
        for i in json_msg:
            jsonStr = json.dumps(i)
            parse = json.loads(jsonStr)
        self.id = parse["id"]
        self.kind = parse["kind"]
        self.ticker = parse["ticker"]
        self.segmentId = parse["segmentId"]
        self.price = parse["price"]
        self.qty = parse["qty"]
        self.volume = parse["volume"]
        self.oi = parse["oi"]
        timestamp = datetime.datetime(1980,1,1)
        seconds = datetime.timedelta(seconds=parse["time"])
        self.time = timestamp+seconds


class Best:
    def __init__(self, json_msg):  # Result
        for i in json_msg:
            jsonStr = json.dumps(i)
            parse = json.loads(jsonStr)
        self.ticker = parse["ticker"]  # selff.ticker=NIFTY-1
        self.segmentId = parse["segmentId"]  # 2
        self.kind = parse["kind"]  # B
        self.bidPrice = parse["bidPrice"]  # 13135.5
        self.bidQty = parse["bidQty"]  # 300
        self.askPrice = parse["askPrice"]  # 29865.25
        self.askQty = parse["askQty"]  # 50
        timestamp = datetime.datetime(1980,1,1)
        seconds = datetime.timedelta(seconds=parse["time"])
        self.time = timestamp+seconds


class RefsSnapshot:
    def __init__(self, json_msg):
        for i in json_msg:
            jStr = json.dumps(i)
            parse = json.loads(jStr)
        self.kind = parse["kind"]
        self.ticker = parse["ticker"]
        self.segmentId = parse["segmentId"]
        self.open = parse["open"]
        self.close = parse["close"]
        self.high = parse["high"]
        self.low = parse["low"]
        self.avg = parse["avg"]
        self.oi = parse["oi"]
        self.upperBand = parse["upperBand"]
        self.lowerBand = parse["lowerBand"]
        


class Refs:
    def __init__(self, json_msg):
        for i in json_msg:
            jStr = json.dumps(i)
            parse = json.loads(jStr)
        self.kind = parse["kind"]
        self.ticker = parse["ticker"]
        self.segmentId = parse["segmentId"]
        self.price = parse["price"]

class Greeks:
    def __init__(self, json_msg):
        for i in json_msg:
            jsonStr = json.dumps(i)
            parse = json.loads(jsonStr)
        self.kind = parse["kind"]
        self.ticker = parse["ticker"]
        self.iv = parse["iv"]
        self.delta = parse["delta"]
        self.theta = parse["theta"]
        self.vega = parse["vega"]
        self.gamma = parse["gamma"]
        self.ivvwap = parse["ivvwap"]
        self.vanna = parse["vanna"]
        self.charm = parse["charm"]
        self.speed = parse["speed"]
        self.zomma = parse["zomma"]
        self.color = parse["color"]
        self.volga = parse["volga"]
        self.veta = parse["veta"]
        self.tgr = parse["tgr"]
        self.tv = parse["tv"]
        self.dtr = parse["dtr"]


# End Module
