
BASE_URL  = "http://apidata5.accelpix.in/api/fda/rest"
import asyncio
import aiohttp
import urllib.parse
import csv
from datetime import datetime, timedelta
import pytz
from motor.motor_asyncio import AsyncIOMotorClient

API_TOKEN = urllib.parse.quote("FYr2LSwxufy8fMvnQCggOJK1nJ8=")
IST = pytz.timezone("Asia/Kolkata")

# --- Mongo setup ---
mongo_client = AsyncIOMotorClient("mongodb://localhost:27017")
db = mongo_client["CandleData"]
ohlc_coll = db["OHLC_MINUTE_1"]

# --- Helpers ---
def start_of_minute(dt):
    return dt.replace(second=0, microsecond=0)

def end_of_minute(dt):
    return dt.replace(second=59, microsecond=0)

def load_tickers(mapping_file):
    tickers = []
    with open(mapping_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tickers.append({
                "eid": row["exchangeInstrumentID"],
                "ticker": row["json_style_symbol"]
            })
    return tickers

async def fetch_1min(session, ticker, start_dt, end_dt):
    start_dt = start_of_minute(start_dt.astimezone(IST))
    end_dt   = end_of_minute(end_dt.astimezone(IST))

    start_str = start_dt.strftime("%Y%m%d %H:%M:%S")
    end_str   = end_dt.strftime("%Y%m%d %H:%M:%S")

    url = f"{BASE_URL}/{urllib.parse.quote(ticker)}/{start_str}/{end_str}/1?api_token={API_TOKEN}"

    print(url)

    async with session.get(url, timeout=1) as r:
        r.raise_for_status()
        return await r.json()

async def poll_loop(mapping_file):
    tickers = load_tickers(mapping_file)
    seen = {t["ticker"]: set() for t in tickers}

    async with aiohttp.ClientSession() as session:
        while True:
            # wait until minute completes
            now = datetime.now(IST)
            await asyncio.sleep(60 - now.second)

            candle_start = start_of_minute(datetime.now(IST) - timedelta(minutes=1))
            candle_end   = end_of_minute(candle_start)

            tasks = [
                fetch_1min(session, t["ticker"], candle_start, candle_end)
                for t in tickers
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for t, res in zip(tickers, results):
                ticker = t["ticker"]
                eid = t["eid"]

                if isinstance(res, Exception):
                    print(f"[{ticker}] error: {res}")
                    continue

                for bar in res:
                    ts = bar["td"]  # "yyyy-mm-dd HH:MM:00"
                    if ts not in seen[ticker]:
                        seen[ticker].add(ts)

                        doc = {
                            "eid": eid,
                            "ticker": ticker,
                            "timestamp": ts,
                            "open": bar.get("op"),
                            "high": bar.get("hp"),
                            "low": bar.get("lp"),
                            "close": bar.get("cp"),
                            "fetched_at": datetime.now(IST)
                        }

                        await ohlc_coll.insert_one(doc)
                        print(f"[{ticker}] inserted {ts}")
                    else:
                        print(f"[{ticker}] duplicate skipped {ts}")

# --- Run ---
if __name__ == "__main__":
    asyncio.run(poll_loop("/root/gyan_tests/csv_to_json_symbols.csv"))
