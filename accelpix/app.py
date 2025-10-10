#!/usr/bin/env python3
"""
Candle Browser — minimal columns + symbols listing & search

Shows only these columns (with human-readable times):

LastTradeTime, Symbol, Close, High, Low, Open, OpenInterest, Volume,
ConsolidationSource, ConsolidationTime

Features added in this file:
 - /symbols : HTML page listing available symbols with pagination and a search box
 - /symbols.json : JSON endpoint returning symbol list (supports ?q=substring&limit&skip)
 - Home page includes quick search and link to symbols list
 - Clicking a symbol opens /data?symbol=... to show the minimal columns

ConsolidationTime may be milliseconds; LastTradeTime may be seconds.
"""
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
from typing import Optional, List
import os
from configparser import ConfigParser
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import pytz
import html
import re

# ---------------- config ----------------
CONFIG_PATH = "/root/ParallelCandles/config.ini"
config = ConfigParser()
config.read(CONFIG_PATH)
MONGO_URI = config.get("live_db", "MONGO_URI")
DB_NAME = "CandleData"
OUTPUT_COLL = os.getenv("OUTPUT_COLL", "complete_candle")

IST = pytz.timezone("Asia/Kolkata")

app = FastAPI(title="Complete Candle Browser (Minimal Columns + Symbols)")

# create global motor client (started lazily)
mongo_client: Optional[AsyncIOMotorClient] = None

async def get_db():
    global mongo_client
    if mongo_client is None:
        mongo_client = AsyncIOMotorClient(MONGO_URI)
    return mongo_client[DB_NAME]

# ---------------- time / formatting helpers ----------------
def to_epoch_seconds(ts: Optional[str]) -> Optional[int]:
    if not ts:
        return None
    ts = str(ts).strip()
    try:
        v = int(ts)
        if v > 1_000_000_000_000:  # milliseconds
            v //= 1000
        return v
    except Exception:
        pass
    fmts = ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d")
    for f in fmts:
        try:
            dt = datetime.strptime(ts, f)
            dt = IST.localize(dt)
            return int(dt.timestamp())
        except Exception:
            continue
    return None


def format_epoch_readable(epoch_val: Optional[int]) -> str:
    """Format an epoch value (seconds) into human readable IST string."""
    if epoch_val is None:
        return ""
    try:
        return datetime.fromtimestamp(int(epoch_val), IST).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return str(epoch_val)


def format_maybe_ms(val) -> str:
    """Accepts int/str epoch in seconds or milliseconds. Returns human readable string.
    If value looks like milliseconds (>1e12) it will be converted accordingly.
    """
    if val is None:
        return ""
    try:
        v = int(val)
    except Exception:
        return str(val)
    # treat as ms when large
    if v > 1_000_000_000_000:
        return format_epoch_readable(v // 1000)
    return format_epoch_readable(v)


def escape(s: Optional[str]) -> str:
    if s is None:
        return ""
    return html.escape(str(s))

# ---------------- HTML small helpers ----------------
def page_wrap(title: str, body_html: str) -> HTMLResponse:
    # Build HTML without using an f-string to avoid accidental interpolation of CSS braces
    html_page = (
        "<!doctype html>\n"
        "<html>\n"
        "<head>\n"
        "  <meta charset=\"utf-8\"/>\n"
        "  <title>" + escape(title) + "</title>\n"
        "  <style>\n"
        "    body{font-family: Arial, Helvetica, sans-serif; margin:18px;}\n"
        "    table{border-collapse:collapse; width:100%;}\n"
        "    th,td{border:1px solid #ddd; padding:6px; text-align:left; font-size:13px;}\n"
        "    th{background:#f4f4f4;}\n"
        "    tr:nth-child(even){background:#fafafa;}\n"
        "    .small{font-size:12px; color:#666}\n"
        "    .search{\n"
        "      margin-bottom:12px;\n"
        "      padding:8px 12px;\n"
        "      display:flex;\n"
        "      gap:8px;\n"
        "      align-items:center;\n"
        "    }\n"
        "    input[type=text], input[type=number]{padding:6px; font-size:14px;}\n"
        "    button{padding:6px 10px; font-size:14px;}\n"
        "    .symbol-list{ font-family:monospace; }\n"
        "    .pager{ margin-top:8px; }\n"
        "  </style>\n"
        "</head>\n"
        "<body>\n"
        "  <h2>" + escape(title) + "</h2>\n"
        + body_html +
        "  <hr/>\n"
        "  <div class=\"small\">Connected DB: " + escape(DB_NAME) + "." + escape(OUTPUT_COLL) + "</div>\n"
        "</body>\n"
        "</html>"
    )
    return HTMLResponse(content=html_page)

# ---------------- endpoints ----------------

@app.get("/", response_class=HTMLResponse)
async def home():
    body = """
    <div class="search">
      <form action="/symbols" method="get" style="display:flex; gap:8px; align-items:center;">
        <input name="q" placeholder="search symbol substring (e.g. NIFTY)" style="width:320px"/>
        <input name="limit" placeholder="limit (default 100)" style="width:100px"/>
        <button type="submit">Search Symbols</button>
      </form>
      <form action="/data" method="get" style="display:flex; gap:8px; align-items:center; margin-left:18px;">
        <input name="symbol" placeholder="symbol e.g. NIFTY 50" style="width:240px"/>
        <input name="limit" placeholder="limit (default 50)" style="width:100px"/>
        <button type="submit">Get Data</button>
      </form>
    </div>
    <div>
      <a href="/symbols">View symbols (HTML)</a> ·
      <a href="/symbols.json">Symbols (JSON)</a>
    </div>
    <p class="small">Tip: use /data?symbol=SYMBOL&from=YYYY-MM-DD&to=YYYY-MM-DD or from/to as epoch seconds</p>
    """
    return page_wrap("Complete Candle Browser (minimal columns)", body)


@app.get("/symbols", response_class=HTMLResponse)
async def symbols_page(q: Optional[str] = Query(None), limit: int = 200, skip: int = 0):
    """HTML page listing symbols. Supports ?q=substring, ?limit, ?skip"""
    db = await get_db()
    coll = db[OUTPUT_COLL]
    try:
        if q:
            regex = re.compile(re.escape(q), re.IGNORECASE)
            symbols = await coll.distinct("Symbol", {"Symbol": {"$regex": regex}})
        else:
            symbols = await coll.distinct("Symbol")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    total = len(symbols)
    slice_ = symbols[skip: skip + limit]

    body = "<form action=\"/symbols\" method=\"get\" style=\"margin-bottom:12px;\">"
    body += f"<input name=\"q\" placeholder=\"search symbols\" value=\"{escape(q or '')}\" style=\"width:320px\"/>"
    body += "<button type=\"submit\">Filter</button>"
    body += "</form>"

    body += f"<div class=\"small\">Total symbols: {total} — showing {len(slice_)}</div>"
    body += "<ul class=\"symbol-list\">"
    for s in slice_:
        body += f"<li><a href=\"/data?symbol={html.escape(s)}&limit=100\">{html.escape(s)}</a></li>"
    body += "</ul>"

    # pager links
    prev_skip = max(0, skip - limit)
    next_skip = skip + limit
    body += "<div class=\"pager\">"
    body += f"<a href=\"/symbols?q={html.escape(q or '')}&skip={prev_skip}&limit={limit}\">&laquo; Prev</a> | "
    body += f"<a href=\"/symbols?q={html.escape(q or '')}&skip={next_skip}&limit={limit}\">Next &raquo;</a>"
    body += "</div>"

    return page_wrap("Symbols", body)


@app.get("/symbols.json", response_class=JSONResponse)
async def symbols_json(q: Optional[str] = Query(None), limit: int = 1000, skip: int = 0):
    db = await get_db()
    coll = db[OUTPUT_COLL]
    try:
        if q:
            regex = re.compile(re.escape(q), re.IGNORECASE)
            symbols = await coll.distinct("Symbol", {"Symbol": {"$regex": regex}})
        else:
            symbols = await coll.distinct("Symbol")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    slice_ = symbols[skip: skip + limit]
    return {"total": len(symbols), "skip": skip, "limit": limit, "symbols": slice_}


@app.get("/data", response_class=HTMLResponse)
async def get_data(
    symbol: str = Query(..., min_length=1),
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    limit: int = 100
):
    db = await get_db()
    coll = db[OUTPUT_COLL]

    from_epoch = to_epoch_seconds(from_) if from_ else None
    to_epoch = to_epoch_seconds(to) if to else None

    q = {"Symbol": symbol}
    if from_epoch is not None and to_epoch is not None:
        q["LastTradeTime"] = {"$gte": from_epoch, "$lte": to_epoch}
    elif from_epoch is not None:
        q["LastTradeTime"] = {"$gte": from_epoch}
    elif to_epoch is not None:
        q["LastTradeTime"] = {"$lte": to_epoch}

    try:
        cursor = coll.find(q).sort([("LastTradeTime", -1)]).limit(limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    rows = []
    async for doc in cursor:
        rows.append(doc)

    if not rows:
        body = f"<p>No rows found for symbol={escape(symbol)}</p>"
        return page_wrap(f"Data for {symbol}", body)

    # only these minimal columns (order preserved)
    cols = [
        "LastTradeTime",
        "Symbol",
        "Close",
        "High",
        "Low",
        "Open",
        "OpenInterest",
        "Volume",
        "ConsolidationSource",
        "ConsolidationTime",
    ]

    # build table header
    body = "<table><thead><tr>"
    body += "<th>#</th>"
    body += "".join(f"<th>{escape(c)}</th>" for c in cols)
    body += "</tr></thead><tbody>"

    for i, r in enumerate(rows, start=1):
        body += "<tr>"
        body += f"<td>{i}</td>"
        for c in cols:
            val = None
            # handle consolidation time specially if nested under Metric
            if c == "ConsolidationSource":
                val = r.get(c) or (r.get("Metric") or {}).get(c)
            elif c == "ConsolidationTime":
                # could be ms or nested
                raw = r.get(c)
                if raw is None:
                    raw = (r.get("Metric") or {}).get(c)
                # present human readable
                cell = escape(format_maybe_ms(raw))
                body += f"<td>{cell}</td>"
                continue
            elif c == "LastTradeTime":
                raw = r.get("LastTradeTime")
                cell = escape(format_maybe_ms(raw))
                body += f"<td>{cell}</td>"
                continue
            else:
                val = r.get(c)

            cell = escape(val) if val is not None else ""
            body += f"<td>{cell}</td>"
        body += "</tr>"
    body += "</tbody></table>"

    return page_wrap(f"Data for {symbol}", body)


@app.get("/api/data")
async def api_data(
    symbol: str = Query(..., min_length=1),
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    limit: int = 100
):
    db = await get_db()
    coll = db[OUTPUT_COLL]

    from_epoch = to_epoch_seconds(from_) if from_ else None
    to_epoch = to_epoch_seconds(to) if to else None

    q = {"Symbol": symbol}
    if from_epoch is not None and to_epoch is not None:
        q["LastTradeTime"] = {"$gte": from_epoch, "$lte": to_epoch}
    elif from_epoch is not None:
        q["LastTradeTime"] = {"$gte": from_epoch}
    elif to_epoch is not None:
        q["LastTradeTime"] = {"$lte": to_epoch}

    cursor = coll.find(q).sort([("LastTradeTime", -1)]).limit(limit)
    out = []
    async for doc in cursor:
        # minimal projection for API
        row = {k: doc.get(k) for k in [
            "LastTradeTime",
            "Symbol",
            "Close",
            "High",
            "Low",
            "Open",
            "OpenInterest",
            "Volume",
            "ConsolidationSource",
            "ConsolidationTime",
        ]}
        # add human-readable fields
        row["LastTradeTime_readable"] = format_maybe_ms(row.get("LastTradeTime"))
        row["ConsolidationTime_readable"] = format_maybe_ms(row.get("ConsolidationTime"))
        row.pop("_id", None)
        out.append(row)
    return {"count": len(out), "rows": out}

# cleanup
@app.on_event("shutdown")
async def shutdown_event():
    global mongo_client
    if mongo_client:
        mongo_client.close()
        mongo_client = None

# ---------------- run ----------------
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8001, reload=False)
