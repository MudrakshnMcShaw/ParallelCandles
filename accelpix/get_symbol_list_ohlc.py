#!/usr/bin/env python3
"""
Updated script: collects instruments from Redis (various keys), dedupes,
and classifies symbols robustly into F&O or non-F&O.

Outputs:
 - instrument_list_ohlc.csv   (full deduped list)
 - xts_fno_only.csv           (only futures & options: exchangeInstrumentID,symbol)
 - xts_non_fno.csv            (non-F&O rows with reason)
"""
import json
import csv
import re
from pathlib import Path

import redis

# ===== Redis connection (hardcoded as in your code) =====
redis_client = redis.Redis(
    host='172.16.162.133',
    port=6379,
    db=9,
    password='mudraksh',
    decode_responses=True
)

# ===== Processing Functions =====
def process_index_sub_docs(value):
    """Process index subscription docs: dict or json-string -> list of dicts"""
    try:
        docs = json.loads(value) if isinstance(value, str) else value
        return [{"exchangeInstrumentID": str(k), "symbol": v} for k, v in (docs or {}).items()]
    except Exception as e:
        print(f"Error processing index_sub_docs: {e}")
        return []

def process_eq_id_map(value):
    try:
        eq_map = json.loads(value) if isinstance(value, str) else value
        return [{"exchangeInstrumentID": str(k), "symbol": v} for k, v in (eq_map or {}).items()]
    except Exception as e:
        print(f"Error processing eq_id_map: {e}")
        return []

def process_candledata_idmap(value):
    try:
        candle_map = json.loads(value) if isinstance(value, str) else value
        return [{"exchangeInstrumentID": str(k), "symbol": v} for k, v in (candle_map or {}).items()]
    except Exception as e:
        print(f"Error processing candledata_idmap: {e}")
        return []

def process_fut_id_map(value):
    try:
        fut_map = json.loads(value) if isinstance(value, str) else value
        return [{"exchangeInstrumentID": str(k), "symbol": v} for k, v in (fut_map or {}).items()]
    except Exception as e:
        print(f"Error processing fut_id_map: {e}")
        return []

# ===== Map Redis Keys to Processors =====
PROCESSORS = {
    "index_sub_docs": process_index_sub_docs,
    "eq_id_map": process_eq_id_map,
    "candledata_idmap": process_candledata_idmap,
    "fut_id_map": process_fut_id_map
}

# ===== Classification helpers (robust edge-case handling) =====
# Remove trailing exchange suffixes like "-BSE", "-NSE", "-MCX", "-NFO"
RE_SUFFIX_DASH = re.compile(r"-(?:BSE|NSE|MCX|NFO)$", re.IGNORECASE)

# FUT detection variants
RE_FUT_VARIANTS = re.compile(r"(?i)(?:\bFUT\b|\bFUTIDX\b|FUTURE|[-_]FUT$|FUT$)")

# Option detection: ends with CE or PE and contains digits (strike)
RE_OPTION_END = re.compile(r"(?i).*\d.*(?:CE|PE)$")
RE_OPTION_STRICT = re.compile(r"(?i)\d{2,6}(?:CE|PE)$")

def normalize_symbol(sym: str) -> str:
    """Strip, remove trailing exchange suffixes, and collapse spaces."""
    if sym is None:
        return ""
    s = str(sym).strip()
    s = RE_SUFFIX_DASH.sub("", s).strip()
    s = re.sub(r"\s+", "", s)  # collapse any whitespace
    return s

def classify_symbol(sym: str):
    """
    Return tuple (keep:bool, category:str, reason:str)
    category: 'FUT', 'OPT', 'NONFNO'
    """
    s = (sym or "").upper()
    if not s:
        return False, "NONFNO", "empty_symbol"

    # direct FUT matches (common suffix or embedded)
    if RE_FUT_VARIANTS.search(s):
        return True, "FUT", "fut_variant_match"

    # direct option loose match: ends with CE/PE and has digits
    if RE_OPTION_END.match(s):
        # prefer strict strike detection
        if RE_OPTION_STRICT.search(s):
            return True, "OPT", "strict_digits_cepe"
        # fallback accept if digits exist but strict pattern not found
        if re.search(r"\d", s):
            return True, "OPT", "loose_digits_cepe"
        return False, "NONFNO", "endswith_cepe_no_digits"

    # sometimes CE/PE present but not captured by regex; fallback
    if s.endswith("CE") or s.endswith("PE"):
        if re.search(r"\d", s):
            return True, "OPT", "endswith_cepe_with_digits_fallback"

    # catch FUT contained inside (e.g., "SEPFUT" concatenated)
    if "FUT" in s:
        return True, "FUT", "contains_fut"

    # otherwise non-F&O
    return False, "NONFNO", "no_fut_or_opt_pattern"

# ===== CSV helpers =====
def save_to_csv(data, filename, fieldnames):
    """Save instrument data (list of dicts) to CSV"""
    if not data:
        print(f"⚠️ No data to save for {filename}")
        return
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    print(f"✅ Saved {len(data)} records to {filename}")

# ===== Main Logic =====
def main():
    try:
        redis_client.ping()
    except Exception as e:
        print("ERROR: cannot connect to Redis:", e)
        return

    all_instruments = []
    for key, processor in PROCESSORS.items():
        redis_type = redis_client.type(key)
        if redis_type == "string":
            value = redis_client.get(key)
        elif redis_type == "hash":
            value = redis_client.hgetall(key)
        else:
            print(f"Skipping {key} (unsupported type {redis_type})")
            continue

        instruments = processor(value)
        for it in instruments:
            if 'exchangeInstrumentID' in it and 'symbol' in it and it['symbol'] is not None:
                # normalize whitespace only here; classification will handle further normalizing
                it['symbol'] = str(it['symbol']).strip()
                it['exchangeInstrumentID'] = str(it['exchangeInstrumentID']).strip()
                all_instruments.append(it)

    if not all_instruments:
        print("No instruments collected from Redis keys.")
        return

    # deduplicate by exchangeInstrumentID (keep first)
    deduped = {}
    for itm in all_instruments:
        eid = itm['exchangeInstrumentID']
        if eid not in deduped:
            deduped[eid] = itm
    deduped_list = list(deduped.values())

    # save full deduped list
    full_csv = "instrument_list_ohlc.csv"
    save_to_csv(deduped_list, full_csv, fieldnames=['exchangeInstrumentID', 'symbol'])

    # classify and split into F&O and non-F&O (with reasons)
    fno_rows = []
    nonfno_rows = []
    for row in deduped_list:
        eid = row['exchangeInstrumentID']
        orig_sym = row['symbol']
        norm = normalize_symbol(orig_sym)
        keep, category, reason = classify_symbol(norm)
        if keep:
            fno_rows.append({'exchangeInstrumentID': eid, 'symbol': orig_sym})
        else:
            nonfno_rows.append({'exchangeInstrumentID': eid, 'symbol': orig_sym, 'reason': reason})

    # write outputs
    save_to_csv(fno_rows, "instrument_list_ohlc.csv", fieldnames=['exchangeInstrumentID', 'symbol'])
    save_to_csv(nonfno_rows, "xts_non_fno.csv", fieldnames=['exchangeInstrumentID', 'symbol', 'reason'])

    print(f"Done. Kept {len(fno_rows)} F&O rows, dropped {len(nonfno_rows)} non-F&O rows.")
    print("You can inspect xts_non_fno.csv for drop reasons if needed.")

if __name__ == "__main__":
    main()
