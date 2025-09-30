
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

main()


import csv
import re
import os
from typing import List, Tuple, Optional, Dict

# month maps
MONTH_TO_MM = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
    "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"
}

def _convert_symbol_to_truedata(symbol: str) -> Optional[str]:
    """
    Convert a single symbol (like 'NIFTY23SEP2525650CE' or 'NIFTY25SEPFUT')
    into the TrueData expected format:
      - Options: BASE + YYMMDD + STRIKE + CE|PE
      - Futures: keep BASE + YYMONFUT (already used in your code)
    Returns None on failure.
    """
    if not symbol:
        return None
    s = symbol.strip().upper().replace(" ", "")

    # 1) Already TrueData-like: BASE + YYMMDD (6 digits) + STRIKE + CE|PE
    m = re.match(r"^([A-Z]+)(\d{6})(\d+)(CE|PE)$", s)
    if m:
        # already in TrueData option format
        return s

    # 2) Already normalized variant used in repo: BASE + DD + MON + YY + STRIKE + CE|PE
    #    Example: NIFTY23SEP2525650CE  -> base=NIFTY, dd=23, mon=SEP, yy=25, strike=25650, CE
    m = re.match(r"^([A-Z]+)(\d{2})([A-Z]{3})(\d{2})(\d+)(CE|PE)$", s)
    if m:
        base, dd, mon, yy, strike, opt_type = m.groups()
        mon = mon.upper()
        mm = MONTH_TO_MM.get(mon)
        if mm:
            yymmdd = f"{yy}{mm}{dd}"
            return f"{base}{yymmdd}{strike}{opt_type}"
        else:
            return None

    # 3) Pattern: BASE + YY + MON + FUT  (e.g., NIFTY25SEPFUT) -> keep as-is
    m = re.match(r"^([A-Z]+)(\d{2})([A-Z]{3})FUT$", s)
    if m:
        return s

    # 4) Pattern: BASE + DD MON YYYY STRIKE CE/PE  (with spaces removed earlier so detect concatenated)
    #    Example: NIFTY28AUG202525950CE  (rare); try to extract by more permissive regex
    m = re.match(r"^([A-Z]+)(\d{1,2})([A-Z]{3})(\d{4}|\d{2})(\d+)(CE|PE)$", s)
    if m:
        base, dd, mon, yy, strike, opt_type = m.groups()
        if len(yy) == 4:
            yy = yy[2:]  # convert 4-digit year to 2-digit
        mm = MONTH_TO_MM.get(mon)
        if mm:
            yymmdd = f"{yy}{mm}{int(dd):02d}"
            return f"{base}{yymmdd}{strike}{opt_type}"
        return None

    # 5) Pattern: BASE + YYMMDD + STRIKE + CE/PE (with no separator) but YYMMDD may be present
    m = re.match(r"^([A-Z]+)(\d{6})(\d{3,6})(CE|PE)$", s)
    if m:
        return s

    # 6) Fallback: try extracting CE/PE at end and a 2-letter year and 3-letter month somewhere before
    m = re.match(r"^([A-Z]+)(\d{2})([A-Z]{3})(\d{2})(\d+)(CE|PE)$", s)
    if m:
        base, dd, mon, yy, strike, opt_type = m.groups()
        mm = MONTH_TO_MM.get(mon)
        if mm:
            yymmdd = f"{yy}{mm}{dd}"
            return f"{base}{yymmdd}{strike}{opt_type}"

    # Could not parse
    return None


def convert_csv_to_truedata(
    in_csv_path: str,
    out_csv_path: Optional[str] = None,
    id_col: str = "exchangeInstrumentID",
    sym_col: str = "symbol"
) -> List[Tuple[str, str, Optional[str]]]:
    """
    Read the CSV at `in_csv_path` (must contain columns id_col and sym_col),
    convert each symbol into TrueData format, return list of tuples:
      (exchangeInstrumentID, original_symbol, truedata_symbol_or_None)

    If out_csv_path is provided, write a CSV with columns:
      exchangeInstrumentID,original_symbol,truedata_symbol
    """
    if not os.path.exists(in_csv_path):
        raise FileNotFoundError(f"Input CSV not found: {in_csv_path}")

    results: List[Tuple[str, str, Optional[str]]] = []

    with open(in_csv_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        # ensure required columns exist
        if id_col not in reader.fieldnames or sym_col not in reader.fieldnames:
            raise ValueError(f"CSV must contain columns: {id_col} and {sym_col}")

        for row in reader:
            exch_id = row[id_col].strip()
            orig_sym = row[sym_col].strip()
            td = _convert_symbol_to_truedata(orig_sym)
            if td is None:
                # try one more permissive attempt: remove stray characters and retry
                cleaned = re.sub(r"[^A-Z0-9]", "", orig_sym.upper())
                td = _convert_symbol_to_truedata(cleaned)
            results.append((exch_id, orig_sym, td))

    # Optionally write output CSV
    if out_csv_path:
        out_dir = os.path.dirname(out_csv_path)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        with open(out_csv_path, "w", newline="", encoding="utf-8") as outcsv:
            writer = csv.writer(outcsv)
            writer.writerow([id_col, "original_symbol", "truedata_symbol"])
            for r in results:
                writer.writerow(r)

    return results

sample_csv = "instrument_list_ohlc.csv"
res = convert_csv_to_truedata(sample_csv, out_csv_path="sample_truedata_out.csv")
print(res)
