import csv
import json
import redis

# --- Hardcoded values for testing ---
XTS_CSV_PATH = "instrument_list_ohlc.csv"
PROVIDER_JSON_PATH = "instru.json"
TTL_SECONDS = 36000  # 1 day

# --- Load XTS CSV ---
xts_map = {}
with open(XTS_CSV_PATH, "r", encoding="utf-8") as fh:
    reader = csv.DictReader(fh)
    for row in reader:
        try:
            xts_id = int(row["exchangeInstrumentID"])
            xts_map[xts_id] = row["symbol"].strip()
        except Exception:
            continue

print(f"Loaded {len(xts_map)} instruments from XTS CSV")

# --- Load Provider JSON ---
with open(PROVIDER_JSON_PATH, "r", encoding="utf-8") as fh:
    provider_data = json.load(fh)

print(f"Loaded {len(provider_data)} entries from provider JSON")

# --- Connect to Redis ---
r = redis.Redis(
    host='139.5.189.229',
    port=6379,
    db=8,
    password='mudraksh_test',
    decode_responses=True
)

# --- Build mappings ---
written = 0
unmatched = []

for item in provider_data:
    tkr = item.get("tkr")
    tk = item.get("tk")

    if not tkr or not tk:
        unmatched.append(item)
        continue

    try:
        tk_int = int(tk)
    except Exception:
        unmatched.append(item)
        continue

    xts_symbol = xts_map.get(tk_int)
    if xts_symbol:
        key = f"dv1:{tkr}"
        r.set(key, xts_symbol, ex=TTL_SECONDS)
        written += 1
    else:
        unmatched.append(item)

print(f"Written {written} mappings to Redis (TTL={TTL_SECONDS}s)")
if unmatched:
    print(f"Unmatched provider entries: {len(unmatched)}")
    for u in unmatched[:5]:
        print("  Sample unmatched:", json.dumps(u))
