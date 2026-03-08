import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
TIMEOUT = 30

def env(name, default=None):
    return os.getenv(name, default)

def safe_float(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None

def coingecko_btc_price():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "bitcoin", "vs_currencies": "usd"}
    r = requests.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return float(r.json()["bitcoin"]["usd"])

def coinbase_btc_ticker():
    url = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return float(r.json()["price"])

def build_coinglass_headers():
    headers = {}
    h1n = env("COINGLASS_HEADER_1_NAME")
    h1v = env("COINGLASS_HEADER_1_VALUE")
    h2n = env("COINGLASS_HEADER_2_NAME")
    h2v = env("COINGLASS_HEADER_2_VALUE")
    if h1n and h1v:
        headers[h1n] = h1v
    if h2n and h2v:
        headers[h2n] = h2v
    return headers

def coinglass_get(path, params=None):
    base = "https://open-api-v4.coinglass.com"
    url = f"{base}{path}"
    r = requests.get(url, headers=build_coinglass_headers(), params=params or {}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def extract_payload(obj):
    if isinstance(obj, list):
        return obj
    if not isinstance(obj, dict):
        return []
    for key in ("data", "result", "payload", "rows", "list"):
        if key in obj:
            return extract_payload(obj[key])
    return []

def guess_timestamp(row):
    if isinstance(row, dict):
        for key in ("time", "timestamp", "ts", "date", "t"):
            if key in row:
                return row[key]
    elif isinstance(row, list) and row:
        return row[0]
    return None

def guess_value(row):
    if isinstance(row, dict):
        for key in ("close", "c", "value", "oi", "fundingRate", "funding_rate", "netInflowUsd", "netFlowUsd"):
            if key in row:
                v = safe_float(row[key])
                if v is not None:
                    return v
        for v in reversed(list(row.values())):
            v = safe_float(v)
            if v is not None:
                return v
    elif isinstance(row, list):
        if len(row) >= 5:
            return safe_float(row[4])
        if len(row) >= 2:
            return safe_float(row[1])
    return None

def payload_last_value(payload):
    rows = extract_payload(payload)
    parsed = []
    for row in rows:
        ts = guess_timestamp(row)
        val = guess_value(row)
        if ts is None or val is None:
            continue
        ts_num = safe_float(ts)
        if ts_num is not None:
            dt = pd.to_datetime(int(ts_num), unit="ms" if ts_num > 10_000_000_000 else "s", utc=True)
        else:
            dt = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(dt):
            continue
        parsed.append((dt, val))
    if not parsed:
        return None
    parsed.sort(key=lambda x: x[0])
    return safe_float(parsed[-1][1])

def latest_funding():
    payload = coinglass_get("/api/futures/funding-rate/history", {"symbol": "BTC", "interval": "1h", "limit": 24})
    return payload_last_value(payload)

def latest_oi():
    payload = coinglass_get("/api/futures/open-interest/aggregated-history", {"symbol": "BTC", "interval": "1h", "limit": 24})
    return payload_last_value(payload)

def latest_etf_flow():
    payload = coinglass_get("/api/etf/bitcoin/flow-history", {"interval": "1d", "limit": 7})
    return payload_last_value(payload)

def upsert_current_metrics(row):
    url = env("SUPABASE_URL")
    key = env("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL oder SUPABASE_SERVICE_ROLE_KEY fehlt")
    url = url.strip().rstrip("/")
    endpoint = f"{url}/rest/v1/current_metrics"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    r = requests.post(endpoint, headers=headers, json=[row], timeout=TIMEOUT)
    r.raise_for_status()

def main():
    btc = coingecko_btc_price()
    cb = coinbase_btc_ticker()
    premium_abs = cb - btc
    premium_pct = ((cb - btc) / btc) * 100 if btc else None

    row = {
        "id": 1,
        "btc_price_usd": btc,
        "coinbase_btc_usd": cb,
        "coinbase_premium_abs": premium_abs,
        "coinbase_premium_pct": premium_pct,
        "funding_last": latest_funding(),
        "oi_last": latest_oi(),
        "etf_flow_today_usd": latest_etf_flow(),
    }
    upsert_current_metrics(row)
    print("current_metrics updated", row)

if __name__ == "__main__":
    main()
