import os
import math
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

TIMEOUT = 30


def env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def safe_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def coingecko_btc_price() -> float:
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "bitcoin", "vs_currencies": "usd"}
    r = requests.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return float(data["bitcoin"]["usd"])


def coinbase_btc_ticker() -> float:
    url = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return float(data["price"])


def build_coinglass_headers() -> dict:
    """
    WICHTIG:
    CoinGlass zeigt je nach Plan / API-Version die benötigten Header im Dashboard.
    Trage sie als Env/Secrets ein, z.B.:

    COINGLASS_HEADER_1_NAME=...
    COINGLASS_HEADER_1_VALUE=...
    COINGLASS_HEADER_2_NAME=...
    COINGLASS_HEADER_2_VALUE=...
    """
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


def coinglass_get(path: str, params: dict | None = None) -> dict:
    base = "https://open-api-v4.coinglass.com"
    url = f"{base}{path}"
    headers = build_coinglass_headers()
    r = requests.get(url, headers=headers, params=params or {}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def extract_payload(obj):
    """
    Macht die Parser robuster gegen unterschiedliche Response-Formate.
    """
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
        return None

    if isinstance(row, list) and row:
        return row[0]

    return None


def guess_close_value(row):
    if isinstance(row, dict):
        for key in ("close", "c", "value", "oi", "fundingRate", "funding_rate", "netInflowUsd", "netFlowUsd"):
            if key in row:
                v = safe_float(row[key])
                if v is not None:
                    return v
        # letzte numerische Spalte als Fallback
        for v in reversed(list(row.values())):
            v = safe_float(v)
            if v is not None:
                return v
        return None

    if isinstance(row, list):
        # Candle-Format [ts, open, high, low, close]
        if len(row) >= 5:
            return safe_float(row[4])
        # Falls nur [ts, value]
        if len(row) >= 2:
            return safe_float(row[1])

    return None


def payload_to_series(payload) -> pd.DataFrame:
    rows = extract_payload(payload)
    data = []

    for row in rows:
        ts = guess_timestamp(row)
        val = guess_close_value(row)
        if ts is None or val is None:
            continue

        # CoinGlass liefert oft ms timestamps
        ts_num = safe_float(ts)
        if ts_num is not None:
            if ts_num > 10_000_000_000:
                dt = pd.to_datetime(int(ts_num), unit="ms", utc=True)
            else:
                dt = pd.to_datetime(int(ts_num), unit="s", utc=True)
        else:
            dt = pd.to_datetime(ts, utc=True, errors="coerce")

        if pd.isna(dt):
            continue

        data.append({"time": dt, "value": float(val)})

    df = pd.DataFrame(data)
    if df.empty:
        return df

    return df.sort_values("time").drop_duplicates("time", keep="last")


def fetch_funding_metrics() -> tuple[float | None, float | None]:
    payload = coinglass_get(
        "/api/futures/funding-rate/history",
        params={"symbol": "BTC", "interval": "1d", "limit": 14},
    )
    df = payload_to_series(payload)
    if df.empty:
        return None, None

    funding_last = safe_float(df["value"].iloc[-1])
    funding_7d_avg = safe_float(df["value"].tail(7).mean())
    return funding_last, funding_7d_avg


def fetch_oi_metrics() -> tuple[float | None, float | None]:
    payload = coinglass_get(
        "/api/futures/open-interest/aggregated-history",
        params={"symbol": "BTC", "interval": "1d", "limit": 14},
    )
    df = payload_to_series(payload)
    if df.empty:
        return None, None

    oi_last = safe_float(df["value"].iloc[-1])

    if len(df) >= 8 and df["value"].iloc[-8] not in (0, None):
        old = safe_float(df["value"].iloc[-8])
        new = oi_last
        oi_7d_change_pct = ((new - old) / old) * 100 if old else None
    else:
        oi_7d_change_pct = None

    return oi_last, oi_7d_change_pct


def fetch_etf_metrics() -> tuple[float | None, float | None]:
    payload = coinglass_get(
        "/api/etf/bitcoin/flow-history",
        params={"interval": "1d", "limit": 14},
    )
    rows = extract_payload(payload)

    records = []
    for row in rows:
        ts = guess_timestamp(row)
        if ts is None:
            continue

        val = None
        if isinstance(row, dict):
            for key in (
                "netInflowUsd", "netFlowUsd", "netAssetsFlow", "flowUsd",
                "net_inflow_usd", "net_flow_usd"
            ):
                if key in row:
                    val = safe_float(row[key])
                    break

        if val is None:
            val = guess_close_value(row)

        if val is None:
            continue

        ts_num = safe_float(ts)
        if ts_num is not None:
            if ts_num > 10_000_000_000:
                dt = pd.to_datetime(int(ts_num), unit="ms", utc=True)
            else:
                dt = pd.to_datetime(int(ts_num), unit="s", utc=True)
        else:
            dt = pd.to_datetime(ts, utc=True, errors="coerce")

        if pd.isna(dt):
            continue

        records.append({"time": dt, "value": val})

    df = pd.DataFrame(records)
    if df.empty:
        return None, None

    df = df.sort_values("time").drop_duplicates("time", keep="last")
    today_flow = safe_float(df["value"].iloc[-1])
    flow_7d_sum = safe_float(df["value"].tail(7).sum())
    return today_flow, flow_7d_sum


def fetch_coinbase_premium() -> tuple[float | None, float | None]:
    """
    1) Wenn CoinGlass Premium-Endpoint bei dir funktioniert, nutze ihn.
    2) Fallback: selbst berechnen = Coinbase-Preis minus CoinGecko-Preis.
    """
    try:
        payload = coinglass_get(
            "/api/indicator/coinbase-premium-index",
            params={"symbol": "BTC", "interval": "1d", "limit": 2},
        )
        df = payload_to_series(payload)
        if not df.empty:
            premium_pct = safe_float(df["value"].iloc[-1])

            # Abs-Wert aus aktuellen Preisen zusätzlich berechnen
            btc = coingecko_btc_price()
            cb = coinbase_btc_ticker()
            premium_abs = cb - btc
            return premium_abs, premium_pct
    except Exception:
        pass

    btc = coingecko_btc_price()
    cb = coinbase_btc_ticker()
    premium_abs = cb - btc
    premium_pct = ((cb - btc) / btc) * 100 if btc else None
    return premium_abs, premium_pct


def upsert_supabase(row: dict) -> None:
    url = env("SUPABASE_URL")
    key = env("SUPABASE_SERVICE_ROLE_KEY") or env("SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY fehlt.")

    endpoint = f"{url.rstrip('/')}/rest/v1/daily_metrics"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    r = requests.post(endpoint, headers=headers, json=[row], timeout=TIMEOUT)
    if r.status_code >= 300:
        raise RuntimeError(f"Supabase upsert failed: {r.status_code} {r.text}")


def main():
    snapshot_date = datetime.now(timezone.utc).date().isoformat()

    btc_price = coingecko_btc_price()
    coinbase_price = coinbase_btc_ticker()
    premium_abs, premium_pct = fetch_coinbase_premium()
    funding_last, funding_7d_avg = fetch_funding_metrics()
    oi_last, oi_7d_change_pct = fetch_oi_metrics()
    etf_today, etf_7d = fetch_etf_metrics()

    row = {
        "snapshot_date": snapshot_date,
        "btc_price_usd": btc_price,
        "coinbase_btc_usd": coinbase_price,
        "coinbase_premium_abs": premium_abs,
        "coinbase_premium_pct": premium_pct,
        "funding_last": funding_last,
        "funding_7d_avg": funding_7d_avg,
        "oi_last": oi_last,
        "oi_7d_change_pct": oi_7d_change_pct,
        "etf_flow_today_usd": etf_today,
        "etf_flow_7d_sum_usd": etf_7d,
    }

    upsert_supabase(row)
    print("Snapshot gespeichert:", row)


if __name__ == "__main__":
    main()
