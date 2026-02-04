# run_mentions_watchlist.py
# Pull per-ticker news via Google News RSS and write out/mentions_watchlist.csv
#
# Usage:
#   python run_mentions_watchlist.py
#
# Optional env vars:
#   UNIVERSE_FILE             path to universe csv (ticker,cik)
#   MENTIONS_RSS_DAYS         lookback window in days (default 30)
#   MENTIONS_RSS_MAX_ITEMS    max items per ticker (default 15)
#   MENTIONS_RSS_SLEEP        sleep seconds between tickers (default 0.4)
#   MENTIONS_RSS_CATALYST     1 = add FDA/catalyst keywords (default 1), 0 = ticker only
#   RSS_USER_AGENT            user-agent for RSS requests (default lia-biopharma/0.1)
#   MENTIONS_RSS_DEBUG_TICKER if set (e.g. IBRX), only fetch this ticker

import os
import time
import re
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus
import xml.etree.ElementTree as ET

import pandas as pd
import requests

OUT = "out/mentions_watchlist.csv"

UNIVERSE_CANDIDATES = [
    os.getenv("UNIVERSE_FILE", "").strip(),
    "data/universe_biopharma.csv",
    "data/universe.csv",
    "out/universe_biopharma.csv",
    "out/universe.csv",
    "universe_biopharma.csv",
    "universe_all.csv",
]

LOOKBACK_DAYS = int(os.getenv("MENTIONS_RSS_DAYS", "30"))
MAX_ITEMS = int(os.getenv("MENTIONS_RSS_MAX_ITEMS", "15"))
SLEEP = float(os.getenv("MENTIONS_RSS_SLEEP", "0.4"))
CATALYST_MODE = os.getenv("MENTIONS_RSS_CATALYST", "1").strip() != "0"
DEBUG_TICKER = os.getenv("MENTIONS_RSS_DEBUG_TICKER", "").strip().upper()
UA = os.getenv("RSS_USER_AGENT", "lia-biopharma/0.1")

BASE_URL = "https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"

# Add these only in catalyst mode to reduce noise
CATALYST_SUFFIX = (
    '(FDA OR PDUFA OR "action date" OR sBLA OR BLA OR NDA OR resubmit OR resubmission '
    'OR "complete response letter" OR CRL OR "advisory committee" OR AdCom OR "clinical hold")'
)

def find_universe_file() -> str | None:
    for p in UNIVERSE_CANDIDATES:
        if p and os.path.exists(p) and os.path.getsize(p) > 0:
            return p
    return None

def load_universe_tickers(path: str) -> list[str]:
    df = pd.read_csv(path, dtype=str).fillna("")
    if "ticker" not in df.columns and "symbol" in df.columns:
        df = df.rename(columns={"symbol": "ticker"})
    if "ticker" not in df.columns:
        raise ValueError(f"Universe missing ticker column: {path}")
    tickers = df["ticker"].astype(str).str.upper().str.strip().tolist()
    tickers = [t for t in tickers if re.fullmatch(r"[A-Z]{1,6}", t)]
    return sorted(set(tickers))

def parse_rss_items(xml_text: str) -> list[dict]:
    # Google News RSS is RSS2.0; items are under channel/item
    root = ET.fromstring(xml_text)
    out = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        source = ""
        src_node = item.find("source")
        if src_node is not None and src_node.text:
            source = src_node.text.strip()
        out.append({"title": title, "url": link, "published_raw": pub, "source": source})
    return out

def parse_pubdate(pub: str):
    # pandas handles RFC822-ish
    dt = pd.to_datetime(pub, errors="coerce", utc=True)
    return dt

def build_query(ticker: str) -> str:
    if not CATALYST_MODE:
        return ticker
    # For tickers, include both ticker and a finance-style token
    # Example: "IBRX (FDA OR PDUFA ...)"
    return f"{ticker} {CATALYST_SUFFIX}"

def main():
    os.makedirs("out", exist_ok=True)

    uni = find_universe_file()
    if not uni:
        raise FileNotFoundError(
            "Could not find universe file. Set UNIVERSE_FILE or create data/universe_biopharma.csv"
        )

    tickers = load_universe_tickers(uni)
    if DEBUG_TICKER:
        tickers = [t for t in tickers if t == DEBUG_TICKER]
        print(f"DEBUG: limiting to ticker={DEBUG_TICKER}, universe rows={len(tickers)}")

    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
    })

    rows = []
    fetched = 0
    kept_total = 0

    for t in tickers:
        q = build_query(t)
        url = BASE_URL.format(q=quote_plus(q))

        try:
            r = s.get(url, timeout=25)
            r.raise_for_status()
            items = parse_rss_items(r.text)
            fetched += 1
        except Exception as e:
            # don't kill the whole run on one ticker
            time.sleep(SLEEP)
            continue

        kept = 0
        for it in items:
            dt = parse_pubdate(it["published_raw"])
            if pd.isna(dt):
                continue
            if dt < cutoff:
                continue

            rows.append({
                "ticker": t,
                "title": it["title"],
                "url": it["url"],
                "published": dt.isoformat(),
                "source": it["source"],
                "query": q,
            })
            kept += 1
            if kept >= MAX_ITEMS:
                break

        kept_total += kept
        time.sleep(SLEEP)

    out = pd.DataFrame(rows)
    if out.empty:
        out = pd.DataFrame(columns=["ticker","title","url","published","source","query"])
    else:
        out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
        out = out.drop_duplicates(subset=["ticker","title","url"], keep="first")
        out = out.sort_values(["published"], ascending=False)

    out.to_csv(OUT, index=False)
    print(f"Wrote {OUT} with {len(out)} rows")
    print(f"Universe tickers: {len(load_universe_tickers(uni))} | Queried: {len(tickers)} | Fetched: {fetched} | Kept: {kept_total}")
    print(f"Catalyst mode: {'ON' if CATALYST_MODE else 'OFF'} | Lookback: {LOOKBACK_DAYS}d | Max/ticker: {MAX_ITEMS}")

if __name__ == "__main__":
    main()
