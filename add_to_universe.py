# add_to_universe.py
# Add tickers to your universe CSV (ticker,cik), with optional SEC CIK auto-lookup.
# Usage:
#   python add_to_universe.py --file data/universe_biopharma.csv --ticker IBRX
#   python add_to_universe.py --ticker IBRX NVAX
#   python add_to_universe.py   (interactive mode)
#
# Env vars:
#   UNIVERSE_FILE   -> default universe path override
#   SEC_USER_AGENT  -> required by SEC (set this!)

import os
import json
import argparse
from typing import Dict, Optional, List

import pandas as pd
import requests

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
CACHE_PATH = "data/cache/sec_company_tickers.json"


def norm_ticker(t: str) -> str:
    return (t or "").strip().upper()


def find_universe_file(cli_path: Optional[str]) -> str:
    if cli_path:
        return cli_path

    env = os.getenv("UNIVERSE_FILE", "").strip()
    if env:
        return env

    candidates = [
        "data/universe_biopharma.csv",
        "data/universe.csv",
        "out/universe_biopharma.csv",
        "out/universe.csv",
        "universe_biopharma.csv",
        "universe.csv",
    ]
    for p in candidates:
        if os.path.exists(p) and os.path.getsize(p) > 0:
            return p

    # default if none exist yet
    return "data/universe_biopharma.csv"


def ensure_parent_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def load_universe(path: str) -> pd.DataFrame:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame(columns=["ticker", "cik"])

    df = pd.read_csv(path, dtype=str).fillna("")
    # normalize column names (accept Symbol/CIK etc)
    cols = {c.lower().strip(): c for c in df.columns}

    if "ticker" not in df.columns:
        if "ticker" in cols:
            df.rename(columns={cols["ticker"]: "ticker"}, inplace=True)
        elif "symbol" in cols:
            df.rename(columns={cols["symbol"]: "ticker"}, inplace=True)

    if "cik" not in df.columns:
        if "cik" in cols:
            df.rename(columns={cols["cik"]: "cik"}, inplace=True)
        elif "cik10" in cols:
            df.rename(columns={cols["cik10"]: "cik"}, inplace=True)

    # Ensure required columns exist
    if "ticker" not in df.columns:
        df["ticker"] = ""
    if "cik" not in df.columns:
        df["cik"] = ""

    # normalize tickers
    df["ticker"] = df["ticker"].astype(str).apply(norm_ticker)
    df["cik"] = df["cik"].astype(str).str.strip()

    return df


def save_universe(df: pd.DataFrame, path: str) -> None:
    ensure_parent_dir(path)

    # keep at least ticker,cik first, preserve other columns if present
    if "ticker" not in df.columns:
        df["ticker"] = ""
    if "cik" not in df.columns:
        df["cik"] = ""

    # Dedup by ticker (keep first non-empty CIK if duplicates exist)
    df = df.copy()
    df["ticker"] = df["ticker"].astype(str).apply(norm_ticker)
    df["cik"] = df["cik"].astype(str).str.strip()

    # sort so non-empty cik comes first for each ticker
    df["_cik_empty"] = (df["cik"].astype(str).str.len() == 0).astype(int)
    df = df.sort_values(["ticker", "_cik_empty"])
    df = df.drop_duplicates(subset=["ticker"], keep="first").drop(columns=["_cik_empty"])

    # column order
    cols = list(df.columns)
    cols = ["ticker", "cik"] + [c for c in cols if c not in ("ticker", "cik")]
    df = df[cols]

    df.to_csv(path, index=False)


def pad_cik(cik: str) -> str:
    cik = (cik or "").strip()
    if not cik:
        return ""
    # allow user to paste as int-like
    try:
        return str(int(cik)).zfill(10)
    except Exception:
        # if already something weird, return raw
        return cik


def load_sec_ticker_map(user_agent: str) -> Dict[str, str]:
    """
    Returns mapping: TICKER -> 10-digit zero-padded CIK as string
    Uses cache if available.
    """
    ensure_parent_dir(CACHE_PATH)

    if os.path.exists(CACHE_PATH) and os.path.getsize(CACHE_PATH) > 0:
        try:
            data = json.load(open(CACHE_PATH, "r", encoding="utf-8"))
            return data
        except Exception:
            pass

    if not user_agent or "example.com" in user_agent:
        print("WARNING: SEC_USER_AGENT not set or still default. Set it for reliable SEC access.")
        print('Example: set SEC_USER_AGENT="your-app/0.1 (email: you@domain.com)"')

    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Encoding": "gzip, deflate",
    })
    r = s.get(SEC_TICKERS_URL, timeout=60)
    r.raise_for_status()
    raw = r.json()

    # raw is dict keyed by integers as strings: {"0": {...}, "1": {...}}
    mp: Dict[str, str] = {}
    for _, row in raw.items():
        t = str(row.get("ticker", "")).upper().strip()
        cik = str(row.get("cik_str", "")).strip()
        if t and cik:
            mp[t] = str(int(cik)).zfill(10)

    json.dump(mp, open(CACHE_PATH, "w", encoding="utf-8"), indent=2)
    return mp


def try_lookup_cik(ticker: str, mp: Dict[str, str]) -> str:
    """
    SEC sometimes uses dashes for class tickers (e.g., BRK-B).
    We'll try a couple normalizations.
    """
    t = norm_ticker(ticker)
    if t in mp:
        return mp[t]
    # try dot -> dash
    t2 = t.replace(".", "-")
    if t2 in mp:
        return mp[t2]
    # try dash -> dot
    t3 = t.replace("-", ".")
    if t3 in mp:
        return mp[t3]
    return ""


def interactive_tickers() -> List[str]:
    print("Enter tickers to add (space-separated), e.g.: IBRX NVAX")
    line = input("> ").strip()
    if not line:
        return []
    return [x for x in line.split() if x.strip()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", help="Universe CSV path (defaults to auto-detect)")
    ap.add_argument("--ticker", nargs="*", help="One or more tickers to add (e.g. IBRX NVAX)")
    ap.add_argument("--no-sec-lookup", action="store_true", help="Disable SEC CIK lookup; will prompt for CIK")
    ap.add_argument("--allow-missing-cik", action="store_true", help="Add ticker even if CIK is unknown (not recommended)")
    args = ap.parse_args()

    uni_path = find_universe_file(args.file)
    df = load_universe(uni_path)

    tickers = args.ticker or []
    if not tickers:
        tickers = interactive_tickers()

    tickers = [norm_ticker(t) for t in tickers if norm_ticker(t)]
    tickers = list(dict.fromkeys(tickers))  # dedupe preserve order

    if not tickers:
        print("No tickers provided. Nothing to do.")
        return

    existing = set(df["ticker"].astype(str).apply(norm_ticker).tolist())

    sec_map = {}
    user_agent = os.getenv("SEC_USER_AGENT", "lia-biopharma/0.1 (contact: you@example.com)")
    if not args.no_sec_lookup:
        try:
            sec_map = load_sec_ticker_map(user_agent=user_agent)
        except Exception as e:
            print(f"SEC lookup failed ({type(e).__name__}): {e}")
            print("Continuing without SEC lookup; will prompt for CIK.")

    added = 0
    skipped = 0

    for t in tickers:
        if t in existing:
            print(f"SKIP {t}: already in universe")
            skipped += 1
            continue

        cik = ""
        if sec_map:
            cik = try_lookup_cik(t, sec_map)

        if not cik:
            print(f"CIK not found automatically for {t}.")
            cik_in = input(f"Enter CIK for {t} (or press Enter to skip): ").strip()
            cik = pad_cik(cik_in)

        if not cik and not args.allow_missing_cik:
            print(f"SKIP {t}: missing CIK (use --allow-missing-cik to force add)")
            skipped += 1
            continue

        df = pd.concat([df, pd.DataFrame([{"ticker": t, "cik": cik}])], ignore_index=True)
        existing.add(t)
        added += 1
        print(f"ADD  {t}  cik={cik or '(missing)'}")

    save_universe(df, uni_path)
    print(f"\nDone. Universe file: {uni_path}")
    print(f"Added: {added}, Skipped: {skipped}")


if __name__ == "__main__":
    main()
