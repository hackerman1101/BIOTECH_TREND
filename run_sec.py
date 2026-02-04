# run_sec.py — Rolling-window SEC submissions fetcher (auto-detect universe)
import os
import json
import time
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

# ----------------------------
# REQUIRED: identify yourself
# ----------------------------
USER_AGENT = os.getenv("SEC_USER_AGENT", "lia-biopharma/0.1 (contact: you@example.com)")  # CHANGE THIS

# ----------------------------
# Output + state
# ----------------------------
OUT_NEW = "out/sec_new_filings.csv"
STATE_PATH = "data/cache/sec_state.json"

# ----------------------------
# Rolling window controls
# ----------------------------
REFRESH_DAYS = int(os.getenv("SEC_REFRESH_DAYS", "14"))      # recommended: 7–30
MAX_PER_CIK = int(os.getenv("SEC_MAX_PER_CIK", "80"))        # cap per CIK from 'recent'
SLEEP_SEC = float(os.getenv("SEC_SLEEP_SEC", "0.25"))        # throttle

# Optional: limit forms early (comma-separated). Leave empty to include all forms.
# Example: set SEC_FORMS=8-K,8-K/A,6-K
SEC_FORMS = os.getenv("SEC_FORMS", "").strip()

MAX_RETRIES = 7
RETRY_STATUSES = {403, 429, 503}

# ----------------------------
# Universe auto-detect
# ----------------------------
UNIVERSE_CANDIDATES = [
    os.getenv("UNIVERSE_FILE", "").strip(),  # optional override
    "data/universe_biopharma.csv",
    "data/universe.csv",
    "out/universe_biopharma.csv",
    "out/universe.csv",
    "universe_biopharma.csv",
    "universe_all.csv",
]

def find_universe_file():
    for p in UNIVERSE_CANDIDATES:
        if not p:
            continue
        if os.path.exists(p) and os.path.getsize(p) > 0:
            return p
    return None

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Accept variants like Ticker/CIK
    lower_map = {c.lower().strip(): c for c in df.columns}
    if "ticker" not in df.columns:
        if "ticker" in lower_map:
            df.rename(columns={lower_map["ticker"]: "ticker"}, inplace=True)
        elif "symbol" in lower_map:
            df.rename(columns={lower_map["symbol"]: "ticker"}, inplace=True)

    if "cik" not in df.columns:
        if "cik" in lower_map:
            df.rename(columns={lower_map["cik"]: "cik"}, inplace=True)
        elif "cik10" in lower_map:
            df.rename(columns={lower_map["cik10"]: "cik"}, inplace=True)

    return df

def pad_cik(cik: str) -> str:
    return str(cik).zfill(10)

def cik_int(cik10: str) -> str:
    return str(int(str(cik10)))

def accession_with_dashes(acc: str) -> str:
    a = str(acc).strip()
    # Sometimes returned without dashes (18 digits)
    if a.isdigit() and len(a) == 18:
        return f"{a[:10]}-{a[10:12]}-{a[12:]}"
    return a

def accession_no_dashes(acc: str) -> str:
    return str(acc).replace("-", "").strip()

def load_state():
    if os.path.exists(STATE_PATH) and os.path.getsize(STATE_PATH) > 0:
        try:
            return json.load(open(STATE_PATH, "r", encoding="utf-8"))
        except Exception:
            pass
    return {"last_run_utc": "", "last_seen_filingDate_by_cik": {}}

def save_state(st):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    json.dump(st, open(STATE_PATH, "w", encoding="utf-8"), indent=2)

def backoff_sleep(i: int):
    time.sleep(min(30.0, (2 ** i) + random.random()))

def sec_get_json_with_retry(s: requests.Session, url: str) -> dict:
    last = None
    for i in range(MAX_RETRIES):
        r = s.get(url, timeout=60)
        last = r.status_code
        if r.status_code == 200:
            return r.json()
        if r.status_code in RETRY_STATUSES:
            backoff_sleep(i)
            continue
        r.raise_for_status()
    raise RuntimeError(f"SEC fetch failed after retries: status={last} url={url}")

def parse_form_filter():
    if not SEC_FORMS:
        return None
    forms = [x.strip().upper() for x in SEC_FORMS.split(",") if x.strip()]
    return set(forms) if forms else None

def main():
    os.makedirs("out", exist_ok=True)

    uni_path = find_universe_file()
    if not uni_path:
        raise FileNotFoundError(
            "Could not find a universe CSV.\n"
            "Tried:\n- " + "\n- ".join([p for p in UNIVERSE_CANDIDATES if p]) + "\n\n"
            "Fix options:\n"
            "1) Put your universe file at data/universe_biopharma.csv\n"
            "2) Or set an env var: set UNIVERSE_FILE=path\\to\\your.csv\n"
            "Universe must include columns: ticker,cik"
        )

    uni = pd.read_csv(uni_path, dtype=str).fillna("")
    uni = normalize_columns(uni)

    if "ticker" not in uni.columns or "cik" not in uni.columns:
        raise ValueError(
            f"Universe file {uni_path} must have columns ticker,cik. "
            f"Found columns: {list(uni.columns)}"
        )

    uni["ticker"] = uni["ticker"].astype(str).str.upper().str.strip()
    uni["cik"] = uni["cik"].astype(str).str.strip()

    # remove empties
    uni = uni[(uni["ticker"] != "") & (uni["cik"] != "")].copy()
    print(f"Using universe file: {uni_path} ({len(uni)} rows)")

    st = load_state()
    last_seen = st.get("last_seen_filingDate_by_cik", {})

    now = datetime.now(timezone.utc)
    cutoff_date = now.date() - timedelta(days=REFRESH_DAYS)
    cutoff = cutoff_date.isoformat()

    form_filter = parse_form_filter()
    if form_filter:
        print(f"Form filter enabled: {sorted(form_filter)}")

    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
        "Accept": "application/json,text/plain,*/*",
    })

    rows = []
    failures = 0

    for _, r in uni.iterrows():
        ticker = r["ticker"]
        cik10 = pad_cik(r["cik"])
        url = f"https://data.sec.gov/submissions/CIK{cik10}.json"

        try:
            data = sec_get_json_with_retry(s, url)
        except Exception:
            failures += 1
            time.sleep(SLEEP_SEC)
            continue

        recent = (data.get("filings", {}) or {}).get("recent", {}) or {}

        forms = recent.get("form", []) or []
        dates = recent.get("filingDate", []) or []
        accs  = recent.get("accessionNumber", []) or []
        docs  = recent.get("primaryDocument", []) or []

        n = min(len(forms), len(dates), len(accs), len(docs), MAX_PER_CIK)

        for i in range(n):
            form = str(forms[i]).upper().strip()
            fdate = str(dates[i]).strip()
            acc = str(accs[i]).strip()
            primary = str(docs[i]).strip()

            if not fdate or not acc:
                continue

            # Rolling window: only keep filings >= cutoff
            if fdate < cutoff:
                continue

            if form_filter and form not in form_filter:
                continue

            acc_dash = accession_with_dashes(acc)
            acc_nodash = accession_no_dashes(acc_dash)

            doc_url = ""
            if primary:
                doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int(cik10)}/{acc_nodash}/{primary}"

            rows.append({
                "ticker": ticker,
                "cik": cik10,
                "form": form,
                "filingDate": fdate,
                "accessionNumber": acc_dash,
                "primaryDocument": primary,
                "doc_url": doc_url,
            })

        # update last-seen
        if len(dates) > 0 and str(dates[0]).strip():
            last_seen[cik10] = str(dates[0]).strip()

        time.sleep(SLEEP_SEC)

    out = pd.DataFrame(rows)
    cols = ["ticker","cik","form","filingDate","accessionNumber","primaryDocument","doc_url"]
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    out = out[cols]

    # de-dupe
    if not out.empty:
        out = out.drop_duplicates(subset=["cik","accessionNumber","primaryDocument","filingDate","form"])

    out.to_csv(OUT_NEW, index=False)
    print(f"Wrote {OUT_NEW} with {len(out)} rows (refresh_days={REFRESH_DAYS}, cutoff={cutoff}, failures={failures})")

    st["last_run_utc"] = now.isoformat()
    st["last_seen_filingDate_by_cik"] = last_seen
    save_state(st)

if __name__ == "__main__":
    main()
