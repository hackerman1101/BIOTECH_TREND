# diagnose_calendar_yield.py
import pandas as pd
import requests
import re

USER_AGENT = "lia-biopharma/0.1 (contact: you@example.com)"  # CHANGE THIS
IN_EVENTS = "out/sec_events_consolidated.csv"

DATE_RELEVANT = {"PDUFA", "ADCOM", "FILING_ACCEPTANCE", "NDA_BLA_SUBMISSION", "TOPLINE"}

def get_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
    })
    return s

def quick_types_from_index(html: str, maxn=12):
    # very lightweight: find td cells that look like "EX-99.1", "99.1", "8-K", etc.
    cells = re.findall(r"(?is)<td[^>]*>(.*?)</td>", html)
    texts = [re.sub(r"(?s)<.*?>", " ", c).strip() for c in cells]
    types = []
    for t in texts:
        if re.match(r"^(EX-?\d+(\.\d+)?|EX-?99(\.\d+)?|99(\.\d+)?|8-K|10-Q|10-K)$", t, re.I):
            types.append(t)
    # de-dupe preserve order
    seen, out = set(), []
    for t in types:
        u = t.upper()
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out[:maxn]

def main():
    df = pd.read_csv(IN_EVENTS, dtype=str)
    if df.empty:
        print("No rows in sec_events_consolidated.csv")
        return

    df["confidence"] = pd.to_numeric(df.get("confidence", 0), errors="coerce").fillna(0.0)

    print("\n=== Event type counts ===")
    print(df["event_type"].value_counts().head(20))

    ddf = df[df["event_type"].isin(DATE_RELEVANT)].copy()
    print(f"\nDate-relevant rows in consolidated events: {len(ddf)} / {len(df)}")

    if ddf.empty:
        print("=> This explains the 6-row calendar: there just aren’t many date-bearing event types yet.")
        return

    s = get_session()

    print("\n=== Checking first 20 date-relevant rows: index_url status + top types ===")
    for i, r in ddf.head(20).iterrows():
        ticker = r.get("ticker", "")
        et = r.get("event_type", "")
        idx = r.get("index_url", "")
        doc = r.get("doc_url", "")
        if not isinstance(idx, str) or not idx.startswith("http"):
            print(f"{ticker} {et}: missing index_url (doc={doc})")
            continue
        try:
            resp = s.get(idx, timeout=30)
            status = resp.status_code
            types = quick_types_from_index(resp.text) if status == 200 else []
            print(f"{ticker} {et}: index {status} | types: {types}")
        except Exception as e:
            print(f"{ticker} {et}: index ERROR {type(e).__name__}: {e}")

if __name__ == "__main__":
    main()
