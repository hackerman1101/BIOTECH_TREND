# sec_extract_events_from_txt.py (v2)
import os, re, time, random
import pandas as pd
import requests

USER_AGENT = "lia-biopharma/0.1 (contact: you@example.com)"  # CHANGE THIS

WORKLIST = "out/sec_worklist.csv"
OUT_EVENTS = "out/sec_events.csv"
OUT_LOG = "out/sec_scan_log.csv"

CACHE_DIR = "data/cache/sec_filing_txt_for_events"

SLEEP_BASE_SEC = 0.8
MAX_RETRIES = 7
RETRY_STATUSES = {403, 429, 503}

PATTERNS = [
    ("CRL", re.compile(r"\bcomplete response letter\b|\bCRL\b", re.I)),
    ("PDUFA", re.compile(r"\bPDUFA\b|Prescription Drug User Fee Act|action date", re.I)),
    ("NDA_BLA_SUBMISSION", re.compile(r"\b(NDA|BLA)\b.{0,140}\b(submitted|submission)\b", re.I)),
    ("FILING_ACCEPTANCE", re.compile(r"\b(accepted for filing|filing acceptance)\b", re.I)),
    ("ADCOM", re.compile(r"\b(advisory committee|AdCom|ODAC|VRBPAC)\b", re.I)),
    ("CLINICAL_HOLD", re.compile(r"\b(partial\s+clinical\s+hold|clinical\s+hold|trial\s+hold)\b", re.I)),
    ("TOPLINE", re.compile(r"\b(top-?line|topline|primary endpoint|met the primary endpoint|did not meet)\b", re.I)),
]

def get_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s

def backoff_sleep(i):
    time.sleep(min(30.0, (2 ** i) + random.random() + SLEEP_BASE_SEC))

def sec_get_with_retry(s, url: str) -> str:
    last = None
    for i in range(MAX_RETRIES):
        r = s.get(url, timeout=60)
        last = r.status_code
        if r.status_code == 200:
            return r.text
        if r.status_code in RETRY_STATUSES:
            backoff_sleep(i)
            continue
        r.raise_for_status()
    raise RuntimeError(f"SEC fetch failed: status={last} url={url}")

def cik_int(cik10: str) -> str:
    return str(int(str(cik10)))

def filing_txt_url(cik10: str, accession: str) -> str:
    # /Archives/edgar/data/{cik}/{accession-with-dashes}.txt
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int(cik10)}/{accession}.txt"

def strip_html(x: str) -> str:
    x = re.sub(r"(?is)<script.*?>.*?</script>", " ", x)
    x = re.sub(r"(?is)<style.*?>.*?</style>", " ", x)
    x = re.sub(r"(?s)<.*?>", " ", x)
    x = re.sub(r"\s+", " ", x)
    return x.strip()

def parse_documents(filing_txt: str):
    # Requires <DOCUMENT> blocks; if missing, this wasn't a real submission file
    parts = filing_txt.split("<DOCUMENT>")
    docs = []
    for p in parts[1:]:
        mtype = re.search(r"(?im)^<TYPE>(.+)$", p)
        mtext = re.search(r"(?is)<TEXT>(.*)</TEXT>", p)
        dtype = (mtype.group(1).strip() if mtype else "")
        body = (mtext.group(1) if mtext else "")
        if body:
            docs.append((dtype, strip_html(body)))
    return docs

def doc_score(dtype: str) -> int:
    """
    Relaxed matching:
    - EX-99.1, EX-99.01 etc count
    - 8-K/A counts as 8-K
    """
    t = (dtype or "").upper().strip()
    if t.startswith("EX-99") or t.startswith("EX99"):
        return 100
    if re.match(r"^99(\.\d+)?", t):
        return 90
    if t.startswith("8-K"):
        return 80
    if t.startswith("10-Q"):
        return 70
    if t.startswith("10-K"):
        return 65
    return 0

def snippet(text: str, start: int, end: int, pad: int = 220) -> str:
    a = max(0, start - pad)
    b = min(len(text), end + pad)
    return text[a:b]

def main():
    os.makedirs("out", exist_ok=True)

    event_cols = ["ticker","cik","form","filingDate","accessionNumber","doc_type",
                  "event_type","confidence","snippet","doc_url"]
    log_cols = ["ticker","cik","form","filingDate","accessionNumber","doc_url",
                "fetch_ok","has_document_blocks","doc_count","selected_doc_types","notes"]

    # Always write outputs (even empty)
    if not os.path.exists(WORKLIST) or os.path.getsize(WORKLIST) == 0:
        pd.DataFrame(columns=event_cols).to_csv(OUT_EVENTS, index=False)
        pd.DataFrame(columns=log_cols).to_csv(OUT_LOG, index=False)
        print(f"{WORKLIST} empty -> wrote empty {OUT_EVENTS} and {OUT_LOG}")
        return

    wk = pd.read_csv(WORKLIST, dtype=str).fillna("")
    if wk.empty:
        pd.DataFrame(columns=event_cols).to_csv(OUT_EVENTS, index=False)
        pd.DataFrame(columns=log_cols).to_csv(OUT_LOG, index=False)
        print(f"{WORKLIST} has 0 rows -> wrote empty {OUT_EVENTS} and {OUT_LOG}")
        return

    s = get_session()
    events = []
    scanlog = []

    for _, r in wk.iterrows():
        ticker = str(r.get("ticker","")).upper()
        cik10 = str(r.get("cik",""))
        accession = str(r.get("accessionNumber",""))
        form = str(r.get("form",""))
        fdate = str(r.get("filingDate",""))

        if not cik10 or not accession:
            scanlog.append({
                "ticker": ticker, "cik": cik10, "form": form, "filingDate": fdate,
                "accessionNumber": accession, "doc_url": "",
                "fetch_ok": 0, "has_document_blocks": 0, "doc_count": 0,
                "selected_doc_types": "", "notes": "missing cik/accession"
            })
            continue

        url = filing_txt_url(cik10, accession)
        cache_path = os.path.join(CACHE_DIR, cik_int(cik10), f"{accession}.txt")
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)

        fetch_ok = 1
        notes = ""
        try:
            if os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
                filing_txt = open(cache_path, "r", encoding="utf-8", errors="ignore").read()
            else:
                filing_txt = sec_get_with_retry(s, url)
                open(cache_path, "w", encoding="utf-8", errors="ignore").write(filing_txt)
                time.sleep(SLEEP_BASE_SEC)
        except Exception as e:
            fetch_ok = 0
            notes = f"download_error: {type(e).__name__}: {str(e)[:120]}"
            # log a "download error" event row so you see it in output
            events.append({
                "ticker": ticker, "cik": cik10, "form": form, "filingDate": fdate,
                "accessionNumber": accession, "doc_type": "",
                "event_type": "DOWNLOAD_ERROR", "confidence": 0.0,
                "snippet": notes, "doc_url": url
            })
            scanlog.append({
                "ticker": ticker, "cik": cik10, "form": form, "filingDate": fdate,
                "accessionNumber": accession, "doc_url": url,
                "fetch_ok": 0, "has_document_blocks": 0, "doc_count": 0,
                "selected_doc_types": "", "notes": notes
            })
            continue

        has_docs = 1 if "<DOCUMENT>" in filing_txt else 0
        if not has_docs:
            # SEC sometimes returns an HTML/rate-limit page with status 200; treat as bad fetch
            head = strip_html(filing_txt[:600])
            notes = ("bad_fetch_no_document_blocks: " + head[:180]).strip()
            events.append({
                "ticker": ticker, "cik": cik10, "form": form, "filingDate": fdate,
                "accessionNumber": accession, "doc_type": "",
                "event_type": "BAD_FETCH", "confidence": 0.0,
                "snippet": notes, "doc_url": url
            })
            scanlog.append({
                "ticker": ticker, "cik": cik10, "form": form, "filingDate": fdate,
                "accessionNumber": accession, "doc_url": url,
                "fetch_ok": 1, "has_document_blocks": 0, "doc_count": 0,
                "selected_doc_types": "", "notes": notes
            })
            continue

        docs = parse_documents(filing_txt)
        ranked = sorted([(doc_score(dt), dt, tx) for dt, tx in docs], reverse=True, key=lambda x: x[0])
        ranked = [x for x in ranked if x[0] > 0][:8]  # scan top 8 docs

        selected_types = ",".join([dt for _, dt, _ in ranked])

        # scan
        hits_before = len(events)
        for _, dtype, text in ranked:
            for etype, pat in PATTERNS:
                for m in pat.finditer(text):
                    snip = snippet(text, m.start(), m.end())
                    base = {
                        "CRL":0.9,"CLINICAL_HOLD":0.85,"PDUFA":0.8,"FILING_ACCEPTANCE":0.75,
                        "NDA_BLA_SUBMISSION":0.7,"ADCOM":0.7,"TOPLINE":0.65
                    }.get(etype, 0.5)
                    conf = min(0.99, base + (0.05 if dtype.upper().startswith("EX-99") else 0.0))
                    events.append({
                        "ticker": ticker, "cik": cik10, "form": form, "filingDate": fdate,
                        "accessionNumber": accession, "doc_type": dtype,
                        "event_type": etype, "confidence": conf,
                        "snippet": snip[:500], "doc_url": url
                    })

        hit_count = len(events) - hits_before
        scanlog.append({
            "ticker": ticker, "cik": cik10, "form": form, "filingDate": fdate,
            "accessionNumber": accession, "doc_url": url,
            "fetch_ok": fetch_ok, "has_document_blocks": has_docs, "doc_count": len(docs),
            "selected_doc_types": selected_types[:220],
            "notes": f"hits={hit_count}"
        })

    pd.DataFrame(events, columns=event_cols).to_csv(OUT_EVENTS, index=False)
    pd.DataFrame(scanlog, columns=log_cols).to_csv(OUT_LOG, index=False)
    print(f"Wrote {OUT_EVENTS} with {len(events)} rows")
    print(f"Wrote {OUT_LOG} with {len(scanlog)} rows")

if __name__ == "__main__":
    main()
