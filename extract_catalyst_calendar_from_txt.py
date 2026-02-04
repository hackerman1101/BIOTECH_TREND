# extract_catalyst_calendar_from_txt.py
from multiprocessing import context
import os
import re
import time
import random
from datetime import datetime, timedelta, date
from typing import List, Tuple, Dict, Optional
import html
import pandas as pd
import requests




# -----------------------------
# CONFIG
# -----------------------------
USER_AGENT = "lia-biopharma/0.1 (contact: you@example.com)"  # <-- CHANGE THIS
INPUT_EVENTS = "out/sec_events_consolidated_with_accession.csv"

OUT_CAL = "out/catalyst_calendar.csv"
OUT_MD = "out/catalyst_calendar.md"

CACHE_DIR = "data/cache/sec_filing_txt"

# Be gentle with SEC
SLEEP_BASE_SEC = 0.8
MAX_RETRIES = 7
RETRY_STATUSES = {403, 429, 503}
HORIZON_DAYS = 730  # 2 years

# Only these event types usually produce calendar-able dates
DATE_RELEVANT = {"PDUFA", "ADCOM", "FILING_ACCEPTANCE", "NDA_BLA_SUBMISSION", "TOPLINE"}

# If you want to limit rows for testing, set to an int (e.g. 50). Use None for all.
MAX_ROWS = None

#-----------------------------------------
#HTML entity unescape + text normalization
#------------------------------------------
MONTH = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}

def norm_text(s: str) -> str:
    s = html.unescape(str(s or ""))
    s = s.replace("\xa0", " ")  # NBSP
    s = re.sub(r"\s+", " ", s).strip()
    return s

def approx_window_from_context(context: str, filing_date: str):
    """
    Returns (token, start_date, end_date) or (None,None,None)
    Handles: Q1 2026, Q2 2026, 1H26, 2H26, early/mid/late 2026, year-end (infer year from filing_date)
    """
    ctx = norm_text(context).upper()
    year = None
    try:
        year = int(str(filing_date)[:4])
    except Exception:
        year = datetime.utcnow().year

    # Qx YYYY
    m = re.search(r"\bQ([1-4])\s*(20\d{2})\b", ctx)
    if m:
        q = int(m.group(1)); y = int(m.group(2))
        if q == 1: return f"Q1 {y}", date(y,1,1), date(y,3,31)
        if q == 2: return f"Q2 {y}", date(y,4,1), date(y,6,30)
        if q == 3: return f"Q3 {y}", date(y,7,1), date(y,9,30)
        if q == 4: return f"Q4 {y}", date(y,10,1), date(y,12,31)

    # 1H26 / 2H26 / 1H 2026
    m = re.search(r"\b([12])H\s*(20\d{2}|\d{2})\b", ctx)
    if m:
        h = int(m.group(1))
        y_raw = m.group(2)
        y = int(y_raw) if len(y_raw) == 4 else int("20" + y_raw)
        if h == 1: return f"1H{str(y)[-2:]}", date(y,1,1), date(y,6,30)
        if h == 2: return f"2H{str(y)[-2:]}", date(y,7,1), date(y,12,31)

    # early/mid/late YYYY
    m = re.search(r"\b(EARLY|MID|LATE)\s+(20\d{2})\b", ctx)
    if m:
        w = m.group(1); y = int(m.group(2))
        if w == "EARLY": return f"early {y}", date(y,1,1), date(y,4,30)
        if w == "MID":   return f"mid {y}",   date(y,5,1), date(y,8,31)
        if w == "LATE":  return f"late {y}",  date(y,9,1), date(y,12,31)

    # year-end / year end / end of year (infer year from filing date)
    m = re.search(r"\b(year[- ]end|end of (the )?year)\b", ctx)
    if m:
        y = year
        return f"year-end {y}", date(y,12,1), date(y,12,31)

    return None, None, None

# -----------------------------
# REGEX: ANCHORS + DATES
# -----------------------------
ANCHORS = {
    "PDUFA": re.compile(r"\bPDUFA\b|Prescription Drug User Fee Act|action date", re.I),
    "ADCOM": re.compile(r"\b(advisory committee|AdCom|ODAC|VRBPAC)\b", re.I),
    "TOPLINE": re.compile(r"\b(top-?line|topline|primary endpoint)\b", re.I),
    "NDA_BLA_SUBMISSION": re.compile(r"\b(NDA|BLA)\b.{0,80}\b(submitted|submission)\b", re.I),
    "FILING_ACCEPTANCE": re.compile(r"\b(accepted for filing|filing acceptance)\b", re.I),
}

MONTHS = r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"

# Exact date formats
DATE_PATTERNS_EXACT = [
    re.compile(rf"\b{MONTHS}\s+\d{{1,2}},\s+\d{{4}}\b", re.I),  # March 15, 2026
    re.compile(rf"\b\d{{1,2}}\s+{MONTHS}\s+\d{{4}}\b", re.I),  # 15 March 2026
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),                      # 2026-03-15
    re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b"),                # 3/15/2026
]

# Approximate windows
PAT_Q = re.compile(r"\bQ([1-4])\s+(20\d{2})\b", re.I)         # Q2 2026
PAT_H = re.compile(r"\bH([12])\s+(20\d{2})\b", re.I)          # H1 2026
PAT_EARLYMID = re.compile(r"\b(early|mid|late)\s+(20\d{2})\b", re.I)  # mid 2026


# -----------------------------
# SEC / EDGAR HELPERS
# -----------------------------
def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s


def jitter_sleep(base: float, i: int) -> None:
    # exponential-ish backoff with jitter
    delay = min(30.0, (2 ** i) + random.random() + base)
    time.sleep(delay)


def sec_get_with_retry(s: requests.Session, url: str) -> str:
    last_status = None
    for i in range(MAX_RETRIES):
        r = s.get(url, timeout=60)
        last_status = r.status_code
        if r.status_code == 200:
            return r.text
        if r.status_code in RETRY_STATUSES:
            jitter_sleep(SLEEP_BASE_SEC, i)
            continue
        # other errors: hard fail
        r.raise_for_status()
    raise RuntimeError(f"SEC fetch failed after retries: status={last_status} url={url}")


def cik_int(cik10: str) -> str:
    # Convert "0000123456" -> "123456"
    return str(int(str(cik10)))


def filing_txt_url(cik10: str, accession: str) -> str:
    # EDGAR complete submission .txt
    # /Archives/edgar/data/{cik}/{accession-with-dashes}.txt
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int(cik10)}/{accession}.txt"


def load_filing_txt(s: requests.Session, cik10: str, accession: str) -> str:
    os.makedirs(os.path.join(CACHE_DIR, cik_int(cik10)), exist_ok=True)
    path = os.path.join(CACHE_DIR, cik_int(cik10), f"{accession}.txt")

    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()

    url = filing_txt_url(cik10, accession)
    txt = sec_get_with_retry(s, url)

    with open(path, "w", encoding="utf-8", errors="ignore") as f:
        f.write(txt)

    # Polite pacing
    time.sleep(SLEEP_BASE_SEC)
    return txt


# -----------------------------
# PARSING EDGAR COMPLETE SUBMISSION (.txt)
# -----------------------------
def strip_html(x: str) -> str:
    # Good enough for keyword scanning
    x = re.sub(r"(?is)<script.*?>.*?</script>", " ", x)
    x = re.sub(r"(?is)<style.*?>.*?</style>", " ", x)
    x = re.sub(r"(?s)<.*?>", " ", x)
    x = re.sub(r"\s+", " ", x)
    return x.strip()


def parse_documents(filing_txt: str) -> List[Tuple[str, str]]:
    """
    Extract (doc_type, text_content) from <DOCUMENT> blocks.
    """
    parts = filing_txt.split("<DOCUMENT>")
    docs: List[Tuple[str, str]] = []
    for p in parts[1:]:
        mtype = re.search(r"(?im)^<TYPE>(.+)$", p)
        mtext = re.search(r"(?is)<TEXT>(.*)</TEXT>", p)
        dtype = (mtype.group(1).strip() if mtype else "").strip()
        body = (mtext.group(1) if mtext else "")
        if not body:
            continue
        docs.append((dtype, strip_html(body)))
    return docs


def doc_priority(dtype: str) -> int:
    """
    Higher is better. We want EX-99.* press releases first.
    """
    t = (dtype or "").upper().strip()
    if t.startswith("EX-99"):
        return 100
    if re.fullmatch(r"99(\.\d+)?", t):
        return 90
    if t == "8-K":
        return 80
    if t in {"10-Q", "10-K"}:
        return 70
    return 0


def select_candidate_docs(docs: List[Tuple[str, str]], max_docs: int = 6) -> List[Tuple[str, str]]:
    ranked = [(doc_priority(dt), dt, tx) for (dt, tx) in docs]
    ranked = [x for x in ranked if x[0] > 0]
    ranked.sort(key=lambda x: x[0], reverse=True)
    return [(dt, tx) for (_, dt, tx) in ranked[:max_docs]]


# -----------------------------
# DATE EXTRACTION
# -----------------------------
def approximate_to_date(token: str, year: int) -> date:
    """
    Map approximate window to a representative date.
    """
    tok = token.lower()
    if tok.startswith("q"):
        q = int(tok[1])
        month = {1: 2, 2: 5, 3: 8, 4: 11}[q]
        return date(year, month, 15)
    if tok.startswith("h"):
        h = int(tok[1])
        return date(year, 4, 1) if h == 1 else date(year, 10, 1)
    # early/mid/late
    if tok == "early":
        return date(year, 2, 15)
    if tok == "mid":
        return date(year, 6, 15)
    if tok == "late":
        return date(year, 10, 15)
    return date(year, 6, 15)
def extract_dates_from_text(text: str) -> Tuple[List[date], List[Tuple[date, str]]]:
    """
    Returns:
      exact_dates: list[date]
      approx_dates: list[(date, token_str)]  token_str like "Q2 2026" or "mid 2026"
    """
    exact: List[date] = []
    approx: List[Tuple[date, str]] = []

    # Exact formats
    for pat in DATE_PATTERNS_EXACT:
        for m in pat.finditer(text):
            ds = m.group(0)
            dt = pd.to_datetime(ds, errors="coerce")
            if pd.notna(dt):
                exact.append(dt.date())

    # Approximate formats
    for m in PAT_Q.finditer(text):
        q = m.group(1)
        y = int(m.group(2))
        approx.append((approximate_to_date(f"Q{q}", y), m.group(0)))

    for m in PAT_H.finditer(text):
        h = m.group(1)
        y = int(m.group(2))
        approx.append((approximate_to_date(f"H{h}", y), m.group(0)))

    for m in PAT_EARLYMID.finditer(text):
        w = m.group(1).lower()
        y = int(m.group(2))
        approx.append((approximate_to_date(w, y), m.group(0)))

    # de-dupe exact dates while preserving order
    seen = set()
    exact_out = []
    for d in exact:
        if d not in seen:
            seen.add(d)
            exact_out.append(d)

    # de-dupe approx by (date, token)
    seen2 = set()
    approx_out = []
    for d, tok in approx:
        k = (d, tok.lower())
        if k not in seen2:
            seen2.add(k)
            approx_out.append((d, tok))

    return exact_out, approx_out


def pick_best_future(dates: List[date], today: date, horizon_days: int) -> Optional[date]:
    if not dates:
        return None
    hi = today + timedelta(days=horizon_days)
    future = [d for d in dates if today <= d <= hi]
    return min(future) if future else None


def windows_around_anchor(text: str, anchor: re.Pattern, win: int = 1400, max_windows: int = 8) -> List[str]:
    """
    Extract a few windows around anchor hits to avoid picking unrelated dates.
    """
    windows = []
    for m in anchor.finditer(text):
        a = max(0, m.start() - win)
        b = min(len(text), m.end() + win)
        windows.append(text[a:b])
        if len(windows) >= max_windows:
            break
    return windows if windows else [text[:9000]]


# -----------------------------
# APPROXIMATE TOKEN TO WINDOW
# -----------------------------
def approx_token_to_window(token: str):
    t = (token or "").strip().upper()

    # Qx YYYY
    m = re.search(r"\bQ([1-4])\s+(20\d{2})\b", t)
    if m:
        q = int(m.group(1)); y = int(m.group(2))
        if q == 1: return date(y,1,1), date(y,3,31)
        if q == 2: return date(y,4,1), date(y,6,30)
        if q == 3: return date(y,7,1), date(y,9,30)
        if q == 4: return date(y,10,1), date(y,12,31)

    # Hx YYYY
    m = re.search(r"\bH([12])\s+(20\d{2})\b", t)
    if m:
        h = int(m.group(1)); y = int(m.group(2))
        if h == 1: return date(y,1,1), date(y,6,30)
        if h == 2: return date(y,7,1), date(y,12,31)

    # early/mid/late YYYY
    m = re.search(r"\b(EARLY|MID|LATE)\s+(20\d{2})\b", t)
    if m:
        w = m.group(1); y = int(m.group(2))
        if w == "EARLY": return date(y,1,1), date(y,4,30)
        if w == "MID":   return date(y,5,1), date(y,8,31)
        if w == "LATE":  return date(y,9,1), date(y,12,31)

    # fallback: whole year
    m = re.search(r"\b(20\d{2})\b", t)
    if m:
        y = int(m.group(1))
        return date(y,1,1), date(y,12,31)

    return None, None

# -----------------------------
# MAIN
# -----------------------------
def write_empty_outputs(today: date) -> None:
    os.makedirs("out", exist_ok=True)
    cols = [
        "ticker","event_type","catalyst_date","days_to_event",
        "approximate","approx_token","filingDate","confidence",
        "date_source","doc_url","context"
    ]
    pd.DataFrame(columns=cols).to_csv(OUT_CAL, index=False)
    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write(
            f"# Catalyst Calendar ({today.isoformat()} UTC)\n\n"
            f"Rows: 0\n\n"
            "No future catalyst dates found.\n"
        )
    print(f"Wrote {OUT_CAL} (0 rows) and {OUT_MD}")


def main():
    today = datetime.utcnow().date()
    os.makedirs("out", exist_ok=True)

    if not os.path.exists(INPUT_EVENTS) or os.path.getsize(INPUT_EVENTS) == 0:
        write_empty_outputs(today)
        return

    try:
        df = pd.read_csv(INPUT_EVENTS, dtype=str).fillna("")
    except pd.errors.EmptyDataError:
        write_empty_outputs(today)
        return

    if df.empty:
        write_empty_outputs(today)
        return

    # Require needed columns
    for need in ["ticker", "cik", "event_type", "accessionNumber", "filingDate", "confidence"]:
        if need not in df.columns:
            df[need] = ""

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["event_type"] = df["event_type"].astype(str).str.strip()

    # Filter to date-relevant rows with accessionNumber
    df = df[df["event_type"].isin(DATE_RELEVANT)].copy()
    df = df[df["accessionNumber"].astype(str).str.len() > 0].copy()

    if df.empty:
        write_empty_outputs(today)
        return

    # Optional row limit for quick tests
    if MAX_ROWS is not None:
        df = df.head(int(MAX_ROWS)).copy()

    # numeric coercions
    df["confidence_num"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0)

    s = get_session()
    rows_out: List[Dict] = []

    for idx, r in df.iterrows():
        ticker = r["ticker"]
        cik10 = r["cik"]
        etype = r["event_type"]
        accession = r["accessionNumber"]
        filingDate = r.get("filingDate", "")
        conf = float(r.get("confidence_num", 0.0))

        anchor = ANCHORS.get(etype)

        # Pull filing txt (cached + retry)
        try:
            filing_txt = load_filing_txt(s, cik10, accession)
        except Exception as e:
            # Skip if SEC blocks this one
            continue

        docs = parse_documents(filing_txt)
        candidates = select_candidate_docs(docs, max_docs=6)
        if not candidates:
            continue

        exact_all: List[date] = []
        approx_all: List[Tuple[date, str]] = []
        best_context = ""
        best_src = ""

        for dtype, text in candidates:
            # focus windows near anchor when possible
            if anchor:
                regions = windows_around_anchor(text, anchor, win=1400, max_windows=8)
            else:
                regions = [text[:9000]]

            for region in regions:
                exact, approx = extract_dates_from_text(region)
                if exact:
                    exact_all.extend(exact)
                if approx:
                    approx_all.extend(approx)

                if not best_context and (exact or approx):
                    best_context = region[:500]
                    best_src = dtype

        # choose best exact future date; otherwise approximate
        best_date = pick_best_future(exact_all, today, HORIZON_DAYS)
        approximate = 0
        approx_token = ""

        if best_date is None and approx_all:
            approx_dates = [d for d, _ in approx_all]
            best_date = pick_best_future(approx_dates, today, HORIZON_DAYS)
            if best_date:
                approximate = 1
                for d, tok in approx_all:
                    if d == best_date:
                        approx_token = tok
                        break
        if best_date is None: 
            continue 
        win_start = best_date 
        win_end = best_date 
        if approximate == 1: 
            ws, we = approx_token_to_window(approx_token) 
            if ws and we: 
                win_start, win_end = ws, we 
        ctx_text = norm_text(best_context)
        m = re.search(r"\bwithin\s+(\d{1,3})\s+days\b", ctx_text, re.I) 
        if m and filingDate: 
            n = int(m.group(1)) 
            base = pd.to_datetime(filingDate, errors="coerce") 
            if (not catalyst_date) or (str(catalyst_date).strip() == ""): 
                tok, ws, we = approx_window_from_context(ctx_text, filingDate) 
                if tok and ws and we: 
                    approximate = 1
                    approx_token = tok
            # keep your existing catalyst_date field as midpoint so days_to_event works
            mid = ws + (we - ws) // 2
            catalyst_date = mid.isoformat()
            if pd.notna(base): 
                approximate = 1
                approx_token = f"within {n} days"
                catalyst_date = (base + pd.Timedelta(days=n)).date().isoformat()
        rows_out.append({
            "ticker": ticker,
            "event_type": etype,
            "catalyst_date": best_date.isoformat(),
            "days_to_event": (best_date - today).days,
            "approximate": approximate,
            "approx_token": approx_token,
            "filingDate": filingDate,
            "confidence": conf,
            "date_source": f"filing_txt:{best_src or 'UNKNOWN'}",
            "doc_url": filing_txt_url(cik10, accession),
            "context": html.unescape((best_context or "").strip()[:500]),
            "catalyst_window_start": win_start.isoformat() if win_start else "",
            "catalyst_window_end": win_end.isoformat() if win_end else "",
        })

    # Output
    cols = [
        "ticker","event_type","catalyst_date","days_to_event",
        "approximate","approx_token","filingDate","confidence",
        "date_source","doc_url","context","catalyst_window_start","catalyst_window_end"
    ]

    out = pd.DataFrame(rows_out)

    if out.empty:
        write_empty_outputs(today)
        return

    for c in cols:
        if c not in out.columns:
            out[c] = ""

    out["days_to_event"] = pd.to_numeric(out["days_to_event"], errors="coerce").fillna(9999).astype(int)
    out["confidence"] = pd.to_numeric(out["confidence"], errors="coerce").fillna(0.0)

    out = out.sort_values(["catalyst_date", "days_to_event", "confidence"], ascending=[True, True, False])
    out = out[cols]
    out.to_csv(OUT_CAL, index=False)

    # Markdown calendar
    lines = [
        f"# Catalyst Calendar ({today.isoformat()} UTC)\n",
        f"Rows: {len(out)}\n"
    ]

    for cdate, g in out.groupby("catalyst_date"):
        lines.append(f"## {cdate}\n")
        for _, rr in g.iterrows():
            approx = " (approx)" if int(rr.get("approximate", 0)) == 1 else ""
            lines.append(
                f"- **{rr['ticker']}** | **{rr['event_type']}**{approx} | D-{int(rr['days_to_event'])} | conf {float(rr['confidence']):.2f} | {rr['date_source']}"
            )
            tok = (rr.get("approx_token", "") or "").strip()
            if tok:
                lines.append(f"  - token: {tok}")
            ctx = (rr.get("context", "") or "").strip()
            if ctx:
                lines.append(f"  - context: {ctx[:260]}{'…' if len(ctx) > 260 else ''}")
        lines.append("")

    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Wrote {OUT_CAL} ({len(out)} rows) and {OUT_MD}")


if __name__ == "__main__":
    main()
