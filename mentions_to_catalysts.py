# mentions_to_catalysts.py
# Converts mentions-style feeds into out/news_catalysts.csv
# Safe: will write an empty output and exit cleanly if inputs are missing/empty.

import os, re
from datetime import datetime, timezone, date, timedelta
from typing import List, Optional

import pandas as pd

# Accept either a single path or multiple sources
MENTION_SOURCES = [
    "out/mentions.csv",
    "out/mentions_watchlist.csv",  # optional; ok if missing
]

OUT_NEWS = "out/news_catalysts.csv"

UNIVERSE_CANDIDATES = [
    os.getenv("UNIVERSE_FILE", "").strip(),
    "data/universe_biopharma.csv",
    "data/universe.csv",
    "out/universe_biopharma.csv",
    "out/universe.csv",
    "universe_biopharma.csv",
    "universe_all.csv",
]

TEXT_COL_CANDIDATES = ["title","headline","summary","snippet","description","text","content","body"]
URL_COL_CANDIDATES  = ["url","link","source_url","article_url"]
DATE_COL_CANDIDATES = ["created_at_utc","published","published_at","pubdate","date","datetime","time","created_at"]

EVENT_PATTERNS = [
    ("NDA_BLA_RESUBMISSION", re.compile(r"\b(resubmission|re-?submit|resubmit)\b.*\b(sBLA|BLA|NDA)\b|\b(sBLA|BLA|NDA)\b.*\b(resubmission|re-?submit|resubmit)\b", re.I)),
    ("NDA_BLA_SUBMISSION",   re.compile(r"\b(sBLA|BLA|NDA)\b.{0,140}\b(submit|submitted|submission|filed|filing)\b|\b(submit|submitted|submission|filed|filing)\b.{0,140}\b(sBLA|BLA|NDA)\b", re.I)),
    ("PDUFA",                re.compile(r"\bPDUFA\b|\bDUFA\b|action date\b", re.I)),
    ("CRL",                  re.compile(r"\bcomplete response letter\b|\bCRL\b", re.I)),
    ("ADCOM",                re.compile(r"\b(advisory committee|AdCom|ODAC|VRBPAC)\b", re.I)),
    ("CLINICAL_HOLD",        re.compile(r"\b(partial\s+clinical\s+hold|clinical\s+hold|trial\s+hold)\b", re.I)),
    ("TOPLINE",              re.compile(r"\b(top-?line|topline|data readout|readout|primary endpoint)\b", re.I)),
]
EVENT_PRIORITY = {"PDUFA":5,"ADCOM":4,"CRL":4,"CLINICAL_HOLD":4,"NDA_BLA_RESUBMISSION":4,"NDA_BLA_SUBMISSION":3,"TOPLINE":2}

MONTHS = {"JAN":1,"JANUARY":1,"FEB":2,"FEBRUARY":2,"MAR":3,"MARCH":3,"APR":4,"APRIL":4,"MAY":5,"JUN":6,"JUNE":6,
          "JUL":7,"JULY":7,"AUG":8,"AUGUST":8,"SEP":9,"SEPT":9,"SEPTEMBER":9,"OCT":10,"OCTOBER":10,"NOV":11,"NOVEMBER":11,
          "DEC":12,"DECEMBER":12}

def pick_first_existing_col(df, candidates) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def norm_ticker(t: str) -> str:
    return (t or "").strip().upper()

def find_universe_file() -> Optional[str]:
    for p in UNIVERSE_CANDIDATES:
        if p and os.path.exists(p) and os.path.getsize(p) > 0:
            return p
    return None

def load_universe_tickers() -> set[str]:
    p = find_universe_file()
    if not p:
        return set()
    df = pd.read_csv(p, dtype=str).fillna("")
    if "ticker" not in df.columns and "symbol" in df.columns:
        df = df.rename(columns={"symbol":"ticker"})
    if "ticker" not in df.columns:
        return set()
    return set(df["ticker"].astype(str).str.upper().str.strip().tolist())

def safe_read_many(paths: List[str]) -> pd.DataFrame:
    parts = []
    for p in paths:
        if p and os.path.exists(p) and os.path.getsize(p) > 0:
            try:
                parts.append(pd.read_csv(p, dtype=str).fillna(""))
            except pd.errors.EmptyDataError:
                continue
            except Exception:
                continue
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

def safe_parse_base_date(val) -> date:
    s = str(val or "").strip()
    if not s:
        return datetime.now(timezone.utc).date()
    dt = pd.to_datetime(s, errors="coerce", utc=True)
    if pd.isna(dt):
        return datetime.now(timezone.utc).date()
    return dt.date()

def build_text_blob(row: pd.Series, text_col: Optional[str]) -> str:
    parts = []
    if text_col and text_col in row.index:
        parts.append(str(row[text_col] or ""))
    for c in TEXT_COL_CANDIDATES:
        if c in row.index and c != text_col:
            v = str(row[c] or "")
            if v:
                parts.append(v)
    blob = " ".join(parts)
    blob = re.sub(r"\s+", " ", blob).strip()
    return blob

def extract_best_event(text: str) -> Optional[str]:
    best, best_pr = None, -1
    for etype, pat in EVENT_PATTERNS:
        if pat.search(text):
            pr = EVENT_PRIORITY.get(etype, 1)
            if pr > best_pr:
                best_pr = pr
                best = etype
    return best

def infer_year(month: int, day: int, base: date, year_hint: Optional[int]):
    if year_hint:
        return year_hint
    y = base.year
    try:
        d0 = date(y, month, day)
    except ValueError:
        return y
    return y + 1 if d0 < base else y

def extract_date_from_text(text: str, base: date):
    # ISO: 2026-02-19
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", text)
    if m:
        y, mo, da = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try: return date(y, mo, da), 0, "", 0.15
        except ValueError: pass

    # within N days/weeks/months
    m = re.search(r"\bwithin\s+(\d{1,3})\s+(day|days|week|weeks|month|months)\b", text, re.I)
    if m:
        n = int(m.group(1)); unit = m.group(2).lower()
        days = n * 7 if unit.startswith("week") else (n * 30 if unit.startswith("month") else n)
        return base + timedelta(days=days), 1, f"within {n} {unit}", 0.08

    # by/before/no later than Feb 19(, 2026)
    m = re.search(r"\b(by|before|no later than)\s+([A-Za-z]{3,9})\.?\s+(\d{1,2})(?:,?\s*(20\d{2}))?\b", text, re.I)
    if m:
        mon = m.group(2).strip().upper()
        day = int(m.group(3))
        year_hint = int(m.group(4)) if m.group(4) else None
        if mon in MONTHS:
            y = infer_year(MONTHS[mon], day, base, year_hint)
            try:
                d = date(y, MONTHS[mon], day)
                return d, (1 if year_hint is None else 0), f"{m.group(1).lower()} {m.group(2)} {day}", 0.10
            except ValueError:
                pass

    return None, 0, "", 0.0

def extract_tickers(row: pd.Series, blob: str, universe: set[str]) -> List[str]:
    # Prefer explicit ticker columns
    for col in ["ticker","symbol"]:
        if col in row.index and str(row[col]).strip():
            t = norm_ticker(str(row[col]))
            return [t] if (not universe or t in universe) else []

    # Fallback: detect tokens in text but intersect with universe to avoid junk
    if universe:
        found = set(re.findall(r"\b[A-Z]{1,6}\b", blob))
        found = {norm_ticker(x) for x in found if norm_ticker(x) in universe}
        return sorted(found)

    return []

def write_empty():
    cols = ["ticker","event_type","catalyst_date","days_to_event","approximate","approx_token",
            "mention_date","confidence","date_source","doc_url","context"]
    pd.DataFrame(columns=cols).to_csv(OUT_NEWS, index=False)

def main():
    os.makedirs("out", exist_ok=True)

    df = safe_read_many(MENTION_SOURCES)
    if df.empty:
        write_empty()
        print(f"No mention inputs available/non-empty ({MENTION_SOURCES}) -> wrote empty {OUT_NEWS}")
        return

    universe = load_universe_tickers()

    text_col = pick_first_existing_col(df, TEXT_COL_CANDIDATES)
    url_col  = pick_first_existing_col(df, URL_COL_CANDIDATES)
    date_col = pick_first_existing_col(df, DATE_COL_CANDIDATES)

    total = len(df)
    c_tickers = c_event = c_date = 0
    out_rows = []
    today = datetime.now(timezone.utc).date()

    for _, row in df.iterrows():
        base = safe_parse_base_date(row[date_col] if (date_col and date_col in row.index) else "")
        blob = build_text_blob(row, text_col)
        if not blob:
            continue

        tickers = extract_tickers(row, blob, universe)
        if not tickers:
            continue
        c_tickers += 1

        etype = extract_best_event(blob)
        if not etype:
            continue
        c_event += 1

    d, approx, token, boost = extract_date_from_text(blob, base)
    if d is None:
        # allow undated catalysts as a "fresh news" item
        d = base                     # anchor to mention_date
        approx = 1
        token = "undated_news"
        boost = -0.05                # slight penalty
    else:
        c_date += 1

        base_conf = 0.55 + 0.08 * EVENT_PRIORITY.get(etype, 1)
        conf = min(0.95, base_conf + boost)
        
        if token == "undated_news":
         conf = min(conf, 0.70)

        ctx = blob[:500]
        doc_url = str(row[url_col]) if (url_col and url_col in row.index) else ""

        for t in tickers[:3]:
            out_rows.append({
                "ticker": t,
                "event_type": etype,
                "catalyst_date": d.isoformat(),
                "days_to_event": str((d - today).days),
                "approximate": str(int(approx)),
                "approx_token": token or "",
                "mention_date": str(base),
                "confidence": f"{conf:.2f}",
                "date_source": "mentions",
                "doc_url": doc_url,
                "context": ctx,
            })

    out = pd.DataFrame(out_rows)
    cols = ["ticker","event_type","catalyst_date","days_to_event","approximate","approx_token",
            "mention_date","confidence","date_source","doc_url","context"]

    if out.empty:
        write_empty()
        print(f"Wrote {OUT_NEWS} with 0 rows")
        print(f"Funnel: mentions={total} -> tickers={c_tickers} -> event={c_event} -> date={c_date} -> rows=0")
        return

    out = out[cols].copy()
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["_conf"] = pd.to_numeric(out["confidence"], errors="coerce").fillna(0.0)
    out = out.sort_values(["_conf"], ascending=False).drop(columns=["_conf"])
    out = out.drop_duplicates(subset=["ticker","event_type","catalyst_date","approximate","approx_token"], keep="first")

    out.to_csv(OUT_NEWS, index=False)
    print(f"Wrote {OUT_NEWS} with {len(out)} rows")
    print(f"Funnel: mentions={total} -> tickers={c_tickers} -> event={c_event} -> date={c_date} -> rows={len(out)}")

if __name__ == "__main__":
    main()
