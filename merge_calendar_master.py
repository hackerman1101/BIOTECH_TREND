# merge_calendar_master.py
# Merge daily catalyst sources into a stable master calendar.
#
# Inputs (default):
#   - out/catalyst_calendar.csv      (SEC-derived)
#   - out/news_catalysts.csv         (mentions/news-derived)
# Output:
#   - out/catalyst_calendar_master.csv
#
# Behavior:
#   - schema-safe union of columns
#   - dedupe by (ticker,event_type,catalyst_date,approximate,approx_token)
#   - exact rows preferred over approx
#   - confidence/source/date/link/context used to choose "best" row per key
#   - drops past events, recompute days_to_event
#   - keeps first_seen_utc, updates last_seen_utc when an item appears again

import os
from datetime import datetime, timezone
import pandas as pd

SOURCES = [
    "out/catalyst_calendar.csv",
    "out/news_catalysts.csv",
]

MASTER_OUT = "out/catalyst_calendar_master.csv"

# Unique identity for one "event record" in master
KEY_COLS = ["ticker", "event_type", "catalyst_date", "approximate", "approx_token"]

META_COLS = ["first_seen_utc", "last_seen_utc"]

# If your pipeline already outputs these, we’ll keep them; otherwise we’ll create them.
WINDOW_COLS = ["catalyst_window_start", "catalyst_window_end"]

# Prefer SEC filings over mentions when both describe the same thing
SOURCE_RANK = {
    "filing_txt": 3,   # your SEC extractor uses date_source like "filing_txt:EX-99.1"
    "sec": 3,
    "edgar": 3,
    "mentions": 1,
    "news": 1,
}

def safe_read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()

def ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df

def norm_text(x) -> str:
    return str(x or "").strip()

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # required base cols
    df = ensure_cols(df, KEY_COLS)
    df = ensure_cols(df, META_COLS)
    df = ensure_cols(df, WINDOW_COLS)

    # normalize types
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["event_type"] = df["event_type"].astype(str).str.strip()
    df["catalyst_date"] = df["catalyst_date"].astype(str).str.strip()
    df["approx_token"] = df["approx_token"].astype(str).str.strip()

    # approximate -> "0"/"1"
    df["approximate"] = pd.to_numeric(df["approximate"], errors="coerce").fillna(0).astype(int).astype(str)

    # ensure confidence exists and numeric helper
    if "confidence" not in df.columns:
        df["confidence"] = ""
    df["_conf"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0)

    # ensure doc_url exists
    if "doc_url" not in df.columns:
        df["doc_url"] = ""

    # ensure date_source exists
    if "date_source" not in df.columns:
        df["date_source"] = ""

    # window cols: if missing, fill from catalyst_date
    # (keep as strings; later we may parse)
    for wc in WINDOW_COLS:
        if wc not in df.columns:
            df[wc] = ""
    df.loc[df["catalyst_window_start"].astype(str).str.strip() == "", "catalyst_window_start"] = df["catalyst_date"]
    df.loc[df["catalyst_window_end"].astype(str).str.strip() == "", "catalyst_window_end"] = df["catalyst_date"]

    return df

def source_rank_val(ds: str) -> int:
    s = (ds or "").lower()
    for k, v in SOURCE_RANK.items():
        if k in s:
            return v
    return 0

def recompute_days_to_event(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    today = datetime.now(timezone.utc).date()
    dt = pd.to_datetime(df["catalyst_date"], errors="coerce").dt.date
    df["days_to_event"] = (dt - today).apply(lambda x: x.days if pd.notna(x) else 9999)
    df["days_to_event"] = pd.to_numeric(df["days_to_event"], errors="coerce").fillna(9999).astype(int).astype(str)
    return df

def drop_past(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    today = datetime.now(timezone.utc).date()
    d = pd.to_datetime(df["catalyst_date"], errors="coerce").dt.date
    keep = (d.isna()) | (d >= today)
    return df[keep].copy()

def prefer_best_per_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    For duplicate KEY_COLS, keep the "best" row:
      - exact (approx=0) > approx
      - higher confidence
      - higher source rank (SEC > mentions)
      - newer filingDate/mention_date
      - has doc_url
      - longer context
    """
    if df.empty:
        return df

    # helper columns
    df["_approx_int"] = pd.to_numeric(df["approximate"], errors="coerce").fillna(0).astype(int)
    df["_src_rank"] = df["date_source"].apply(source_rank_val)

    # Prefer newer dates if available
    if "filingDate" not in df.columns:
        df["filingDate"] = ""
    if "mention_date" not in df.columns:
        df["mention_date"] = ""
    df["_filing_dt"] = pd.to_datetime(df["filingDate"], errors="coerce")
    df["_mention_dt"] = pd.to_datetime(df["mention_date"], errors="coerce")

    # doc_url present?
    df["_has_url"] = (df["doc_url"].astype(str).str.len() > 0).astype(int)

    # context richness
    if "context" not in df.columns:
        df["context"] = ""
    df["_ctx_len"] = df["context"].astype(str).str.len()

    # last_seen recency
    df["_last_seen"] = pd.to_datetime(df["last_seen_utc"], errors="coerce")

    # sort: exact first, then higher conf, then better source, then newer dates, then has url, then context len
    df = df.sort_values(
        by=["_approx_int", "_conf", "_src_rank", "_filing_dt", "_mention_dt", "_last_seen", "_has_url", "_ctx_len"],
        ascending=[True, False, False, False, False, False, False, False],
    )

    # Preserve earliest first_seen_utc across duplicates
    first_seen_map = (
        df.groupby(KEY_COLS, dropna=False)["first_seen_utc"]
        .apply(lambda s: sorted([x for x in s if str(x).strip()])[:1][0] if any(str(x).strip() for x in s) else "")
    )

    df = df.drop_duplicates(subset=KEY_COLS, keep="first").copy()
    df["first_seen_utc"] = df.apply(lambda r: first_seen_map.get(tuple(r[c] for c in KEY_COLS), r["first_seen_utc"]), axis=1)

    # cleanup helpers
    for c in ["_approx_int","_src_rank","_filing_dt","_mention_dt","_has_url","_ctx_len","_last_seen","_conf"]:
        if c in df.columns:
            df.drop(columns=[c], inplace=True)

    return df

def drop_approx_if_exact_exists(df: pd.DataFrame) -> pd.DataFrame:
    """
    exact-over-approx rule for the same (ticker,event_type).
    - If exact exists, drop approx rows for that pair.
    - If window columns exist and are parseable, only drop approx when exact falls in the window.
    """
    if df.empty:
        return df

    d = df.copy()
    d["_approx_int"] = pd.to_numeric(d["approximate"], errors="coerce").fillna(0).astype(int)

    # exact rows
    exact = d[d["_approx_int"] == 0].copy()
    if exact.empty:
        d.drop(columns=["_approx_int"], inplace=True)
        return d

    exact_dates = exact[["ticker","event_type","catalyst_date"]].copy()
    exact_dates["_exact_dt"] = pd.to_datetime(exact_dates["catalyst_date"], errors="coerce").dt.date
    exact_pairs = set(exact_dates[["ticker","event_type"]].astype(str).apply(tuple, axis=1).tolist())

    # If we can parse window start/end, do window containment
    has_window = all(c in d.columns for c in WINDOW_COLS)
    if has_window:
        d["_ws"] = pd.to_datetime(d["catalyst_window_start"], errors="coerce").dt.date
        d["_we"] = pd.to_datetime(d["catalyst_window_end"], errors="coerce").dt.date

        # Build a lookup for exact dates per pair
        pair_to_dates = {}
        for _, r in exact_dates.iterrows():
            pair = (str(r["ticker"]), str(r["event_type"]))
            dt0 = r["_exact_dt"]
            if pd.notna(dt0):
                pair_to_dates.setdefault(pair, []).append(dt0)

        def should_drop(row):
            pair = (str(row["ticker"]), str(row["event_type"]))
            if pair not in exact_pairs:
                return False
            if int(row["_approx_int"]) != 1:
                return False
            ws, we = row["_ws"], row["_we"]
            # if no window, drop (fallback)
            if pd.isna(ws) or pd.isna(we):
                return True
            # drop only if any exact date falls within [ws,we]
            for dt0 in pair_to_dates.get(pair, []):
                if ws <= dt0 <= we:
                    return True
            # if exact exists but not in window, keep (could be different program)
            return False

        mask = d.apply(should_drop, axis=1)
        d = d[~mask].copy()

        for c in ["_ws","_we"]:
            if c in d.columns:
                d.drop(columns=[c], inplace=True)
    else:
        # Simple rule: exact exists => drop all approx for that pair
        mask = d.apply(lambda r: (str(r["ticker"]), str(r["event_type"])) in exact_pairs and int(r["_approx_int"]) == 1, axis=1)
        d = d[~mask].copy()

    d.drop(columns=["_approx_int"], inplace=True)
    return d

def main():
    os.makedirs("out", exist_ok=True)

    now_iso = datetime.now(timezone.utc).isoformat()

    # Load new sources
    parts = []
    for p in SOURCES:
        x = safe_read_csv(p)
        if not x.empty:
            parts.append(x)

    new = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    # Load master
    master = safe_read_csv(MASTER_OUT)

    # If everything empty, write empty master with a reasonable header
    if new.empty and master.empty:
        base_cols = KEY_COLS + ["days_to_event","confidence","date_source","doc_url","context"] + WINDOW_COLS + META_COLS
        pd.DataFrame(columns=base_cols).to_csv(MASTER_OUT, index=False)
        print(f"Wrote {MASTER_OUT} (0 rows)")
        return

    # Union schema
    all_cols = sorted(set(list(new.columns) + list(master.columns) + KEY_COLS + META_COLS + WINDOW_COLS))
    new = ensure_cols(new, all_cols)
    master = ensure_cols(master, all_cols)

    # Normalize
    new = normalize(new)
    master = normalize(master)

    # Stamp meta for new rows
    new["first_seen_utc"] = now_iso
    new["last_seen_utc"] = now_iso

    # Combine
    combined = pd.concat([master, new], ignore_index=True)

    # If an item appears again this run, last_seen should become now
    # We implement this by setting last_seen=now for all rows that match any KEY in 'new'
    if not new.empty:
        new_keys = set(new[KEY_COLS].astype(str).apply(tuple, axis=1).tolist())
        combined["__key__"] = combined[KEY_COLS].astype(str).apply(tuple, axis=1)
        combined.loc[combined["__key__"].isin(new_keys), "last_seen_utc"] = now_iso
        combined.drop(columns=["__key__"], inplace=True)

    # Dedupe within identical key
    combined = prefer_best_per_key(combined)

    # Apply exact-over-approx cleanup
    combined = drop_approx_if_exact_exists(combined)

    # Drop past events + recompute days_to_event
    combined = drop_past(combined)
    combined = recompute_days_to_event(combined)

    # Final sort: soonest first, then confidence
    combined["_days"] = pd.to_numeric(combined["days_to_event"], errors="coerce").fillna(9999).astype(int)
    combined["_conf2"] = pd.to_numeric(combined.get("confidence",""), errors="coerce").fillna(0.0)
    combined = combined.sort_values(["_days","_conf2"], ascending=[True, False]).drop(columns=["_days","_conf2"])

    combined.to_csv(MASTER_OUT, index=False)
    print(f"Wrote {MASTER_OUT} ({len(combined)} rows)")

if __name__ == "__main__":
    main()
