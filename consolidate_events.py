# consolidate_events.py (v2) — works with sec_extract_events_from_txt outputs
import os
import pandas as pd

IN_PATH = "out/sec_events.csv"
OUT_PATH = "out/sec_events_consolidated.csv"

# Canonical columns for consolidated output
OUT_COLS = [
    "ticker", "cik", "form", "filingDate",
    "accessionNumber", "doc_type",
    "event_type", "confidence", "snippet",
    "doc_url", "hits"
]

def write_empty():
    os.makedirs("out", exist_ok=True)
    pd.DataFrame(columns=OUT_COLS).to_csv(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH} (0 rows)")
    return

def main():
    # Missing or truly empty file
    if (not os.path.exists(IN_PATH)) or (os.path.getsize(IN_PATH) == 0):
        print(f"{IN_PATH} missing/empty -> writing empty consolidated file")
        return write_empty()

    try:
        df = pd.read_csv(IN_PATH, dtype=str)
    except pd.errors.EmptyDataError:
        print(f"{IN_PATH} had no parsable columns -> writing empty consolidated file")
        return write_empty()

    if df is None or df.empty:
        print(f"{IN_PATH} has 0 rows -> writing empty consolidated file")
        return write_empty()

    # Ensure required columns exist (so groupby never KeyErrors)
    required = {
        "ticker": "",
        "cik": "",
        "form": "",
        "filingDate": "",
        "accessionNumber": "",
        "doc_type": "",
        "event_type": "",
        "confidence": "0",
        "snippet": "",
        "doc_url": "",
    }
    for c, default in required.items():
        if c not in df.columns:
            df[c] = default

    # Normalize
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0)
    df["snippet"] = (
        df["snippet"].fillna("")
          .astype(str)
          .str.replace(r"\s+", " ", regex=True)
          .str.strip()
    )

    # Optional: if you don’t want noise rows included in “consolidated events”
    # comment this block out if you DO want to see these in consolidated output
    df = df[~df["event_type"].isin(["BAD_FETCH", "DOWNLOAD_ERROR"])].copy()

    if df.empty:
        print("After filtering BAD_FETCH/DOWNLOAD_ERROR, there are 0 rows -> empty consolidated")
        return write_empty()

    # Grouping keys (only columns that definitely exist now)
    gcols = ["ticker", "cik", "form", "filingDate", "accessionNumber", "doc_type", "event_type", "doc_url"]

    out = (
        df.sort_values("confidence", ascending=False)
          .groupby(gcols, as_index=False)
          .agg(
              confidence=("confidence", "max"),
              snippet=("snippet", lambda x: x.iloc[0][:500]),
              hits=("event_type", "size"),
          )
          .sort_values(["filingDate", "confidence"], ascending=[False, False])
    )

    # Ensure final column order
    for c in OUT_COLS:
        if c not in out.columns:
            out[c] = ""
    out = out[OUT_COLS]

    out.to_csv(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH} ({len(out)} rows)")

if __name__ == "__main__":
    main()
