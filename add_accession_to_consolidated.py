# add_accession_to_consolidated.py (v2) — idempotent + schema-safe
import os
import pandas as pd

CONS_IN  = "out/sec_events_consolidated.csv"
WORKLIST = "out/sec_worklist.csv"
CONS_OUT = "out/sec_events_consolidated_with_accession.csv"

def write_empty():
    os.makedirs("out", exist_ok=True)
    pd.DataFrame().to_csv(CONS_OUT, index=False)
    print(f"Wrote {CONS_OUT} (empty)")
    return

def main():
    if (not os.path.exists(CONS_IN)) or os.path.getsize(CONS_IN) == 0:
        print(f"{CONS_IN} missing/empty -> writing empty output")
        return write_empty()

    try:
        cons = pd.read_csv(CONS_IN, dtype=str).fillna("")
    except pd.errors.EmptyDataError:
        print(f"{CONS_IN} unreadable -> writing empty output")
        return write_empty()

    if cons.empty:
        print(f"{CONS_IN} has 0 rows -> writing empty output")
        return write_empty()

    # If accessionNumber already exists and non-empty, just forward it
    if "accessionNumber" in cons.columns and cons["accessionNumber"].astype(str).str.len().sum() > 0:
        cons.to_csv(CONS_OUT, index=False)
        filled = (cons["accessionNumber"].astype(str).str.len() > 0).mean() * 100
        print(f"{CONS_IN} already has accessionNumber -> wrote {CONS_OUT}. Filled {filled:.1f}%")
        return

    # Otherwise, merge from worklist
    if (not os.path.exists(WORKLIST)) or os.path.getsize(WORKLIST) == 0:
        # can't merge, just write cons as-is
        cons.to_csv(CONS_OUT, index=False)
        print(f"{WORKLIST} missing/empty -> wrote {CONS_OUT} without accessionNumber merge")
        return

    wk = pd.read_csv(WORKLIST, dtype=str).fillna("")

    # Normalize expected join keys
    for c in ["ticker", "cik", "filingDate", "accessionNumber", "primaryDocument"]:
        if c not in wk.columns:
            wk[c] = ""
    for c in ["ticker", "cik", "filingDate", "doc_url"]:
        if c not in cons.columns:
            cons[c] = ""

    cons["ticker"] = cons["ticker"].astype(str).str.upper().str.strip()
    wk["ticker"] = wk["ticker"].astype(str).str.upper().str.strip()

    # extract primaryDocument from doc_url if possible
    cons["primaryDocument"] = cons["doc_url"].fillna("").apply(lambda x: x.split("/")[-1] if isinstance(x, str) and "/" in x else "")

    # best-effort merge keys
    keys = ["ticker", "cik", "filingDate"]
    wk_small = wk[keys + ["accessionNumber"]].drop_duplicates(subset=keys)

    out = cons.merge(wk_small, on=keys, how="left")

    # if accessionNumber column still missing, create it
    if "accessionNumber" not in out.columns:
        out["accessionNumber"] = ""

    filled = (out["accessionNumber"].astype(str).str.len() > 0).mean() * 100
    out.to_csv(CONS_OUT, index=False)
    print(f"Wrote {CONS_OUT}. accessionNumber filled {filled:.1f}%")

if __name__ == "__main__":
    main()
