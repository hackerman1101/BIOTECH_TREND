# filter_filings.py
import pandas as pd
from datetime import datetime, timedelta

IN_PATH = "out/sec_new_filings.csv"
OUT_PATH = "out/sec_worklist.csv"

DAYS_BACK = 30
KEEP_PER_TICKER = 10
FORMS = {"8-K"}  # start with 8-K, expand later to 10-Q/10-K if you want

def main():
    df = pd.read_csv(IN_PATH, dtype=str)
    df["filingDate"] = pd.to_datetime(df["filingDate"], errors="coerce")

    cutoff = datetime.utcnow() - timedelta(days=DAYS_BACK)
    df = df[df["form"].isin(FORMS)]
    df = df[df["filingDate"] >= cutoff]

    # newest first
    df = df.sort_values(["ticker", "filingDate"], ascending=[True, False])

    # cap per ticker
    df = df.groupby("ticker", as_index=False).head(KEEP_PER_TICKER)

    df.to_csv(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH} with {len(df)} rows (last {DAYS_BACK}d, forms={FORMS})")

if __name__ == "__main__":
    main()
