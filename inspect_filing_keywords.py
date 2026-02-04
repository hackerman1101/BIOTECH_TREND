# inspect_filing_keywords.py
import os, re
import pandas as pd

CACHE_DIR = "data/cache/sec_filing_txt_for_events"  # same as extractor cache
LOG = "out/sec_scan_log.csv"

KEYWORDS = [
    "FDA","PDUFA","DUFA","NDA","BLA","sNDA","IND","clinical hold","complete response",
    "Phase 1","Phase 2","Phase 3","topline","primary endpoint","data","readout",
    "offering","ATM","PIPE","private placement","warrant","merger","acquisition",
    "collaboration","license","Regulation FD","conference","presentation"
]

def cik_int(cik10): return str(int(str(cik10)))

def main():
    df = pd.read_csv(LOG, dtype=str).fillna("")
    for _, r in df.iterrows():
        ticker = r["ticker"]
        cik = r["cik"]
        acc = r["accessionNumber"]
        path = os.path.join(CACHE_DIR, cik_int(cik), f"{acc}.txt")
        if not os.path.exists(path):
            print(ticker, "missing cache", path)
            continue
        text = open(path, "r", encoding="utf-8", errors="ignore").read()
        # quick normalize
        t = re.sub(r"\s+", " ", text).upper()
        hits = []
        for k in KEYWORDS:
            if k.upper() in t:
                hits.append(k)
        print(f"{ticker}: keyword hits -> {hits}")

if __name__ == "__main__":
    main()
