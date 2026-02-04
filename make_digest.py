# make_digest.py
import pandas as pd
from datetime import datetime

IN_PATH = "out/sec_events_consolidated.csv"
OUT_MD = "out/daily_digest.md"

# Hard-priority for trading catalysts
PRIORITY = {
    "CRL": 100,
    "CLINICAL_HOLD": 90,
    "PDUFA": 80,
    "FILING_ACCEPTANCE": 70,
    "NDA_BLA_SUBMISSION": 65,
    "ADCOM": 60,
    "TOPLINE": 55,
    "DOWNLOAD_ERROR": 0,
}

def score_row(r):
    p = PRIORITY.get(r["event_type"], 10)
    c = float(r["confidence"]) if pd.notna(r["confidence"]) else 0.0
    return p + 20 * c  # confidence becomes a tie-breaker

def main():
    df = pd.read_csv(IN_PATH, dtype=str)
    if df.empty:
        md = "# Daily Biopharma Digest\n\nNo events.\n"
        open(OUT_MD, "w", encoding="utf-8").write(md)
        print(f"Wrote {OUT_MD}")
        return

    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0)
    df["filingDate_dt"] = pd.to_datetime(df["filingDate"], errors="coerce")
    df["rank_score"] = df.apply(score_row, axis=1)

    # Keep top N overall, and cap per ticker
    df = df.sort_values(["rank_score", "filingDate_dt"], ascending=[False, False])
    df = df.groupby("ticker", as_index=False).head(3)  # top 3 per ticker
    df = df.head(80)  # cap digest size

    today = datetime.utcnow().strftime("%Y-%m-%d")
    lines = []
    lines.append(f"# Daily Biopharma Digest ({today} UTC)\n")
    lines.append(f"Events shown: {len(df)} (top 3 per ticker)\n")

    for ticker, g in df.groupby("ticker"):
        lines.append(f"## {ticker}\n")
        for _, r in g.iterrows():
            et = r["event_type"]
            fd = r["filingDate"]
            conf = float(r["confidence"])
            url = r.get("doc_url", "")
            snip = (r.get("snippet", "") or "").replace("\n", " ").strip()
            snip = snip[:260] + ("…" if len(snip) > 260 else "")
            lines.append(f"- **{et}** | filingDate: {fd} | conf: {conf:.2f}")
            if url:
                lines.append(f"  - doc: {url}")
            if snip:
                lines.append(f"  - snippet: {snip}")
        lines.append("")

    open(OUT_MD, "w", encoding="utf-8").write("\n".join(lines))
    print(f"Wrote {OUT_MD}")

if __name__ == "__main__":
    main()
