# rank_watchlist.py
import os
import pandas as pd
from datetime import datetime, timezone, timedelta

CAL = "out/catalyst_calendar_master.csv"
MENTIONS = "out/mentions.csv"
OUT_TREND = "out/trend_scores.csv"
OUT_RANK  = "out/ranked_watchlist.csv"

def score_sec_events(path="out/sec_events_consolidated.csv"):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()

    df = pd.read_csv(path, dtype=str).fillna("")
    if df.empty:
        return df

    # Ensure expected columns exist (adjust if your consolidated schema differs)
    for c in ["ticker","event_type","confidence","filingDate","doc_type","context","doc_url"]:
        if c not in df.columns:
            df[c] = ""

    df["ticker"] = df["ticker"].str.upper().str.strip()
    df["_conf"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0)

    # Recency score (newer filings score higher)
    fd = pd.to_datetime(df["filingDate"], errors="coerce", utc=True)
    today = datetime.now(timezone.utc)
    df["_age_days"] = (today - fd).dt.days
    df["_age_days"] = pd.to_numeric(df["_age_days"], errors="coerce").fillna(999).clip(lower=0)

    # Event severity weights (tune later if needed)
    W = {
        "PDUFA": 6,
        "ADCOM": 5,
        "CRL": 5,
        "CLINICAL_HOLD": 5,
        "NDA_BLA_RESUBMISSION": 5,   # add this if you introduce it
        "NDA_BLA_SUBMISSION": 4,
        "TOPLINE": 3,
    }
    df["_w_event"] = df["event_type"].map(W).fillna(2)

    # Doc quality bonus: EX-99.* is often the press release
    df["_doc_bonus"] = 0.0
    df["_doc_bonus"] += df["doc_type"].str.contains(r"EX-99", case=False, na=False).astype(float) * 0.6

    # Context keywords bonus (helps pull “FDA update” language upward)
    ctx = df["context"].astype(str)
    df["_kw_bonus"] = 0.0
    df["_kw_bonus"] += ctx.str.contains(r"\bFDA\b", case=False, na=False).astype(float) * 0.4
    df["_kw_bonus"] += ctx.str.contains(r"\bresubmit|resubmission|re-submit\b", case=False, na=False).astype(float) * 0.6
    df["_kw_bonus"] += ctx.str.contains(r"additional information|information requested", case=False, na=False).astype(float) * 0.4

    # Recency curve: 0 days old => 1.0, 7 days => ~0.5, 14 days => ~0.25
    df["_recency"] = (0.5 ** (df["_age_days"] / 7.0)).astype(float)

    # Final SEC score
    df["score"] = (
        df["_w_event"] * (0.6 + df["_conf"]) * df["_recency"]
        + df["_doc_bonus"]
        + df["_kw_bonus"]
    )

    df["bucket"] = "SEC_FRESH"
    df["why"] = (
        "SEC " + df["event_type"].astype(str)
        + " | conf=" + df["_conf"].round(2).astype(str)
        + " | age_d=" + df["_age_days"].astype(int).astype(str)
    )

    keep = ["ticker","event_type","filingDate","confidence","doc_type","doc_url","context","score","bucket","why"]
    return df[keep].sort_values("score", ascending=False)




def main():
    cal = pd.read_csv(CAL, dtype=str)
    if cal.empty:
        print("Calendar is empty; cannot rank.")
        return

    cal["days_to_event"] = pd.to_numeric(cal["days_to_event"], errors="coerce").fillna(9999)
    cal["confidence"] = pd.to_numeric(cal["confidence"], errors="coerce").fillna(0.0)
    cal["approximate"] = pd.to_numeric(cal.get("approximate", 0), errors="coerce").fillna(0).astype(int)

    # Catalyst score: nearer date + higher confidence; penalize approximate a bit
    cal["catalyst_score"] = (
        (1 / (1 + cal["days_to_event"].clip(lower=0))) * 100
        + cal["confidence"] * 20
        - cal["approximate"] * 5
    )

    # Trend score from mentions
    try:
        m = pd.read_csv(MENTIONS, dtype=str)
    except Exception:
        m = pd.DataFrame(columns=["ticker", "created_at_utc"])

    if not m.empty:
        m["created_at_utc"] = pd.to_datetime(m["created_at_utc"], errors="coerce", utc=True)
        now = datetime.now(timezone.utc)
        w24 = now - timedelta(hours=24)
        w7d = now - timedelta(days=7)

        m24 = m[m["created_at_utc"] >= w24].groupby("ticker").size().rename("mentions_24h")
        m7  = m[m["created_at_utc"] >= w7d].groupby("ticker").size().rename("mentions_7d")

        trend = pd.concat([m24, m7], axis=1).fillna(0).reset_index()
        trend["mentions_24h"] = trend["mentions_24h"].astype(int)
        trend["mentions_7d"] = trend["mentions_7d"].astype(int)

    # Trend score: 24h velocity weighted, plus baseline 7d
    try:
        trend = pd.read_csv("out/trends_v2.csv", dtype=str).fillna("")
        trend["trend_score"] = pd.to_numeric(trend["trend_score"], errors="coerce").fillna(0.0)
    except Exception:
        trend = pd.DataFrame({"ticker": cal["ticker"].unique(), "trend_score": 0})


    trend.to_csv(OUT_TREND, index=False)

    # Merge and final score
    merged = cal.merge(trend, on="ticker", how="left").fillna({"mentions_24h":0, "mentions_7d":0, "trend_score":0})
    merged["final_score"] = merged["catalyst_score"] * 0.7 + merged["trend_score"] * 0.3
    # ----------------------------
    # Add SEC_FRESH ranking stream
    # ----------------------------
    ranked_cal = merged.copy()
    ranked_cal["bucket"] = "CALENDAR"
    ranked_cal = ranked_cal.rename(columns={"final_score": "score"})

    ranked_sec = score_sec_events("out/sec_events_consolidated.csv")
    if ranked_sec.empty:
        # fallback to raw sec_events if consolidated isn't available
        ranked_sec = score_sec_events("out/sec_events.csv")

    # Make sure both have a 'score' column
    ranked_cal["score"] = pd.to_numeric(ranked_cal["score"], errors="coerce").fillna(0.0)
    if "score" in ranked_sec.columns:
        ranked_sec["score"] = pd.to_numeric(ranked_sec["score"], errors="coerce").fillna(0.0)

    combined = pd.concat([ranked_cal, ranked_sec], ignore_index=True, sort=False)

    # One row per ticker: keep best score
    combined = combined.sort_values("score", ascending=False)
    combined = combined.drop_duplicates(subset=["ticker"], keep="first")

    combined.to_csv(OUT_RANK, index=False)


    print(f"Wrote {OUT_TREND} and {OUT_RANK} (top rows: {len(combined)})")


if __name__ == "__main__":
    main()
