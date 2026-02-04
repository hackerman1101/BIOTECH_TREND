# daily_brief.py
import os, json
import pandas as pd
from datetime import datetime, timezone, timedelta

CAL = "out/catalyst_calendar.csv"
RANK = "out/ranked_watchlist.csv"
MENTIONS = "out/mentions.csv"

STATE = "data/cache/brief_state.json"
OUT_MD = "out/daily_brief.md"

EVENT_PRIORITY = {"PDUFA": 5, "ADCOM": 4, "CRL": 4, "CLINICAL_HOLD": 4, "TOPLINE": 3, "NDA_BLA_SUBMISSION": 2, "FILING_ACCEPTANCE": 2}
MIN_CONF_SOON = 0.80
def one_per_ticker(df):
    if df.empty:
        return df
    d = df.copy()
    d["_prio"] = d["event_type"].map(EVENT_PRIORITY).fillna(1)
    d["_conf"] = pd.to_numeric(d.get("confidence", 0), errors="coerce").fillna(0.0)
    d["_days"] = pd.to_numeric(d.get("days_to_event", 9999), errors="coerce").fillna(9999).astype(int)
    d = d.sort_values(["_days", "_prio", "_conf"], ascending=[True, False, False])
    d = d.drop_duplicates(subset=["ticker"], keep="first")
    return d.drop(columns=["_prio", "_conf", "_days"], errors="ignore")

def load_state():
    if os.path.exists(STATE) and os.path.getsize(STATE) > 0:
        return json.load(open(STATE, "r", encoding="utf-8"))
    return {"seen": {}}

def save_state(st):
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    json.dump(st, open(STATE, "w", encoding="utf-8"), indent=2)

def key_row(r):
    return f"{r.get('ticker','')}|{r.get('event_type','')}|{r.get('catalyst_date','')}|{r.get('approximate',0)}"

def main():
    os.makedirs("out", exist_ok=True)
    now = datetime.now(timezone.utc)
    today = now.date()

    cal = pd.read_csv(CAL, dtype=str).fillna("")
    rank = pd.read_csv(RANK, dtype=str).fillna("")

    for c in ["days_to_event","confidence","approximate","final_score","catalyst_score","trend_score","mentions_24h","mentions_7d"]:
        if c in cal.columns:
            cal[c] = pd.to_numeric(cal[c], errors="coerce").fillna(0)
        if c in rank.columns:
            rank[c] = pd.to_numeric(rank[c], errors="coerce").fillna(0)

    cal["days_to_event"] = pd.to_numeric(cal.get("days_to_event", 9999), errors="coerce").fillna(9999).astype(int)
    cal["approximate"] = pd.to_numeric(cal.get("approximate", 0), errors="coerce").fillna(0).astype(int)
    cal["confidence"] = pd.to_numeric(cal.get("confidence", 0.0), errors="coerce").fillna(0.0)

    st = load_state()
    seen = st.get("seen", {})

    # Identify new calendar entries
    new_items = []
    for _, r in cal.iterrows():
        k = key_row(r)
        if k not in seen:
            seen[k] = {"first_seen": now.isoformat()}
            new_items.append(r)

    # Buckets
    soon7  = cal[cal["days_to_event"] & (cal["approximate"] == 0) <= 7].sort_values(["days_to_event","confidence"], ascending=[True, False])
    soon14 = cal[cal["days_to_event"] <= 14 & (cal["approximate"] == 0)].sort_values(["days_to_event","confidence"], ascending=[True, False])
    soon30 = cal[cal["days_to_event"] <= 30].sort_values(["days_to_event","confidence"], ascending=[True, False])
    soon7  = one_per_ticker(soon7).head(12)
    soon14 = one_per_ticker(soon14).head(18)
    soon30 = one_per_ticker(soon30).head(25)


    # Highest conviction: non-approx + higher confidence
    high_conv = cal[cal["approximate"] == 0].sort_values(["confidence","days_to_event"], ascending=[False, True]).head(25)

    # Top ranked overall
    # Ensure final_score exists (new ranker uses "score")
    if "final_score" not in rank.columns:
        if "score" in rank.columns:
            rank["final_score"] = rank["score"]
    else:
        rank["final_score"] = 0
    rank["final_score"] = pd.to_numeric(rank["final_score"], errors="coerce").fillna(0.0)

    top_ranked = rank.sort_values("final_score", ascending=False).head(25)

    
    
    # Trend spike (if present)
    spike = rank[(rank.get("mentions_24h", 0) >= 5)].sort_values(["mentions_24h","final_score"], ascending=[False, False]).head(15)

    # Build Markdown
    lines = []
    lines.append(f"# Daily Catalyst Brief ({today.isoformat()} UTC)\n")
    lines.append(f"- Calendar rows: {len(cal)}")
    lines.append(f"- Ranked rows: {len(rank)}\n")

    if new_items:
        lines.append(f"## New catalysts since last run ({len(new_items)})\n")
        df_new = pd.DataFrame(new_items).sort_values(["days_to_event","confidence"], ascending=[True, False]).head(30)
        for _, r in df_new.iterrows():
            approx = " (approx)" if int(r.get("approximate",0)) == 1 else ""
            lines.append(f"- **{r['ticker']}** | **{r['event_type']}**{approx} | {r['catalyst_date']} (D-{int(r['days_to_event'])}) | conf {float(r['confidence']):.2f}")
        lines.append("")
    else:
        lines.append("## New catalysts since last run\n- None\n")

    def section(title, df):
        lines.append(f"## {title}\n")
        if df.empty:
            lines.append("- None\n")
            return
        for _, r in df.iterrows():
            approx = " (approx)" if int(r.get("approximate",0)) == 1 else ""
            lines.append(f"- **{r['ticker']}** | **{r['event_type']}**{approx} | {r['catalyst_date']} (D-{int(r['days_to_event'])}) | conf {float(r['confidence']):.2f}")
        lines.append("")
  
    section("Coming in 7 days", soon7)
    section("Coming in 14 days", soon14)
    section("Coming in 30 days", soon30)

    lines.append("## Highest conviction (non-approx)\n")
    if high_conv.empty:
        lines.append("- None\n")
    else:
        for _, r in high_conv.iterrows():
            lines.append(f"- **{r['ticker']}** | **{r['event_type']}** | {r['catalyst_date']} (D-{int(r['days_to_event'])}) | conf {float(r['confidence']):.2f}")
        lines.append("")

    lines.append("## Top ranked (combined catalyst + trend)\n")
    for _, r in top_ranked.iterrows():
        lines.append(
            f"- **{r.get('ticker','')}** | {r.get('event_type','')} {r.get('catalyst_date','')} (D-{int(r.get('days_to_event',0))})"
            f" | final {float(r.get('final_score',0)):.1f} | trend24h {int(r.get('mentions_24h',0))}"
        )
    lines.append("")
    cal["catalyst_window_start_dt"] = pd.to_datetime(cal.get("catalyst_window_start",""), errors="coerce")
    approx = cal[cal["approximate"] == 1].sort_values(["catalyst_window_start_dt","confidence"], ascending=[True, False]).head(40)

    lines.append("## Approximate catalyst windows (unconfirmed)\n")
    if approx.empty:
     lines.append("- None\n")
    else:
     for _, r in approx.iterrows():
        tok = (r.get("approx_token","") or "").strip()
        ws = r.get("catalyst_window_start","")
        we = r.get("catalyst_window_end","")
        lines.append(f"- **{r['ticker']}** | **{r['event_type']}** | window {ws} → {we} | token: {tok} | conf {float(r['confidence']):.2f}")
    lines.append("")


    lines.append("## Trend spikes (mentions_24h >= 5)\n")
    if spike.empty:
        lines.append("- None\n")
    else:
        for _, r in spike.iterrows():
            lines.append(f"- **{r.get('ticker','')}** | mentions_24h={int(r.get('mentions_24h',0))} | final={float(r.get('final_score',0)):.1f}")
        lines.append("")

    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("approximate counts:\n", cal["approximate"].value_counts(dropna=False))
    print("min days_to_event exact:", cal[cal["approximate"]==0]["days_to_event"].min())
    print("top 10 soonest exact:\n", cal[cal["approximate"]==0].sort_values("days_to_event")[["ticker","event_type","catalyst_date","days_to_event","confidence"]].head(10))

    save_state({"seen": seen})
    print(f"Wrote {OUT_MD}")

if __name__ == "__main__":
    main()
