# alerts.py
import os, json
import pandas as pd
from datetime import datetime, timezone

RANKED = "out/ranked_watchlist.csv"
CAL    = "out/catalyst_calendar.csv"
EVENTS = "out/sec_events_consolidated_with_accession.csv"  # for CRL/HOLD breaking

STATE_PATH = "data/cache/alert_state.json"
OUT_ALERTS_MD = "out/alerts.md"

# Tunables (MVP defaults)
TOP_N = 25
DAYS_SOON = 45
FINAL_SCORE_JUMP = 15.0
TREND_SPIKE_24H = 5         # >=5 mentions in 24h
TREND_SPIKE_MULT = 3.0      # 24h mentions >= 3x (7d/7) approx

BREAKING_TYPES = {"CRL", "CLINICAL_HOLD"}


def safe_int(x, default=0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, float) and pd.isna(x):
            return default
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return default
        return int(float(s))
    except Exception:
        return default


def load_state():
    if os.path.exists(STATE_PATH):
        return json.load(open(STATE_PATH, "r", encoding="utf-8"))
    return {"seen_keys": {}, "last_run": ""}

def save_state(st):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    json.dump(st, open(STATE_PATH, "w", encoding="utf-8"), indent=2)

def key_for_row(r):
    # Uniquely identify a catalyst item
    return f"{r.get('ticker','')}|{r.get('event_type','')}|{r.get('catalyst_date','')}"

def main():
    os.makedirs("out", exist_ok=True)
    st = load_state()
    seen = st.get("seen_keys", {})

    now = datetime.now(timezone.utc).isoformat()

    ranked = pd.read_csv(RANKED, dtype=str)
    # --- schema compatibility ---
    score_col = "final_score" if "final_score" in ranked.columns else ("score" if "score" in ranked.columns else None)
    if score_col is None:
        raise KeyError(f"No score column found. Expected 'final_score' or 'score'. Columns: {list(ranked.columns)}")

# Ensure days_to_event exists and is numeric (SEC_FRESH rows may have blank)
    if "days_to_event" not in ranked.columns:
        ranked["days_to_event"] = 9999

    ranked["days_to_event"] = pd.to_numeric(ranked["days_to_event"], errors="coerce").fillna(9999).astype(int)
    ranked[score_col] = pd.to_numeric(ranked[score_col], errors="coerce").fillna(0.0)

    if ranked.empty:
        print("ranked_watchlist.csv is empty; no alerts.")
        return

    # numeric columns
    for c in ["final_score", "catalyst_score", "trend_score", "days_to_event", "confidence", "mentions_24h", "mentions_7d"]:
        if c in ranked.columns:
            ranked[c] = pd.to_numeric(ranked[c], errors="coerce").fillna(0.0)

    ranked["days_to_event"] = ranked["days_to_event"].fillna(9999)

    # ---- Breaking alerts from SEC (CRL / HOLD) ----
    breaking_lines = []
    try:
        ev = pd.read_csv(EVENTS, dtype=str)
        if not ev.empty:
            ev = ev[ev["event_type"].isin(BREAKING_TYPES)].copy()
            ev["confidence"] = pd.to_numeric(ev.get("confidence", 0), errors="coerce").fillna(0.0)
            ev = ev.sort_values(["filingDate", "confidence"], ascending=[False, False]).head(30)

            for _, r in ev.iterrows():
                k = f"BREAK|{r.get('ticker','')}|{r.get('event_type','')}|{r.get('filingDate','')}"
                if k in seen:
                    continue
                seen[k] = {"first_seen": now}

                breaking_lines.append(
                    f"- **{r.get('ticker','')}** | **{r.get('event_type','')}** | filingDate {r.get('filingDate','')} | conf {float(r.get('confidence',0)):.2f}\n"
                    f"  - doc: {r.get('doc_url','')}\n"
                    f"  - snippet: {(r.get('snippet','') or '')[:260]}{'…' if len((r.get('snippet','') or ''))>260 else ''}"
                )
    except Exception:
        pass

    # ---- Catalyst alerts (calendar-based) ----
    cal_lines = []
    # pick top-N + soonest catalysts
    score_col = "final_score" if "final_score" in ranked.columns else ("score" if "score" in ranked.columns else None)
    if score_col is None:
     raise KeyError(f"No score column found. Expected 'final_score' or 'score'. Columns: {list(ranked.columns)}")

    ranked_top = ranked.sort_values(score_col, ascending=False).head(TOP_N)

    soon = ranked[ranked["days_to_event"] <= DAYS_SOON].sort_values(["days_to_event", score_col], ascending=[True, False]).head(40)

    candidates = pd.concat([ranked_top, soon], ignore_index=True).drop_duplicates(
        subset=["ticker", "event_type", "catalyst_date"], keep="first"
    )

    for _, r in candidates.iterrows():
        k = key_for_row(r)
        prev = seen.get(k, {})
        prev_score = float(prev.get("final_score", -1e9))
        cur_score = float(r.get("final_score", 0.0))

        is_new = (k not in seen)
        jumped = (cur_score - prev_score) >= FINAL_SCORE_JUMP

        if is_new or jumped:
            seen[k] = {
                "first_seen": prev.get("first_seen", now),
                "final_score": cur_score,
                "last_seen": now
            }
            approx = ""
            if safe_int(r.get("approximate", 0)) == 1:
                approx = " (approx)"

            cal_lines.append(
                f"- **{r.get('ticker','')}** | **{r.get('event_type','')}**{approx} | **{r.get('catalyst_date','')}** (D-{int(r.get('days_to_event',0))})\n"
                f"  - final {cur_score:.1f} | catalyst {float(r.get('catalyst_score',0)):.1f} | trend {float(r.get('trend_score',0)):.1f}\n"
                f"  - mentions_24h {int(r.get('mentions_24h',0))} | mentions_7d {int(r.get('mentions_7d',0))}\n"
                f"  - doc: {r.get('doc_url','')}"
            )

    # ---- Trend spike alerts ----
    spike_lines = []
    for _, r in ranked.iterrows():
        m24 = int(r.get("mentions_24h", 0))
        m7  = int(r.get("mentions_7d", 0))
        baseline = max(1.0, m7 / 7.0)

        spikey = (m24 >= TREND_SPIKE_24H) and (m24 >= TREND_SPIKE_MULT * baseline)
        if not spikey:
            continue

        k = f"SPIKE|{r.get('ticker','')}|{datetime.now(timezone.utc).date().isoformat()}"
        if k in seen:
            continue
        seen[k] = {"first_seen": now}

        spike_lines.append(
            f"- **{r.get('ticker','')}** | trend spike: 24h={m24}, 7d={m7} (baseline~{baseline:.1f}/day)\n"
            f"  - top catalyst: {r.get('event_type','')} {r.get('catalyst_date','')} (D-{int(r.get('days_to_event',0))})"
        )

    # ---- Write markdown ----
    lines = []
    lines.append(f"# Alerts (generated {now} UTC)\n")

    if breaking_lines:
        lines.append("## Breaking (SEC)\n")
        lines += breaking_lines
        lines.append("")

    if cal_lines:
        lines.append("## Catalyst alerts\n")
        lines += cal_lines
        lines.append("")

    if spike_lines:
        lines.append("## Trend spikes\n")
        lines += spike_lines
        lines.append("")

    if not (breaking_lines or cal_lines or spike_lines):
        lines.append("No new alerts (all signals already seen, or changes below thresholds).\n")

    open(OUT_ALERTS_MD, "w", encoding="utf-8").write("\n".join(lines))
    print(f"Wrote {OUT_ALERTS_MD}")

    st["seen_keys"] = seen
    st["last_run"] = now
    save_state(st)

if __name__ == "__main__":
    main()
