import pandas as pd
import streamlit as st
from pathlib import Path

#To run: streamlit run ui_app.py


st.set_page_config(page_title="Biopharma Catalyst Monitor", layout="wide")

OUT = Path("out")

def load_csv(name):
    p = OUT / name
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(p, dtype=str).fillna("")

rank = load_csv("ranked_watchlist.csv")
cal  = load_csv("catalyst_calendar_master.csv")
sec  = load_csv("sec_events_consolidated.csv")
trn  = load_csv("trends_v2.csv")

st.title("Biopharma Catalyst Monitor")

tab1, tab2, tab3, tab4 = st.tabs(["Ranked", "Calendar", "SEC Fresh", "Trends"])

with tab1:
    st.subheader("Ranked Watchlist")
    if rank.empty:
        st.info("out/ranked_watchlist.csv is empty.")
    else:
        # normalize
        if "final_score" not in rank.columns and "score" in rank.columns:
            rank["final_score"] = rank["score"]
        rank["final_score"] = pd.to_numeric(rank["final_score"], errors="coerce").fillna(0.0)

        q = st.text_input("Search ticker")
        bucket = st.multiselect("Bucket", sorted(rank.get("bucket", "").unique().tolist()) if "bucket" in rank.columns else [], default=None)
        etype = st.multiselect("Event type", sorted(rank.get("event_type", "").unique().tolist()) if "event_type" in rank.columns else [], default=None)

        df = rank.copy()
        if q:
            df = df[df["ticker"].str.contains(q.upper(), na=False)]
        if bucket:
            df = df[df.get("bucket","").isin(bucket)]
        if etype:
            df = df[df.get("event_type","").isin(etype)]

        df = df.sort_values("final_score", ascending=False)
        st.dataframe(df, use_container_width=True, height=650)

with tab2:
    st.subheader("Calendar (Master)")
    if cal.empty:
        st.info("out/catalyst_calendar_master.csv is empty.")
    else:
        if "days_to_event" in cal.columns:
            cal["days_to_event"] = pd.to_numeric(cal["days_to_event"], errors="coerce").fillna(9999).astype(int)
        q = st.text_input("Search ticker (calendar)", key="cal_search")
        max_days = st.slider("Max days_to_event", 0, 365, 30)
        df = cal.copy()
        if q:
            df = df[df["ticker"].str.contains(q.upper(), na=False)]
        if "days_to_event" in df.columns:
            df = df[df["days_to_event"] <= max_days]
        st.dataframe(df.sort_values(["days_to_event","confidence"], ascending=[True, False]), use_container_width=True, height=650)

with tab3:
    st.subheader("SEC Fresh")
    if sec.empty:
        st.info("out/sec_events_consolidated.csv is empty.")
    else:
        q = st.text_input("Search ticker (SEC)", key="sec_search")
        etype = st.multiselect("Event type (SEC)", sorted(sec.get("event_type","").unique().tolist()) if "event_type" in sec.columns else [], default=None)
        df = sec.copy()
        if q:
            df = df[df["ticker"].str.contains(q.upper(), na=False)]
        if etype:
            df = df[df.get("event_type","").isin(etype)]
        st.dataframe(df, use_container_width=True, height=650)

with tab4:
    st.subheader("Trends v2")
    if trn.empty:
        st.info("out/trends_v2.csv is empty. Run: python trend_v2.py")
    else:
        trn["trend_score"] = pd.to_numeric(trn["trend_score"], errors="coerce").fillna(0.0)
        q = st.text_input("Search ticker (trends)", key="trn_search")
        df = trn.copy()
        if q:
            df = df[df["ticker"].str.contains(q.upper(), na=False)]
        st.dataframe(df.sort_values("trend_score", ascending=False), use_container_width=True, height=650)

