# trend_v2_1.py
# v2.1: Same output columns as trend_v2.py, but with:
# - feed_url fallback for source domain inference
# - vectorized recency + time windows
# - no groupby.apply (faster on large mention sets)
#
# Output: out/trends_v2.csv

import os, re, html
from datetime import datetime, timezone
from urllib.parse import urlparse

import numpy as np
import pandas as pd

IN_MENTIONS = "out/mentions.csv"
OUT_TRENDS = "out/trends_v2.csv"

# ----------------------------
# Tunables
# ----------------------------
HALF_LIFE_HOURS = 12.0   # recency decay: 12h half-life for trending
LOOKBACK_DAYS = 7

# Weights per intent (signal strength)
INTENT_WEIGHTS = {
    "FDA_ACTION": 2.2,
    "RESUBMISSION": 2.0,
    "PDUFA": 2.3,
    "CRL": 2.3,
    "ADCOM": 2.0,
    "CLINICAL_HOLD": 2.2,
    "TOPLINE": 1.8,
    "FINANCING_DILUTION": 1.6,
    "MNA": 1.6,
    "GUIDANCE_EARNINGS": 1.2,
    "GENERAL": 1.0,
}

# Domain/source weights (cheap heuristic)
DOMAIN_WEIGHTS = {
    "sec.gov": 2.4,
    "www.sec.gov": 2.4,
    "ir.": 1.6,  # prefix match handled separately
    "investor.": 1.6,
    "globenewswire.com": 1.5,
    "prnewswire.com": 1.5,
    "businesswire.com": 1.5,
    "fiercebiotech.com": 1.3,
    "endpts.com": 1.3,
    "statnews.com": 1.3,
    "reuters.com": 1.35,
    "bloomberg.com": 1.35,
    "seekingalpha.com": 1.1,
    "benzinga.com": 1.0,
    "reddit.com": 0.7,
    "twitter.com": 0.9,
    "x.com": 0.9,
}

# Intent regexes
INTENT_PATTERNS = [
    ("CRL", re.compile(r"\bcomplete response letter\b|\bCRL\b", re.I)),
    ("PDUFA", re.compile(r"\bPDUFA\b|\bDUFA\b|action date\b", re.I)),
    ("ADCOM", re.compile(r"\b(advisory committee|adcom|ODAC|VRBPAC)\b", re.I)),
    ("CLINICAL_HOLD", re.compile(r"\b(partial\s+clinical\s+hold|clinical\s+hold|trial\s+hold)\b", re.I)),
    ("RESUBMISSION", re.compile(r"\b(resubmission|re-?submit|resubmit)\b", re.I)),
    ("FDA_ACTION", re.compile(r"\bFDA\b|\bBLA\b|\bNDA\b|\bsBLA\b|\bfiling\b|\bsubmission\b", re.I)),
    ("TOPLINE", re.compile(r"\b(top-?line|topline|data readout|readout|primary endpoint)\b", re.I)),
    ("FINANCING_DILUTION", re.compile(r"\bATM\b|\bregistered direct\b|\bPIPE\b|\boffer(ing)?\b|\bS-3\b|\bdilution\b", re.I)),
    ("MNA", re.compile(r"\b(acquisition|acquire|merger|buyout|strategic alternatives)\b", re.I)),
    ("GUIDANCE_EARNINGS", re.compile(r"\bearnings\b|\bguidance\b|\bquarter\b|\bQ[1-4]\b", re.I)),
]

def safe_read_mentions(path: str) -> pd.DataFrame:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()

    # Try header first
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
        if "ticker" in df.columns and "created_at_utc" in df.columns:
            return df
    except Exception:
        df = None

    # Fallback: headerless
    df = pd.read_csv(path, dtype=str, header=None).fillna("")
    cols = ["id", "ticker", "kind", "source", "feed_url", "created_at_utc", "title_html", "url"]
    for i in range(len(df.columns)):
        if i < len(cols):
            df.rename(columns={i: cols[i]}, inplace=True)
        else:
            df.rename(columns={i: f"col_{i}"}, inplace=True)
    return df

def strip_html(a: str) -> str:
    s = html.unescape(str(a or ""))
    s = re.sub(r"<[^>]+>", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def get_domain(u: str) -> str:
    try:
        return urlparse(u).netloc.lower().strip()
    except Exception:
        return ""

def domain_weight(domain: str) -> float:
    if not domain:
        return 1.0
    if domain.startswith("ir.") or domain.startswith("investor."):
        return 1.6
    if domain in DOMAIN_WEIGHTS:
        return float(DOMAIN_WEIGHTS[domain])
    if domain.startswith("www.") and domain[4:] in DOMAIN_WEIGHTS:
        return float(DOMAIN_WEIGHTS[domain[4:]])
    return 1.0

def classify_intent(text: str) -> list[str]:
    intents = []
    for name, pat in INTENT_PATTERNS:
        if pat.search(text):
            intents.append(name)
    if not intents:
        intents = ["GENERAL"]
    return intents

def main():
    os.makedirs("out", exist_ok=True)

    df = safe_read_mentions(IN_MENTIONS)
    if df.empty:
        pd.DataFrame(columns=[
            "ticker","trend_score","velocity_6h","accel_6h",
            "mentions_6h","mentions_prev6h","mentions_24h","mentions_7d",
            "top_intents_24h","sources_24h","best_source","best_title","best_url"
        ]).to_csv(OUT_TRENDS, index=False)
        print(f"{IN_MENTIONS} missing/empty -> wrote empty {OUT_TRENDS}")
        return

    if "ticker" not in df.columns:
        raise ValueError("mentions.csv missing ticker column (or headerless mapping failed)")

    if "created_at_utc" not in df.columns:
        for alt in ["published", "published_at", "date", "created_at"]:
            if alt in df.columns:
                df["created_at_utc"] = df[alt]
                break
        if "created_at_utc" not in df.columns:
            df["created_at_utc"] = ""

    if "url" not in df.columns:
        df["url"] = ""
    if "feed_url" not in df.columns:
        df["feed_url"] = ""

    title_col = "title_html" if "title_html" in df.columns else ("title" if "title" in df.columns else "")
    if not title_col:
        df["title_text"] = ""
    else:
        df["title_text"] = df[title_col].apply(strip_html)

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df[df["ticker"] != ""].copy()

    # Parse times
    df["ts"] = pd.to_datetime(df["created_at_utc"], errors="coerce", utc=True)
    now_dt = datetime.now(timezone.utc)
    now_ts = pd.Timestamp(now_dt)

    cutoff_ts = now_ts - pd.Timedelta(days=LOOKBACK_DAYS)
    df = df[df["ts"].isna() | df["ts"].ge(cutoff_ts)].copy()

    # ----------------------------
    # Domain + source weights (with feed_url fallback)
    # ----------------------------
    url_s = df["url"].astype(str)
    feed_s = df["feed_url"].astype(str)
    df["url_eff"] = url_s.where(url_s.str.len() > 0, feed_s)   # fallback
    df["domain"] = df["url_eff"].apply(get_domain)
    df["src_w"] = df["domain"].apply(domain_weight).astype(float)

    # ----------------------------
    # Intent + intent weights
    # ----------------------------
    df["intents"] = df["title_text"].astype(str).apply(classify_intent)
    df["intent_w"] = df["intents"].apply(lambda xs: max(INTENT_WEIGHTS.get(x, 1.0) for x in xs)).astype(float)

    # ----------------------------
    # Vectorized recency weighting
    # ----------------------------
    age_hours = (now_ts - df["ts"]).dt.total_seconds() / 3600.0
    age_hours = age_hours.clip(lower=0)  # protect against future timestamps
    rec_w = np.power(0.5, age_hours / HALF_LIFE_HOURS)
    df["rec_w"] = pd.Series(rec_w, index=df.index).where(df["ts"].notna(), 0.0).astype(float)

    # mention score
    df["mention_score"] = (df["rec_w"] * df["src_w"] * df["intent_w"]).astype(float)

    # ----------------------------
    # Vectorized time windows
    # ----------------------------
    w6 = now_ts - pd.Timedelta(hours=6)
    w12 = now_ts - pd.Timedelta(hours=12)
    w24 = now_ts - pd.Timedelta(hours=24)

    ts = df["ts"]
    df["in_6h"] = ts.ge(w6).fillna(False)
    df["in_prev6h"] = (ts.ge(w12) & ts.lt(w6)).fillna(False)
    df["in_24h"] = ts.ge(w24).fillna(False)

    # ----------------------------
    # Aggregate per ticker (NO groupby.apply)
    # ----------------------------
    df["score_6h"] = df["mention_score"].where(df["in_6h"], 0.0)
    df["score_prev6h"] = df["mention_score"].where(df["in_prev6h"], 0.0)
    df["score_24h"] = df["mention_score"].where(df["in_24h"], 0.0)

    agg = df.groupby("ticker", as_index=False).agg(
        mentions_6h=("in_6h", "sum"),
        mentions_prev6h=("in_prev6h", "sum"),
        mentions_24h=("in_24h", "sum"),
        mentions_7d=("ticker", "size"),
        score_6h=("score_6h", "sum"),
        score_prev6h=("score_prev6h", "sum"),
        score_24h=("score_24h", "sum"),
        score_7d=("mention_score", "sum"),
    )

    # sources_24h
    df_24 = df.loc[df["in_24h"]].copy()
    src24 = (
        df_24.groupby("ticker")["domain"]
            .nunique()
            .rename("sources_24h")
            .reset_index()
    )
    agg = agg.merge(src24, on="ticker", how="left")
    agg["sources_24h"] = agg["sources_24h"].fillna(0).astype(int)

    # Ensure mention counts are ints
    for c in ["mentions_6h", "mentions_prev6h", "mentions_24h", "mentions_7d"]:
        agg[c] = pd.to_numeric(agg[c], errors="coerce").fillna(0).astype(int)

    # Velocity + acceleration
    agg["velocity_6h"] = agg["score_6h"] / (agg["score_24h"] + 1e-6)
    agg["accel_6h"] = agg["score_6h"] - agg["score_prev6h"]

    # Trend score (same formula as v2)
    base = (2.2 * agg["score_6h"]) + (1.0 * agg["score_24h"]) + (0.3 * agg["score_7d"])
    boost = (1.0 + 0.6 * agg["velocity_6h"].clip(lower=0, upper=3)) + (0.15 * agg["accel_6h"].clip(lower=0))
    src_boost = (1.0 + 0.10 * agg["sources_24h"].clip(lower=0, upper=6))
    agg["trend_score"] = base * boost * src_boost

    # ----------------------------
    # Top intents (24h) WITHOUT groupby.apply
    # ----------------------------
    if df_24.empty:
        top_int = pd.DataFrame({"ticker": agg["ticker"], "top_intents_24h": ""})
    else:
        ex = df_24[["ticker", "intents", "mention_score"]].explode("intents")
        ex = ex.rename(columns={"intents": "intent"})
        ti = (
            ex.groupby(["ticker", "intent"], as_index=False)["mention_score"]
              .sum()
              .rename(columns={"mention_score": "w"})
        )
        ti = ti.sort_values(["ticker", "w"], ascending=[True, False], kind="mergesort")
        ti["rank"] = ti.groupby("ticker").cumcount() + 1
        ti = ti[ti["rank"] <= 3].copy()
        ti["pair"] = ti["intent"].astype(str) + ":" + ti["w"].map(lambda v: f"{v:.2f}")

        top_int = (
            ti.groupby("ticker", as_index=False)["pair"]
              .agg(lambda s: ";".join(s.tolist()))
              .rename(columns={"pair": "top_intents_24h"})
        )
        top_int = agg[["ticker"]].merge(top_int, on="ticker", how="left")
        top_int["top_intents_24h"] = top_int["top_intents_24h"].fillna("")

    agg = agg.merge(top_int[["ticker", "top_intents_24h"]], on="ticker", how="left")

    # ----------------------------
    # Best mention (highest mention_score in 24h, else overall) WITHOUT groupby.apply
    # ----------------------------
    idx24 = pd.Index([])
    if not df_24.empty:
        idx24 = df_24.groupby("ticker")["mention_score"].idxmax()

    idx_all = df.groupby("ticker")["mention_score"].idxmax()

    chosen = {}
    if len(idx24):
        for t, i in idx24.items():
            chosen[t] = int(i)
    for t, i in idx_all.items():
        if t not in chosen:
            chosen[t] = int(i)

    best_df = df.loc[list(chosen.values()), ["ticker", "domain", "title_text", "url", "feed_url"]].copy()
    best_df["best_source"] = best_df["domain"].fillna("")
    best_df["best_title"] = best_df["title_text"].fillna("").astype(str).str.slice(0, 180)

    # best_url: url if present else feed_url (keeps column identical, improves coverage)
    u = best_df["url"].fillna("").astype(str)
    f = best_df["feed_url"].fillna("").astype(str)
    best_df["best_url"] = u.where(u.str.len() > 0, f)

    out = agg.merge(best_df[["ticker", "best_source", "best_title", "best_url"]], on="ticker", how="left")

    # Final columns (identical order to v2)
    out = out[[
        "ticker","trend_score","velocity_6h","accel_6h",
        "mentions_6h","mentions_prev6h","mentions_24h","mentions_7d",
        "top_intents_24h","sources_24h","best_source","best_title","best_url"
    ]].copy()

    out["trend_score"] = pd.to_numeric(out["trend_score"], errors="coerce").fillna(0.0)
    out = out.sort_values("trend_score", ascending=False)

    out.to_csv(OUT_TRENDS, index=False)
    print(f"Wrote {OUT_TRENDS} with {len(out)} rows")

if __name__ == "__main__":
    main()
