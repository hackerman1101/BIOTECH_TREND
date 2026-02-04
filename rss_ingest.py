
import os, re, time, hashlib
import pandas as pd
import requests
import feedparser
from datetime import datetime, timezone

USER_AGENT = "lia-biopharma/0.1 (contact: huyminhhoangtrong1101@gmail.com)"  # CHANGE THIS

UNIVERSE = "universe_biopharma.csv"
CALENDAR = "out/catalyst_calendar.csv"   # we’ll prioritize names for these tickers
OUT_MENTIONS = "out/mentions.csv"
OUT_FEED_STATUS = "out/rss_feed_status.csv"

# Biotech-focused editorial RSS (far fewer ads than PR-wire blasts)
RSS_FEEDS = [
    # Fierce provides RSS feeds for biotech/pharma sections
    "https://www.fiercebiotech.com/rss/xml",
    "https://www.fiercebiotech.com/rss/biotech/xml",
    "https://www.fiercepharma.com/rss/xml",

    # BioSpace category feeds (their RSS page links to these)
    "https://www.biospace.com/all-news.rss",
    "https://www.biospace.com/fda.rss",
    "https://www.biospace.com/drug-development.rss",
    "https://www.biospace.com/deals.rss",
    "https://www.biospace.com/business.rss",

    # STAT provides RSS feeds by category
    "https://www.statnews.com/category/biotech/feed",
    "https://www.statnews.com/category/pharma/feed",

    # FDA MedWatch (optional: more regulatory/safety than “trader trend”)
    "http://www.fda.gov/AboutFDA/ContactFDA/StayInformed/RSSFeeds/MedWatch/rss.xml",
]

AD_WORDS = re.compile(
    r"\b(sponsored|advertis(e|ing)|promotion|webinar|whitepaper|partner content|job(s)?|hiring|career|newsletter)\b",
    re.I,
)

EXCHANGE_TICKER = re.compile(r"\b(NASDAQ|NYSE|AMEX)\s*[:\-]?\s*([A-Z]{1,5})\b")

def load_universe():
    u = pd.read_csv(UNIVERSE, dtype=str).fillna("")
    u["ticker"] = u["ticker"].str.upper().str.strip()

    # best-effort company name column detection
    name_col = None
    for c in ["company_name", "company", "name", "securityName", "issuer", "issuer_name"]:
        if c in u.columns:
            name_col = c
            break
    if name_col is None:
        u["company_name"] = ""
        name_col = "company_name"

    u["company_name"] = u[name_col].astype(str).str.strip()
    return u[["ticker", "company_name"]].drop_duplicates()

def build_company_aliases(name: str):
    n = (name or "").strip()
    if not n or len(n) < 4:
        return []

    # remove common suffixes (keep conservative to avoid false positives)
    n2 = re.sub(r"\b(inc\.?|corp\.?|corporation|ltd\.?|limited|plc|s\.a\.|co\.?)\b", "", n, flags=re.I).strip()
    n2 = re.sub(r"\s+", " ", n2)

    aliases = []
    # full name
    aliases.append(n2)

    # first two tokens if reasonably specific (avoid 1-token “Rhythm” type ambiguity)
    toks = [t for t in re.split(r"\s+", n2) if t]
    if len(toks) >= 2:
        aliases.append(" ".join(toks[:2]))

    # de-dupe
    out, seen = [], set()
    for a in aliases:
        a = a.strip()
        if len(a) >= 6 and a.lower() not in seen:
            seen.add(a.lower())
            out.append(a)
    return out

def fetch_feed(url: str):
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
    return r.status_code, r.text

def normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def entry_time(e):
    # try multiple fields
    for k in ["published", "updated", "created"]:
        if k in e and e[k]:
            dt = pd.to_datetime(e[k], errors="coerce", utc=True)
            if pd.notna(dt):
                return dt.to_pydatetime()
    return datetime.now(timezone.utc)

def main():
    os.makedirs("out", exist_ok=True)

    uni = load_universe()

    # focus name-matching on tickers that actually have catalysts (keeps false positives low)
    cal = pd.read_csv(CALENDAR, dtype=str) if os.path.exists(CALENDAR) else pd.DataFrame(columns=["ticker"])
    focus = set(cal["ticker"].astype(str).str.upper().unique().tolist())

    # Ticker set (for exchange ticker matches)
    all_tickers = set(uni["ticker"].tolist())

    # Build name alias map only for focus tickers
    name_map = {}
    for _, r in uni.iterrows():
        tk = r["ticker"]
        if tk in focus and r["company_name"]:
            name_map[tk] = build_company_aliases(r["company_name"])

    mention_rows = []
    feed_status = []

    for feed in RSS_FEEDS:
        try:
            status, body = fetch_feed(feed)
            feed_status.append({"feed": feed, "http_status": status, "bytes": len(body or "")})

            if status != 200 or not body:
                continue

            parsed = feedparser.parse(body)
            for e in parsed.entries:
                title = normalize(getattr(e, "title", ""))
                summary = normalize(getattr(e, "summary", ""))[:2000]
                link = getattr(e, "link", "")
                text = (title + " " + summary).strip()

                if not text:
                    continue
                if AD_WORDS.search(text):
                    continue

                # stable id
                uid = hashlib.sha1((title + "|" + link).encode("utf-8", errors="ignore")).hexdigest()
                created = entry_time(e).isoformat()

                found = []

                # 1) exchange ticker patterns (NASDAQ: TVTX)
                for m in EXCHANGE_TICKER.finditer(text.upper()):
                    tk = m.group(2).upper()
                    if tk in all_tickers:
                        found.append((tk, "exchange_ticker"))

                # 2) $TICKER and bare-word tickers
                # keep conservative: only tickers that are in your universe and 2-5 chars
                for tk in all_tickers:
                    if len(tk) < 2 or len(tk) > 5:
                        continue
                    if re.search(rf"(?<![A-Z0-9])(\${tk}|{tk})(?![A-Z0-9])", text.upper()):
                        found.append((tk, "ticker"))

                # 3) company name aliases (only for focus tickers with catalysts)
                for tk, aliases in name_map.items():
                    for a in aliases:
                        if re.search(rf"\b{re.escape(a)}\b", text, flags=re.I):
                            found.append((tk, "name"))
                            break

                if not found:
                    continue

                # de-dupe per entry/ticker
                seen = set()
                for tk, how in found:
                    key = (uid, tk)
                    if key in seen:
                        continue
                    seen.add(key)
                    mention_rows.append({
                        "mention_id": uid,
                        "ticker": tk,
                        "matched_by": how,
                        "source": "rss",
                        "feed": feed,
                        "created_at_utc": created,
                        "title": title[:300],
                        "link": link,
                    })

        except Exception:
            feed_status.append({"feed": feed, "http_status": "ERR", "bytes": 0})
        time.sleep(0.2)

    pd.DataFrame(feed_status).to_csv(OUT_FEED_STATUS, index=False)

    mdf = pd.DataFrame(mention_rows)
    if mdf.empty:
        mdf.to_csv(OUT_MENTIONS, index=False)
        print(f"Wrote {OUT_MENTIONS} (0 rows). Also wrote {OUT_FEED_STATUS}.")
        return

    mdf = mdf.drop_duplicates(subset=["mention_id", "ticker", "link"]).reset_index(drop=True)
    mdf.to_csv(OUT_MENTIONS, index=False)
    print(f"Wrote {OUT_MENTIONS} ({len(mdf)} rows). Also wrote {OUT_FEED_STATUS}.")

if __name__ == "__main__":
    main()
