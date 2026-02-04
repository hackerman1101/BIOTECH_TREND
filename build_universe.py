# build_universe.py
import re
import io
import json
import requests
import pandas as pd

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_URL  = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

SEC_TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"

# Set a real contact string (SEC recommends identifying your app; also helps avoid 403s)
USER_AGENT = "lia-biopharma-universe/0.1 (contact: your_email@example.com)"

INCLUDE_REGEX = re.compile(
    r"(?:bio|biotech|therapeut|pharma|pharmaceut|oncolog|genom|biologic|bioscien|life\s+scien)",
    re.IGNORECASE
)

EXCLUDE_NAME_REGEX = re.compile(
    r"(?:warrant|right|unit|preferred|depositary|note|bond|debenture|etf|trust|fund)",
    re.IGNORECASE
)


def http_get_text(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
    r.raise_for_status()
    return r.text

def http_get_json(url: str):
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
    r.raise_for_status()
    return r.json()

def parse_nasdaq_listed(txt: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(txt), sep="|", dtype=str)
    df = df[df["Symbol"].notna()]
    df = df[~df["Symbol"].str.contains("File Creation Time", na=False)]
    df["exchange"] = "NASDAQ"
    df.rename(columns={"Symbol": "ticker", "Security Name": "security_name"}, inplace=True)
    return df[["ticker", "security_name", "exchange", "ETF", "Test Issue"]]

def parse_other_listed(txt: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(txt), sep="|", dtype=str)
    df = df[df["ACT Symbol"].notna()]
    df = df[~df["ACT Symbol"].str.contains("File Creation Time", na=False)]
    # Use ACT Symbol as the main ticker symbol in this file
    df.rename(columns={"ACT Symbol": "ticker", "Security Name": "security_name"}, inplace=True)
    # Exchange codes are in the file; keep them (N=NYSE, A=NYSE MKT, P=NYSE ARCA, etc.)
    df["exchange"] = df["Exchange"]
    return df[["ticker", "security_name", "exchange", "ETF", "Test Issue"]]

def load_listed_universe() -> pd.DataFrame:
    nas = parse_nasdaq_listed(http_get_text(NASDAQ_LISTED_URL))
    oth = parse_other_listed(http_get_text(OTHER_LISTED_URL))
    df = pd.concat([nas, oth], ignore_index=True)

    # Clean tickers
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df = df[df["ticker"].str.len().between(1, 10)]

    # Filter: not ETF, not test issue
    df = df[(df["ETF"] != "Y") & (df["Test Issue"] != "Y")]

    # Filter: security name exclusions (warrants/rights/units/etc.)
    df = df[~df["security_name"].fillna("").str.contains(EXCLUDE_NAME_REGEX)]

    # Deduplicate tickers (prefer first occurrence)
    df = df.drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)
    return df

def load_sec_ticker_map() -> pd.DataFrame:
    data = http_get_json(SEC_TICKERS_EXCHANGE_URL)

    # SEC format: {"fields":[...], "data":[[...], ...]}
    if isinstance(data, dict) and "fields" in data and "data" in data:
        df = pd.DataFrame(data["data"], columns=data["fields"])
    else:
        # Fallback for any future alternative formats (list-of-dicts, etc.)
        df = pd.json_normalize(data)

    # Normalize column names to what your pipeline expects
    df = df.rename(columns={"name": "company_name", "exchange": "sec_exchange"})

    if "ticker" not in df.columns:
        raise ValueError(f"SEC ticker map missing 'ticker'. Got columns: {list(df.columns)}")

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()

    # Optional: keep cik numeric but nullable
    if "cik" in df.columns:
        df["cik"] = pd.to_numeric(df["cik"], errors="coerce").astype("Int64")

    keep = [c for c in ["ticker", "cik", "company_name", "sec_exchange"] if c in df.columns]
    return df[keep].copy()


def tag_biopharma(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "company_name" not in df.columns:
        df["company_name"] = ""

    name = (df["security_name"].fillna("") + " " + df["company_name"].fillna("")).str.strip()
    df["biopharma_flag"] = name.str.contains(INCLUDE_REGEX, na=False) & ~name.str.contains(EXCLUDE_NAME_REGEX, na=False)
    df["biopharma_reason"] = df["biopharma_flag"].map(lambda x: "keyword_match" if x else "")
    return df


def main():
    listed = load_listed_universe()
    secmap = load_sec_ticker_map()

    listed["ticker"] = listed["ticker"].str.upper()
    merged = listed.merge(secmap, on="ticker", how="left")

    merged = tag_biopharma(merged)

    merged.to_csv("universe_all.csv", index=False)
    merged[merged["biopharma_flag"]].to_csv("universe_biopharma.csv", index=False)

    print("Wrote universe_all.csv and universe_biopharma.csv")
    print("Counts:", len(merged), "total,", int(merged["biopharma_flag"].sum()), "biopharma candidates")

if __name__ == "__main__":
    main()
