# BIOTECH_TREND

---

# Biopharma Catalyst Scanner (SEC + News + Trending)

This project scans biotech/pharma tickers for:

* **SEC filings** (8-K exhibits, etc.) → extracts FDA/catalyst events
* **Catalyst calendar** (exact + approximate windows) → builds a master calendar
* **Mentions/trending** from RSS feeds → calculates stronger trend signals (`trend_v2`)
* **Ranking + alerts + daily brief** → produces watchlists and summaries

Outputs are written into the `out/` folder as CSV files.

---

## Folder outputs

After a successful run you should see (examples):

* `out/sec_new_filings.csv`
* `out/sec_worklist.csv`
* `out/sec_events.csv`
* `out/sec_events_consolidated.csv`
* `out/catalyst_calendar.csv`
* `out/catalyst_calendar_master.csv`
* `out/mentions.csv`
* `out/trends_v2.csv`
* `out/ranked_watchlist.csv`
* alert/digest outputs (depends on your script config)
The out uploaded is an example of what it should look like
---

## Requirements

* Python 3.10+ (you’re using 3.12 — that’s fine)
* Internet access (SEC + RSS)
* Recommended: a fresh virtual environment

---

## Quickstart (Windows PowerShell)

### 1) Clone + enter repo

```powershell
git clone <YOUR_REPO_URL>
cd biotech
```

### 2) Create venv + activate

```powershell
python -m venv .venv
.venv\Scripts\activate
```

### 3) Install dependencies

If you have a `requirements.txt`:

```powershell
pip install -r requirements.txt
```

If you don’t yet, install the basics:

```powershell
pip install pandas requests python-dateutil
```

### 4) Set required environment variables

**SEC requires a User-Agent** identifying you (name + email is best practice):

```powershell
$env:SEC_USER_AGENT="Your Name your@email.com"
```

(Optional) If you keep your universe file somewhere else:

```powershell
$env:UNIVERSE_FILE="data/universe_biopharma.csv"
```

### 5) Run everything

```powershell
python run_all.py
```

Optional clean run:

```powershell
python run_all.py --fresh
```

---

## Quickstart (macOS/Linux)

```bash
git clone <YOUR_REPO_URL>
cd biotech
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # if present
pip install pandas requests python-dateutil  # if no requirements.txt yet

export SEC_USER_AGENT="Your Name your@email.com"
export UNIVERSE_FILE="data/universe_biopharma.csv"

python run_all.py
```

---

## Recommended run order (pipeline)

If you ever want to run scripts manually (debugging), this is the intended flow:

### A) Universe setup

* `build_universe.py` (optional) – generate a universe CSV
* `add_to_universe.py` – add a ticker manually
* Or you can use the csv files I have created beforehand.

### B) SEC ingestion + extraction

* `rss_ingest.py` / `_sec.py` / `_all.py` (depends on how your runner is wired)
* `filter_filings.py`
* `fix_index_urls.py`
* `sec_extract_events_from_txt.py`
* `consolidate_events.py`
* `add_accession_to_consolidated.py`
* `extract_catalyst_calendar_from_txt.py`
* `merge_calendar_master.py`

### C) Mentions + trending

* `rss_ingest.py` (or your mentions fetcher)
* `_mentions_watchlist.py` (optional per-ticker RSS search feed)
* `mentions_to_catalysts.py` (optional – converts mentions to dated/undated catalysts)
* `trend_v2.py` (**recommended**) – builds `out/trends_v2.csv`

### D) Ranking + outputs

* `rank_watchlist.py`
* `alerts.py`
* `daily_brief.py`
* `make_digest.py`

### E) Utilities

* `print_stats.py`
* `diagnose_calendar_yield.py`
* `inspect_filing_keywords.py`
* `check_count.bat`

---

## Configuration notes

### Universe file

Your universe file should include at least:

* `ticker`
* `cik` (recommended for SEC scanning)

Default paths searched by scripts commonly include:

* `data/universe_biopharma.csv`
* `data/universe.csv`
* `out/universe_biopharma.csv`
* `out/universe.csv`

You can override by setting:

```powershell
$env:UNIVERSE_FILE="path/to/universe.csv"
```

### SEC User-Agent (important)

Set:

```powershell
$env:SEC_USER_AGENT="Name email@domain.com"
```

Without it, SEC requests can fail or get blocked.

---

## Common issues & fixes

### 1) “ModuleNotFoundError: pandas”

You’re not in the venv or didn’t install requirements:

```powershell
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2) Output CSV is empty

This can be normal (no catalysts found in the time window), but check:

* `out/sec_scan_log.csv` (hits per filing)
* `print_stats.py` for row counts
* `diagnose_calendar_yield.py` if the calendar is empty

### 3) Ranking/alerts break due to schema changes

If you upgraded from `final_score` → `score`, update scripts to accept both (you already patched alerts/daily brief).

---


