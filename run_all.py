import subprocess, sys

SCRIPTS = [
    "run_sec.py",
    "filter_filings.py",
    "sec_extract_events_from_txt.py",
    "consolidate_events.py",
    "add_accession_to_consolidated.py",
    "extract_catalyst_calendar_from_txt.py",
    "run_mentions_watchlist.py",
    "mentions_to_catalysts.py",
    "merge_calendar_master.py",
    "rss_ingest.py",
    "trend_v2.py",
    "rank_watchlist.py",
    "alerts.py",
    "daily_brief.py",
]

py = sys.executable
for s in SCRIPTS:
    subprocess.run([py, s], check=True)
