# print_stats.py
import os
import pandas as pd

FILES = [
    ("sec_new_filings", "out/sec_new_filings.csv"),
    ("sec_worklist", "out/sec_worklist.csv"),
    ("sec_events", "out/sec_events.csv"),
    ("sec_events_consolidated", "out/sec_events_consolidated.csv"),
    ("calendar", "out/catalyst_calendar.csv"),
    ("mentions", "out/mentions.csv"),
    ("ranked", "out/ranked_watchlist.csv"),
]

def rows(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return 0
    try:
        return len(pd.read_csv(path))
    except Exception:
        return -1

def main():
    for name, path in FILES:
        print(f"{name:22s} rows={rows(path):6d}  file={path}")

if __name__ == "__main__":
    main()
