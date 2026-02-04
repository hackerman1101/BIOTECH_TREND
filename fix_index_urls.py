# fix_index_urls.py
import pandas as pd

IN_PATH  = "out/sec_events_consolidated.csv"
OUT_PATH = "out/sec_events_consolidated.csv"  # overwrite in place (you can change)

def main():
    df = pd.read_csv(IN_PATH, dtype=str)
    if df.empty:
        print("No rows found.")
        return

    def compute_index_url(doc_url: str) -> str:
        if not isinstance(doc_url, str) or "/Archives/edgar/data/" not in doc_url:
            return ""
        base = doc_url.rsplit("/", 1)[0] + "/"         # .../{acc_nodash}/
        folder = base.rstrip("/").split("/")[-1]       # acc_nodash
        return base + folder + "-index.html"           # correct index file name

    df["index_url"] = df["doc_url"].apply(compute_index_url)
    df.to_csv(OUT_PATH, index=False)
    print(f"Patched index_url for {len(df)} rows -> {OUT_PATH}")

if __name__ == "__main__":
    main()
