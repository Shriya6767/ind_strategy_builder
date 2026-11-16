"""Append newly downloaded daily zips to the raw Sensex trees and (optionally)
build the merged engine files for those days -- the routine for every data
update.

The vendor (GFDL, Dropbox) delivers one zip per trading day, one CSV inside:
    BSE_INDICES_DDMMYYYY.zip       -> spot -> <SENSEX_SPOT_PATH>/<YYYY>/<MON_YYYY>/BSE_INDICES_DDMMYYYY.parquet
    BFO_BACKADJUSTED_DDMMYYYY.zip  -> F&O  -> <SENSEX_FNO_PATH>/<YYYY>/<MON_YYYY>/BFO_BACKADJUSTED_DDMMYYYY.parquet

Each zip is read straight into pyarrow (no temp CSV, one day in memory at a
time) and written with the exact schema of the existing raw files, so old and
new days are indistinguishable to scripts/preprocess_sensex.py.

A day whose raw parquet already exists is NEVER rewritten: it is compared
with the download and reported as identical or different. Re-running is safe.

Usage (from repo root, venv active):
    python scripts/append_raw_data.py <folder-or-zip> [<folder-or-zip> ...] [--recursive] [--preprocess]

    python scripts/append_raw_data.py C:\\Users\\me\\Downloads\\new_data --recursive --preprocess

--recursive also looks inside sub-folders (Dropbox folder downloads nest
them). --preprocess then runs preprocess_sensex.py for exactly the days
written, so the merged tree gets the new files in the same go. Afterwards
restart the API (or POST /api/load-data with reload: true).
"""
import argparse
import io
import os
import subprocess
import sys
import time
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.modules import re, pa, pcsv, pq
from src.core.config import SENSEX_SPOT_PATH, SENSEX_FNO_PATH

ZIP_PATTERN = re.compile(r"^(BSE_INDICES|BFO_BACKADJUSTED)_(\d{2})(\d{2})(\d{4})\.zip$")
MONTHS = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")

# Schema of the raw files (identical for spot and F&O), fixed here so even
# the first day of a brand-new tree is written exactly like every existing one.
RAW_SCHEMA = pa.schema([
    ("Ticker", pa.string()),
    ("Date", pa.string()),
    ("Time", pa.time32("ms")),
    ("Open", pa.float64()),
    ("High", pa.float64()),
    ("Low", pa.float64()),
    ("Close", pa.float64()),
    ("Volume", pa.int64()),
    ("Open Interest", pa.int64()),
])


def read_zip(zip_path: str) -> pa.Table:
    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise ValueError(f"{zip_path}: expected exactly one CSV inside, found {z.namelist()}")
        raw = z.read(names[0])
    table = pcsv.read_csv(
        io.BytesIO(raw),
        convert_options=pcsv.ConvertOptions(column_types={f.name: f.type for f in RAW_SCHEMA}),
    )
    missing = [f.name for f in RAW_SCHEMA if f.name not in table.column_names]
    if missing:
        raise ValueError(f"{zip_path}: CSV is missing columns {missing}")
    return table.select(RAW_SCHEMA.names).cast(RAW_SCHEMA)


def collect_zips(paths, recursive: bool) -> list[str]:
    found = []
    for p in paths:
        p = os.path.expanduser(p)
        if os.path.isfile(p):
            found.append(p)
        elif os.path.isdir(p):
            if recursive:
                for root, _dirs, files in os.walk(p):
                    found.extend(os.path.join(root, f) for f in files)
            else:
                found.extend(os.path.join(p, f) for f in os.listdir(p))
        else:
            sys.exit(f"Not found: {p}")
    return sorted(f for f in found if ZIP_PATTERN.match(os.path.basename(f)))


def main():
    ap = argparse.ArgumentParser(description="Append downloaded daily zips to the raw Sensex parquet trees.")
    ap.add_argument("sources", nargs="+", help="Zip files and/or folders of zips (spot and F&O may be mixed).")
    ap.add_argument("--recursive", action="store_true", help="Also look inside sub-folders of the given folders.")
    ap.add_argument("--preprocess", action="store_true",
                    help="After appending, run preprocess_sensex.py for the days written.")
    args = ap.parse_args()

    if not (SENSEX_SPOT_PATH and SENSEX_FNO_PATH):
        sys.exit("SENSEX_SPOT_PATH and SENSEX_FNO_PATH must be set in .env")

    zips = collect_zips(args.sources, args.recursive)
    if not zips:
        sys.exit("No BSE_INDICES_*.zip / BFO_BACKADJUSTED_*.zip files found in the given sources.")

    written, same, different, dates = [], [], [], set()
    t0 = time.perf_counter()
    for zip_path in zips:
        prefix, dd, mm, yyyy = ZIP_PATTERN.match(os.path.basename(zip_path)).groups()
        root = SENSEX_SPOT_PATH if prefix == "BSE_INDICES" else SENSEX_FNO_PATH
        out_dir = os.path.join(root, yyyy, f"{MONTHS[int(mm) - 1]}_{yyyy}")
        out_path = os.path.join(out_dir, f"{prefix}_{dd}{mm}{yyyy}.parquet")
        label = f"{yyyy}-{mm}-{dd} {prefix}"

        table = read_zip(zip_path)
        if table.num_rows == 0:
            print(f"  !! {label}: empty CSV, skipped")
            continue
        if os.path.exists(out_path):
            existing = pq.read_table(out_path).replace_schema_metadata(None)
            (same if table.equals(existing) else different).append(label)
            continue
        os.makedirs(out_dir, exist_ok=True)
        pq.write_table(table, out_path)
        written.append((label, table.num_rows, out_path))
        dates.add(f"{yyyy}-{mm}-{dd}")

    print(f"done in {time.perf_counter() - t0:.1f}s")
    print(f"\nWRITTEN ({len(written)} new raw files):")
    for label, rows, path in written:
        print(f"  {rows:>7} rows  {path}")
    print(f"\nALREADY PRESENT and identical to the download, left untouched ({len(same)}):")
    print("  " + (", ".join(same) or "-"))
    print(f"\nALREADY PRESENT but DIFFERENT from the download, left untouched ({len(different)}):")
    print("  " + (", ".join(different) or "-"))

    if args.preprocess and dates:
        start, end = min(dates), max(dates)
        print(f"\nRunning preprocess_sensex.py --start {start} --end {end} ...")
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "preprocess_sensex.py")
        rc = subprocess.call([sys.executable, script, "--start", start, "--end", end])
        if rc != 0:
            sys.exit(f"preprocess_sensex.py exited with status {rc}")
        print("Merged files built. Restart the API (or POST /api/load-data with reload: true) to load them.")
    elif args.preprocess:
        print("\nNothing new was written, preprocessing skipped.")


if __name__ == "__main__":
    main()




# cd D:\ind_strategy_builder && venv\Scripts\python.exe scripts\append_raw_data.py "C:\Users\shriy\Downloads\Sensex_Spot_1 Min Data" "C:\Users\shriy\Downloads\Sensex(Futures+Options_1 Min Data" --recursive --preprocess