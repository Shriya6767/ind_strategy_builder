#venv/bin/python scripts/append_raw_data.py "/home/pradip/Downloads/Sensex_Spot_1 Min Data" "/home/pradip/Downloads/Sensex(Futures+Options_1 Min Data" --recursive

"""Append newly downloaded daily zips to the raw Sensex trees and (optionally)
build the merged engine files for those days -- the routine for every data
update.

The vendor (GFDL, Dropbox) delivers one zip per trading day, one CSV inside:
    BSE_INDICES_DDMMYYYY.zip       -> spot -> <SENSEX_SPOT_PATH>/<YYYY>/<MON_YYYY>/BSE_INDICES_DDMMYYYY.parquet
    BFO_BACKADJUSTED_DDMMYYYY.zip  -> F&O  -> <SENSEX_FNO_PATH>/<YYYY>/<MON_YYYY>/BFO_BACKADJUSTED_DDMMYYYY.parquet

Older history comes packed one zip per MONTH instead (MAY_2024.zip holding
MAY_2024/BSE_INDICES_02052024.csv, ...), and a day may also sit around as a
bare BSE_INDICES_DDMMYYYY.csv / BFO_BACKADJUSTED_DDMMYYYY.csv. All three
forms are accepted and may be mixed in one run; what matters is the daily
file's own name, which carries the prefix and the date.

Each day's CSV is read straight into pyarrow (no temp CSV, one day in memory
at a time) and written with the exact schema of the existing raw files, so
old and new days are indistinguishable to scripts/preprocess_sensex.py.

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

# One trading day, as a zip (daily download) or a CSV (bare file, or a member
# of a month zip). The groups are the same either way: prefix, dd, mm, yyyy.
DAY_PATTERN = re.compile(r"^(BSE_INDICES|BFO_BACKADJUSTED)_(\d{2})(\d{2})(\d{4})\.(zip|csv)$", re.IGNORECASE)
# A month archive of daily CSVs: MAY_2024.zip
MONTH_ZIP_PATTERN = re.compile(r"^[A-Z]{3}_\d{4}\.zip$", re.IGNORECASE)
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


def read_csv_bytes(raw: bytes, source: str) -> pa.Table:
    table = pcsv.read_csv(
        io.BytesIO(raw),
        convert_options=pcsv.ConvertOptions(column_types={f.name: f.type for f in RAW_SCHEMA}),
    )
    missing = [f.name for f in RAW_SCHEMA if f.name not in table.column_names]
    if missing:
        raise ValueError(f"{source}: CSV is missing columns {missing}")
    return table.select(RAW_SCHEMA.names).cast(RAW_SCHEMA)


def read_daily_zip(zip_path: str) -> pa.Table:
    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise ValueError(f"{zip_path}: expected exactly one CSV inside, found {z.namelist()}")
        raw = z.read(names[0])
    return read_csv_bytes(raw, zip_path)


def read_zip_member(zip_path: str, member: str) -> pa.Table:
    with zipfile.ZipFile(zip_path) as z:
        raw = z.read(member)
    return read_csv_bytes(raw, f"{zip_path}!{member}")


def read_csv_file(csv_path: str) -> pa.Table:
    with open(csv_path, "rb") as fh:
        return read_csv_bytes(fh.read(), csv_path)


def collect_days(paths, recursive: bool) -> list[tuple[str, str, str, str, str, object]]:
    """Every trading-day file reachable from `paths`, as
    (prefix, dd, mm, yyyy, source label, loader) sorted by date then prefix.
    Daily zips, bare CSVs and the CSV members of month zips all count; any
    other file is ignored. The loader is a zero-arg callable returning the
    day's table, so nothing is read until the day is actually processed."""
    files = []
    for p in paths:
        p = os.path.expanduser(p)
        if os.path.isfile(p):
            files.append(p)
        elif os.path.isdir(p):
            if recursive:
                for root, _dirs, names in os.walk(p):
                    files.extend(os.path.join(root, f) for f in names)
            else:
                files.extend(os.path.join(p, f) for f in os.listdir(p))
        else:
            sys.exit(f"Not found: {p}")

    days = []
    for path in files:
        name = os.path.basename(path)
        m = DAY_PATTERN.match(name)
        if m:
            prefix, dd, mm, yyyy, ext = m.groups()
            loader = (lambda pth=path: read_daily_zip(pth)) if ext.lower() == "zip" \
                else (lambda pth=path: read_csv_file(pth))
            days.append((prefix.upper(), dd, mm, yyyy, path, loader))
            continue
        if MONTH_ZIP_PATTERN.match(name):
            with zipfile.ZipFile(path) as z:
                members = z.namelist()
            for member in members:
                mm_ = DAY_PATTERN.match(os.path.basename(member))
                if not mm_ or mm_.group(5).lower() != "csv":
                    continue
                prefix, dd, mm, yyyy, _ext = mm_.groups()
                days.append((prefix.upper(), dd, mm, yyyy, f"{path}!{member}",
                             lambda pth=path, mem=member: read_zip_member(pth, mem)))
    days.sort(key=lambda d: (d[3], d[2], d[1], d[0]))
    return days


def main():
    ap = argparse.ArgumentParser(description="Append downloaded daily files to the raw Sensex parquet trees.")
    ap.add_argument("sources", nargs="+",
                    help="Daily zips, month zips, CSVs and/or folders of them (spot and F&O may be mixed).")
    ap.add_argument("--recursive", action="store_true", help="Also look inside sub-folders of the given folders.")
    ap.add_argument("--preprocess", action="store_true",
                    help="After appending, run preprocess_sensex.py for the days written.")
    args = ap.parse_args()

    if not (SENSEX_SPOT_PATH and SENSEX_FNO_PATH):
        sys.exit("SENSEX_SPOT_PATH and SENSEX_FNO_PATH must be set in .env")

    days = collect_days(args.sources, args.recursive)
    if not days:
        sys.exit("No BSE_INDICES_* / BFO_BACKADJUSTED_* daily files (zip or csv, loose or in month zips) "
                 "found in the given sources.")
    print(f"{len(days)} daily files found")

    written, same, different, failed, dates = [], [], [], [], set()
    t0 = time.perf_counter()
    for i, (prefix, dd, mm, yyyy, source, load) in enumerate(days, start=1):
        root = SENSEX_SPOT_PATH if prefix == "BSE_INDICES" else SENSEX_FNO_PATH
        out_dir = os.path.join(root, yyyy, f"{MONTHS[int(mm) - 1]}_{yyyy}")
        out_path = os.path.join(out_dir, f"{prefix}_{dd}{mm}{yyyy}.parquet")
        label = f"{yyyy}-{mm}-{dd} {prefix}"

        try:
            table = load()
        except Exception as exc:
            print(f"  !! {label}: {exc}")
            failed.append(label)
            continue
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
        if i % 100 == 0:
            print(f"  ... {i}/{len(days)} ({time.perf_counter() - t0:.0f}s)")

    print(f"done in {time.perf_counter() - t0:.1f}s")
    print(f"\nWRITTEN ({len(written)} new raw files):")
    if len(written) > 40:
        print(f"  {written[0][2]}\n  ... {len(written) - 2} more ...\n  {written[-1][2]}")
    else:
        for label, rows, path in written:
            print(f"  {rows:>7} rows  {path}")
    print(f"\nALREADY PRESENT and identical to the download, left untouched ({len(same)}):")
    print("  " + (", ".join(same) or "-"))
    print(f"\nALREADY PRESENT but DIFFERENT from the download, left untouched ({len(different)}):")
    print("  " + (", ".join(different) or "-"))
    print(f"\nFAILED to read ({len(failed)}):")
    print("  " + (", ".join(failed) or "-"))

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
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()