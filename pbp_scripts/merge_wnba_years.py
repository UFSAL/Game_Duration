#!/usr/bin/env python3
"""
Merge WNBA pbp CSVs across years into one file, with robust reading.

Example:
  python merge_wnba_years.py --root /path/to/wnba_data_raw --output wnba_data_raw.csv --debug
"""

import argparse
import csv
import os
from glob import glob
from typing import Optional, Tuple, List

import pandas as pd


def preview(path: str, n: int = 400) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read(n)
    except Exception as e:
        return f"<preview failed: {e!r}>"


def robust_read_csv(path: str, debug: bool = False) -> Optional[pd.DataFrame]:
    """
    Try several encodings/separators/engines. IMPORTANT:
    Do not pass low_memory when using engine='python' (pandas limitation).
    """
    # (sep, engine, quoting) — quoting=None means "leave default"
    attempts: List[Tuple[Optional[str], str, Optional[int]]] = [
        (None, "c", None),         # let pandas sniff with C engine
        (",", "c", None),
        ("\t", "c", None),
        ("|", "c", None),
        (",", "python", None),
        ("\t", "python", None),
        ("|", "python", None),
        # Try "no-quote" mode for nasty quote chars in text
        (",", "python", csv.QUOTE_NONE),
    ]

    encodings = ["utf-8", "utf-8-sig", "latin-1"]

    for enc in encodings:
        for sep, engine, quoting in attempts:
            try:
                # Base kwargs (shared)
                kwargs = dict(
                    encoding=enc,
                    on_bad_lines="warn",  # skip/warn malformed rows
                )
                if sep is not None:
                    kwargs["sep"] = sep
                # Only set low_memory for C engine
                if engine == "c":
                    kwargs["engine"] = "c"
                    kwargs["low_memory"] = False
                else:
                    kwargs["engine"] = "python"
                    # DO NOT set low_memory for python engine
                if quoting is not None:
                    kwargs["quoting"] = quoting
                    # For QUOTE_NONE, python engine needs an escapechar
                    if engine == "python" and quoting == csv.QUOTE_NONE:
                        kwargs["escapechar"] = "\\"

                df = pd.read_csv(path, **kwargs)
                # Basic sanity: must have at least a few columns/rows
                if df.shape[1] == 0:
                    raise ValueError("Parsed zero columns")
                return df
            except Exception as e:
                if debug:
                    label = f"auto-sep" if sep is None else (
                        "comma" if sep == "," else "tab" if sep == "\t" else "pipe"
                    )
                    if quoting == csv.QUOTE_NONE:
                        label += "-noquote"
                    print(f"   parse fail via {label}: {e}")

    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Root folder containing year subfolders")
    ap.add_argument("--output", required=True, help="Output CSV path")
    ap.add_argument("--debug", action="store_true", help="Print debug previews and parse attempts")
    args = ap.parse_args()

    # Find all *_pbp.csv under root
    pattern = os.path.join(args.root, "**", "*_pbp.csv")
    files = sorted(glob(pattern, recursive=True))
    print(f"Found {len(files)} files. Reading…")

    dataframes = []
    for path in files:
        df = robust_read_csv(path, debug=args.debug)
        if df is None:
            print(f"⚠️  Skipped (unreadable after robust attempts): {path}")
            if args.debug:
                print("----- DEBUG PREVIEW (first ~400 chars) -----")
                print(preview(path))
                print("-------------------------------------------")
            continue

        # Add lightweight provenance columns (year/team from filename if possible)
        base = os.path.basename(path)
        year, team = None, None
        try:
            # e.g., wnba_2021_Aces_pbp.csv
            parts = base.split("_")
            if len(parts) >= 4 and parts[0] == "wnba":
                year = parts[1]
                team = parts[2]
        except Exception:
            pass
        if year is not None:
            df["_year"] = year
        if team is not None:
            df["_team_from_path"] = team
        df["_source_file"] = os.path.relpath(path, args.root)

        dataframes.append(df)

    if not dataframes:
        print("⚠️  Nothing to merge after reading files.")
        return

    # Union of columns, preserve order by first occurrence
    # pd.concat with sort=False keeps first-seen order for columns encountered
    merged = pd.concat(dataframes, ignore_index=True, sort=False)

    # Write out
    out_path = args.output
    merged.to_csv(out_path, index=False)
    print(f"✅ Wrote {len(merged):,} rows to {out_path}")


if __name__ == "__main__":
    main()