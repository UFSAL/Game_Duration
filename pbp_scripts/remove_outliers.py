#!/usr/bin/env python3
import argparse
import re
from collections import Counter

import pandas as pd


LOWER_BOUND = 90  # minutes
UPPER_BOUND = 180  # minutes


TIME_REGEX = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*([AP]M)\s*$", re.IGNORECASE)


def parse_clock_to_minutes(s: str):
    """Parse 'H:MM AM/PM' -> minutes since midnight. Return None if unparsable."""
    if not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    m = TIME_REGEX.match(s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    ampm = m.group(3).upper()
    hh = hh % 12  # 12 -> 0 for AM/PM math
    if ampm == "PM":
        hh += 12
    return hh * 60 + mm


def first_last_valid_times(g: pd.DataFrame):
    """Return (start_time_str, end_time_str, duration_min) from WCTIMESTRING within a game."""
    times = g["WCTIMESTRING"].astype(str)

    # Keep only non-empty, parsable strings (but preserve original first/last string forms)
    parsed = []
    for s in times:
        m = parse_clock_to_minutes(s)
        if m is not None:
            parsed.append((s, m))

    if not parsed:
        return (None, None, None)

    start_str, start_min = parsed[0]
    end_str, end_min = parsed[-1]

    duration = end_min - start_min
    # If negative, assume we crossed midnight (very rare; just to be safe)
    if duration < 0:
        duration += 24 * 60

    return (start_str, end_str, float(duration))


def guess_teams(g: pd.DataFrame):
    """Heuristic: pick two most frequent team abbreviations across PLAYER*_TEAM_ABBREVIATION."""
    cols = [
        "PLAYER1_TEAM_ABBREVIATION",
        "PLAYER2_TEAM_ABBREVIATION",
        "PLAYER3_TEAM_ABBREVIATION",
    ]
    cnt = Counter()
    for c in cols:
        if c in g.columns:
            vals = g[c].dropna().astype(str).str.strip()
            for v in vals:
                if v and v.lower() != "nan":
                    cnt[v] += 1
    if not cnt:
        return ""
    common = [t for t, _ in cnt.most_common(2)]
    if len(common) == 1:
        return common[0]
    return f"{common[0]} @ {common[1]}"


def derive_game_summary(g: pd.DataFrame):
    start_s, end_s, dur = first_last_valid_times(g)
    row_count = len(g)
    periods = int(pd.to_numeric(g.get("PERIOD", pd.Series([None])), errors="coerce").max() or 0)
    teams = guess_teams(g)
    return pd.Series(
        {
            "GAME_ID": g.name,
            "teams": teams,
            "periods": periods,
            "start_time_str": start_s,
            "end_time_str": end_s,
            "duration_min": dur,
            "row_count": row_count,
        }
    )


def main():
    ap = argparse.ArgumentParser(description="Flag and remove WNBA PBP outlier games by duration (107–177 min).")
    ap.add_argument("--input", required=True, help="Path to merged play-by-play CSV (from merge_wnba_years.py)")
    ap.add_argument("--summary-out", default="wnba_game_times.csv", help="Where to write per-game summary CSV")
    ap.add_argument("--cleaned-out", default="wnba_data_clean.csv", help="Where to write cleaned play-by-play CSV")
    ap.add_argument("--show", type=int, default=50, help="How many outlier rows to print")
    args = ap.parse_args()

    print(f"Reading PBP: {args.input}")
    df = pd.read_csv(args.input, low_memory=False)

    # Sanity check
    if "GAME_ID" not in df.columns or "WCTIMESTRING" not in df.columns:
        raise ValueError("Input file must contain 'GAME_ID' and 'WCTIMESTRING' columns.")

    # Build per-game summary
    print("Building per-game summaries…")
    summaries = df.groupby("GAME_ID", sort=False).apply(derive_game_summary).reset_index(drop=True)

    # Flag outliers using fixed bounds
    outlier_mask = (
        summaries["duration_min"].isna()
        | (summaries["duration_min"] < LOWER_BOUND)
        | (summaries["duration_min"] > UPPER_BOUND)
    )
    outliers = summaries[outlier_mask].copy()
    keepers = summaries[~outlier_mask].copy()

    total_games = len(summaries)
    num_outliers = len(outliers)
    print(f"Using fixed bounds: [{LOWER_BOUND}, {UPPER_BOUND}] minutes")
    print(f"⚠️ Outlier games detected: {num_outliers} of {total_games} total parsed games")

    if num_outliers:
        to_show = outliers.head(args.show)
        # Display a compact table
        with pd.option_context("display.max_rows", None, "display.max_colwidth", 60):
            print(to_show[["GAME_ID", "teams", "periods", "start_time_str", "end_time_str", "duration_min", "row_count"]])

    # Write summary CSV
    summaries.to_csv(args.summary_out, index=False)
    print(f"📝 Wrote per-game summary to: {args.summary_out}")

    # Write cleaned PBP (remove outlier games)
    bad_ids = set(outliers["GAME_ID"].tolist())
    cleaned = df[~df["GAME_ID"].isin(bad_ids)].copy()
    cleaned.to_csv(args.cleaned_out, index=False)
    print(f"✅ Wrote cleaned PBP to: {args.cleaned_out}  (rows kept: {len(cleaned):,})")


if __name__ == "__main__":
    main()