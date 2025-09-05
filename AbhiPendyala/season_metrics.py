#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Season metrics from WNBA/NBA play-by-play.

Inputs: a cleaned play-by-play CSV with at least:
- GAME_ID
- EVENTNUM (for ordering)
- PERIOD (int-like)
- WCTIMESTRING (like "8:24 PM")
- PCTIMESTRING (game clock)
- HOMEDESCRIPTION / NEUTRALDESCRIPTION / VISITORDESCRIPTION
- EVENTMSGTYPE (numeric code; 3=FT, 6=Foul in NBA/WNBA feeds)

Outputs: season_metrics.csv with columns:
Season, Average Game Duration (min), Average Challenges Per Game, Average Timeouts Per Game,
Average Timeout Length (seconds), Average Replays Per Game, Average Replay Length (seconds),
Average Halftime Length (minutes), Free Throws Per Game, 4th Quarter Length (minutes), Average Fouls Per Game
"""

import argparse
import warnings
from typing import Optional, Tuple

import numpy as np
import pandas as pd


# ---- Quiet noisy warnings ----------------------------------------------------
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)


# ---- Helpers -----------------------------------------------------------------
def infer_season_from_game_id(game_id) -> float:
    """
    Many GAME_IDs look like '1029700010' -> '97' maps to 1997; '1042400212' -> '24' maps to 2024.
    We read characters at positions [3:5].
    Returns float for pandas compatibility with NaN.
    """
    try:
        s = str(int(game_id))
    except Exception:
        return np.nan
    # Defensive: require the common "10..." prefix and at least 5 chars
    if len(s) < 5 or not s.startswith("10"):
        return np.nan
    yy = int(s[3:5])
    year = 1900 + yy if yy >= 90 else 2000 + yy
    return float(year)


def _to_seconds_from_wctime(series: pd.Series) -> pd.Series:
    """
    Parse 'WCTIMESTRING' like '8:24 PM' to wall-clock seconds since day start.
    Then make it monotonically non-decreasing across the game by adding +86400 when it rolls over.
    """
    # Parse to datetime.time anchored to an arbitrary date
    dt = pd.to_datetime(series, format="%I:%M %p", errors="coerce")
    sec = (dt.dt.hour.fillna(0).astype(int) * 3600 +
           dt.dt.minute.fillna(0).astype(int) * 60)

    # Enforce monotonic with day rollover
    out = sec.copy().astype("float")
    day_add = 0.0
    prev = None
    for i, v in enumerate(out):
        if np.isnan(v):
            out.iloc[i] = np.nan if prev is None else prev
            continue
        vv = float(v) + day_add
        if prev is not None and vv < prev:  # crossed midnight
            day_add += 86400.0
            vv = float(v) + day_add
        out.iloc[i] = vv
        prev = vv
    return out


def _contains_substring(*cols: pd.Series, substr: str) -> pd.Series:
    """
    Case-insensitive substring search across multiple text columns.
    Uses regex=False to avoid unintended grouping warnings.
    """
    masks = []
    for c in cols:
        if c is None:
            continue
        s = c.fillna("").astype(str).str.contains(substr, case=False, regex=False, na=False)
        masks.append(s)
    return np.logical_or.reduce(masks) if masks else pd.Series(False, index=cols[0].index if cols else None)


def _first_time(g: pd.DataFrame, desc_text: str) -> Optional[float]:
    """Return the first wall-clock time (seconds) for a given exact desc snippet (case-insensitive) in NEUTRALDESCRIPTION."""
    nd = g["NEUTRALDESCRIPTION"] if "NEUTRALDESCRIPTION" in g.columns else pd.Series("", index=g.index)
    mask = nd.fillna("").str.lower().str.contains(desc_text.lower(), regex=False)
    if not mask.any():
        return None
    return float(g.loc[mask, "_wall_sec"].iloc[0])


def _period_boundary(g: pd.DataFrame, period_num: int, kind: str) -> Optional[float]:
    """
    Get wall-clock time for 'Start of Nth Period' or 'End of Nth Period' from NEUTRALDESCRIPTION.
    kind in {"start", "end"}.
    """
    nd = g["NEUTRALDESCRIPTION"] if "NEUTRALDESCRIPTION" in g.columns else pd.Series("", index=g.index)
    # Build simple substring; avoid regex quirks
    target = f"{'Start' if kind=='start' else 'End'} of {period_num}st Period"
    # Also handle 2nd / 3rd / 4th suffixes quickly:
    suffix = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th"}.get(period_num, f"{period_num}th")
    target = f"{'Start' if kind=='start' else 'End'} of {suffix} Period"
    mask = nd.fillna("").str.contains(target, case=False, regex=False)
    if not mask.any():
        return None
    return float(g.loc[mask, "_wall_sec"].iloc[0])


def _segments_total_length(g: pd.DataFrame, start_mask: pd.Series, exclude_mask: Optional[pd.Series] = None) -> float:
    """
    For each True in start_mask (e.g., timeout or replay events), measure length
    = wall_time[next_row] - wall_time[this_row], where 'next_row' is the next event
      whose row does NOT match the same start_mask (and also not excluded by exclude_mask).
    Sum positive lengths. Returns seconds.
    """
    idx = g.index.to_list()
    wall = g["_wall_sec"].values
    sm = start_mask.values
    ex = exclude_mask.values if exclude_mask is not None else None

    total = 0.0
    n = len(idx)
    for i in range(n):
        if not sm[i]:
            continue
        # find next row that is not the same start class (and not excluded if ex given)
        j = i + 1
        while j < n and (sm[j] or (ex is not None and ex[j])):
            j += 1
        if j < n:
            delta = float(wall[j]) - float(wall[i])
            if delta > 0:
                total += delta
    return total


def _safe_mean(s: pd.Series) -> float:
    s = pd.to_numeric(s, errors="coerce")
    if s.notna().any():
        return float(s.mean())
    return np.nan


# ---- Per-game metrics --------------------------------------------------------
def per_game_metrics(g: pd.DataFrame) -> dict:
    """
    Compute all requested per-game metrics from one game's PBP.
    """
    g = g.sort_values("EVENTNUM", kind="mergesort").copy()

    # Convenience columns
    hd = g["HOMEDESCRIPTION"] if "HOMEDESCRIPTION" in g.columns else pd.Series("", index=g.index)
    vd = g["VISITORDESCRIPTION"] if "VISITORDESCRIPTION" in g.columns else pd.Series("", index=g.index)
    nd = g["NEUTRALDESCRIPTION"] if "NEUTRALDESCRIPTION" in g.columns else pd.Series("", index=g.index)

    # Build monotonically increasing wall clock seconds
    g["_wall_sec"] = _to_seconds_from_wctime(g["WCTIMESTRING"] if "WCTIMESTRING" in g.columns else pd.Series("", index=g.index))

    # --- Game Duration (start = Start of 1st Period; end = End of 4th Period or End of Game or last event) ---
    start = _period_boundary(g, 1, "start")
    if start is None:
        # Fallback: first non-NaN wall time
        start = float(g["_wall_sec"].dropna().iloc[0]) if g["_wall_sec"].notna().any() else np.nan

    # Prefer "End of 4th Period", else "End of Game", else last event time
    end = _period_boundary(g, 4, "end")
    if end is None:
        # try neutraldesc "End of Game"
        nd_lower = nd.fillna("").str.lower()
        end_mask = nd_lower.str.contains("end of game", regex=False)
        if end_mask.any():
            end = float(g.loc[end_mask, "_wall_sec"].iloc[-1])
        elif g["_wall_sec"].notna().any():
            end = float(g["_wall_sec"].dropna().iloc[-1])
        else:
            end = np.nan

    game_dur_min = (end - start) / 60.0 if (not np.isnan(start) and not np.isnan(end) and end >= start) else np.nan

    # --- Challenges ---
    challenge_mask = _contains_substring(hd, vd, nd, substr="challenge")
    challenges = int(challenge_mask.sum())

    # --- Timeouts ---
    timeout_mask = _contains_substring(hd, vd, nd, substr="timeout")
    timeouts = int(timeout_mask.sum())
    avg_timeout_len_sec = np.nan
    if timeouts > 0:
        ttl = _segments_total_length(g, start_mask=timeout_mask)
        avg_timeout_len_sec = ttl / timeouts if timeouts > 0 else np.nan

    # --- Replays ---
    replay_mask = _contains_substring(hd, vd, nd, substr="instant replay")
    replays = int(replay_mask.sum())
    avg_replay_len_sec = np.nan
    if replays > 0:
        # If there are consecutive replay rows, treat them as one contiguous block:
        # We exclude subsequent replay rows when searching for the "next non-replay" event.
        ttl_rep = _segments_total_length(g, start_mask=replay_mask, exclude_mask=replay_mask)
        avg_replay_len_sec = ttl_rep / replays if replays > 0 else np.nan

    # --- Halftime Length (minutes) ---
    # Compute all inter-period gaps among first few periods and take the largest (halftime is the longest planned break).
    gaps_sec = []
    # Find all "End of N" and "Start of N+1" pairs that exist
    for p in [1, 2, 3]:
        e = _period_boundary(g, p, "end")
        s = _period_boundary(g, p + 1, "start")
        if e is not None and s is not None and s > e:
            gaps_sec.append(s - e)
    halftime_len_min = (max(gaps_sec) / 60.0) if gaps_sec else np.nan

    # --- Free Throws ---
    if "EVENTMSGTYPE" in g.columns:
        ft = int((g["EVENTMSGTYPE"] == 3).sum())
    else:
        ft = int(_contains_substring(hd, vd, nd, substr="free throw").sum())

    # --- 4th Quarter Length (minutes) ---
    q4_start = _period_boundary(g, 4, "start")
    q4_end = _period_boundary(g, 4, "end")
    if q4_start is not None and q4_end is not None and q4_end > q4_start:
        q4_len_min = (q4_end - q4_start) / 60.0
    else:
        q4_len_min = np.nan

    # --- Fouls ---
    if "EVENTMSGTYPE" in g.columns:
        fouls = int((g["EVENTMSGTYPE"] == 6).sum())
    else:
        fouls = int(_contains_substring(hd, vd, nd, substr="foul").sum())

    return {
        "GAME_ID": g["GAME_ID"].iloc[0],
        "season": g["season"].iloc[0],
        "game_duration_min": game_dur_min,
        "challenges": challenges,
        "timeouts": timeouts,
        "avg_timeout_len_sec": avg_timeout_len_sec,
        "replays": replays,
        "avg_replay_len_sec": avg_replay_len_sec,
        "halftime_len_min": halftime_len_min,
        "free_throws": ft,
        "q4_len_min": q4_len_min,
        "fouls": fouls,
    }


# ---- CLI / Main --------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Compute season-level metrics from play-by-play.")
    ap.add_argument("--input", required=True, help="Path to cleaned PBP CSV (wnba_data_clean.csv).")
    ap.add_argument("--output", required=True, help="Path to write season_metrics.csv.")
    ap.add_argument("--show", type=int, default=0, help="Print first N season rows.")
    ap.add_argument("--progress-every", type=int, default=1000, help="How often to print per-game progress.")
    args = ap.parse_args()

    print(f"Reading PBP: {args.input}")
    df = pd.read_csv(args.input, low_memory=False)

    # Normalize/ensure season
    if "season" not in df.columns and "SEASON" in df.columns:
        df = df.rename(columns={"SEASON": "season"})

    if "season" not in df.columns:
        df["season"] = df["GAME_ID"].apply(infer_season_from_game_id)
    else:
        miss = df["season"].isna()
        if miss.any():
            fill = df.loc[miss, "GAME_ID"].apply(infer_season_from_game_id)
            df.loc[miss, "season"] = fill

    n_nan = int(df["season"].isna().sum())
    n_distinct = df["season"].nunique(dropna=True)
    print(f"Seasons found: {n_distinct} distinct; missing season rows: {n_nan}")

    # Basic selects & dtypes
    keep_cols = [
        "GAME_ID", "EVENTNUM", "PERIOD", "WCTIMESTRING",
        "HOMEDESCRIPTION", "NEUTRALDESCRIPTION", "VISITORDESCRIPTION",
        "EVENTMSGTYPE", "season",
    ]
    existing = [c for c in keep_cols if c in df.columns]
    df = df[existing].copy()
    # Clean period
    if "PERIOD" in df.columns:
        df["PERIOD"] = pd.to_numeric(df["PERIOD"], errors="coerce")

    # Iterate game-by-game for robustness & progress logs
    games = []
    gb = df.groupby("GAME_ID", sort=False)
    total = gb.ngroups
    print(f"Found {total} games. Computing metrics…")
    for i, (_, g) in enumerate(gb, start=1):
        try:
            row = per_game_metrics(g)
            games.append(row)
        except Exception as e:
            # Don't crash the whole job; skip pathological games
            # but still keep a note in console
            print(f"  ! Skipped GAME_ID {g['GAME_ID'].iloc[0]} due to error: {e}")
        if args.progress_every and i % args.progress_every == 0:
            print(f"  … processed {i}/{total} games")

    per_game_df = pd.DataFrame(games)

    # Aggregate to season
    season_table = (
        per_game_df
        .groupby("season", dropna=True)
        .agg(
            **{
                "Average Game Duration (min)": ("game_duration_min", _safe_mean),
                "Average Challenges Per Game": ("challenges", _safe_mean),
                "Average Timeouts Per Game": ("timeouts", _safe_mean),
                "Average Timeout Length (seconds)": ("avg_timeout_len_sec", _safe_mean),
                "Average Replays Per Game": ("replays", _safe_mean),
                "Average Replay Length (seconds)": ("avg_replay_len_sec", _safe_mean),
                "Average Halftime Length (minutes)": ("halftime_len_min", _safe_mean),
                "Free Throws Per Game": ("free_throws", _safe_mean),
                "4th Quarter Length (minutes)": ("q4_len_min", _safe_mean),
                "Average Fouls Per Game": ("fouls", _safe_mean),
            }
        )
        .reset_index()
        .rename(columns={"season": "Season"})
        .sort_values("Season", kind="mergesort")
    )

    # Write & optional preview
    season_table.to_csv(args.output, index=False)
    print(f"✅ Wrote season table to: {args.output}")

    if args.show and args.show > 0:
        # Pretty print first N rows
        show_n = min(args.show, len(season_table))
        if show_n > 0:
            print(season_table.head(show_n).to_string(index=False))


if __name__ == "__main__":
    main()