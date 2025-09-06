#!/usr/bin/env python3
import argparse
import warnings
warnings.filterwarnings("ignore")  # keep terminal quiet

import re
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd


# -----------------------------
# Parsing and utility functions
# -----------------------------

TIME_FMT = "%I:%M %p"  # e.g., "8:24 PM"

def parse_wc_time(s: str) -> Optional[datetime]:
    """Parse WCTIMESTRING (time-of-day) into a datetime (date-less; we’ll anchor to Jan 1)."""
    if pd.isna(s):
        return None
    s = str(s).strip()
    if not s:
        return None
    # Some rows show "8:24 PM EST" etc.; strip trailing zone text.
    s = s.replace("EST", "").replace("EDT", "").replace("CST", "").replace("CDT", "")
    s = re.sub(r"\s+", " ", s).strip()
    try:
        t = datetime.strptime(s, TIME_FMT)
        # Anchor to an arbitrary date so we can do arithmetic; day=1.
        t = t.replace(year=2000, month=1, day=1)
        return t
    except Exception:
        return None

def diff_seconds(t1: Optional[datetime], t2: Optional[datetime]) -> Optional[float]:
    """Difference t2 - t1 in seconds, handling None and midnight rollover (assume +1 day at most)."""
    if t1 is None or t2 is None:
        return None
    delta = (t2 - t1).total_seconds()
    if delta < -12 * 3600:
        # Rollover (e.g., 11:58 PM -> 12:02 AM next day)
        t2 = t2 + timedelta(days=1)
        delta = (t2 - t1).total_seconds()
    return float(delta)

def safe_mean(x: pd.Series) -> Optional[float]:
    x = pd.to_numeric(x, errors="coerce").dropna()
    return float(x.mean()) if len(x) else None


# -----------------------------
# Event detectors (robust regex)
# -----------------------------

def contains_any(text_series: pd.Series, patterns: List[str]) -> pd.Series:
    """Case-insensitive substring search for any of the patterns."""
    s = text_series.fillna("").astype(str)
    pat = "|".join([re.escape(pat) for pat in patterns])
    return s.str.contains(pat, case=False, regex=True)

def flag_timeout(row_texts: List[pd.Series]) -> pd.Series:
    pats = ["timeout", "20-second timeout", "full timeout"]
    m = sum(contains_any(s, pats) for s in row_texts)
    return (m > 0)

def flag_challenge(row_texts: List[pd.Series]) -> pd.Series:
    pats = ["challenge", "coach's challenge", "coaches challenge", "coach challenge"]
    m = sum(contains_any(s, pats) for s in row_texts)
    return (m > 0)

def flag_replay(row_texts: List[pd.Series]) -> pd.Series:
    pats = ["instant replay", "replay review", "reviewed", "review"]
    m = sum(contains_any(s, pats) for s in row_texts)
    return (m > 0)

def flag_foul(row_texts: List[pd.Series]) -> pd.Series:
    # Many forms like "S.FOUL", "P.FOUL", "OFF.FOUL", "SHOOT.FOUL", etc.
    pats = ["foul"]
    m = sum(contains_any(s, pats) for s in row_texts)
    return (m > 0)

def flag_start_game(row_texts: List[pd.Series]) -> pd.Series:
    pats = ["start of 1st half", "start of 1st period", "start of first period",
            "start of game", "tip to "]  # last one helps when start marker missing
    m = sum(contains_any(s, pats) for s in row_texts)
    return (m > 0)

def flag_end_game(row_texts: List[pd.Series]) -> pd.Series:
    pats = ["end of game", "final", "end of 4th period", "end of 4th quarter"]
    m = sum(contains_any(s, pats) for s in row_texts)
    return (m > 0)

def flag_start_period(row_texts: List[pd.Series]) -> pd.Series:
    pats = ["start of", "start of 2nd period", "start of 3rd period", "start of 4th period",
            "start of 2nd half", "start of second half"]
    m = sum(contains_any(s, pats) for s in row_texts)
    return (m > 0)

def flag_end_period(row_texts: List[pd.Series]) -> pd.Series:
    pats = ["end of 1st period", "end of 2nd period", "end of 3rd period", "end of 4th period",
            "end of 1st half", "end of first half", "end of 2nd half"]
    m = sum(contains_any(s, pats) for s in row_texts)
    return (m > 0)

def flag_half_markers(row_texts: List[pd.Series]) -> Tuple[pd.Series, pd.Series]:
    """Return (end_of_first_half, start_of_second_half) boolean series."""
    end_1st_half = sum(contains_any(s, ["end of 1st half", "end of first half"]) for s in row_texts) > 0
    start_2nd_half = sum(contains_any(s, ["start of 2nd half", "start of second half"]) for s in row_texts) > 0
    return end_1st_half, start_2nd_half


# -----------------------------
# Per-game metric computation
# -----------------------------

def per_game_metrics(g: pd.DataFrame) -> Optional[pd.Series]:
    """
    Compute metrics for a single GAME_ID.
    Expects columns:
      - GAME_ID, PERIOD (int), WCTIMESTRING (e.g. '8:24 PM'), PCTIMESTRING
      - HOMEDESCRIPTION, NEUTRALDESCRIPTION, VISITORDESCRIPTION
      - _year (season)
    """
    # Fast sanity checks
    if g.empty:
        return None

    # Normalize and parse wall-clock times
    g = g.sort_values(["PERIOD", "EVENTNUM"], kind="mergesort").reset_index(drop=True)

    # Text columns
    hd = g.get("HOMEDESCRIPTION", pd.Series(dtype=object))
    nd = g.get("NEUTRALDESCRIPTION", pd.Series(dtype=object))
    vd = g.get("VISITORDESCRIPTION", pd.Series(dtype=object))
    text_cols = [hd, nd, vd]

    # Parse WCTIMESTRING
    wc = g.get("WCTIMESTRING", pd.Series(dtype=object)).apply(parse_wc_time)

    # Season/year
    season = g.get("_year").iloc[0] if "_year" in g.columns else np.nan

    # --- Game duration ---
    # Start candidate: first "start" marker; else first non-empty WCTIMESTRING
    start_mask = flag_start_game(text_cols)
    if start_mask.any():
        t_start = wc[start_mask].iloc[0]
    else:
        t_start = wc.dropna().iloc[0] if wc.notna().any() else None

    # End candidate: last "end of game" or last event time
    end_mask = flag_end_game(text_cols)
    if end_mask.any():
        t_end = wc[end_mask].iloc[-1]
    else:
        t_end = wc.dropna().iloc[-1] if wc.notna().any() else None

    game_duration_min = None
    ds = diff_seconds(t_start, t_end)
    if ds is not None and ds >= 0:
        game_duration_min = ds / 60.0

    # --- Timeouts & timeout length ---
    timeout_mask = flag_timeout(text_cols)
    timeouts = int(timeout_mask.sum())

    timeout_lengths = []
    if timeouts > 0:
        idxs = list(np.where(timeout_mask)[0])
        for i in idxs:
            t0 = wc.iloc[i]
            # length until next non-timeout event (use next event time that has a WCTIMESTRING)
            j = i + 1
            t1 = None
            while j < len(wc):
                if not timeout_mask.iloc[j] and wc.iloc[j] is not None:
                    t1 = wc.iloc[j]
                    break
                j += 1
            sec = diff_seconds(t0, t1)
            # keep only plausible TV timeout window (15s..5min)
            if sec is not None and 15 <= sec <= 300:
                timeout_lengths.append(sec)

    avg_timeout_len_sec = np.mean(timeout_lengths) if timeout_lengths else None

    # --- Challenges ---
    challenges = int(flag_challenge(text_cols).sum())

    # --- Replays & replay length ---
    replay_mask = flag_replay(text_cols)
    replays = int(replay_mask.sum())

    replay_lengths = []
    if replays > 0:
        ridxs = list(np.where(replay_mask)[0])
        for i in ridxs:
            t0 = wc.iloc[i]
            # end at first subsequent non-replay, non-empty time
            j = i + 1
            t1 = None
            while j < len(wc):
                if not replay_mask.iloc[j] and wc.iloc[j] is not None:
                    t1 = wc.iloc[j]
                    break
                j += 1
            sec = diff_seconds(t0, t1)
            # Plausible replay window (5s..180s); drop weird gaps across quarter breaks, etc.
            if sec is not None and 5 <= sec <= 180:
                replay_lengths.append(sec)

    avg_replay_len_sec = np.mean(replay_lengths) if replay_lengths else None

    # --- Halftime length (support halves OR quarters) ---
    # Prefer explicit half markers; else fall back to end of 2nd -> start of 3rd.
    # End-of-1st-half
    e1h = sum(contains_any(s, ["end of 1st half", "end of first half"]) for s in text_cols) > 0
    s2h = sum(contains_any(s, ["start of 2nd half", "start of second half"]) for s in text_cols) > 0

    halftime_len_min = None
    if e1h.any() if hasattr(e1h, "any") else bool(e1h):
        # Take last end-of-1st-half time and first start-of-2nd-half time
        e_mask = sum(contains_any(s, ["end of 1st half", "end of first half"]) for s in text_cols) > 0
        s_mask = sum(contains_any(s, ["start of 2nd half", "start of second half"]) for s in text_cols) > 0
        t_e = wc[e_mask].iloc[-1] if wc[e_mask].notna().any() else None
        t_s = wc[s_mask].iloc[0] if wc[s_mask].notna().any() else None
        sec = diff_seconds(t_e, t_s)
        if sec is not None and 120 <= sec <= 1800:  # 2–30 minutes
            halftime_len_min = sec / 60.0
    else:
        # Quarters: End of 2nd Period -> Start of 3rd Period
        e2_mask = sum(contains_any(s, ["end of 2nd period"]) for s in text_cols) > 0
        s3_mask = sum(contains_any(s, ["start of 3rd period"]) for s in text_cols) > 0
        if (e2_mask.any() if hasattr(e2_mask, "any") else bool(e2_mask)) and \
           (s3_mask.any() if hasattr(s3_mask, "any") else bool(s3_mask)):
            t_e = wc[e2_mask].iloc[-1] if wc[e2_mask].notna().any() else None
            t_s = wc[s3_mask].iloc[0] if wc[s3_mask].notna().any() else None
            sec = diff_seconds(t_e, t_s)
            if sec is not None and 120 <= sec <= 1800:
                halftime_len_min = sec / 60.0

    # --- Free throws (count events mentioning "free throw") ---
    ft_mask = sum(contains_any(s, ["free throw"]) for s in text_cols) > 0
    free_throws = int(ft_mask.sum())

    # --- Fouls (count rows mentioning "foul") ---
    foul_mask = flag_foul(text_cols)
    fouls = int(foul_mask.sum())

    # --- 4th quarter (or 2nd half) wall-clock length ---
    q4_len_min = None
    # Prefer Start of 4th Period -> End of 4th Period
    s4_mask = sum(contains_any(s, ["start of 4th period", "start of 4th quarter"]) for s in text_cols) > 0
    e4_mask = sum(contains_any(s, ["end of 4th period", "end of 4th quarter", "end of game"]) for s in text_cols) > 0
    if (s4_mask.any() if hasattr(s4_mask, "any") else bool(s4_mask)) and \
       (e4_mask.any() if hasattr(e4_mask, "any") else bool(e4_mask)):
        t_s4 = wc[s4_mask].iloc[0] if wc[s4_mask].notna().any() else None
        t_e4 = wc[e4_mask].iloc[-1] if wc[e4_mask].notna().any() else None
        sec = diff_seconds(t_s4, t_e4)
        if sec is not None and 60 <= sec <= 7200:
            q4_len_min = sec / 60.0
    else:
        # For halves-era, approximate 2nd-half length
        s2h_mask = sum(contains_any(s, ["start of 2nd half", "start of second half"]) for s in text_cols) > 0
        end_game_mask = flag_end_game(text_cols)
        if (s2h_mask.any() if hasattr(s2h_mask, "any") else bool(s2h_mask)) and \
           (end_game_mask.any() if hasattr(end_game_mask, "any") else bool(end_game_mask)):
            t_s2 = wc[s2h_mask].iloc[0] if wc[s2h_mask].notna().any() else None
            t_eg = wc[end_game_mask].iloc[-1] if wc[end_game_mask].notna().any() else None
            sec = diff_seconds(t_s2, t_eg)
            if sec is not None and 60 <= sec <= 7200:
                q4_len_min = sec / 60.0

    return pd.Series({
        "season": season,
        "game_duration_min": game_duration_min,
        "challenges": challenges,
        "timeouts": timeouts,
        "avg_timeout_len_sec": avg_timeout_len_sec,
        "replays": replays,
        "avg_replay_len_sec": avg_replay_len_sec,
        "halftime_len_min": halftime_len_min,
        "free_throws": free_throws,
        "q4_wall_minutes": q4_len_min,
        "fouls": fouls,
    })


# -----------------------------
# CLI / main
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Aggregate WNBA per-season game-duration metrics.")
    ap.add_argument("--input", required=True, help="Path to wnba_data_clean.csv (play-by-play).")
    ap.add_argument("--output", required=True, help="Path to write season_metrics.csv.")
    ap.add_argument("--show", type=int, default=0, help="Print first N season rows after write.")
    ap.add_argument("--progress-every", type=int, default=1000, help="Progress print frequency by games.")
    ap.add_argument("--duration-bounds", type=str, default="",
                    help="Optional filter like '90,180' to keep only games with duration in [min,max] minutes.")
    args = ap.parse_args()

    print(f"Reading PBP: {args.input}")
    # Read only columns we use for speed
    usecols = [
        "GAME_ID","EVENTNUM","EVENTMSGTYPE","EVENTMSGACTIONTYPE","PERIOD",
        "WCTIMESTRING","PCTIMESTRING","HOMEDESCRIPTION","NEUTRALDESCRIPTION","VISITORDESCRIPTION",
        "_year"
    ]
    df = pd.read_csv(args.input, usecols=[c for c in usecols if c in pd.read_csv(args.input, nrows=0).columns])

    # Basic cleaning
    df["PERIOD"] = pd.to_numeric(df.get("PERIOD"), errors="coerce").fillna(0).astype(int)

    # Process per game with periodic progress output
    game_ids = df["GAME_ID"].dropna().unique().tolist()
    n_games = len(game_ids)
    print(f"Found {n_games} games. Computing metrics…")

    rows = []
    for i, gid in enumerate(game_ids, 1):
        g = df.loc[df["GAME_ID"] == gid]
        try:
            row = per_game_metrics(g)
            if row is not None:
                rows.append(row)
        except Exception as e:
            print(f"  ! Skipped GAME_ID {gid} due to error: {e}")
        if args.progress_every and (i % args.progress_every == 0 or i == n_games):
            print(f"  … processed {i}/{n_games} games")

    per_game_df = pd.DataFrame(rows)

    # Optional duration filter
    if args.duration_bounds:
        try:
            lo, hi = [float(x.strip()) for x in args.duration_bounds.split(",")]
            before = len(per_game_df)
            per_game_df = per_game_df[
                per_game_df["game_duration_min"].between(lo, hi, inclusive="both")
            ]
            after = len(per_game_df)
            print(f"Filtered by duration [{lo},{hi}] minutes: kept {after} / {before} games")
        except Exception:
            print("  (ignored --duration-bounds; must be like '90,180')")

    # Season aggregation
    def _mean(series): return safe_mean(series)

    season_table = (
        per_game_df
        .groupby("season", dropna=True)
        .agg(
            **{
                "Average Game Duration (min)": ("game_duration_min", _mean),
                "Average Challenges Per Game": ("challenges", _mean),
                "Average Timeouts Per Game": ("timeouts", _mean),
                "Average Timeout Length (seconds)": ("avg_timeout_len_sec", _mean),
                "Average Replays Per Game": ("replays", _mean),
                "Average Replay Length (seconds)": ("avg_replay_len_sec", _mean),
                "Average Halftime Length (minutes)": ("halftime_len_min", _mean),
                "Free Throws Per Game": ("free_throws", _mean),
                "4th Quarter Length (minutes)": ("q4_wall_minutes", _mean),
                "Average Fouls Per Game": ("fouls", _mean),
            }
        )
        .reset_index()
        .rename(columns={"season": "Season"})
        .sort_values("Season")
    )

    # Tidy types
    season_table["Season"] = pd.to_numeric(season_table["Season"], errors="coerce")

    # Write
    season_table.to_csv(args.output, index=False)
    print(f"✅ Wrote season table to: {args.output}")

    if args.show:
        # Pretty print the first N rows
        print(season_table.head(args.show).to_string(index=False))


if __name__ == "__main__":
    main()