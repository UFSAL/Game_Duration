#!/usr/bin/env python3
import argparse
import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def parse_args():
    p = argparse.ArgumentParser(
        description="Plot average game duration by season (rounded labels)."
    )
    p.add_argument("--input", required=True, help="CSV with per-game durations OR a season aggregate.")
    p.add_argument("--out", required=True, help="Output PNG path.")
    p.add_argument(
        "--round",
        type=int,
        default=0,
        dest="round_digits",
        help="Round value labels to this many decimals (default: 0).",
    )
    p.add_argument(
        "--pbp",
        default=None,
        help="Path to wnba_data_clean.csv (only needed if --input lacks a 'season' column; used to map GAME_ID -> _year).",
    )
    return p.parse_args()


def load_aggregated(df: pd.DataFrame) -> pd.DataFrame:
    """If the file is already aggregated with Season & Average Game Duration (min), normalize columns."""
    if {"Season", "Average Game Duration (min)"} <= set(df.columns):
        out = df[["Season", "Average Game Duration (min)"]].copy()
        out = out.dropna(subset=["Season", "Average Game Duration (min)"])
        out["Season"] = out["Season"].astype(int)
        out = out.sort_values("Season")
        out.rename(columns={"Average Game Duration (min)": "avg_min"}, inplace=True)
        return out
    return None


def load_per_game_with_season(df: pd.DataFrame) -> pd.DataFrame:
    """If the file has per-game durations and a 'season' column, aggregate."""
    if {"season", "duration_min"} <= set(df.columns):
        tmp = df.dropna(subset=["season", "duration_min"]).copy()
        tmp["season"] = tmp["season"].astype(int)
        return (
            tmp.groupby("season", as_index=False)["duration_min"]
            .mean()
            .rename(columns={"season": "Season", "duration_min": "avg_min"})
            .sort_values("Season")
        )
    return None


def load_per_game_needs_merge(df: pd.DataFrame, pbp_path: str) -> pd.DataFrame:
    """If the file has per-game durations + GAME_ID (but no 'season'), merge with PBP to get season."""
    if {"GAME_ID", "duration_min"} <= set(df.columns):
        if not pbp_path:
            raise SystemExit(
                "Error: input has no 'season' column; please provide --pbp pointing to wnba_data_clean.csv"
            )
        pbp = pd.read_csv(pbp_path, usecols=["GAME_ID", "_year"]).drop_duplicates("GAME_ID")
        merged = (
            df.merge(pbp, on="GAME_ID", how="left")
              .rename(columns={"_year": "season"})
        )
        missing = int(merged["season"].isna().sum())
        if missing:
            print(f"⚠️  Missing season for {missing} games — dropping those rows")
            merged = merged.dropna(subset=["season"])
        merged["season"] = merged["season"].astype(int)
        return (
            merged.groupby("season", as_index=False)["duration_min"]
            .mean()
            .rename(columns={"season": "Season", "duration_min": "avg_min"})
            .sort_values("Season")
        )
    return None


def main():
    warnings.filterwarnings("ignore", category=FutureWarning)
    args = parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise SystemExit(f"Error: input not found: {in_path}")

    print(f"Reading: {in_path}")
    df = pd.read_csv(in_path)

    # Try three schemas in order:
    season_avg = (
        load_aggregated(df)
        or load_per_game_with_season(df)
        or load_per_game_needs_merge(df, args.pbp)
    )

    if season_avg is None:
        cols = list(df.columns)
        raise SystemExit(
            "Error: Input must have either:\n"
            "  • columns ['Season', 'Average Game Duration (min)']  (pre-aggregated), OR\n"
            "  • columns ['season', 'duration_min'] (per-game), OR\n"
            "  • columns ['GAME_ID', 'duration_min'] (per-game, will merge with --pbp).\n"
            f"Found columns: {cols}"
        )

    # Plot
    seasons = season_avg["Season"].astype(int).tolist()
    values = season_avg["avg_min"].tolist()

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(seasons, values, marker="o")
    ax.set_title("Average Game Duration by Season (minutes)")
    ax.set_xlabel("Season")
    ax.set_ylabel("Minutes")
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)

    # Label each point with the rounded value
    rd = args.round_digits
    for x, y in zip(seasons, values):
        ax.annotate(
            f"{round(y, rd)}",
            (x, y),
            textcoords="offset points",
            xytext=(0, 6),
            ha="center",
            fontsize=8,
        )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"✅ Saved plot to: {out_path}")


if __name__ == "__main__":
    main()