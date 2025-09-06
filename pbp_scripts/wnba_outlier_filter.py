import argparse, os, sys, re, pandas as pd, numpy as np

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=True)
    ap.add_argument("--outdir", default="./out")
    ap.add_argument("--delimiter", default=None)
    return ap.parse_args()

def coerce_datetime(s): return pd.to_datetime(s, errors="coerce", utc=True)
def coerce_clock_to_seconds(cs):
    try:
        if pd.isna(cs): return np.nan
        m, s = map(int, str(cs).split(":"))
        return m*60 + s
    except: return np.nan
def parse_score(ss):
    try:
        if pd.isna(ss): return (np.nan, np.nan, np.nan)
        nums = re.findall(r"\d+", str(ss))
        if len(nums) == 2:
            a, b = int(nums[0]), int(nums[1])
            return (a, b, max(a, b))
        return (np.nan, np.nan, np.nan)
    except: return (np.nan, np.nan, np.nan)

def main():
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    try: df = pd.read_csv(args.infile, sep=args.delimiter)
    except Exception as e: sys.exit(f"Failed to read CSV: {e}")

    cols_lower = {c.lower(): c for c in df.columns}
    def col(*opts): return next((cols_lower[o.lower()] for o in opts if o.lower() in cols_lower), None)
    game_col = col("GAME_ID"); 
    if game_col is None: sys.exit("No GAME_ID col")
    period_col = col("PERIOD","QTR"); clock_col = col("PCTIMESTRING","clock")
    score_col = col("SCORE"); evt_type_col = col("EVENTMSGTYPE")
    home_desc, away_desc = col("HOMEDESCRIPTION"), col("VISITORDESCRIPTION")
    start_time_col, end_time_col = col("start_time"), col("end_time")
    wall_clock_cols = list(filter(None,[col("EVENT_DATETIME"), col("EVENT_DT"), col("WCTIMESTRING")]))

    work = df.copy()
    work["__PER"] = pd.to_numeric(work[period_col], errors="coerce") if period_col else np.nan
    work["__CLOCK_S"] = work[clock_col].apply(coerce_clock_to_seconds) if clock_col else np.nan
    if score_col:
        parsed = work[score_col].apply(parse_score)
        work["__SCORE_MAX"] = parsed.apply(lambda x: x[2])
    else: work["__SCORE_MAX"] = np.nan
    text_cols = [c for c in [home_desc, away_desc] if c]
    def is_to(r):
        if evt_type_col and not pd.isna(r.get(evt_type_col)):
            try:
                if int(r[evt_type_col]) == 9: return True
            except: pass
        return any("timeout" in str(r.get(tc,"")).lower() for tc in text_cols)
    work["__IS_TIMEOUT"] = work.apply(is_to, axis=1)

    work["__START_DT"], work["__END_DT"] = pd.NaT, pd.NaT
    if start_time_col: work["__START_DT"] = coerce_datetime(work[start_time_col])
    if end_time_col: work["__END_DT"] = coerce_datetime(work[end_time_col])
    work["__WALL_DT"] = pd.NaT
    if work["__START_DT"].isna().all() and wall_clock_cols:
        wall_dt = pd.Series(pd.NaT, index=work.index, dtype="datetime64[ns, UTC]")
        for wc in wall_clock_cols: wall_dt = wall_dt.combine_first(coerce_datetime(work[wc]))
        work["__WALL_DT"] = wall_dt

    def summarize_game(g):
        sdt = g["__START_DT"].min() if g["__START_DT"].notna().any() else g["__WALL_DT"].min()
        edt = g["__END_DT"].max() if g["__END_DT"].notna().any() else g["__WALL_DT"].max()
        dur = (edt - sdt).total_seconds()/60 if pd.notna(sdt) and pd.notna(edt) else np.nan
        n_per = pd.to_numeric(g["__PER"], errors="coerce").dropna().astype(int).max() if g["__PER"].notna().any() else np.nan
        clock_reg = 0
        if g["__PER"].notna().any() and g["__CLOCK_S"].notna().any():
            for _, gp in g.groupby("__PER"):
                prev = None
                for v in gp["__CLOCK_S"]:
                    if np.isnan(v): continue
                    if prev is not None and v > prev: clock_reg += 1
                    prev = v
        score_drop = 0
        if g["__SCORE_MAX"].notna().any():
            prev = -1
            for v in g["__SCORE_MAX"]:
                if np.isnan(v): continue
                if v < prev: score_drop += 1
                prev = max(prev, v)
        return pd.Series({
            "game_duration_minutes": dur,
            "periods_observed_max": n_per,
            "clock_regressions": clock_reg,
            "score_drops": score_drop,
            "timeouts_total": int(g["__IS_TIMEOUT"].sum())
        })

    game_flags = work.groupby(game_col).apply(summarize_game).reset_index()
    dur = game_flags["game_duration_minutes"].dropna()
    if not dur.empty:
        Q1, Q3 = dur.quantile(0.25), dur.quantile(0.75)
        IQR = Q3 - Q1
        low_thr, high_thr = Q1 - 1.5*IQR, Q3 + 1.5*IQR
        game_flags["duration_outlier_low"] = game_flags["game_duration_minutes"] < low_thr
        game_flags["duration_outlier_high"] = game_flags["game_duration_minutes"] > high_thr
    game_flags["periods_outlier"] = (game_flags["periods_observed_max"] < 4) | (game_flags["periods_observed_max"] > 6)
    game_flags["timeouts_outlier"] = game_flags["timeouts_total"] > 8
    game_flags["clock_anomaly"] = game_flags["clock_regressions"] > 0
    game_flags["score_anomaly"] = game_flags["score_drops"] > 0
    game_flags["has_any_anomaly"] = game_flags[["duration_outlier_low","duration_outlier_high",
        "periods_outlier","timeouts_outlier","clock_anomaly","score_anomaly"]].any(axis=1)

    event_anoms = work[work[game_col].isin(game_flags.loc[game_flags["has_any_anomaly"], game_col])]
    game_flags.to_csv(os.path.join(args.outdir, "game_flags.csv"), index=False)
    event_anoms.to_csv(os.path.join(args.outdir, "event_anomalies.csv"), index=False)

if __name__ == "__main__": main()