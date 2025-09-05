import os
import pandas as pd

input_folder = "/Users/abhinavapendyala/Downloads/wnba/"
output_file = "/Users/abhinavapendyala/Downloads/wnba/wnba_game_durations_all_years.csv"

all_data = []

# Process all *_duration_rows.csv files
for filename in os.listdir(input_folder):
    if filename.endswith("_duration_rows.csv"):
        filepath = os.path.join(input_folder, filename)
        df = pd.read_csv(filepath)

        # Ensure required columns exist
        if {"GAME_ID", "WCTIMESTRING", "EVENT_LABEL"}.issubset(df.columns):
            games = df.groupby("GAME_ID")
            for game_id, group in games:
                start_row = group[group["EVENT_LABEL"] == "START"]
                end_row = group[group["EVENT_LABEL"] == "END"]

                if not start_row.empty and not end_row.empty:
                    start_time = start_row.iloc[0]["WCTIMESTRING"]
                    end_time = end_row.iloc[-1]["WCTIMESTRING"]
                    season = start_row.iloc[0].get("SEASON", "")
                    source = start_row.iloc[0].get("SOURCE_FILE", "")

                    all_data.append({
                        "GAME_ID": game_id,
                        "start_time": start_time,
                        "end_time": end_time,
                        "season": season,
                        "source_file": source
                    })

# Save to CSV
df_out = pd.DataFrame(all_data)
df_out.to_csv(output_file, index=False)
print(f"✅ Saved paired game durations to {output_file}")