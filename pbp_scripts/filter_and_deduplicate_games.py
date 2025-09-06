import pandas as pd

# Path to the input file with all durations
input_file = "/Users/abhinavapendyala/Downloads/wnba/wnba_game_durations_only_all_years.csv"

# Load the CSV file
df = pd.read_csv(input_file)

# ✅ Extract season from the source_file column (e.g., wnba_2014_Team_pbp_duration_rows.csv → 2014)
df["season"] = df["source_file"].str.extract(r"wnba_(\d{4})_")[0].astype(int)

# ✅ Filter out games with durations that are unrealistic (<90 or >180 minutes)
filtered_df = df[(df["game_duration_minutes"] >= 90) & (df["game_duration_minutes"] <= 180)]

# ✅ Drop duplicates — keep only one entry per GAME_ID
deduped_df = filtered_df.drop_duplicates(subset="GAME_ID")

# ✅ Save the cleaned and deduplicated data
output_file = "/Users/abhinavapendyala/Downloads/wnba/wnba_cleaned_durations_1997_2024.csv"
deduped_df.to_csv(output_file, index=False)

# ✅ Optional: Print basic info
print(f"✅ Cleaned data saved to {output_file}")
print(f"🧮 Total unique games: {len(deduped_df)}")