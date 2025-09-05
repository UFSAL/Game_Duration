import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

INPUT = "/Users/abhinavapendyala/Downloads/wnba_data_raw/wnba_game_times_clean.csv"
OUTPUT_PNG = "/Users/abhinavapendyala/Downloads/wnba_data_raw/avg_duration_by_season.png"

# Load data
df = pd.read_csv(INPUT)
df["duration_min"] = pd.to_numeric(df["duration_min"], errors="coerce")

# If no season column, extract from GAME_ID
if "season" not in df.columns:
    if "GAME_ID" not in df.columns:
        raise ValueError("Need either 'season' or 'GAME_ID' column to group by season.")
    # Extract last two digits of the year from GAME_ID
    df["season"] = df["GAME_ID"].astype(str).str[3:5].astype(int)
    # Convert to full year (e.g., 97 → 1997, 05 → 2005, 21 → 2021)
    df["season"] = df["season"].apply(lambda x: 1900 + x if x >= 97 else 2000 + x)

avg_durations = df.groupby("season")["duration_min"].mean().reset_index()

# Plot
plt.figure(figsize=(10, 6))
plt.plot(avg_durations["season"], avg_durations["duration_min"], marker="o")
for i, row in avg_durations.iterrows():
    plt.text(row["season"], row["duration_min"] + 0.2, f"{row['duration_min']:.1f}", ha="center")

plt.title("Average WNBA Game Duration by Season")
plt.xlabel("Season")
plt.ylabel("Average Duration (minutes)")
plt.grid(True)
plt.tight_layout()
plt.savefig(OUTPUT_PNG)
plt.close()

print(f"📊 Saved average duration graph to: {OUTPUT_PNG}")