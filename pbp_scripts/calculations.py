import pandas as pd
from scipy.stats import skew

# File paths
input_file = "/Users/abhinavapendyala/Downloads/wnba_data_raw/wnba_game_times.csv"
cleaned_summary_file = "/Users/abhinavapendyala/Downloads/wnba_data_raw/wnba_game_times_clean.csv"

# Load the per-game summary CSV
df = pd.read_csv(input_file)

# Filter out games outside the 90–180 minute range and drop NaN
df_clean = df[(df["duration_min"] >= 90) & (df["duration_min"] <= 180)].dropna(subset=["duration_min"])

# Save cleaned per-game summary
df_clean.to_csv(cleaned_summary_file, index=False)
print(f"✅ Cleaned per-game summary saved to: {cleaned_summary_file}")

# Calculate stats
mean_duration = df_clean["duration_min"].mean()
median_duration = df_clean["duration_min"].median()
mode_duration = df_clean["duration_min"].mode()[0]
q1 = df_clean["duration_min"].quantile(0.25)
q3 = df_clean["duration_min"].quantile(0.75)
min_duration = df_clean["duration_min"].min()
max_duration = df_clean["duration_min"].max()
std_dev = df_clean["duration_min"].std()
skewness_val = skew(df_clean["duration_min"])

# Print results
print(f"Mean: {mean_duration:.2f} minutes")
print(f"Median: {median_duration:.2f} minutes")
print(f"Mode: {mode_duration} minutes")
print(f"Lower Quartile (Q1): {q1:.2f} minutes")
print(f"Upper Quartile (Q3): {q3:.2f} minutes")
print(f"Minimum: {min_duration} minutes")
print(f"Maximum: {max_duration} minutes")
print(f"Standard Deviation: {std_dev:.2f}")
print(f"Skewness: {skewness_val:.2f}")