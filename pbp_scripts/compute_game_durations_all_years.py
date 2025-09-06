import pandas as pd
from datetime import datetime

input_file = "/Users/abhinavapendyala/Downloads/wnba/wnba_game_durations_all_years.csv"
output_file = "/Users/abhinavapendyala/Downloads/wnba/wnba_game_durations_only_all_years.csv"

# Load data
df = pd.read_csv(input_file)

# Parse time and calculate duration in minutes
def parse_time(t):
    try:
        return datetime.strptime(t.strip(), "%I:%M %p")
    except:
        return None

durations = []
for _, row in df.iterrows():
    start = parse_time(row["start_time"])
    end = parse_time(row["end_time"])
    if start and end:
        # Handle games crossing midnight
        if end < start:
            end = end.replace(day=start.day + 1)
        duration = (end - start).total_seconds() / 60
        durations.append(duration)
    else:
        durations.append(None)

df["game_duration_minutes"] = durations
df.to_csv(output_file, index=False)
print(f"✅ Saved game durations with times to {output_file}")