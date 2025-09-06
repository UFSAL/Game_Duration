import os
import pandas as pd

# Folder containing raw CSVs from 1997 to 2024
input_folder = "/Users/abhinavapendyala/Downloads/wnba_data_raw/"
output_folder = "/Users/abhinavapendyala/Downloads/wnba/"

# Ensure output folder exists
os.makedirs(output_folder, exist_ok=True)

# Loop through each CSV in the raw data folder
for filename in os.listdir(input_folder):
    if filename.endswith(".csv"):
        filepath = os.path.join(input_folder, filename)

        try:
            df = pd.read_csv(filepath)

            # Get start and end rows
            start_mask = df["NEUTRALDESCRIPTION"].fillna("").str.contains("Start of 1st Period", case=False, na=False)
            end_mask = df["NEUTRALDESCRIPTION"].fillna("").str.contains("End of 4th Period", case=False, na=False)

            start_rows = df[start_mask].copy()
            end_rows = df[end_mask].copy()

            # Tag rows
            start_rows["EVENT_LABEL"] = "START"
            end_rows["EVENT_LABEL"] = "END"

            # Combine and add season + filename for traceability
            filtered = pd.concat([start_rows, end_rows])
            filtered["SEASON"] = df["SEASON"].iloc[0] if "SEASON" in df.columns else filename.split("_")[1]
            filtered["SOURCE_FILE"] = filename

            # Output file path
            output_path = os.path.join(output_folder, filename.replace(".csv", "_duration_rows.csv"))
            filtered.to_csv(output_path, index=False)

            print(f"✅ Processed {filename} → {len(filtered)} rows saved to {output_path}")

        except Exception as e:
            print(f"Failed to process {filename}: {e}")

print("🏁 All done!")