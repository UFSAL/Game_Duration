library(tidyverse)

# Set the top-level folder path
base_path <- "C:/Users/YOUR_USERNAME/Downloads/nba_pbp_data"

# Recursively list all CSV files
csv_files <- list.files(path = base_path, pattern = "\\.csv$", recursive = TRUE, full.names = TRUE)

# Combine all CSVs into one tibble (force all columns to character to avoid type mismatches)
all_data <- csv_files %>%
  map_dfr(~ read_csv(.x, col_types = cols(.default = "c")))