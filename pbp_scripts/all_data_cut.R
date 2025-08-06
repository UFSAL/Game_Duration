library(dplyr)

# Step 1: Extract valid seasons and regular season IDs
all_data_cut <- all_data %>%
  mutate(
    SEASON = substr(GAME_ID, 4, 5),           # Extract season (4th–5th digits)
    TYPE_CODE = substr(GAME_ID, 3, 3)         # Extract 3rd digit = type
  ) %>%
  filter(
    SEASON %in% sprintf("%02d", 12:24),       # Seasons 2012–2024
    TYPE_CODE == "2"                          # Regular season
  )

# Step 2: Remove any game with overtime (PERIOD >= 5)
overtime_games <- all_data_cut %>%
  filter(PERIOD >= 5) %>%
  distinct(GAME_ID)

all_data_cut <- all_data_cut %>%
  filter(!GAME_ID %in% overtime_games$GAME_ID)