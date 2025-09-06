library(dplyr)

# Step 1: Extract valid seasons and regular season IDs
all_data_cut <- all_data %>%
  mutate(
    SEASON = substr(GAME_ID, 2, 3),           # Extract season (4th–5th digits)
    TYPE_CODE = substr(GAME_ID, 1, 1)         # Extract 3rd digit = type
  ) %>%
  filter(
    TYPE_CODE == "2"                          # Regular season
  )

# Step 2: Remove any game with overtime (PERIOD >= 5)
overtime_games <- all_data_cut %>%
  filter(PERIOD >= 5) %>%
  distinct(GAME_ID)

all_data_cut <- all_data_cut %>%
  filter(!GAME_ID %in% overtime_games$GAME_ID)
all_data_cut <- all_data_cut %>%
  semi_join(
    Game_Durations %>%
      filter(GAME_DURATION_MINUTES >= 105, GAME_DURATION_MINUTES <= 180),
    by = "GAME_ID"
  )
all_data_cut <- all_data_cut %>%
  distinct()