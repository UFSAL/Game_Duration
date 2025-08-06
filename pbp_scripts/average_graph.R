library(dplyr)
library(ggplot2)

# Step 1: Identify and filter out overtime games (PERIOD == 5)
overtime_games <- all_data %>%
  filter(PERIOD == 5) %>%
  distinct(GAME_ID)

# Step 2: Filter to regular season games (3rd digit is 2), exclude overtime, and join with durations
season_chart_data <- all_data %>%
  filter(
    substr(GAME_ID, 3, 3) == "2",                     # Regular season only
    !GAME_ID %in% overtime_games$GAME_ID             # Remove overtime games
  ) %>%
  distinct(GAME_ID) %>%
  inner_join(Game_Durations, by = "GAME_ID") %>%
  filter(
    GAME_DURATION_MINUTES >= 105,                    # Trim short/long games
    GAME_DURATION_MINUTES <= 220,
    SEASON %in% sprintf("%02d", 12:24)               # Keep seasons 2012–2024
  ) %>%
  group_by(SEASON) %>%
  summarise(AVERAGE_DURATION = mean(GAME_DURATION_MINUTES, na.rm = TRUE)) %>%
  arrange(SEASON)

# Step 3: Plot
ggplot(season_chart_data, aes(x = SEASON, y = AVERAGE_DURATION, group = 1)) +
  geom_line(color = "#F5275E", size = 1) +
  geom_point(color = "black") +
  labs(
    title = "Average NBA Game Duration (Regular Season, No OT) — 2012–2024",
    x = "Season",
    y = "Average Duration (Minutes)"
  ) +
  theme_minimal()