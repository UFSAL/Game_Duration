library(dplyr)
library(ggplot2)

# Step 1: Identify and filter out overtime games (PERIOD == 5)
overtime_games <- all_data %>%
  filter(PERIOD == 5) %>%
  distinct(GAME_ID)

# Step 2: Same as before, but order SEASON properly (96–99, then 00–24)
season_chart_data <- all_data %>%
  filter(
    substr(GAME_ID, 1, 1) == "2",
    !GAME_ID %in% overtime_games$GAME_ID
  ) %>%
  distinct(GAME_ID) %>%
  inner_join(Game_Durations, by = "GAME_ID") %>%
  filter(
    GAME_DURATION_MINUTES >= 105,
    GAME_DURATION_MINUTES <= 180
  ) %>%
  mutate(
    SEASON = factor(
      SEASON,
      levels = c(sprintf("%02d", 96:99), sprintf("%02d", 0:24))
    )
  ) %>%
  group_by(SEASON) %>%
  summarise(AVERAGE_DURATION = mean(GAME_DURATION_MINUTES, na.rm = TRUE)) %>%
  arrange(SEASON)

# Step 3: Plot
ggplot(season_chart_data, aes(x = SEASON, y = AVERAGE_DURATION, group = 1)) +
  geom_line(color = "#F5275E", linewidth = 1) +
  geom_point(color = "black") +
  labs(
    title = "Average NBA Game Duration (Regular Season, No OT) — 1996–2024",
    x = "Season",
    y = "Average Duration (Minutes)"
  ) +
  theme_minimal()