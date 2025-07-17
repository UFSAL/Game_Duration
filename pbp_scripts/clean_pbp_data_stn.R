# load required packages
library(tidyverse)

# Read combined data frame from RDS file
# nba_pbp_all <- readRDS("nba_pbp_data/nba_pbp_all.rds")
# Takes about 1 minute to load on my MacBook Pro.

# How many rows are duplicated?
# nba_pbp_all_duplicates <- nba_pbp_all %>%
#   group_by(GAME_ID, EVENTNUM) %>%
#   filter(n() > 1) %>%
#   ungroup()

# What percentage of the data is duplicated?
# nba_pbp_all_duplicates_percent <- nrow(nba_pbp_all_duplicates) /
#   nrow(nba_pbp_all) # 99.879%
# This actually makes sense, as there are 2 teams per game and
# we collected data for both teams, so each play is (or should be) duplicated.

# Remove all duplicated rows
# nba_pbp_all <- nba_pbp_all %>%
#   distinct(GAME_ID, EVENTNUM, .keep_all = TRUE)
# rm(nba_pbp_all_duplicates)

# Save the deduplicated data frame as a RDS file
# saveRDS(nba_pbp_all, "nba_pbp_data/nba_pbp_deduped.rds")

# Read in the deduplicated data frame from RDS file
nba_pbp_all <- readRDS("nba_pbp_data/nba_pbp_deduped.rds")
# Should only take about 30 sections to load.

# Compute the start time and end time of each game
# Naive approach: assuming the first and last play in each game represent the start and end times
nba_pbp_all <- nba_pbp_all %>%
  group_by(GAME_ID) %>%
  mutate(
    start_time = min(WCTIMESTRING),
    end_time = max(WCTIMESTRING)
  ) %>%
  ungroup()

# Convert the start and end times to POSIXct format
nba_pbp_all <- nba_pbp_all %>%
  mutate(
    start_time = as.POSIXct(start_time, format = "%H:%M:%S"),
    end_time = as.POSIXct(end_time, format = "%H:%M:%S")
  )
# This will take awhile to run.

# Compute the game duration from the start time and end time
nba_pbp_all <- nba_pbp_all %>%
  mutate(
    game_duration = as.numeric(difftime(end_time, start_time, units = "mins"))
  )
# But this is fast.

# Compute summary statistics for game duration
nba_pbp_duration_summary <- nba_pbp_all %>%
  summarise(
    min_duration = min(game_duration, na.rm = TRUE), # 84
    max_duration = max(game_duration, na.rm = TRUE), # 1439
    mean_duration = mean(game_duration, na.rm = TRUE), # 314.03
    median_duration = median(game_duration, na.rm = TRUE) # 137
  )

# Create a mode function
get_mode <- function(x) {
  tab <- table(x)
  names(tab)[which.max(tab)]
}
get_mode(nba_pbp_all$game_duration) # 1439
# Which is also the max.  Ouch.

# Let's find all jump balls in the data
# nba_pbp_jump_balls <- nba_pbp_all %>%
#   filter(EVENTMSGTYPE == 10) %>%
#   select(GAME_ID, EVENTNUM, WCTIMESTRING, HOMEDESCRIPTION)

# How many jump balls total?
# nba_pbp_jump_balls_count <- nrow(nba_pbp_jump_balls) # 63603

# How many games in the data set?
# nba_pbp_games_count <- length(unique(nba_pbp_all$GAME_ID)) # 36429

# So how many jump balls per game?
# nba_pbp_jump_balls_per_game <- nba_pbp_jump_balls_count / nba_pbp_games_count # 1.7549
# Does this make any sense?  Let's look at the distribution of jump balls per game.
# nba_pbp_jump_balls_per_game_dist <- nba_pbp_jump_balls %>%
#   group_by(GAME_ID) %>%
#   summarise(jump_balls = n()) %>%
#   ungroup()

# Let's look at a histogram of the distribution
# hist(
#   nba_pbp_jump_balls_per_game_dist$jump_balls,
#   breaks = 30,
#   main = "Distribution of Jump Balls per Game",
#   xlab = "Number of Jump Balls",
#   ylab = "Frequency"
#)
# 1 makes sense, 2 (or possibly 3+ for OT games)
# Going to "hold that thought" on jump balls for now.

# Let's look at the distribution of game durations
nba_pbp_game_duration_dist <- nba_pbp_all %>%
  group_by(GAME_ID) %>%
  summarise(game_duration = mean(game_duration, na.rm = TRUE)) %>%
  ungroup()
# Let's look at a histogram of the distribution
hist(
  nba_pbp_game_duration_dist$game_duration,
  breaks = 30,
  main = "Distribution of Game Durations",
  xlab = "Game Duration (mins)",
  ylab = "Frequency"
)

library(lubridate)

# Function to calculate duration handling midnight crossover
calculate_duration_with_midnight <- function(start_str, end_str) {
  # Convert to POSIXct with a reference date
  start_time <- as.POSIXct(
    paste("2024-01-01", start_str),
    format = "%Y-%m-%d %H:%M:%S"
  )
  end_time <- as.POSIXct(
    paste("2024-01-01", end_str),
    format = "%Y-%m-%d %H:%M:%S"
  )

  # If end time is before start time, assume it's the next day
  if (!is.na(start_time) && !is.na(end_time) && end_time < start_time) {
    end_time <- end_time + lubridate::days(1)
  }

  # Calculate duration in minutes
  duration_mins <- as.numeric(difftime(end_time, start_time, units = "mins"))
  return(duration_mins)
}

# Fix games that cross midnight
# Create an improved function that filters out obviously bad data
calculate_duration_improved <- function(start_str, end_str) {
  # Convert to POSIXct with a reference date
  start_time <- as.POSIXct(
    paste("2024-01-01", start_str),
    format = "%Y-%m-%d %H:%M:%S"
  )
  end_time <- as.POSIXct(
    paste("2024-01-01", end_str),
    format = "%Y-%m-%d %H:%M:%S"
  )

  # Check for obviously bad data (00:00:00 to 23:59:00)
  if (!is.na(start_time) && !is.na(end_time)) {
    if (start_str == "00:00:00" && end_str == "23:59:00") {
      return(NA) # Mark as missing data
    }
  }

  # If end time is before start time, assume it's the next day (midnight crossover)
  if (!is.na(start_time) && !is.na(end_time) && end_time < start_time) {
    end_time <- end_time + lubridate::days(1)
  }

  # Calculate duration in minutes
  duration_mins <- as.numeric(difftime(end_time, start_time, units = "mins"))

  # Filter out unrealistic durations (less than 1 hour or more than 6 hours)
  if (!is.na(duration_mins) && (duration_mins < 60 || duration_mins > 220)) {
    return(NA)
  }

  return(duration_mins)
}

# Apply the improved function - replace the nba_pbp_corrected section
nba_pbp_corrected <- nba_pbp_all %>%
  group_by(GAME_ID) %>%
  summarise(
    start_time_str = min(WCTIMESTRING, na.rm = TRUE),
    end_time_str = max(WCTIMESTRING, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(
    !is.na(start_time_str) &
      !is.na(end_time_str) &
      start_time_str != "Inf" &
      end_time_str != "-Inf"
  ) %>%
  mutate(
    game_duration_corrected = mapply(
      calculate_duration_improved,
      start_time_str,
      end_time_str
    )
  )

# Look at the improved summary statistics
summary_stats <- nba_pbp_corrected %>%
  summarise(
    min_duration = min(game_duration_corrected, na.rm = TRUE),
    max_duration = max(game_duration_corrected, na.rm = TRUE),
    mean_duration = mean(game_duration_corrected, na.rm = TRUE),
    median_duration = median(game_duration_corrected, na.rm = TRUE),
    count = n(),
    valid_games = sum(!is.na(game_duration_corrected))
  )

print("Improved duration summary:")
print(summary_stats)

# Join the corrected durations back to the main dataset
nba_pbp_all <- nba_pbp_all %>%
  left_join(
    nba_pbp_corrected %>% select(GAME_ID, game_duration_corrected),
    by = "GAME_ID"
  ) %>%
  mutate(
    game_duration = ifelse(
      is.na(game_duration_corrected),
      game_duration,
      game_duration_corrected
    )
  ) %>%
  select(-game_duration_corrected)

# Save the final cleaned data frame to an RDS file
saveRDS(nba_pbp_all, "nba_pbp_data/nba_pbp_cleaned.rds")
# Save the final cleaned data frame to a .csv file
write_csv(nba_pbp_all, "nba_pbp_data/nba_pbp_cleaned.csv")
