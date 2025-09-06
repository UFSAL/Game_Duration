library(dplyr)
library(stringr)

# Call in all End of Game times
last_rows <- all_data %>%
  group_by(GAME_ID) %>%
  slice_tail(n = 1) %>%
  ungroup()

# Call in all Start of Game (tipoff) times

# Time values of tipoffs including unconventional outliers
tipoff_times <- c("12:00", "11:59", "11:58", "10:00")

# Identify all possible tipoffs
possible_tipoffs <- all_data %>%
  filter(PERIOD == 1, PCTIMESTRING %in% tipoff_times)

# Assign a priority based on the order in tipoff_times
tipoff_rows <- possible_tipoffs %>%
  mutate(priority = match(PCTIMESTRING, tipoff_times)) %>%
  group_by(GAME_ID) %>%
  arrange(priority, desc(row_number())) %>%  # prefer higher-priority time, then last row
  slice_head(n = 1) %>%
  ungroup() %>%
  select(-priority)

# Create Game Durations table with Game ID, Season, Start, and End
Game_Durations <- tipoff_rows %>%
  mutate(SEASON = substr(GAME_ID, 2, 3)) %>%
  select(GAME_ID, SEASON, START_TIME = WCTIMESTRING) %>%
  left_join(
    last_rows %>%
      select(GAME_ID, END_TIME = WCTIMESTRING),
    by = "GAME_ID"
  ) %>%
  
  # Extract hour and minute components
  mutate(
    START_HOUR = as.numeric(str_extract(START_TIME, "^\\d+")),
    START_MIN  = as.numeric(str_extract(START_TIME, "(?<=:)\\d+")),
    END_HOUR   = as.numeric(str_extract(END_TIME, "^\\d+")),
    END_MIN    = as.numeric(str_extract(END_TIME, "(?<=:)\\d+")),
    
    # Adjust for AM/PM manually
    START_PM = str_detect(START_TIME, "PM"),
    END_PM   = str_detect(END_TIME, "PM"),
    
    # Convert 12AM/PM edge cases properly
    START_HOUR_24 = case_when(
      START_HOUR == 12 & !START_PM ~ 0,
      START_HOUR == 12 & START_PM ~ 12,
      START_PM ~ START_HOUR + 12,
      TRUE ~ START_HOUR
    ),
    END_HOUR_24 = case_when(
      END_HOUR == 12 & !END_PM ~ 0,
      END_HOUR == 12 & END_PM ~ 12,
      END_PM ~ END_HOUR + 12,
      TRUE ~ END_HOUR
    ),
    
    # Convert to total minutes
    START_TOTAL_MIN = START_HOUR_24 * 60 + START_MIN,
    END_TOTAL_MIN_RAW = END_HOUR_24 * 60 + END_MIN,
    
    # Add 24 hours if the game ended past midnight
    END_TOTAL_MIN = if_else(END_TOTAL_MIN_RAW < START_TOTAL_MIN,
                            END_TOTAL_MIN_RAW + 1440,
                            END_TOTAL_MIN_RAW),
    
    # Final duration
    GAME_DURATION_MINUTES = END_TOTAL_MIN - START_TOTAL_MIN
  ) %>%
  
  # Keep only needed columns
  select(GAME_ID, SEASON, START_TIME, END_TIME, GAME_DURATION_MINUTES)

Game_Durations <- tipoff_rows %>%
  mutate(SEASON = substr(GAME_ID, 2, 3)) %>%
  select(GAME_ID, SEASON, START_TIME = WCTIMESTRING) %>%
  left_join(
    last_rows %>% select(GAME_ID, END_TIME = WCTIMESTRING),
    by = "GAME_ID"
  ) %>%
  mutate(
    # Parse 24-hour clock strings into hours/minutes
    START_HOUR = as.numeric(str_extract(START_TIME, "^\\d+")),
    START_MIN  = as.numeric(str_extract(START_TIME, "(?<=:)\\d+")),
    END_HOUR   = as.numeric(str_extract(END_TIME, "^\\d+")),
    END_MIN    = as.numeric(str_extract(END_TIME, "(?<=:)\\d+")),
    
    # Convert to total minutes since midnight
    START_TOTAL_MIN = START_HOUR * 60 + START_MIN,
    END_TOTAL_MIN_RAW = END_HOUR * 60 + END_MIN,
    
    # Add 24h if the game ended after midnight
    END_TOTAL_MIN = if_else(END_TOTAL_MIN_RAW < START_TOTAL_MIN,
                            END_TOTAL_MIN_RAW + 1440,
                            END_TOTAL_MIN_RAW),
    
    # Final duration
    GAME_DURATION_MINUTES = END_TOTAL_MIN - START_TOTAL_MIN
  ) %>%
  select(GAME_ID, SEASON, START_TIME, END_TIME, GAME_DURATION_MINUTES)