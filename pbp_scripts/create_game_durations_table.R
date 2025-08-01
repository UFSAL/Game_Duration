library(dplyr)
library(stringr)

#Call in all End of Game times

last_rows <- all_data %>%
group_by(GAME_ID) %>%
slice_tail(n = 1) %>%
ungroup()
  
#Call in all Start of Game (tipoff) times
  
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
  
#Create Game Durations table with Game ID, Season, Start, and End
  
  Game_Durations <- tipoff_rows %>%
    mutate(SEASON = substr(GAME_ID, 4, 5)) %>%
    select(GAME_ID, SEASON, START_TIME = WCTIMESTRING) %>%
    left_join(
      last_rows %>%
        select(GAME_ID, END_TIME = WCTIMESTRING),
      by = "GAME_ID"
    )

  
#Calculate Game Durations
  
  Game_Durations <- tipoff_rows %>%
    mutate(SEASON = substr(GAME_ID, 4, 5)) %>%
    select(GAME_ID, SEASON, START_TIME = WCTIMESTRING) %>%
    left_join(
      last_rows %>%
        select(GAME_ID, END_TIME = WCTIMESTRING),
      by = "GAME_ID"
    ) %>%
    
    # Extract hour/minute from time strings (even if they say 25:00PM, etc.)
    mutate(
      START_HOUR = as.numeric(str_extract(START_TIME, "^\\d+")),
      START_MIN  = as.numeric(str_extract(START_TIME, "(?<=:)\\d+")),
      END_HOUR   = as.numeric(str_extract(END_TIME, "^\\d+")),
      END_MIN    = as.numeric(str_extract(END_TIME, "(?<=:)\\d+")),
      
      START_TOTAL_MIN = START_HOUR * 60 + START_MIN,
      END_TOTAL_MIN_RAW = END_HOUR * 60 + END_MIN,
      
      END_TOTAL_MIN = if_else(END_TOTAL_MIN_RAW < START_TOTAL_MIN,
                              END_TOTAL_MIN_RAW + 1440,
                              END_TOTAL_MIN_RAW),
      
      GAME_DURATION_MINUTES = END_TOTAL_MIN - START_TOTAL_MIN
    ) %>%
    
    # Keep the final columns clean
    select(GAME_ID, SEASON, START_TIME, END_TIME, GAME_DURATION_MINUTES)
