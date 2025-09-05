import os
import pandas as pd
import re
import unittest

"""
Author: Justin

These functions assume all rows of the pbp_data are in the 12-hour standard format with AM/PM and are consistently 
in order.

NOTE: This function is not intended to be a CLI script, and will likely be moved elsewhere in the future.
      It is written here initially for reference and testing by the team.
"""
def find_many_events_in_short_time_12_hour_standard(pbp_game_df: pd.DataFrame, 
                                                    minutes_threshold: int = 0, 
                                                    event_num_diff: int = 10) -> pd.DataFrame:
    """
    Find unusually large number of events in a short time period in the play-by-play data of a game.

    This function assumes that the play-by-play data is sorted by time and that the time format is consistently
    in the 12-hour standard format with AM/PM.

    Args:
        pbp_game_df (pd.DataFrame): DataFrame containing play-by-play data for a single game.
        minutes_threshold (int): How many minutes difference to consider a short time period. 0 means no change in minutes.
        event_num_diff (int): The number of consecutive events to find. Defaults to 10.

    Returns:
        pd.DataFrame: DataFrame containing events in which event_num_diff events occur within the specified minutes threshold.
    """

    many_events = []
    for i in range(len(pbp_game_df) - event_num_diff + 1):
        start_event = pbp_game_df.iloc[i]
        end_event = pbp_game_df.iloc[i + event_num_diff - 1]
        start_datetime = pd.to_datetime(start_event['WCTIMESTRING'], format='%I:%M %p')
        end_datetime = pd.to_datetime(end_event['WCTIMESTRING'], format='%I:%M %p')

        # Handle midnight rollover
        if end_datetime < start_datetime:
            end_datetime += pd.Timedelta(days=1)

        # Calculate the difference in minutes
        min_diff = (end_datetime - start_datetime)

        # The window of times is too short if the difference is less than or equal to the threshold
        if min_diff < pd.Timedelta(minutes=0):
            raise ValueError("The time difference cannot be negative. Please check the input data.")

        # If the difference is less than or equal to the threshold, we consider it a short time period
        # and we add the triggering event to the list.
        if min_diff <= pd.Timedelta(minutes=minutes_threshold):
            many_event = {
                'GAME_ID': start_event['GAME_ID'],
                'EVENTNUM_START': start_event['EVENTNUM'],
                'EVENTNUM_END': end_event['EVENTNUM'],
                'EVENT_COUNT': event_num_diff,
                'START_TIME': start_event['WCTIMESTRING'],
                'END_TIME': end_event['WCTIMESTRING'],
            }
            many_events.append(many_event)
    return pd.DataFrame(many_events)


def find_long_events_12_hour_standard(pbp_game_df: pd.DataFrame, minutes_threshold: int = 20, event_num_diff: int = 1) -> pd.DataFrame:
    """
    Find long events in the play-by-play data of a game.
    
    A long event is an event where the time difference between it and the next event is greater than
    the specified threshold in minutes.

    This function assumes that the play-by-play data is sorted by time and that the time format is consistently
    in the 12-hour standard format with AM/PM, and that jumps may include the end of the quarter or halftime.

    Args:
        pbp_game_df (pd.DataFrame): DataFrame containing play-by-play data for a single game.
        minutes_threshold (int): The threshold in minutes to consider an event as long.
        event_num_diff (int): The number of consecutive long events to find. Defaults to 1.

    Returns:
        pd.DataFrame: DataFrame containing the long events found.
    """

    # Use two pointers to find ranges that exceed the time threshold
    long_events = []
    for i in range(len(pbp_game_df) - event_num_diff):
        # Get the time strings
        start_event = pbp_game_df.iloc[i]
        end_event = pbp_game_df.iloc[i + event_num_diff]
        start_datetime = pd.to_datetime(start_event['WCTIMESTRING'], format='%I:%M %p')
        end_datetime = pd.to_datetime(end_event['WCTIMESTRING'], format='%I:%M %p')

        # Handle midnight rollover
        if end_datetime < start_datetime:
            end_datetime += pd.Timedelta(days=1)

        min_diff = (end_datetime - start_datetime)

        # Calculate the difference in minutes
        min_diff = (end_datetime - start_datetime)
        if min_diff > pd.Timedelta(minutes=minutes_threshold):
            # Setup tuple format
            long_event = {
                'GAME_ID': start_event['GAME_ID'],
                'EVENTNUM': start_event['EVENTNUM'],
                'START_TIME': start_event['WCTIMESTRING'],
                'END_TIME': end_event['WCTIMESTRING'],
                'MIN_DIFF': min_diff,
            }
            # Append to the list of long events
            long_events.append(long_event)

    return pd.DataFrame(long_events)


class TestFindLongEvents(unittest.TestCase):
    """
    Tests long events with a threshold of 2 minutes.
    """
    am_data_no_jump = {
        'GAME_ID': [1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5],
        'WCTIMESTRING': ['11:30 AM', '11:31 AM', '11:32 AM', '11:33 AM', '11:34 AM'],
    }

    am_data_with_jump = {
        'GAME_ID': [1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5],
        'WCTIMESTRING': ['11:30 AM', '11:31 AM', '11:35 AM', '11:36 AM', '11:37 AM'],
    }

    pm_data_no_jump = {
        'GAME_ID': [1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5],
        'WCTIMESTRING': ['12:30 PM', '12:31 PM', '12:32 PM', '12:33 PM', '12:34 PM'],
    }

    pm_data_with_jump = {
        'GAME_ID': [1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5],
        'WCTIMESTRING': ['12:30 PM', '12:31 PM', '12:35 PM', '12:36 PM', '12:37 PM'],
    }

    turnover_noon_data_no_jump = {
        'GAME_ID': [1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5],
        'WCTIMESTRING': ['11:59 AM', '12:00 PM', '12:01 PM', '12:02 PM', '12:03 PM'],
    }

    turnover_noon_data_with_jump = {
        'GAME_ID': [1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5],
        'WCTIMESTRING': ['11:59 AM', '12:00 PM', '12:03 PM', '12:04 PM', '12:05 PM'],
    }

    turnover_midnight_data_no_jump = {
        'GAME_ID': [1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5],
        'WCTIMESTRING': ['11:59 PM', '12:00 AM', '12:01 AM', '12:02 AM', '12:03 AM'],
    }

    turnover_midnight_data_with_jump = {
        'GAME_ID': [1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5],
        'WCTIMESTRING': ['11:59 PM', '12:00 AM', '12:04 AM', '12:05 AM', '12:06 AM'],
    }

    def test_find_long_events_am_no_jump(self):
        df = pd.DataFrame(self.am_data_no_jump)
        result = find_long_events_12_hour_standard(df, 2)
        self.assertTrue(result.empty, "Expected no long events in AM data without jumps.")

    def test_find_long_events_am_with_jump(self):
        df = pd.DataFrame(self.am_data_with_jump)
        result = find_long_events_12_hour_standard(df, 2)
        self.assertFalse(result.empty, "Expected long events in AM data with jumps.")

    def test_find_long_events_pm_no_jump(self):
        df = pd.DataFrame(self.pm_data_no_jump)
        result = find_long_events_12_hour_standard(df, 2)
        self.assertTrue(result.empty, "Expected no long events in PM data without jumps.")

    def test_find_long_events_pm_with_jump(self):
        df = pd.DataFrame(self.pm_data_with_jump)
        result = find_long_events_12_hour_standard(df, 2)
        self.assertFalse(result.empty, "Expected long events in PM data with jumps.")

    def test_find_long_events_turnover_noon_no_jump(self):
        df = pd.DataFrame(self.turnover_noon_data_no_jump)
        result = find_long_events_12_hour_standard(df, 2)
        self.assertTrue(result.empty, "Expected no long events in turnover noon data without jumps.")

    def test_find_long_events_turnover_noon_with_jump(self):
        df = pd.DataFrame(self.turnover_noon_data_with_jump)
        result = find_long_events_12_hour_standard(df, 2)
        self.assertFalse(result.empty, "Expected long events in turnover noon data with jumps.")

    def test_find_long_events_turnover_midnight_no_jump(self):
        df = pd.DataFrame(self.turnover_midnight_data_no_jump)
        result = find_long_events_12_hour_standard(df, 2)
        self.assertTrue(result.empty, "Expected no long events in turnover midnight data without jumps.")

    def test_find_long_events_turnover_midnight_with_jump(self):
        df = pd.DataFrame(self.turnover_midnight_data_with_jump)
        result = find_long_events_12_hour_standard(df, 2)
        self.assertFalse(result.empty, "Expected long events in turnover midnight data with jumps.")

class TestFindManyEvents(unittest.TestCase):
    """
    Tests for finding many events in play-by-play data with event_num_diff of 4
    """
    am_data_no_dupes = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['11:30 AM', '11:31 AM', '11:32 AM', '11:33 AM', '11:34 AM',
                        '11:35 AM', '11:36 AM', '11:37 AM', '11:38 AM', '11:39 AM'],
    }

    am_data_some_dupes = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['11:30 AM', '11:30 AM', '11:30 AM', '11:30 AM', '11:33 AM',
                        '11:35 AM', '11:36 AM', '11:37 AM', '11:38 AM', '11:39 AM'],
    }

    am_data_most_dupes = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': [ '11:30 AM', '11:30 AM', '11:30 AM', '11:30 AM', '11:30 AM',
                        '11:35 AM', '11:36 AM', '11:37 AM', '11:38 AM', '11:39 AM'],
    }

    pm_data_no_dupes = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['12:30 PM', '12:31 PM', '12:32 PM', '12:33 PM', '12:34 PM',
                        '12:35 PM', '12:36 PM', '12:37 PM', '12:38 PM', '12:39 PM'],
    }

    pm_data_some_dupes = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['12:30 PM', '12:30 PM', '12:30 PM', '12:30 PM', '12:34 PM',
                        '12:35 PM', '12:36 PM', '12:37 PM', '12:38 PM', '12:39 PM'],
    }

    pm_data_most_dupes = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['12:30 PM', '12:30 PM', '12:30 PM', '12:30 PM', '12:33 PM',
                        '12:35 PM', '12:36 PM', '12:37 PM', '12:38 PM', '12:39 PM'],
    }

    turnover_noon_data_no_dupes = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['11:59 AM', '12:00 PM', '12:01 PM', '12:02 PM', '12:03 PM', 
                         '12:04 PM', '12:05 PM', '12:06 PM', '12:07 PM', '12:08 PM'],
    }

    turnover_noon_data_some_dupes_before = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['11:59 AM', '11:59 AM', '11:59 AM', '11:59 AM', '12:01 PM',
                        '12:02 PM', '12:03 PM', '12:04 PM', '12:05 PM', '12:06 PM'],
    }

    turnover_noon_data_some_dupes_after = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['11:58 AM', '12:00 PM', '12:00 PM', '12:00 PM', '12:00 PM',
                        '12:02 PM', '12:03 PM', '12:04 PM', '12:05 PM', '12:06 PM'],
    }

    turnover_noon_data_most_dupes_before = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['11:59 AM', '11:59 AM', '11:59 AM', '11:59 AM', '11:59 AM',
                        '11:59 AM', '12:00 PM', '12:01 PM', '12:02 PM', '12:03 PM'],
    }

    turnover_noon_data_most_dupes_after = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['11:59 AM', '12:00 PM', '12:00 PM', '12:00 PM', '12:00 PM',
                        '12:00 PM', '12:01 PM', '12:02 PM', '12:03 PM', '12:04 PM'],
    }

    turnover_midnight_data_no_dupes = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['11:59 PM', '12:00 AM', '12:01 AM', '12:02 AM', '12:03 AM',
                        '12:04 AM', '12:05 AM', '12:06 AM', '12:07 AM', '12:08 AM'],
    }

    turnover_midnight_data_some_dupes_before = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['11:59 PM', '11:59 PM', '11:59 PM', '11:59 PM', '12:02 AM',
                        '12:03 AM', '12:04 AM', '12:05 AM', '12:06 AM', '12:07 AM'],
    }

    turnover_midnight_data_some_dupes_after = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['11:58 PM', '12:00 AM', '12:00 AM', '12:00 AM', '12:00 AM',
                        '12:02 AM', '12:03 AM', '12:04 AM', '12:05 AM', '12:06 AM'],
    }

    turnover_midnight_data_most_dupes_before = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['11:59 PM', '11:59 PM', '11:59 PM', '11:59 PM', '11:59 PM',
                        '11:59 PM', '12:00 AM', '12:01 AM', '12:02 AM', '12:03 AM'],
    }

    turnover_midnight_data_most_dupes_after = {
        'GAME_ID': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        'EVENTNUM': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'WCTIMESTRING': ['11:59 PM', '12:00 AM', '12:00 AM', '12:00 AM', '12:00 AM',
                        '12:00 AM', '12:01 AM', '12:02 AM', '12:03 AM', '12:04 AM'],
    }

    def test_find_many_events_am_no_dupes(self):
        df = pd.DataFrame(self.am_data_no_dupes)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertTrue(result.empty, "Expected no many events in AM data without duplicates.")

    def test_find_many_events_am_some_dupes(self):
        df = pd.DataFrame(self.am_data_some_dupes)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertTrue(result.empty, "Expected many events in AM data with some duplicates.")

    def test_find_many_events_am_most_dupes(self):
        df = pd.DataFrame(self.am_data_most_dupes)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertFalse(result.empty, "Expected many events in AM data with most duplicates.") 

    def test_find_many_events_pm_no_dupes(self):
        df = pd.DataFrame(self.pm_data_no_dupes)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertTrue(result.empty, "Expected no many events in PM data without duplicates.")

    def test_find_many_events_pm_some_dupes(self):
        df = pd.DataFrame(self.pm_data_some_dupes)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertTrue(result.empty, "Expected many events in PM data with some duplicates.")

    def test_find_many_events_pm_most_dupes(self):
        df = pd.DataFrame(self.pm_data_most_dupes)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertTrue(result.empty, "Expected many events in PM data with most duplicates.")

    def test_find_many_events_turnover_noon_no_dupes(self):
        df = pd.DataFrame(self.turnover_noon_data_no_dupes)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertTrue(result.empty, "Expected no many events in turnover data without duplicates.")

    def test_find_many_events_turnover_noon_some_dupes_before(self):
        df = pd.DataFrame(self.turnover_noon_data_some_dupes_before)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertTrue(result.empty, "Expected no many events in turnover data with some duplicates before.")
    
    def test_find_many_events_turnover_noon_some_dupes_after(self):
        df = pd.DataFrame(self.turnover_noon_data_some_dupes_after)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertTrue(result.empty, "Expected no many events in turnover data with some duplicates after.")

    def test_find_many_events_turnover_noon_most_dupes_before(self):
        df = pd.DataFrame(self.turnover_noon_data_most_dupes_before)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertFalse(result.empty, "Expected no many events in turnover data with most duplicates before.")

    def test_find_many_events_turnover_noon_most_dupes_after(self):
        df = pd.DataFrame(self.turnover_noon_data_most_dupes_after)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertFalse(result.empty, "Expected no many events in turnover data with most duplicates after.")
    
    def test_find_many_events_turnover_midnight_no_dupes(self):
        df = pd.DataFrame(self.turnover_midnight_data_no_dupes)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertTrue(result.empty, "Expected no many events in turnover data with no duplicates.")

    def test_find_many_events_turnover_midnight_some_dupes_before(self):
        df = pd.DataFrame(self.turnover_midnight_data_some_dupes_before)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertTrue(result.empty, "Expected no many events in turnover data with some duplicates before.")

    def test_find_many_events_turnover_midnight_some_dupes_after(self):
        df = pd.DataFrame(self.turnover_midnight_data_some_dupes_after)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertTrue(result.empty, "Expected no many events in turnover data with some duplicates after.")

    def test_find_many_events_turnover_midnight_most_dupes_before(self):
        df = pd.DataFrame(self.turnover_midnight_data_most_dupes_before)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertFalse(result.empty, "Expected many events in turnover data with most duplicates before.")

    def test_find_many_events_turnover_midnight_most_dupes_after(self):
        df = pd.DataFrame(self.turnover_midnight_data_most_dupes_after)
        result = find_many_events_in_short_time_12_hour_standard(df, 1, 5)
        self.assertFalse(result.empty, "Expected many events in turnover data with most duplicates after.")

if __name__ == "__main__":
    # Call this script with 
    # `python -m unittest pbp_scripts.find_event_densities` 
    # to run the tests.
    unittest.main()