import pandas as pd
import time
import random
import argparse
import requests
import os
from requests.exceptions import ReadTimeout
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playbyplayv2
from nba_api.stats.static import teams

# Base Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) # Directory of the current script file
DATA_ROOT = os.path.join(SCRIPT_DIR, "..", "pbp_data") # Root directory for play-by-play data.

# API Settings
DEFAULT_TIMEOUT = 30
MIN_DELAY = 2
MAX_DELAY = 6
TEAM_DELAY_MIN = 3
TEAM_DELAY_MAX = 5

# Retry settings
MAX_ATTEMPTS = 5
INITIAL_BACKOFF = 2
MAX_BACKOFF = 64

def pbp_file_exists(season: str, team_name: str) -> bool:
    """
    Checks if the play-by-play (PBP) data file exists for a given season and team.

    Args:
        season (str): The season identifier (e.g., "2022-23").
        team_name (str): The name of the team.

    Returns:
        bool: True if the PBP data file exists, False otherwise.
    """
    _, file_path = get_pbp_data_filepath(season, team_name)
    return os.path.exists(file_path)

def get_pbp_data_filepath(season: str, team_name: str) -> tuple:
    """
    Constructs the file path for the play-by-play data CSV file.

    Args:
        team_name (str): The nickname of the team.
        season (str): The season in the format 'YYYY-YY'.

    Returns:
        tuple: A tuple containing the directory and file name.
    """
    directory = os.path.join(DATA_ROOT, season)
    file_name = f"{season}_{team_name}_pbp.csv"
    file_path = os.path.join(directory, file_name)
    return directory, file_path

def save_pbp_to_csv(data: pd.DataFrame, season: str, team_name: str) -> None:
    """
    Saves the play-by-play data to a CSV file.

    Args:
        data (pd.DataFrame): The play-by-play data to save.
        team_name (str): The nickname of the team.
        season (str): The season in the format 'YYYY-YY'.
    """
    # Checks if the DataFrame is empty before saving.
    if data.empty:
        print(f"No data available to save for {team_name} in {season}.")
        return

    # Creates the directory if it doesn't exist.
    directory, file_path = get_pbp_data_filepath(season, team_name)
    os.makedirs(directory, exist_ok=True)

    # Save the DataFrame to a CSV file.
    data.to_csv(file_path, index=False)
    print(f"Play-by-play data saved to {file_path}", flush=True)

def fetch_game_pbp(game_id, max_attempts=MAX_ATTEMPTS, initial_backoff=INITIAL_BACKOFF,
                   max_backoff=MAX_BACKOFF, timeout=DEFAULT_TIMEOUT, 
                   min_delay=MIN_DELAY, max_delay=MAX_DELAY) -> pd.DataFrame:
    """
    Attempts to fetch play-by-play data for a given game ID with retries on timeout.

    Parameters:
        game_id: The game identifier.
        max_attempts: Maximum number of retry attempts.
        initial_backoff: Initial backoff time in seconds.
        max_backoff: Maximum backoff time in seconds.
        timeout: Timeout for the API call.
        min_delay: Minimum random delay before each attempt.
        max_delay: Maximum random delay before each attempt.
    Returns:
        pd.DataFrame: The play-by-play data for the game, or None if it could not be fetched.
    """
    backoff = initial_backoff
    for attempt in range(1, max_attempts + 1):
        try:
            time.sleep(random.uniform(min_delay, max_delay)) # Random delay to avoid rate limiting.
            return playbyplayv2.PlayByPlayV2(game_id=game_id, timeout=DEFAULT_TIMEOUT).get_data_frames()[0]
        except (ConnectionError, ReadTimeout, requests.exceptions.ConnectionError):
            if attempt == max_attempts:
                print(f"Max attempts reached for game ID {game_id}. Could not fetch data.", flush=True)
                return None
            print(f"Attempt {attempt}: Timeout or connection error for game ID {game_id}. Retrying with backoff {backoff}s...", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)  # Exponential backoff
        except Exception as e:
            print(f"An unexpected error occurred for game ID {game_id}: {e}", flush=True)
            return None

def load_existing_pbp_data(season: str, team_name: str) -> pd.DataFrame:
    """
    Loads existing play-by-play data from a CSV file if it exists.

    Args:
        season (str): The season in the format 'YYYY-YY'.
        team_name (str): The nickname of the team.

    Returns:
        pd.DataFrame: The play-by-play data if the file exists, otherwise an empty DataFrame.
    """
    if pbp_file_exists(season, team_name):
        _, file_path = get_pbp_data_filepath(season, team_name)
        try:
            return pd.read_csv(file_path)
        except pd.errors.EmptyDataError:
            print(f"Warning: The file {os.path.normpath(file_path)} is empty.", flush=True)
            return pd.DataFrame()
    else:
        print(f"No existing play-by-play data found for {team_name} in {season}.", flush=True)
        return pd.DataFrame()
    
def fetch_team_game_ids(season: str, team_id: str) -> pd.Series:
    """
    Fetches all game IDs for a given team and season.
    Args:
        season (str): The season in the format 'YYYY-YY'. Ex: '2006-07'.
        team_id (str): The ID of the team.
    Returns:
        pd.Series: A pandas Series containing all game IDs for the team in the specified season.
    """
    gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id, season_nullable=season)
    return gamefinder.get_data_frames()[0].GAME_ID

def collect_games_pbp_data(game_ids: pd.Series, team_name: str = "Unknown", season: str = "Unknown") -> pd.DataFrame:
    """Collects play-by-play data for a list of game IDs."""

    game_ids = game_ids.sample(frac=1).reset_index(drop=True) # Shuffle the order.
    total_games = game_ids.shape[0] # Total number of games to process.

    # Tracking
    successful_games = []
    failed_games = []
    all_play_by_play_data = pd.DataFrame()

    # Loop through each game ID and fetch the play-by-play data.
    for count, game_id in enumerate(game_ids, start=1):
        print(f"{count}/{total_games} Fetching play-by-play data for game ID {game_id}...", flush=True)
        play_by_play_data = fetch_game_pbp(game_id)
        if play_by_play_data is None or play_by_play_data.empty:
            failed_games.append(game_id)
            print(f"Skipping game ID {game_id} due to fetch failure or empty data.", flush=True)
            continue

        successful_games.append(game_id)
        all_play_by_play_data = pd.concat([all_play_by_play_data, play_by_play_data], ignore_index=True)

    print(f"Completed {len(successful_games)} / {total_games} games for team '{team_name}' in season '{season}'.", flush=True)

    # If there are any failed games, print their IDs.
    if failed_games:
        print(f"Warning: {len(failed_games)} games failed to process.", flush=True)
        print("Failed game IDs:", flush=True)
        for game_id in failed_games:
            print(game_id, flush=True)
    
    return all_play_by_play_data

def get_team_season_pbp(season: str, team_name: str, save_to_file: bool = False, team_id: str = None) -> pd.DataFrame:
    """
    Gets a team's play-by-play data for every game of a season.
    
    Args:
        season (str): The season in the format 'YYYY-YY'. Ex: '2006-07'.
        team_name (str): The nickname of the team. Ex: 'Celtics'. Case-sensitive.
        save_to_file (bool): If True, saves the play-by-play data to a CSV file. Default is False.
        team_id (str): Optional team ID to use instead of looking it up by name.
    Returns:
        pd.DataFrame: The play-by-play data for the team in the specified season.
    """
    # Loads existing play-by-play data if it exists.
    if pbp_file_exists(season, team_name):
        return load_existing_pbp_data(season, team_name)

    # Gets the team ID from the team name if not provided.
    if team_id is None:
        team_id = teams.find_teams_by_nickname(team_name)[0]['id']
        if not team_id:
            raise ValueError(f"Team '{team_name}' not found. Please check the team name and try again.")

    # Gets all games for the given team and season as a pandas Series.
    games_ids = fetch_team_game_ids(season, team_id)
    if games_ids.empty:
        raise ValueError(f"No games found for team '{team_name}' in season '{season}'. Please check the inputs and try again.", flush=True)

    all_play_by_play_data = collect_games_pbp_data(games_ids)

    if save_to_file:
        save_pbp_to_csv(all_play_by_play_data, season, team_name)

    return all_play_by_play_data

def get_all_teams_season_pbp(season: str) -> int:
    """
    Gets play-by-play data for all NBA teams in a given season.
    Args:
        season (str): The season in the format 'YYYY-YY'. Ex: '2006-07'.
        save_files (bool): If True, saves the play-by-play data to CSV files. Default is False.
    """
    teams_info = teams.get_teams()
    teams_df = pd.DataFrame(teams_info)
    total_teams = teams_df.shape[0]
    teams_df.sample(frac=1).reset_index(drop=True, inplace=True)  # Shuffle the teams.

    # Example: Iterate over the teams DataFrame rows (not strictly necessary, but shown for clarity)
    successful_processed_teams = []
    failed_processed_teams = []
    count = 1
    for index, row in teams_df.iterrows():
        team_name = row['nickname']
        team_id = row['id']
        
        # Check if the play-by-play data file already exists
        if pbp_file_exists(season, team_name):
            print(f"Skipping. Play-by-play data already exists in season {season} for {team_name}.", flush=True)
            count += 1
            continue
        
        # Fetch play-by-play data for the team in the specified season
        time.sleep(random.uniform(TEAM_DELAY_MIN, TEAM_DELAY_MAX))
        print(f"{count}/{total_teams} Processing play-by-play data for season {season}, for {team_name}...")
        team_season_pbp = get_team_season_pbp(season, team_name, save_to_file=True, team_id=team_id)
        if not team_season_pbp.empty:
            count += 1
            successful_processed_teams.append((team_name, team_id))
        else:
            failed_processed_teams.append((team_name, team_id))
            print(f"No play-by-play data available for {team_name} in {season}.", flush=True)

    # Summary of processing results
    print(f"\nProcessing complete for all teams in season {season}.", flush=True)
    print(f"Successfully processed: {len(successful_processed_teams)} / {total_teams}", flush=True)
    if failed_processed_teams:
        print("Failed to process the following teams:")
        for team_name, team_id in failed_processed_teams:
            print(f"Team: {team_name}, ID: {team_id}", flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get play-by-play data for NBA teams in a given season.")

    # Default behavior gets all teams for the season, but can be overridden by specifying a team name.
    parser.add_argument("--season", type=str, required=True, help="Season in the format 'YYYY-YY'. Ex: '2006-07'")
    parser.add_argument("--team_name", type=str, help="Team nickname (e.g. 'Celtics'). If omitted, processes all teams.")

    args = parser.parse_args()

    try:
        if args.team_name:
            get_team_season_pbp(args.season, args.team_name, save_to_file=True)
        else:
            get_all_teams_season_pbp(args.season)
    except ValueError as e:
        print(f"Error: {e}")
        exit(1)