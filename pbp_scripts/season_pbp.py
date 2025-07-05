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

##### CONSTANTS #####
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) # Directory of the current script file
DATA_ROOT = os.path.join(SCRIPT_DIR, "..", "pbp_data") # Root directory for play-by-play data.
#####################

def get_pbp_filepath(season: str, team_name: str) -> tuple:
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

def save_pbp_to_csv(data: pd.DataFrame, season: str, team_name: str):
    """
    Saves the play-by-play data to a CSV file.

    Args:
        data (pd.DataFrame): The play-by-play data to save.
        team_name (str): The nickname of the team.
        season (str): The season in the format 'YYYY-YY'.
    """
    if data.empty:
        print(f"No data available to save for {team_name} in {season}.")
        return

    # Creates the directory if it doesn't exist.
    directory, file_path = get_pbp_filepath(season, team_name)[0]
    os.makedirs(directory, exist_ok=True)

    # Save the DataFrame to a CSV file.
    data.to_csv(file_path, index=False)
    print(f"Play-by-play data saved to {file_path}", flush=True)

def graceful_fetch_pbp(game_id):
    """ Attempts to fetch play-by-play data for a given game ID with retries on timeout."""
    backoff = 2  # Initial backoff time in seconds
    attempt = 0
    while True:
        time.sleep(random.uniform(2, 7))
        try:
            return playbyplayv2.PlayByPlayV2(game_id=game_id, timeout=60).get_data_frames()[0]
        except (ConnectionError, ReadTimeout, requests.exceptions.ConnectionError):
            attempt += 1
            print(f"Attempt {attempt}: Timeout or connection error for game ID {game_id}. Retrying with backoff {backoff}s...", flush=True)
            time.sleep(backoff)  # Exponential backoff
            backoff = min(backoff * 2, 256)  # Double the backoff time for the next attempt
        except Exception as e:
            print(f"An unexpected error occurred for game ID {game_id}: {e}", flush=True)

def pbp_file_exists(season: str, team_name: str) -> bool:
    """
    Checks if the play-by-play CSV file exists for the given season and team.

    Args:
        season (str): The season in the format 'YYYY-YY'.
        team_name (str): The nickname of the team.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    _, file_path = get_pbp_filepath(season, team_name)
    return os.path.exists(file_path)

def get_team_season_pbp(season: str, team_name: str, save_to_file: bool = False) -> pd.DataFrame:
    """
    Gets a team's play-by-play data for an entire season.
    
    Args:
        season (str): The season in the format 'YYYY-YY'. Ex: '2006-07'.
        team_name (str): The nickname of the team. Ex: 'Celtics'. Case-sensitive.
        save_to_file (bool): If True, saves the play-by-play data to a CSV file. Default is False.
    Returns:
        pd.DataFrame: The play-by-play data for the team in the specified season.
    """
    # Check if the play-by-play data file already exists
    if pbp_file_exists(season, team_name):
        _, file_path = get_pbp_filepath(season, team_name)
        print(f"Play-by-play data already exists in season {season} for {team_name}.", flush=True)
        try:
            return pd.read_csv(get_pbp_filepath(season, team_name)[1])
        except pd.errors.EmptyDataError:
            print(f"Warning: The file {os.path.normpath(file_path)} is empty.", flush=True)
            return pd.DataFrame() # Empty dataframe

    # Gets the team id of the given team name
    teams_dict = teams.get_teams()
    team_ids = [team for team in teams_dict if team['nickname'] == team_name][0]
    team_id = team_ids['id']
    if not team_id:
        raise ValueError(f"Team '{team_name}' not found. Please check the team name and try again.")

    # Gets all games for the given team and season as a pandas Series.
    gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id, season_nullable=season)
    games = gamefinder.get_data_frames()[0].GAME_ID
    if games.empty:
        raise ValueError(f"No games found for team '{team_name}' in season '{season}'. Please check the inputs and try again.", flush=True)

    all_play_by_play_data = pd.DataFrame()
    count = 1
    game_ids = set(games)  # Convert to a set for random selection

    while game_ids:
        game_id = random.choice(list(game_ids))  # Randomly select a game ID
        print(f"{count}/{len(games)} Fetching play-by-play data for game ID {game_id}...", flush=True)
        play_by_play_data = graceful_fetch_pbp(game_id)
        if play_by_play_data is None:
            print(f"Skipping game ID {game_id} due to fetch failure.")
            game_ids.remove(game_id)  # Remove the failed game ID
            continue
        elif play_by_play_data.empty:
            print(f"Game ID {game_id} contains no play-by-play data. This might be a preseason game.", flush=True)
            game_ids.remove(game_id)  # Remove the empty game ID
            continue

        count += 1
        all_play_by_play_data = pd.concat([all_play_by_play_data, play_by_play_data], ignore_index=True)
        game_ids.remove(game_id)  # Remove the successfully processed game ID from the set

    print(f"Completed {count - 1} / {len(games)} games for team '{team_name}' in season '{season}'.", flush=True)

    if game_ids:
        print(f"Warning: {len(game_ids)} games were not processed due to fetch failures or empty data.", flush=True)
        print(f"Unprocessed game IDs: {', '.join(game_ids)}", flush=True)

    if save_to_file:
        save_pbp_to_csv(all_play_by_play_data, season, team_name)

    return all_play_by_play_data

def get_all_teams_season_pbp(season: str, save_files=False):
    """
    Gets play-by-play data for all NBA teams in a given season.
    Args:
        season (str): The season in the format 'YYYY-YY'. Ex: '2006-07'.
        save_files (bool): If True, saves the play-by-play data to CSV files. Default is False.
    """
    teams_dict = teams.get_teams()
    team_names = [team['nickname'] for team in teams_dict] 
    teams_set = set(team_names)
    count = 1

    for team_name in teams_set:
        # Check if the play-by-play data file already exists
        if pbp_file_exists(season, team_name):
            print(f"Skipping. Play-by-play data already exists in season {season} for {team_name}.", flush=True)
            count += 1
            continue

        # Fetch play-by-play data for the team in the specified season
        time.sleep(random.uniform(10, 20)) # Random delay between teams to avoid rate limiting
        print(f"{count}/{len(teams_set)} Processing play-by-play data for {team_name} in season {season}...")
        team_season_pbp = get_team_season_pbp(season, team_name, save_to_file=True)
        if not team_season_pbp.empty:
            count += 1
        else:
            print(f"No play-by-play data available for {team_name} in {season}.")

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
            get_all_teams_season_pbp(args.season, save_files=True)
    except ValueError as e:
        print(f"Error: {e}")
        exit(1)