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

def fetch_game_pbp(game_id, max_attempts=5):
    """Attempts to fetch play-by-play data for a given game ID with retries on timeout."""
    backoff = 2  # Initial backoff time in seconds
    for attempt in range(1, max_attempts + 1):
        try:
            # Introduce a random delay to avoid hitting the API too quickly
            time.sleep(random.uniform(2, 7))
            return playbyplayv2.PlayByPlayV2(game_id=game_id, timeout=60).get_data_frames()[0]
        except (ConnectionError, ReadTimeout, requests.exceptions.ConnectionError):
            if attempt == max_attempts:
                print(f"Max attempts reached for game ID {game_id}. Could not fetch data.", flush=True)
                return None
            print(f"Attempt {attempt}: Timeout or connection error for game ID {game_id}. Retrying with backoff {backoff}s...", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 256)
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
    Fetches all games for a given team in a specified season.

    Args:
        season (str): The season in the format 'YYYY-YY'.
        team_id (str): The ID of the team.

    Returns:
        pd.Series: A pandas Series containing game IDs for the specified team and season.
    """
    gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id, season_nullable=season)
    return gamefinder.get_data_frames()[0].GAME_ID

def collect_games_pbp_data(games: pd.Series, team_name: str = "Unknown", season: str = "Unknown") -> pd.DataFrame:
    game_ids = set(games) # Convert to a set for fast random selection
    total_games = games.shape[0]

    # Initialize an empty DataFrame to store all play-by-play data
    all_play_by_play_data = pd.DataFrame()
    count = 1
    while game_ids:
        game_id = random.choice(list(game_ids))  # Randomly select a game ID
        print(f"{count}/{total_games} Fetching play-by-play data for game ID {game_id}...", flush=True)
        play_by_play_data = fetch_game_pbp(game_id)
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

    print(f"Completed {count - 1} / {total_games} games for team '{team_name}' in season '{season}'.", flush=True)

    if game_ids:
        print(f"Warning: {len(game_ids)} games were not processed due to fetch failures or empty data.", flush=True)
        print(f"Unprocessed game IDs: {', '.join(game_ids)}", flush=True)
    
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
    games = fetch_team_game_ids(season, team_id)
    if games.empty:
        raise ValueError(f"No games found for team '{team_name}' in season '{season}'. Please check the inputs and try again.", flush=True)

    all_play_by_play_data = collect_games_pbp_data(games)

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
    teams_dict = teams.get_teams()
    teams_set = set((team['nickname'], team['id']) for team in teams_dict)

    count = 1
    for name, id in teams_set:
        # Check if the play-by-play data file already exists
        if pbp_file_exists(season, name):
            print(f"Skipping. Play-by-play data already exists in season {season} for {id}.", flush=True)
            count += 1
            continue

        # Fetch play-by-play data for the team in the specified season
        time.sleep(random.uniform(10, 20)) # Random delay between teams to avoid rate limiting
        print(f"{count}/{len(teams_set)} Processing play-by-play data for season {season}, for {name}...")
        team_season_pbp = get_team_season_pbp(season, name, save_to_file=True, team_id=id)
        if not team_season_pbp.empty:
            count += 1
        else:
            print(f"No play-by-play data available for {name} in {season}.")
    
    print(f"Completed processing {count - 1} teams for season {season}.", flush=True)
    return count - 1

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