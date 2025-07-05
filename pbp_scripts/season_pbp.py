import pandas as pd
import time
import random
import argparse
import requests
import os
import pickle
import signal
from requests.exceptions import ReadTimeout
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playbyplayv2
from nba_api.stats.static import teams

# Base Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) # Directory of the current script file
DATA_ROOT = os.path.join(SCRIPT_DIR, "..", "pbp_data") # Root directory for play-by-play data.
CHECKPOINTS_ROOT = os.path.join(SCRIPT_DIR, "..", "checkpoints") # Directory for saving progress checkpoints.

# API Settings
DEFAULT_TIMEOUT = 30
MIN_DELAY = 2
MAX_DELAY = 6
TEAM_DELAY_MIN = 3
TEAM_DELAY_MAX = 5

# Retry settings
MAX_ATTEMPTS = 3

# State for session
CURRENT_PBP_DATA = pd.DataFrame()  
CURRENT_SEASON = ""
CURRENT_TEAM_NAME = ""
CURRENT_FAILED_GAMES = []
CURRENT_EMPTY_GAMES = []

def signal_handler(sig, frame):
    """Handle Ctrl+C by saving checkpoint before exiting."""
    print("\nCtrl+C detected! Saving checkpoint before exiting...", flush=True)
    if not CURRENT_PBP_DATA.empty and CURRENT_SEASON and CURRENT_TEAM_NAME:
        save_checkpoint(CURRENT_PBP_DATA, CURRENT_SEASON, CURRENT_TEAM_NAME, CURRENT_FAILED_GAMES, CURRENT_EMPTY_GAMES)
        print(f"Checkpoint saved for {CURRENT_TEAM_NAME} in season {CURRENT_SEASON}.", flush=True)
    else:
        print("No data to save in checkpoint.", flush=True)
    print("Exiting program.", flush=True)
    exit(0)

def completed_pbp_file_exists(season: str, team_name: str) -> bool:
    """
    Checks if the play-by-play (PBP) data file exists for a given season and team.

    Args:
        season (str): The season identifier (e.g., "2022-23").
        team_name (str): The name of the team.

    Returns:
        bool: True if the PBP data file exists, False otherwise.
    """
    _, file_path = get_completed_pbp_data_filepath(season, team_name)
    return os.path.exists(file_path)

def checkpoint_file_exists(season: str, team_name: str) -> bool:
    """
    Checks if the progress checkpoint file exists for a given season and team.

    Args:
        season (str): The season identifier (e.g., "2022-23").
        team_name (str): The name of the team.

    Returns:
        bool: True if the checkpoint file exists, False otherwise.
    """
    _, file_path = get_checkpoint_filepath(season, team_name)
    return os.path.exists(file_path)

def get_completed_pbp_data_filepath(season: str, team_name: str) -> tuple:
    """
    Constructs the file path for the play-by-play data CSV file.

    Args:
        team_name (str): The nickname of the team.
        season (str): The season in the format 'YYYY-YY'.

    Returns:
        tuple: A tuple containing the directory and file path.
    """
    directory = os.path.join(DATA_ROOT, season)
    file_name = f"{season}_{team_name}_pbp.csv"
    file_path = os.path.join(directory, file_name)
    return directory, file_path

def get_checkpoint_filepath(season: str, team_name: str) -> tuple:
    """
    Constructs the file path for the progress checkpoint file.

    Args:
        team_name (str): The nickname of the team.
        season (str): The season in the format 'YYYY-YY'.

    Returns:
        tuple: A tuple containing the directory and file name.
    """
    directory = os.path.join(CHECKPOINTS_ROOT, season)
    file_name = f"{season}_{team_name}_pbp_checkpoint.pickle"
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
    directory, file_path = get_completed_pbp_data_filepath(season, team_name)
    os.makedirs(directory, exist_ok=True)

    # Save the DataFrame to a CSV file.
    data.to_csv(file_path, index=False)
    print(f"Play-by-play data saved to {os.path.normpath(file_path)}", flush=True)

def save_checkpoint(current_play_by_play_data: pd.DataFrame, season: str, team_name: str, failed_games: list = [], empty_games: list = []) -> None:
    """
    Saves the currently processed game IDs and failed games to a file for later resumption.
    """
    # Creates the directory if it doesn't exist.
    directory, file_path = get_checkpoint_filepath(season, team_name)
    os.makedirs(directory, exist_ok=True)

    # Save both DataFrame and failed_games list as a tuple using pickle
    checkpoint_data = {
        "play_by_play_data": current_play_by_play_data,
        "failed_games": failed_games,
        "empty_games": empty_games
    }
    with open(file_path, "wb") as f:
        pickle.dump(checkpoint_data, f)

def fetch_game_pbp(game_id, max_attempts=MAX_ATTEMPTS, min_delay=MIN_DELAY, max_delay=MAX_DELAY) -> pd.DataFrame:
    """
    Fetches play-by-play data for a given game ID with retry logic.
    Args:
        game_id (str): The ID of the game to fetch play-by-play data for.
        max_attempts (int): Maximum number of attempts to fetch data.
        initial_backoff (int): Initial backoff time in seconds for retries.
        max_backoff (int): Maximum backoff time in seconds for retries.
        timeout (int): Timeout for the request in seconds.
        min_delay (int): Minimum delay between requests in seconds.
        max_delay (int): Maximum delay between requests in seconds.
    Returns:
        pd.DataFrame: The play-by-play data for the game, or None if it could not be fetched.
    """
    # backoff = initial_backoff
    for attempt in range(1, max_attempts + 1):
        try:
            time.sleep(random.uniform(min_delay, max_delay)) # Random delay to avoid rate limiting.
            return playbyplayv2.PlayByPlayV2(game_id=game_id, timeout=DEFAULT_TIMEOUT).get_data_frames()[0]
        except (ConnectionError, ReadTimeout, requests.exceptions.ConnectionError):
            if attempt == max_attempts:
                print(f"Max attempts reached for game ID {game_id}. Could not fetch data.", flush=True)
                return None
            print(f"Attempt {attempt}: Timeout or connection error for game ID {game_id}.", flush=True)
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
    if completed_pbp_file_exists(season, team_name):
        _, file_path = get_completed_pbp_data_filepath(season, team_name)
        try:
            return pd.read_csv(file_path)
        except pd.errors.EmptyDataError:
            print(f"Warning: The file {os.path.normpath(file_path)} is empty.", flush=True)
            return pd.DataFrame()
    else:
        print(f"No existing play-by-play data found for {team_name} in {season}.", flush=True)
        return pd.DataFrame()
    
def load_checkpoint_data(season: str, team_name: str) -> tuple:
    """
    Loads the checkpoint data from a pickle file if it exists.
    Args:
        season (str): The season in the format 'YYYY-YY'.
        team_name (str): The nickname of the team.
    Returns:
        pd.DataFrame: The checkpoint data containing processed game IDs and failed games.
    """
    if checkpoint_file_exists(season, team_name):
        _, file_path = get_checkpoint_filepath(season, team_name)
        
        # Try to load the checkpoint data from the pickle file.
        try:
            with open(file_path, "rb") as f:
                checkpoint_data = pickle.load(f)
                if isinstance(checkpoint_data, dict) and 'play_by_play_data' in checkpoint_data:
                    return checkpoint_data['play_by_play_data'], checkpoint_data.get('failed_games', []), checkpoint_data.get('empty_games', [])
                else:
                    print(f"Warning: Checkpoint data format is incorrect in {file_path}.", flush=True)
                    return pd.DataFrame(), [], []
        except (FileNotFoundError, pickle.UnpicklingError) as e:
            print(f"Warning: Could not load checkpoint data from {file_path}. Error: {e}", flush=True)
            return pd.DataFrame(), [], []
    else:
        print(f"No checkpoint data found for {team_name} in season {season}. Starting fresh.", flush=True)
        return pd.DataFrame(), [], []
    
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
    """
    Collects play-by-play data for a list of game IDs, handling retries and checkpoints.
    Args:
        game_ids (pd.Series): A pandas Series containing game IDs to fetch play-by-play data for.
        team_name (str): The nickname of the team. Default is "Unknown".
        season (str): The season in the format 'YYYY-YY'. Default is "Unknown".
    Returns:
        pd.DataFrame: A DataFrame containing the collected play-by-play data for all games.
    """
    global CURRENT_PBP_DATA, CURRENT_SEASON, CURRENT_TEAM_NAME, CURRENT_FAILED_GAMES

    # Set the state
    CURRENT_SEASON = season
    CURRENT_TEAM_NAME = team_name

    # Load checkpoint if it exists.
    checkpoint_data, failed_games, empty_games = load_checkpoint_data(season, team_name)

    # Tracking
    successful_games = []
    failed_games = []
    empty_games = []
    all_play_by_play_data = pd.DataFrame()

    # If checkpoint data exists, we resume from there.
    if not checkpoint_data.empty:
        # Load processed game IDs from the checkpoint.
        successful_games = checkpoint_data['GAME_ID'].unique().tolist()
        failed_games = checkpoint_data.get('failed_games', [])
        empty_games = checkpoint_data.get('empty_games', [])

        # Remove already processed games from the game_ids Series.
        game_ids = game_ids[~game_ids.isin(successful_games + empty_games)]  # Remove already processed games.
        all_play_by_play_data = checkpoint_data

        print(f"Resuming from checkpoint. {len(successful_games)} game(s) already processed, {len(failed_games)} failed games, {len(empty_games)} empty games.", flush=True)

    game_ids = game_ids.sample(frac=1).reset_index(drop=True) # Shuffle the order.
    total_games = len(game_ids) + len(successful_games) + len(empty_games)  # Total games to process including already processed ones.

    # Update state
    CURRENT_PBP_DATA = all_play_by_play_data
    CURRENT_FAILED_GAMES = failed_games

    # Loop through each game ID and fetch the play-by-play data.
    for count, game_id in enumerate(game_ids, start=(len(successful_games) + len(empty_games) + 1)):
        print(f"{count}/{total_games} Fetching play-by-play data for game ID {game_id}...", flush=True)
        play_by_play_data = fetch_game_pbp(game_id)

        if play_by_play_data is None:
            failed_games.append(game_id)
            CURRENT_FAILED_GAMES = failed_games
            print(f"Failed to fetch data for game ID {game_id}. Retrying later.", flush=True)
            save_checkpoint(all_play_by_play_data, season, team_name, failed_games)
            return None
        elif play_by_play_data.empty:
            empty_games.append(game_id)
            CURRENT_PBP_DATA = all_play_by_play_data
            print(f"No data found for game ID {game_id}. This might be a preseason game.", flush=True)
        else:
            successful_games.append(game_id)
            all_play_by_play_data = pd.concat([all_play_by_play_data, play_by_play_data], ignore_index=True)
            CURRENT_PBP_DATA = all_play_by_play_data

    print(f"Completed {len(successful_games)} / {total_games} games for team '{team_name}' in season '{season}'.", flush=True)

    # If there are any failed games, print their IDs.
    if failed_games:
        print(f"Warning: {len(failed_games)} games failed to process.", flush=True)
        print("Failed game IDs:", flush=True)
        for game_id in failed_games:
            print(game_id, flush=True)

    # If there are any empty games, print their IDs.
    if empty_games:
        print(f"Warning: {len(empty_games)} games had no play-by-play data.", flush=True)
        print("Empty game IDs:", flush=True)
        for game_id in empty_games:
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
    if completed_pbp_file_exists(season, team_name):
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

    all_play_by_play_data = collect_games_pbp_data(games_ids, team_name=team_name, season=season)

    if all_play_by_play_data is None:
        print(f"Data incomplete for team '{team_name}' in season '{season}'.", flush=True)
        return None

    if save_to_file:
        save_pbp_to_csv(all_play_by_play_data, season, team_name)

    return all_play_by_play_data

def get_all_teams_season_pbp(season: str):
    """
    Gets play-by-play data for all NBA teams in a given season.
    Args:
        season (str): The season in the format 'YYYY-YY'. Ex: '2006-07'.
        save_files (bool): If True, saves the play-by-play data to CSV files. Default is False.
    """
    teams_info = teams.get_teams()
    teams_df = pd.DataFrame(teams_info)
    total_teams = teams_df.shape[0]
    teams_df = teams_df.sample(frac=1).reset_index(drop=True)  # Shuffle the teams.

    successful_processed_teams = []
    failed_processed_teams = []
    count = 1
    teams_to_process = [(row['nickname'], row['id']) for _, row in teams_df.iterrows()]
    # Process teams until the list is empty
    while teams_to_process:
        # Get the next team to process
        team_name, team_id = teams_to_process.pop(0)
        
        # Check if the play-by-play data file already exists
        if completed_pbp_file_exists(season, team_name):
            print(f"Skipping. Play-by-play data already exists in season {season} for {team_name}.", flush=True)
            count += 1
            continue
        
        # Fetch play-by-play data for the team
        time.sleep(random.uniform(TEAM_DELAY_MIN, TEAM_DELAY_MAX))
        print(f"{count}/{total_teams} Processing play-by-play data for season {season}, for {team_name}...")
        team_season_pbp = get_team_season_pbp(season, team_name, save_to_file=True, team_id=team_id)
        
        if team_season_pbp is None:
            # Data gathering was incomplete - add back to the end of the queue
            print(f"Data incomplete for {team_name}. Will retry later.", flush=True)
            teams_to_process.append((team_name, team_id))
        elif not team_season_pbp.empty:
            count += 1
            successful_processed_teams.append((team_name, team_id))
            # Checkpoints are useless after successful processing, so we can delete them.
            if checkpoint_file_exists(season, team_name):
                _, checkpoint_file_path = get_checkpoint_filepath(season, team_name)
                os.remove(checkpoint_file_path)
                print(f"Checkpoint file {os.path.normpath(checkpoint_file_path)} deleted after successful processing of {team_name}.", flush=True)
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
    # Register the signal handler
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(description="Get play-by-play data for NBA teams in a given season.")

    # Default behavior gets all teams for the season, but can be overridden by specifying a team name.
    parser.add_argument("--season", type=str, required=True, help="Season in the format 'YYYY-YY'. Ex: '2006-07'")
    parser.add_argument("--team", type=str, help="Team nickname (e.g. 'Celtics'). If omitted, processes all teams.")

    args = parser.parse_args()

    try:
        if args.team:
            get_team_season_pbp(args.season, args.team, save_to_file=True)
        else:
            get_all_teams_season_pbp(args.season)
    except ValueError as e:
        print(f"Error: {e}")
        exit(1)