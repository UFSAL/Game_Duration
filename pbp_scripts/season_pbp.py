import pandas as pd
import time
import random
import argparse
import requests
from requests.exceptions import ReadTimeout
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playbyplayv2

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

def get_season_pbp(season: str, team_name: str) -> pd.DataFrame:
    """
    Gets a team's play-by-play data for an entire season.
    
    Args:
        season (str): The season in the format 'YYYY-YY'. Ex: '2006-07'.
        team_name (str): The nickname of the team. Ex: 'Celtics'. Case-sensitive.

    Returns:
        pd.DataFrame: The play-by-play data for the team in the specified season.
    """

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
            continue
        elif play_by_play_data.empty:
            print(f"Game ID {game_id} contains no play-by-play data. This might be a preseason game.", flush=True)
            continue

        count += 1
        all_play_by_play_data = pd.concat([all_play_by_play_data, play_by_play_data], ignore_index=True)
        game_ids.remove(game_id)  # Remove the successfully processed game ID from the set

    print(f"Completed {count - 1} / {len(games)} games for team '{team_name}' in season '{season}'.", flush=True)
    return all_play_by_play_data

def save_pbp_to_csv(data: pd.DataFrame, team_name: str, season: str):
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

    file_name = f"{season}_{team_name}_pbp.csv"
    file_name = f"{season}_{team_name}_pbp.csv"
    data.to_csv(file_name, index=False)
    print(f"Play-by-play data saved to {file_name}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get NBA team's play-by-play data for a season.")
    parser.add_argument("--season", type=str, help="Season in the format 'YYYY-YY'. Ex: '2006-07'.")
    parser.add_argument("--team_name", type=str, help="Nickname of the team. Ex: 'Celtics'. Case-sensitive.")
    parser.add_argument("--save", action="store_true", help="Save the play-by-play data to a CSV file.")

    args = parser.parse_args()

    try:
        total_games = get_season_pbp(args.season, args.team_name)
        if args.save:
            save_pbp_to_csv(total_games, args.team_name, args.season)
        else:
            print(total_games)
    except ValueError as e:
        print(e)