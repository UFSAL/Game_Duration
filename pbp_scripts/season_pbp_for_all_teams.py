import season_pbp as sp
from nba_api.stats.static import teams
import pandas as pd
import argparse
import time
import random

def save_all_teams_pbp_for_season(season: str) -> pd.DataFrame:
    teams_dict = teams.get_teams()
    team_names = [team['nickname'] for team in teams_dict] 
    teams_set = set(team_names)
    count = 1
    for team_name in teams_set:
        print(f"{count}/{len(teams_set)} Processing play-by-play data for {team_name} in season {season}...")

        # Skip data already gathered
        file_name = f"{season}_{team_name}_pbp.csv"
        if pd.io.common.file_exists(file_name):
            print(f"Play-by-play data for {team_name} in {season} already exists. Skipping...")
            count += 1
            continue

        # Random delay between teams to avoid rate limiting
        time.sleep(random.uniform(10, 20))
        season_pbp = sp.get_season_pbp(season, team_name)
        if not season_pbp.empty:
            print(f"Saving play-by-play data for {team_name} in {season}.")
            sp.save_pbp_to_csv(season_pbp, team_name, season)
            count += 1
        else:
            print(f"No play-by-play data available for {team_name} in {season}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Save play-by-play data for all teams in a given season.")
    parser.add_argument("--season", type=str, help="The NBA season to retrieve data for.")
    args = parser.parse_args()
    save_all_teams_pbp_for_season(args.season)
