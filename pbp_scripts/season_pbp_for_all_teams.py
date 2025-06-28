import season_pbp as sp
from nba_api.stats.static import teams
import pandas as pd
import argparse

def save_all_teams_pbp_for_season(season: str) -> pd.DataFrame:
    teams_dict = teams.get_teams()
    team_names = [team['nickname'] for team in teams_dict] 
    for team_name in team_names:
        season_pbp = sp.get_season_pbp(season, team_name)
        if not season_pbp.empty:
            sp.save_pbp_to_csv(season_pbp, team_name, season)
        else:
            print(f"No play-by-play data available for {team_name} in {season}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Save play-by-play data for all teams in a given season.")
    parser.add_argument("--season", type=str, help="The NBA season to retrieve data for.")
    args = parser.parse_args()
    save_all_teams_pbp_for_season(args.season)
