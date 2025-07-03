# Game Duration Script

This project analyzes play-by-play data to calculate game durations.

## Installation

1. Clone the repository.
   ```python
   git clone https://github.com/UFSAL/Game_Duration.git
   ```
2. move into the project directory:
   ```bash
   cd ./Game_Duration/pbp_scripts
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   Depending on your Python installation, you may need to use this instead.
   ```bash
   pip3 install -r requirements.txt
   ```

## Usage

For a single team's season play-by-play data and saving it to a CSV file, use the following command:

```bash
python season_pbp.py --season 2006-07 --team_name Celtics --save
```

The filename `season_pbp.py` is assuming you used `cd pbp_scripts` to change the current working directory to the `pbp_scripts` folder. If you are running the script from a different directory, you will need to provide the full path to the script such as `pbp_scripts/season_pbp.py` instead of just `season_pbp.py`.

To create CSVs for all teams in a season, use the following command:

```bash
python season_pbp_for_all_teams.py --season 2006-07
```
