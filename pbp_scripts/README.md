# Game Duration Scripts

This project attempts to scrape, clean, and analyze play-by-play data to calculate game durations.

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

To get the play-by-play data for **all** teams in a specified season and save it to CSV files, run the following command:

```bash
python season_pbp.py --season 2006-07
```

To get the play-by-play data for a **single** team in a specified season and save it to a CSV file, run the following command:

```bash
python season_pbp.py --season 2006-07 --team Celtics
```

To get WNBA data, use the `--league wnba` flag. If `--league` is not specified, it defaults to `nba`. If getting WNBA data, the season should be specified in the format `YYYY` because WNBA seasons are played entirely in one calendar year.

```bash
python season_pbp.py --season 2023 --team Sparks --league wnba
```

## Notes

You can run this script in different terminals simultaneously, for different seasons, theoretically increasing the request volume.

## Some issues you may encounter:

Make sure to specify the season in the format `YYYY-YY`, and the team's nickname (not full name) as it appears in the NBA data. Example name that would not work: `Boston Celtics`.

The filename `season_pbp.py` is assuming you used `cd ./Game_Duration/pbp_scripts` to change the current working directory to the `pbp_scripts` folder. If you are running the script from a different directory, you will need to provide the full path to the script such as `./Game_Duration/pbp_scripts/season_pbp.py` instead of just `season_pbp.py`. To find your current working directory, you can run the command `pwd` in your terminal or command prompt.

The current script version does not account for disbanded WNBA teams. To resolve this discrepancy, use the `find_missing_wnba_teams.ipynb` notebook.

## When the script self-restarts, it may lose normal `Ctrl+C` functionality. You can stop the script by using `kill terminal` in VSCode.

## Checkpoints

The script currently supports resuming from checkpoints. If the script times out on a request, it will save the current state to a checkpoint file, and come back to it later. This also works if you stop the script manually with `Ctrl+C`.
