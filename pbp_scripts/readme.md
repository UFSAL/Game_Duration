# Game Duration Script

This project analyzes play-by-play data to calculate game durations.

## Installation

1. Clone the repository.
   ```python
   git clone https://github.com/UFSAL/Game_Duration.git
   ```
2. move into the project directory:
   ```bash
   cd pbp_scripts
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
or maybe something more like this:

```bash
python3 pbp_scripts/season_pbp.py --season 2006-07 --team_name Celtics --save
```

To create CSVs for all teams in a season, use the following command:

```bash
python season_pbp_for_all_teams.py --season 2006-07
```

QUESTION: Is there not a --save required at the end of this?
