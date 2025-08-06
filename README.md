NBA Game Duration Tools For Data Analysis R

This script: 1. loads all of the play by play data into the R environment (est ~5 mins), 
2. calculates game durations from tipoff to end of game and displaying them in a table, 
3. finds the average game duration per season cutting out the seasons before 2012, OT games and outlier durations,
4. makes a new version of all play by play data with these cuts made
**IN THAT ORDER**
Scripts:
- load_all_data.R
- create_game_durations_table.R
- average_graph.R
- all_data_cut

Note that for the first script you must input your file directory for your pbp data
