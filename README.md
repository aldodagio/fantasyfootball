# How to use the scraper object to collect our fantasy data for our postgres db
 1. Call the set_up_scraper function inside the main function.\
    The function takes 4 arguments - fantasy_year, week, key, pos.\
    The fantasy_year parameter is simply an integer, ex. 2024.\
    The week parameter is the fantasy week you want to scrape, ex. 1 for week 1.\
    The key parameter is the api key or the session key.\
    The pos paramter is the position you are interested in scraping, ex. WR.\
    The function will return a scraper object.\
    Example URL: https://www.footballdb.com/fantasy-football/index.html?yr=2024&pos=QB&wk=18&key=48ca46aa7d721af4d58dccc0c249a1c4
 2. Use the scrape function from the Scraper object.\
    You will have to loop the function and iterate through weeks 1-18.\
    Example: while week < end_week:\
              scraper.setWeek(week)\
              scraper.setURL()\
              scraper.scrape()\
              week = week + 1
 3. The raw data will now be in the \data\raw_data\{year} folder(s). The next step\
    is to clean the data.
# How to use the cleaner object to prepare the data to insert into the database.
 1. Call the set_up_cleaner function inside the main function.\
    The function takes 2 arguments - path_to_csv, output_csv.\
    The path_to_csv parameter should be the path to the CSV that you want to clean.\
    The output_csv parameter should be the path to the new cleaned CSV. This one will be created if there isn't already a file there.\
    The function will return a cleaner object.
 2. First, clean the 'Team' column from the raw_data file.
    Simply call cleaner.clean_team_names().\
    On the next line, call cleaner.clean_non_numeric_values().\
    Run the main method.
 3. Second, comment the previous functions out - cleaner.clean_team_names() and cleaner.clean_non_numeric_values().\
    Now, we want to clean the 'Player' column.\
    Refresh the CSV files in the cleaner. The path_to_csv should be the path to the file that was just cleaned and \
    the output_csv should be where you want the output csv to be placed.\
    Simply call cleaner.clean_player_column().\
    Run the main method.
 4. Lastly, comment the previous function out - cleaner.clean_player_column().\
    Now, we want to clean the 'Game' column.\
    Refresh the CSV files in the cleaner. The path_to_csv should be the path to the file that was just cleaned and\
    the output_csv should be where you want the output csv to be placed.\
    Simply call cleaner.clean_game_column().\
    Run the main method.
# How to insert csv file data into database entities
## Insert Season
insert into season(year) values (2024);\
This command is simple enough to just run in DataGrip.
## Insert Game(s)
To insert a game, we will need to use the insert_game method from the Connection.py class.\
The insert_game method takes 4 arguments - home_team_id, away_team_id, season_id, week.\
To insert all games for an entire season, you can loop through weeks 1 through 18 for any given season.
```
    week = 1
    end_week = 19
    year = 2024
    db = Connection()
    pos = 'Wide Receiver'
    while week < end_week:
        root = '../fantasyfootball/data'
        output_folder = '/clean_data/'
        output_path = build_path(root + output_folder, year, 'WR', week)
        with open(output_path, "r", newline="") as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                season_id = db.select_season_id(year)
                game = row[2]
                team = Team()
                away_team_id = team.get_away_team_id(game, db)
                home_team_id = team.get_home_team_id(game, db)
                db.insert_game(home_team_id,away_team_id,season_id,week)
        week = week + 1
```
Above is a somewhat working piece of code to insert all games played in a given season, such as 2024.
## Insert Players
To insert new players, we will need to use the insert_player method from the Connection.py class.\
The insert_player method takes 4 arguments - first_name, last_name, position, team_id\
To insert all new players from the new season, you can loop through weeks 1 through 18 for the given season.
```
    week = 1
    end_week = 19
    year = 2024
    db = Connection()
    season_id = 15
    pos = 'Wide Receiver'
    while week < end_week:
        root = '../fantasyfootball/data'
        output_folder = '/clean_data/'
        output_path = build_path(root + output_folder, year, 'WR', week)
        with open(output_path, "r", newline="") as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                full_name = row[0].strip()
                parts = full_name.split()
                if len(parts) >= 2:
                    first_name = parts[0]
                    last_name = " ".join(parts[1:])
                else:
                    raise ValueError(f"Unexpected player name format: '{full_name}'")
                team_id = db.select_team_id(row[1])
                db.insert_player(first_name,last_name,pos,team_id)
        week = week + 1
```
Above is a somewhat working piece of code to insert all new players from a given season, such as 2024.
## Insert Rushing Stats
To insert rushing stats for each player from the data files, we will need to use the insert_rushing method from the Connection.py class.\
The insert_rushing method takes 6 arguments - rushing_attempts, rushing_yards, rushing_tds, rushing_two_pt_conversions, game_id, player_id.\
To insert all new rushing stats from the given season, you can loop through weeks 1 through 18 in the main method.\
Make sure to identify that the columns in the row[index] are valid each season, as the format of the file can change.
```
    week = 1
    end_week = 19
    year = 2024
    db = Connection()
    season_id = 15
    pos = 'Wide Receiver'
    while week < end_week:
        root = '../fantasyfootball/data'
        output_folder = '/clean_data/'
        output_path = build_path(root + output_folder, year, 'WR', week)
        with open(output_path, "r", newline="") as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                full_name = row[0].strip()
                parts = full_name.split()
                if len(parts) >= 2:
                    first_name = parts[0]
                    last_name = " ".join(parts[1:])
                else:
                    raise ValueError(f"Unexpected player name format: '{full_name}'")
                player_id = db.select_player_id(first_name,last_name)
                game_id = db.select_game_id(home_team_id, away_team_id, season_id)
                rushing_attempts = row[10]
                rushing_yards = row[11]
                rushing_tds = row[12]
                rushing_two_pt_conversions = row[13]
                db.insert_rushing(rushing_attempts, rushing_yards, rushing_tds, rushing_two_pt_conversions, game_id,player_id)
        week = week + 1
```
Above is a somewhat working piece of code to insert all new rushing stats from a given season, such as 2024.
## Insert Receiving Stats
To insert receiving stats for each player from the data files, we will need to use the insert_receiving method from the Connection.py class.\
The insert_receiving method takes 6 arguments - receptions, receiving_yards, receiving_tds, receiving_two_pt_conversions, game_id, player_id.\
To insert all new receiving stats from the given season, you can loop through weeks 1 through 18 in the main method.\
Make sure to identify that the columns in the row[index] are valid each season, as the format of the file can change.
```
    week = 1
    end_week = 19
    year = 2024
    db = Connection()
    season_id = 15
    pos = 'Wide Receiver'
    while week < end_week:
        root = '../fantasyfootball/data'
        output_folder = '/clean_data/'
        output_path = build_path(root + output_folder, year, 'WR', week)
        with open(output_path, "r", newline="") as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                full_name = row[0].strip()
                parts = full_name.split()
                if len(parts) >= 2:
                    first_name = parts[0]
                    last_name = " ".join(parts[1:])
                else:
                    raise ValueError(f"Unexpected player name format: '{full_name}'")
                player_id = db.select_player_id(first_name,last_name)
                game_id = db.select_game_id(home_team_id, away_team_id, season_id)
                receptions = row[14]
                receiving_yards = row[15]
                receiving_tds = row[16]
                receiving_two_pt_conversions = row[17]
                db.insert_receiving(receptions,receiving_yards,receiving_tds,receiving_two_pt_conversions,game_id,player_id)
        week = week + 1
```
Above is a somewhat working piece of code to insert all new receiving stats from a given season, such as 2024.
## Insert Passing Stats
To insert passing stats for each player from the data files, we will need to use the insert_receiving method from the Connection.py class.\
The insert_passing method takes 8 arguments - passing_attempts, passing_completions, passing_yards, passing_tds, interceptions, passing_two_pt_conv, game_id, player_id.\
To insert all new passing stats from the given season, you can loop through weeks 1 through 18 in the main method.\
Make sure to identify that the columns in the row[index] are valid each season, as the format of the file can change.
```
    week = 1
    end_week = 19
    year = 2024
    db = Connection()
    season_id = 15
    pos = 'Wide Receiver'
    while week < end_week:
        root = '../fantasyfootball/data'
        output_folder = '/clean_data/'
        output_path = build_path(root + output_folder, year, 'WR', week)
        with open(output_path, "r", newline="") as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                full_name = row[0].strip()
                parts = full_name.split()
                if len(parts) >= 2:
                    first_name = parts[0]
                    last_name = " ".join(parts[1:])
                else:
                    raise ValueError(f"Unexpected player name format: '{full_name}'")
                player_id = db.select_player_id(first_name,last_name)
                game_id = db.select_game_id(home_team_id, away_team_id, season_id)
                passing_attempts = row[4]
                passing_completions = row[5]
                passing_yards = row[6]
                passing_tds = row[7]
                interceptions = row[8]
                passing_two_pt_conv = row[9]
                db.insert_passing(passing_attempts, passing_completions, passing_yards, passing_tds,interceptions,passing_two_pt_conv,game_id,player_id)
        week = week + 1
```
Above is a somewhat working piece of code to insert all new passing stats from a given season, such as 2024.
## Insert Stats
Once we have added all the rushing, receiving, and passing stats for the season, we can add the total stats.
The insert_stats method takes 7 arguments - pass_id, rush_id, reception_id, total_points, fumbles, game_id, player_id.\
To insert all the total stats for a given season, you can loop through weeks 1 through 18 in the main method.\
Make sure to identify that the columns in the row[index] are valid each season, as the format of the file can change.
```
    week = 1
    end_week = 19
    year = 2024
    db = Connection()
    season_id = 15
    pos = 'Wide Receiver'
    while week < end_week:
        root = '../fantasyfootball/data'
        output_folder = '/clean_data/'
        output_path = build_path(root + output_folder, year, 'WR', week)
        with open(output_path, "r", newline="") as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                full_name = row[0].strip()
                parts = full_name.split()
                if len(parts) >= 2:
                    first_name = parts[0]
                    last_name = " ".join(parts[1:])
                else:
                    raise ValueError(f"Unexpected player name format: '{full_name}'")
                player_id = db.select_player_id(first_name,last_name)
                season_id = db.select_season_id(year)
                game = row[2]
                team = Team()
                away_team_id = team.get_away_team_id(game, db)
                home_team_id = team.get_home_team_id(game, db)
                game_id = db.select_game_id(home_team_id, away_team_id, season_id)
                total_points = row[3]
                fumbles = row[18]
                pass_id = db.select_pass_id(game_id, player_id)
                rush_id = db.select_rush_id(game_id, player_id)
                reception_id = db.select_reception_id(game_id, player_id)
                db.insert_stats(pass_id, rush_id, reception_id, total_points, fumbles, game_id, player_id)
        week = week + 1
```
Above is a somewhat working piece of code to insert all new stats from a given season, such as 2024.
