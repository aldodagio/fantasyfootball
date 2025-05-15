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
To insert a game, we will need to use the insert_game method from the Connection.py class.
The insert_game method takes 4 arguments - home_team_id, away_team_id, season_id, week.
To insert all games for an entire season, you can loop through weeks 1 through 18 for any given season.
    week = 1
    end_week = 19
    year = 2024
    db = Connection()
    pos = 'Wide Receiver'
    while week < end_week:
        root = '../fantasyfootball/data'
        output_folder = '/clean_data/'
        output_path = build_path(root + output_folder, year, 'WR', week)
        #Open the CSV file in read mode
        with open(output_path, "r", newline="") as file:
            # Create a CSV reader object
            reader = csv.reader(file)
            next(reader)
            # Iterate through each row in the CSV file
            for row in reader:
                season_id = db.select_season_id(year)
                game = row[2]
                team = Team()
                away_team_id = team.get_away_team_id(game, db)
                home_team_id = team.get_home_team_id(game, db)
                db.insert_game(home_team_id,away_team_id,season_id,week)
Above is a somewhat working piece of code to insert all games played in a given season, such as 2024.
