from psycopg2 import IntegrityError

from Cleaner import Cleaner
from Intersection import Intersection
from Scraper import Scraper
import csv
from nfl.Passing import Passing
from nfl.Receiving import Receiving
from nfl.Rushing import Rushing
from nfl.Stats import Stats
from postgres_db.Connection import Connection

def setup_cleaner(input_csv_path, output_csv_path, cleaner):
    cleaner.setInputCSV(input_csv_path)
    cleaner.setOutputCSV(output_csv_path)

def build_path(path, fantasy_year, position, week_num):
    return path + str(fantasy_year) + '/fantasyfootballdata_' + position + str(fantasy_year) + '_week' + str(week_num) + '.csv'

def get_home_team_id(game, db):
    teams = game.split('@', 1)
    team = teams[0]
    return db.select_team_id(team)

def get_away_team_id(game, db):
    teams = game.split('@', 1)
    team = teams[1]
    return db.select_team_id(team)

def insert_players(row, pos, db):
    player = row[0].split(' ', 1)
    first_name = player[0]
    last_name = player[1]
    team_id = db.select_team_id(row[1])
    try:
        db.insert_player(first_name, last_name, pos, team_id)
    except IntegrityError as e:
        # Handle the exception gracefully (e.g., log an error message)
        print(f"Failed to insert data: {e}")

def insert_games_and_players(row, year, pos, db, week):
    # INSERT GAMES
    game = row[2]
    home_team_id = get_home_team_id(game)
    away_team_id = get_away_team_id(game)
    season_id = db.select_season_id(year)
    try:
        db.insert_game(home_team_id, away_team_id, season_id, week)
    except IntegrityError as e:
        # Handle the exception gracefully (e.g., log an error message)
        print(f"Failed to insert data: {e}")
    # INSERT PLAYERS
    player = row[0].split(' ', 1)
    first_name = player[0]
    last_name = player[1]
    team_id = db.select_team_id(row[1])
    try:
        db.insert_player(first_name, last_name, pos, team_id)
    except IntegrityError as e:
        # Handle the exception gracefully (e.g., log an error message)
        print(f"Failed to insert data: {e}")

def insert_rushing_passing_receiving_stats(row, game_id, player_id, db):
    player_full_name = row[0]
    player = player_full_name.split(' ', 1)
    first_name = player[0]
    last_name = player[1]
    pass_atts = row[4]
    pass_comps = row[5]
    pass_yards = row[6]
    pass_tds = row[7]
    ints = row[8]
    pass_two_pt = row[9]
    rush_atts = row[10]
    rush_yards = row[11]
    rush_tds = row[12]
    rush_two_pt = row[13]
    recs = row[14]
    rec_yards = row[15]
    rec_tds = row[16]
    rec_two_pt = row[17]
    rushing = Rushing(rush_atts, rush_yards, rush_tds, rush_two_pt, game_id, player_id)
    receiving = Receiving(recs, rec_yards, rec_tds, rec_two_pt, game_id, player_id)
    passing = Passing(pass_atts, pass_comps, pass_yards, pass_tds, pass_two_pt, ints, game_id, player_id)
    position = db.select_position(first_name, last_name)
    if position == 'Running Back':
        try:
            db.insert_rushing(rushing)
            if recs:
                db.insert_receiving(receiving)
            if pass_atts:
                db.insert_passing(passing)
        except IntegrityError as e:
            # Handle the exception gracefully (e.g., log an error message)
            print(f"Failed to insert data: {e}")
    elif position == 'Wide Receiver':
        try:
            db.insert_receiving(receiving)
            if rush_atts:
                db.insert_rushing(rushing)
            if pass_atts:
                db.insert_passing(passing)
        except IntegrityError as e:
            # Handle the exception gracefully (e.g., log an error message)
            print(f"Failed to insert data: {e}")
    elif position == 'Quarterback':
        try:
            db.insert_passing(passing)
            if rush_atts:
                db.insert_rushing(rushing)
            if recs:
                db.insert_receiving(receiving)
        except IntegrityError as e:
            # Handle the exception gracefully (e.g., log an error message)
            print(f"Failed to insert data: {e}")

def insert_stats(row, game_id, player_id, db):
    pass_id = db.select_pass_id(game_id, player_id)
    rush_id = db.select_rush_id(game_id, player_id)
    rec_id = db.select_reception_id(game_id, player_id)
    total_points = row[3]
    fumbles = row[18]
    stats = Stats(pass_id, rush_id, rec_id, total_points, fumbles, game_id, player_id)
    try:
        db.insert_stats(stats)
    except IntegrityError as e:
        # Handle the exception gracefully (e.g., log an error message)
        print(f"Failed to insert data: {e}")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    year = 2011
    end_week = 18
    while year < 2024:
        week = 1
        if year == 2023:
            end_week = 19
        while week < end_week:
            pos = 'QB'
            #scraper = Scraper(year,week,'b6406b7aea3872d5bb677f064673c57f', pos)
            #scraper.scrape()
            root = 'C:/Users/aldod/PycharmProjects/fantasyfootball/data'
            #input_folder = '/raw_data/'
            #output_folder = '/clean_data_1/'
            #input_path = build_path(root + input_folder, str(scraper.getYear()), pos, str(scraper.getWeek()))
            #output_path = build_path(root + output_folder,str(scraper.getYear()), pos, str(scraper.getWeek()))
            #cleaner = Cleaner(input_path, output_path)
            #cleaner.clean_player_column()
            #input_folder = '/clean_data_1/'
            output_folder = '/clean_data/'
            #input_path = build_path(root + input_folder, str(scraper.getYear()), pos, str(scraper.getWeek()))
            #pos = 'QB'
            output_path = build_path(root + output_folder, year, pos, week)
            #cleaner = Cleaner(input_path, output_path)
            #cleaner.clean_team_names()

            db = Connection()

            #pos = ''

            #Open the CSV file in read mode
            with open(output_path, "r", newline="") as file:
                # Create a CSV reader object
                reader = csv.reader(file)

                # Skip the first row
                next(reader)

                #Iterate through each row in the CSV file
                for row in reader:
                    #insert_players(row, pos)
                    #insert_games_and_players(row, year, pos)

                    game = row[2]
                    home_team_id = get_home_team_id(game,db)
                    away_team_id = get_away_team_id(game,db)
                    season_id = db.select_season_id(year)
                    game_id = db.select_game_id(home_team_id, away_team_id, season_id)
                    player_full_name = row[0]
                    player = player_full_name.split(' ', 1)
                    first_name = player[0]
                    last_name = player[1]
                    team_name = row[1]
                    team_id = db.select_team_id(team_name)
                    #position = db.select_position(first_name, last_name)
                    player_id = db.select_player_id(first_name, last_name)
                    insert_stats(row, game_id, player_id, db)
                    insert_rushing_passing_receiving_stats(row, game_id, player_id, db)
            week = week + 1
        year = year + 1

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
