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
    return path + fantasy_year + '/fantasyfootballdata_' + position + fantasy_year + '_week' + week_num + '.csv'

def get_home_team_id(game):
    teams = game.split('@', 1)
    team = teams[0]
    return db.select_team_id(team)

def get_away_team_id(game):
    teams = game.split('@', 1)
    team = teams[1]
    return db.select_team_id(team)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    year = 2010
    week = 1
    while week < 18:
        pos = 'OFF'
        scraper = Scraper(year,week,'b6406b7aea3872d5bb677f064673c57f', pos)
        scraper.scrape()
        root = 'C:/Users/aldod/PycharmProjects/fantasyfootball/data'
        input_folder = '/raw_data/'
        output_folder = '/clean_data_1/'
        input_path = build_path(root + input_folder, str(scraper.getYear()), pos, str(scraper.getWeek()))
        output_path = build_path(root + output_path,str(scraper.getYear()), pos, str(scraper.getWeek()))
        cleaner = Cleaner(input_path, output_path)
        cleaner.clean_player_column()
        input_folder = '/clean_data_1/'
        output_folder = '/clean_data/'
        input_path = build_path(root + input_folder, str(scraper.getYear()), pos, str(scraper.getWeek()))
        output_path = build_path(root + output_path, str(scraper.getYear()), pos, str(scraper.getWeek()))
        cleaner.clean_team_names()

        db = Connection()

        #Open the CSV file in read mode
        with open(output_path, "r", newline="") as file:
            # Create a CSV reader object
            reader = csv.reader(file)

            # Skip the first row
            next(reader)

            # Iterate through each row in the CSV file
            for row in reader:
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
                pos = 'Defense/Special Teams'
                try:
                    db.insert_player(first_name, last_name, pos, team_id)
                except IntegrityError as e:
                    # Handle the exception gracefully (e.g., log an error message)
                    print(f"Failed to insert data: {e}")
                # INSERT RUSHING STATS
                game_id = db.select_game_id(home_team_id, away_team_id, year)
                player_id = db.select_player_id(first_name, last_name)
                rushing = Rushing(row[10], row[11], row[12], row[13], game_id, player_id)
                try:
                    db.insert_rushing(rushing)
                except IntegrityError as e:
                    # Handle the exception gracefully (e.g., log an error message)
                    print(f"Failed to insert data: {e}")
                receiving = Receiving(row[14], row[15], row[16], row[17], game_id, player_id)
                try:
                    db.insert_receiving(receiving)
                except IntegrityError as e:
                    # Handle the exception gracefully (e.g., log an error message)
                    print(f"Failed to insert data: {e}")
                passing = Passing(row[4], row[5], row[6], row[7], row[8], row[9], game_id, player_id)
                try:
                    db.insert_passing(passing)
                except IntegrityError as e:
                    # Handle the exception gracefully (e.g., log an error message)
                    print(f"Failed to insert data: {e}")
                pass_id = db.select_pass_id(game_id,player_id)
                rush_id = db.select_rush_id(game_id,player_id)
                rec_id = db.select_reception_id(game_id,player_id)
                stats = Stats(pass_id, rush_id, rec_id, row[3], row[18], game_id, player_id)
                try:
                    db.insert_stats(stats)
                except IntegrityError as e:
                    # Handle the exception gracefully (e.g., log an error message)
                    print(f"Failed to insert data: {e}")
                # receptions = row[14]
                # rec_yards = row[15]
                # rec_td = row[16]
                # rec_two_pt = row[17]
                # INSERT PASSING STATS
                # player = row[0].split(' ', 1)
                # first_name = player[0]
                # last_name = player[1]
                # teams = row[2].split('@', 1)
                # home_team_name = teams[0]
                # away_team_name = teams[1]
                # total_points = row[3]
                # fumbles = row[18]
                # home_team_id = db.select_team_id(home_team_name)
                # away_team_id = db.select_team_id(away_team_name)
                # season_id = db.select_season_id(year)
                # game_id = db.select_game_id(home_team_id, away_team_id)
                # player_id = db.select_player_id(first_name, last_name)
                # pass_id = db.select_pass_id(game_id, player_id)
                # rush_id = db.select_rush_id(game_id, player_id)
                # reception_id = db.select_reception_id(game_id, player_id)
                # try:
                #     db.insert_stats(pass_id, rush_id, reception_id, total_points, fumbles, game_id, player_id)
                #     db.insert_passing(pass_att, pass_comp, pass_yards, pass_td, pass_two_pt, ints, game_id, player_id)
                #     db.insert_receiving(receptions, rec_yards, rec_td, rec_two_pt, game_id, player_id)
                #     db.insert_game(home_team_id, away_team_id, season_id, week)
                #     db.insert_player(first_name, last_name, pos, team_id)
                # except IntegrityError as e:
                #     # Handle the exception gracefully (e.g., log an error message)
                #     print(f"Failed to insert data: {e}")
        week = week + 1
    #insert player
    #insert_player(first_name, last_name, position, team_id)
    #insert game
    #insert_game(home_team_id, away_team_id, season_id, week)
    #insert rushing
    #insert_rushing(rushing_attempts, rushing_yards, rushing_touchdowns, rushing_2pt_conversions)
    #insert passing
    #insert_passing(passing_attempts, passing_completions, passing_yards, passing_touchdowns, interceptions, passing_2pt_conversions)
    #insert receiving
    #insert_receiving(receptions, receiving_yards, receiving_touchdowns, receiving_2pt_conversions)
    #insert stats
    #insert_stats(rush_id, pass_id, reception_id, total_points, fumbles, game_id, player_id)
    # Intersection('/data/raw_data/' + scraper.getYear() + '/fantasyfootballdata_' + scraper.getYear()  + '_week' + scraper.getWeek()  + '.csv',
    #              '/data/raw_data/' + scraper.getYear() + '/fantasyfootballdata_' + scraper.getYear()  + '_week' + scraper.getWeek()  + '.csv')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
