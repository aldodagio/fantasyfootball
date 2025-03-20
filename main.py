from psycopg2 import IntegrityError
from Cleaner import Cleaner
from Intersection import Intersection
from Scraper import Scraper
import csv
from nfl.Team import Team
from nfl.Stats import Stats
from postgres_db.Connection import Connection


def setup_cleaner(input_csv_path, output_csv_path, cleaner):
    cleaner.setInputCSV(input_csv_path)
    cleaner.setOutputCSV(output_csv_path)


def build_path(path, fantasy_year, position, week_num):
    return path + str(fantasy_year) + '/fantasyfootballdata_' + position + '_' + str(fantasy_year) + '_week' + str(
        week_num) + '.csv'

def set_up_scraper(fantasy_year, week, key, pos):
    return Scraper(fantasy_year, week, key, pos)

def set_up_cleaner(path_to_csv, output_csv):
    return Cleaner(path_to_csv, output_csv)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    week = 1
    end_week = 19
    #scraper = set_up_scraper(2024, 1, '48ca46aa7d721af4d58dccc0c249a1c4', 'TE')
    while week < end_week:
        cleaner = set_up_cleaner(
           'C:\\Users\\aldod\\PycharmProjects\\fantasyfootball\\data\\raw_data\\2024\\fantasyfootballdata_WR2024_week' + str(week) + '.csv',
           'C:\\Users\\aldod\\PycharmProjects\\fantasyfootball\\data\\clean_data\\2024\\fantasyfootballdata_WR2024_week' + str(week) + '.csv')
        cleaner.clean_player_column()
        cleaner.clean_non_numeric_values()
        cleaner.clean_team_names()
        week = week + 1
    # year = 2010
    # db = Connection()
    # position = 'Tight End'
    # season_id = 1
    # week = 1
    # end_week = 18
    # #while year < 2024:
    #     #week = 1
    #     #if year == 2023:
    #      #   end_week = 19
    # while week < end_week:
    #     pos = 'TE'
    #     root = 'C:/Users/aldod/PycharmProjects/fantasyfootball/data'
    #     output_folder = '/clean_data/'
    #     output_path = build_path(root + output_folder, year, pos, week)
    #     db = Connection()
    #     # Open the CSV file in read mode
    #     with open(output_path, "r", newline="") as file:
    #         # Create a CSV reader object
    #         reader = csv.reader(file)
    #         next(reader)
    #         # Iterate through each row in the CSV file
    #         for row in reader:
    #             game = row[2]
    #             home_team_id = Team.get_home_team_id(game, db)
    #             away_team_id = Team.get_away_team_id(game, db)
    #             season_id = db.select_season_id(year)
    #             game_id = db.select_game_id(home_team_id, away_team_id, season_id)
    #             player_full_name = row[0]
    #             player = player_full_name.split(' ', 1)
    #             first_name = player[0]
    #             last_name = player[1]
    #             player_id = db.select_player_id(first_name, last_name)
    #             Stats.insert_stats(row, game_id, player_id, db)
    #     week = week + 1
        #year = year + 1
