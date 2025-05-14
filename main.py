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
    return path + str(fantasy_year) + '/fantasyfootballdata_' + position + str(fantasy_year) + '_week' + str(
        week_num) + '.csv'

def set_up_scraper(fantasy_year, week, key, pos):
    return Scraper(fantasy_year, week, key, pos)

def set_up_cleaner(path_to_csv, output_csv):
    return Cleaner(path_to_csv, output_csv)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    week = 1
    end_week = 19
    year = 2024
    db = Connection()
    # position = 'Tight End'
    season_id = 15
    pos = 'Wide Receiver'
    while week < end_week:
        root = 'C:/Users/aldod/PycharmProjects/fantasyfootball/data'
        output_folder = '/clean_data/'
        output_path = build_path(root + output_folder, year, 'WR', week)
        #Open the CSV file in read mode
        with open(output_path, "r", newline="") as file:
            # Create a CSV reader object
            reader = csv.reader(file)
            next(reader)
            # Iterate through each row in the CSV file
            for row in reader:
                full_name = row[0].strip()
                parts = full_name.split()
                if len(parts) >= 2:
                    first_name = parts[0]
                    last_name = " ".join(parts[1:])  # Supports names like "Van Noy" or "De La Hoya"
                else:
                    raise ValueError(f"Unexpected player name format: '{full_name}'")
                team_id = db.select_team_id(row[1])
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
                # passing_attempts = row[4]
                # passing_completions = row[5]
                # passing_yards = row[6]
                # passing_tds = row[7]
                # interceptions = row[8]
                # passing_two_pt_conv = row[9]
                # receptions = row[14]
                # receiving_yards = row[15]
                # receiving_tds = row[16]
                # receiving_two_pt_conversions = row[17]
                # rushing_attempts = row[10]
                # rushing_yards = row[11]
                # rushing_tds = row[12]
                # rushing_two_pt_conversions = row[13]
                # db.insert_passing(passing_attempts, passing_completions, passing_yards, passing_tds,interceptions,passing_two_pt_conv,game_id,player_id)
                # db.insert_receiving(receptions,receiving_yards,receiving_tds,receiving_two_pt_conversions,game_id,player_id)
                # db.insert_rushing(rushing_attempts, rushing_yards, rushing_tds, rushing_two_pt_conversions, game_id,player_id)
                # db.insert_game(home_team_id=home_team_id,away_team_id=away_team_id,season_id=season_id,week=week)
                #game_id = db.select_game_id(home_team_id, away_team_id, season_id)
                db.insert_stats(pass_id, rush_id, reception_id, total_points, fumbles, game_id, player_id)
    #             player_full_name = row[0]
    #             player = player_full_name.split(' ', 1)
    #             first_name = player[0]
    #             last_name = player[1]
    #             player_id = db.select_player_id(first_name, last_name)
    #             Stats.insert_stats(row, game_id, player_id, db)
        week = week + 1
        #year = year + 1
