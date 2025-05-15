from Cleaner import Cleaner
from Scraper import Scraper
import csv
from nfl.Team import Team
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
    season_id = 15
    pos = 'Wide Receiver'
    while week < end_week:
        root = 'C:/Users/aldod/PycharmProjects/fantasyfootball/data'
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
