from Cleaner import Cleaner
from Scraper import Scraper
import csv
from nfl.Team import Team
from postgres_db.Connection import Connection
import re


def setup_cleaner(input_csv_path, output_csv_path, cleaner):
    cleaner.setInputCSV(input_csv_path)
    cleaner.setOutputCSV(output_csv_path)


def build_path(path, fantasy_year, position, week_num):
    return path + str(fantasy_year) + '/fantasyfootballdata_' + position + str(fantasy_year) + '_week' + str(
        week_num) + '.csv'

def set_up_scraper(fantasy_year, week, key, pos):
    return Scraper(fantasy_year, week, key, pos)


def get_other_team_name(team_name_1, game_string):
    # Split on " vs " or " @ ", case-insensitive and surrounded by optional whitespace
    match = re.split(r'\s+vs\s+|\s+@\s+', game_string, flags=re.IGNORECASE)

    if len(match) != 2:
        return None  # Unexpected format

    team_1 = match[0].strip()
    team_2 = match[1].strip()

    if team_name_1 == team_1:
        return team_2
    elif team_name_1 == team_2:
        return team_1
    else:
        return None  # Given team_name_1 not found in string


def set_up_cleaner(path_to_csv, output_csv):
    return Cleaner(path_to_csv, output_csv)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    year = 2010
    end_year = 2025
    db = Connection()
    season_id = 1
    pos = 'Kicker'
    while year < end_year:
        week = 1
        if year >= 2021:
            end_week = 19
        else:
            end_week = 18  # end week will be week 19 after season gets extended
        while week < end_week:
            root = 'C:/Users/aldod/PycharmProjects/fantasyfootball/data'
            output_folder = '/clean_data/'
            output_path = build_path(root + output_folder, year, 'K', week)
            with open(output_path, "r", newline="") as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    team = row[1]
                    game = row[2]
                    team_id = db.select_team_id_from_team_name(team)
                    full_name = row[0].strip()
                    parts = full_name.split()
                    if len(parts) >= 2:
                        first_name = parts[0]
                        last_name = " ".join(parts[1:])
                    else:
                        raise ValueError(f"Unexpected player name format: '{full_name}'")
                    player_id = db.select_player_id(first_name, last_name)
                    game_id = db.select_game_id_based_on_players_team(team_id, season_id, week)
                    if game_id is None:
                        team_name_2 = get_other_team_name(team, game)
                        team_id_2 = db.select_team_id_from_team_name(team_name_2)
                        game_id = db.select_game_id_based_on_players_team(team_id_2, season_id, week)
                    extra_point_attempts = row[4]
                    extra_points_made = row[5]
                    field_goal_attempts = row[6]
                    field_goals_made = row[7]
                    fifty_yard_field_goals_made = row[8]
                    db.insert_kicking(extra_point_attempts, extra_points_made, field_goal_attempts, field_goals_made,
                                        fifty_yard_field_goals_made, player_id, game_id)
            week = week + 1
        print(str(year) + " kicking stats added.")
        year = year + 1
        season_id = season_id + 1
