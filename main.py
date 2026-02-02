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
    c = Cleaner('C:\\Users\\aldod\\PycharmProjects\\fantasyfootball\\data\\ncaaf\\raw_data\\2024\\receiving_stats_2024.csv',
                'C:\\Users\\aldod\\PycharmProjects\\fantasyfootball\\data\\ncaaf\\clean_data_2\\2024\\receiving_stats_2024.csv')
    c.clean_name_display_column()