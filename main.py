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
    year = 2010
    end_year = 2025
    #db = Connection()
    #season_id = 1
    pos = 'K'
    scraper = Scraper(2010,1,'b6406b7aea3872d5bb677f064673c57f',pos)
    while year < end_year:
        week = 1
        scraper.setYear(year)
        if year >= 2021:
            end_week = 19
        else:
            end_week = 18  # end week will be week 19 after season gets extended
        while week < end_week:
            root = 'C:/Users/aldod/PycharmProjects/fantasyfootball/data'
            output_folder = '/raw_data/'
            output_path = build_path(root + output_folder, year, 'K', week)
            scraper.setWeek(week)
            scraper.setURL()
            scraper.scrape()
            week = week + 1
        print(str(year) + " completed data scraping.")
        year = year + 1