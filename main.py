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
    s = Scraper(2009, 1, '', '')
    s.setYear(2009)
    s.set_college_url('rushing')

    table_id_rushing = "rushing_standard"
    columns_rushing = [
        "name_display", "team_name_abbr", "conf_abbr", "games",
        "rush_att", "rush_yds", "rush_yds_per_att", "rush_td", "rush_yds_per_g"
    ]
    s.scrape_ncaaf(table_id_rushing, columns_rushing, "rushing_stats_" + str(s.getYear()) + ".csv")

    s.set_college_url('receiving')
    table_id_receiving = "receiving_standard"
    columns_receiving = [
        "name_display", "team_name_abbr", "conf_abbr", "games",
        "rec", "rec_yds", "rec_yds_per_rec", "rec_td", "rec_yds_per_g"
    ]
    s.scrape_ncaaf(table_id_receiving, columns_receiving, "receiving_stats_" + str(s.getYear()) + ".csv")

    s.set_college_url('passing')
    table_id_passing = "passing_standard"
    columns_passing = [
        "name_display", "team_name_abbr", "conf_abbr", "games",
        "pass_cmp", "pass_att", "pass_cmp_perc", "pass_yds", "pass_td",
        "pass_int", "pass_yds_per_att", "pass_rating"
    ]
    s.scrape_ncaaf(table_id_passing, columns_passing, "passing_stats_" + str(s.getYear()) + ".csv")
