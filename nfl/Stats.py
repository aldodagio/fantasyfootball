from psycopg2 import IntegrityError

from nfl.Passing import Passing
from nfl.Receiving import Receiving
from nfl.Rushing import Rushing


class Stats:
    def __init__(self, pass_id, rush_id, reception_id, id, total_points, fumbles, game_id, player_id):
        self.pass_id = pass_id
        self.rush_id = rush_id
        self.reception_id = reception_id
        self.id = id
        self.total_points = total_points
        self.fumbles = fumbles
        self.game_id = game_id
        self.player_id = player_id
    def __init__(self, pass_id, rush_id, reception_id, total_points, fumbles, game_id, player_id):
        self.pass_id = pass_id
        self.rush_id = rush_id
        self.reception_id = reception_id
        self.total_points = total_points
        self.fumbles = fumbles
        self.game_id = game_id
        self.player_id = player_id

    def insert_stats(self, row, game_id, player_id, db):
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
        elif position == 'Wide Receiver' or position == 'Tight End':
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