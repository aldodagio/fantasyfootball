from sqlalchemy import create_engine, text, insert, select, MetaData

from nfl.Game import Game
from nfl.Passing import Passing
from nfl.Player import Player
from nfl.Receiving import Receiving
from nfl.Rushing import Rushing
from nfl.Season import Season
from nfl.Stats import Stats
from nfl.Team import Team


class Connection:
    def __init__(self):
        self.username = 'postgres'
        self.password = 'root'
        self.host_url = 'localhost'
        self.port = 5432
        self.database_name = 'postgres'
        self.app_schema = 'fantasyfootball'
        self.connection_string = f"postgresql+psycopg2://{self.username}:{self.password}@{self.host_url}:{self.port}/{self.database_name}"
        self.engine = create_engine(self.connection_string,
                                    connect_args={"options": f"-csearch_path={self.app_schema}"})

    def select_players(self):
        players = []
        with self.engine.connect() as conn:
            query = text("SELECT * FROM player")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player(row.first_name, row.last_name, row.position, row.team_id, row.id))
        return players

    def select_teams(self):
        teams = []
        with self.engine.connect() as conn:
            query = text("SELECT * FROM team")
            res = conn.execute(query)
            for row in res.all():
                teams.append(Team(row.name, row.id))
        return teams

    def select_team_id(self, name):
        with self.engine.connect() as conn:
            query = text(f"SELECT id FROM team WHERE name = \'{name}\'")
            result = conn.execute(query)
            row = result.fetchone()
            if row:
                return row[0]
            else:
                return None

    def select_season_id(self, year):
        with self.engine.connect() as conn:
            query = text(f"SELECT season_id FROM season WHERE year = {year}")
            result = conn.execute(query)
            id = result.scalar()
            return id

    def select_stats(self):
        stats = []
        with self.engine.connect() as conn:
            query = text("SELECT * FROM stats")
            res = conn.execute(query)
            for row in res.all():
                stats.append(
                    Stats(row.pass_id, row.rush_id, row.reception_id, row.stat_id, row.total_points, row.fumbles,
                          row.game_id, row.player_id))
        return stats

    def select_seasons(self):
        seasons = []
        with self.engine.connect() as conn:
            query = text("SELECT * FROM season")
            res = conn.execute(query)
            for row in res.all():
                seasons.append(Season(row.year, row.season_id))
        return seasons

    def select_all_rushing(self):
        rushing = []
        with self.engine.connect() as conn:
            query = text("SELECT * FROM rushing")
            res = conn.execute(query)
            for row in res.all():
                rushing.append(Rushing(row.attempts, row.yards, row.touchdowns, row.two_pt_conv, row.rush_id))
        return rushing

    def select_all_receiving(self):
        receiving = []
        with self.engine.connect() as conn:
            query = text("SELECT * FROM receiving")
            res = conn.execute(query)
            for row in res.all():
                receiving.append(
                    Receiving(row.receptions, row.yards, row.touchdowns, row.two_pt_conv, row.reception_id))
        return receiving

    def select_all_passing(self):
        passing = []
        with self.engine.connect() as conn:
            query = text("SELECT * FROM passing")
            res = conn.execute(query)
            for row in res.all():
                passing.append(Passing(row.attempts, row.completions, row.yards, row.touchdowns, row.two_pt_conv,
                                       row.interceptions, row.pass_id))
        return passing

    def select_games(self):
        games = []
        with self.engine.connect() as conn:
            query = text("SELECT * FROM game")
            res = conn.execute(query)
            for row in res.all():
                games.append(Game(row.home_team, row.away_team, row.season_id, row.game_id, row.week))
        return games

    def insert_player(self, first_name, last_name, position, team_id):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO player (first_name, last_name, position, team_id) 
                         SELECT :first_name,:last_name,:position,:team_id 
                         WHERE NOT EXISTS (SELECT 1 FROM player WHERE first_name=:first_name AND last_name=:last_name)""")
            result = conn.execute(query, {'first_name' : first_name,'last_name' : last_name,'position' : position,'team_id' : team_id})
            conn.commit()

    def insert_game(self, home_team_id, away_team_id, season_id, week):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO game (home_team_id, away_team_id, season_id, week) 
                         SELECT :home_team_id,:away_team_id,:season_id,:week 
                         WHERE NOT EXISTS (SELECT 1 FROM game WHERE home_team_id=:home_team_id AND away_team_id=:away_team_id AND season_id=:season_id AND week=:week)""")
            result = conn.execute(query, {'home_team_id' : home_team_id, 'away_team_id' : away_team_id, 'season_id' : season_id, 'week' : week})
            conn.commit()

    def select_player_id(self, first_name, last_name):
        with self.engine.connect() as conn:
            query = text("""SELECT id FROM player WHERE first_name = :first_name AND last_name = :last_name""")
            result = conn.execute(query, {'first_name' : first_name, 'last_name' : last_name})
            id = result.scalar()
            return id

    def select_game_id(self, home_team_id, away_team_id, year):
        with self.engine.connect() as conn:
            query = text("""SELECT game_id FROM game WHERE home_team_id = :home_team_id AND away_team_id = :away_team_id AND season_id = :year""")
            result = conn.execute(query, {'home_team_id' : home_team_id, 'away_team_id' : away_team_id, 'year' : year})
            id = result.scalar()
            return id

    def insert_stats(self, pass_id, rush_id, reception_id, total_points, fumbles, game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO stats (pass_id, rush_id, reception_id, total_points, fumbles, game_id, player_id) 
                         SELECT :pass_id, :rush_id, :reception_id, :total_points, :fumbles, :game_id, :player_id 
                         WHERE NOT EXISTS (SELECT 1 FROM stats WHERE game_id=:game_id AND player_id=:player_id AND pass_id=:pass_id AND rush_id=:rush_id AND reception_id=:reception_id)""")
            result = conn.execute(query, {'fumbles' : int(fumbles), 'game_id' : game_id, 'player_id' : player_id, 'pass_id' : pass_id, 'rush_id' : rush_id, 'reception_id' : reception_id, 'total_points' : float(total_points)})
            conn.commit()

    def insert_stats(self, stats):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO stats (pass_id, rush_id, reception_id, total_points, fumbles, game_id, player_id) 
                         SELECT :stats.pass_id, :stats.rush_id, :stats.reception_id, :stats.total_points, :stats.fumbles, :stats.game_id, :stats.player_id 
                         WHERE NOT EXISTS (SELECT 1 FROM stats WHERE game_id=:stats.game_id AND player_id=:stats.player_id AND pass_id=:stats.pass_id AND rush_id=:stats.rush_id AND reception_id=:stats.reception_id)""")
            result = conn.execute(query, {'fumbles' : int(stats.fumbles), 'game_id' : stats.game_id, 'player_id' : stats.player_id, 'pass_id' : stats.pass_id, 'rush_id' : stats.rush_id, 'reception_id' : stats.reception_id, 'total_points' : float(stats.total_points)})
            conn.commit()

    def select_pass_id(self, game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""SELECT pass_id FROM passing WHERE game_id = :game_id AND player_id = :player_id""")
            result = conn.execute(query, {'game_id' : game_id, 'player_id' : player_id})
            id = result.scalar()
            return id

    def select_rush_id(self, game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""SELECT rush_id FROM rushing WHERE game_id = :game_id AND player_id = :player_id""")
            result = conn.execute(query, {'game_id': game_id, 'player_id': player_id})
            id = result.scalar()
            return id

    def select_reception_id(self, game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""SELECT reception_id FROM receiving WHERE game_id = :game_id AND player_id = :player_id""")
            result = conn.execute(query, {'game_id' : game_id, 'player_id' : player_id})
            id = result.scalar()
            return id

    def insert_season(self, year):
        with self.engine.connect() as conn:
            stmt = insert("season").values(year=year)
            result = conn.execute(stmt)
            conn.commit()

    def insert_passing(self, passing_attempts, passing_completions, passing_yards, passing_touchdowns, interceptions, passing_2pt_conversions):
        with self.engine.connect() as conn:
            stmt = insert("passing").values(passing_attempts=passing_attempts, passing_completions=passing_completions, passing_yards=passing_yards,
                                           passing_touchdowns=passing_touchdowns, interceptions=interceptions, passing_2pt_conversions=passing_2pt_conversions)
            result = conn.execute(stmt)
            conn.commit()

    def insert_rushing(self, rushing_attempts, rushing_yards, rushing_touchdowns, rushing_two_point_conversions, game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO rushing (rushing_attempts, rushing_yards, rushing_touchdowns, rushing_two_point_conversions, game_id, player_id) 
                                SELECT :rushing_attempts, :rushing_yards, :rushing_touchdowns, :rushing_two_point_conversions, :game_id, :player_id
                                WHERE NOT EXISTS (SELECT 1 FROM rushing WHERE game_id=:game_id AND player_id=:player_id)""")
            result = conn.execute(query,
                                  {'rushing_attempts': int(rushing_attempts), 'rushing_yards': int(rushing_yards), 'rushing_touchdowns': int(rushing_touchdowns), 'rushing_two_point_conversions': int(rushing_two_point_conversions),
                                   'game_id': game_id, 'player_id': player_id})
            conn.commit()

    def insert_rushing(self, rushing):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO rushing (rushing_attempts, rushing_yards, rushing_touchdowns, rushing_two_point_conversions, game_id, player_id) 
                                SELECT :rushing_attempts, :rushing.rushing_yards, :rushing.rushing_touchdowns, :rushing.rushing_two_point_conversions, :rushing.game_id, :rushing.player_id
                                WHERE NOT EXISTS (SELECT 1 FROM rushing WHERE game_id=:rushing.game_id AND player_id=:rushing.player_id)""")
            result = conn.execute(query,
                                  {'rushing_attempts': int(rushing.rushing_attempts), 'rushing_yards': int(rushing.rushing_yards), 'rushing_touchdowns': int(rushing.rushing_touchdowns), 'rushing_two_point_conversions': int(rushing.rushing_two_point_conversions),
                                   'game_id': rushing.game_id, 'player_id': rushing.player_id})
            conn.commit()

    def insert_receiving(self, receptions, receiving_yards, receiving_touchdowns, receiving_two_point_conversions, game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO receiving (receptions, receiving_yards, receiving_touchdowns, receiving_two_point_conversions, game_id, player_id) 
                                            SELECT :receptions, :receiving_yards, :receiving_touchdowns, :receiving_two_point_conversions, :game_id, :player_id
                                            WHERE NOT EXISTS (SELECT 1 FROM receiving WHERE game_id=:game_id AND player_id=:player_id)""")
            result = conn.execute(query,
                                  {'receptions': int(receptions), 'receiving_yards': int(receiving_yards),
                                   'receiving_touchdowns': int(receiving_touchdowns),
                                   'receiving_two_point_conversions': int(receiving_two_point_conversions),
                                   'game_id': game_id, 'player_id': player_id})
            conn.commit()

    def insert_receiving(self, receiving):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO receiving (receptions, receiving_yards, receiving_touchdowns, receiving_two_point_conversions, game_id, player_id) 
                                            SELECT :receiving.receptions, :receiving.receiving_yards, :receiving.receiving_touchdowns, :receiving.receiving_two_point_conversions, :receiving.game_id, :receiving.player_id
                                            WHERE NOT EXISTS (SELECT 1 FROM receiving WHERE game_id=:receiving.game_id AND player_id=:receiving.player_id)""")
            result = conn.execute(query,
                                  {'receptions': int(receiving.receptions), 'receiving_yards': int(receiving.receiving_yards),
                                   'receiving_touchdowns': int(receiving.receiving_touchdowns),
                                   'receiving_two_point_conversions': int(receiving.receiving_two_point_conversions),
                                   'game_id': receiving.game_id, 'player_id': receiving.player_id})

    def insert_passing(self, passing):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO passing (passing_attempts, passing_completions, passing_yards, passing_touchdowns, passing_two_point_conversions, interceptions, game_id, player_id) 
                                            SELECT :passing.passing_attempts, :passing.passing_completions, :passing.passing_yards, :passing.passing_touchdowns, :passing.passing_two_point_conversions, :passing.interceptions, :passing.game_id, :passing.player_id
                                            WHERE NOT EXISTS (SELECT 1 FROM passing WHERE game_id=:passing.game_id AND player_id=:passing.player_id)""")
            result = conn.execute(query,
                                  {'passing_attempts' : passing.passing_attempts, 'passing_completions' : passing.passing_completions, 'passing_yards' : passing.passing_yards, 'passing_touchdowns' : passing.passing_touchdowns, 'passing_two_point_conversions' : passing.passing_two_point_conversions, 'interceptions' : passing.interceptions, 'game_id' : passing.game_id, 'player_id' : passing.player_id})
            conn.commit()