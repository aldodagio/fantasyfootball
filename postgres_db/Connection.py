from sqlalchemy import create_engine, text, insert, select, MetaData

from machinelearning.Prediction import Prediction
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

    def get_linear_regression_predictions_qb(self, year):
        predictions = []
        with self.engine.connect() as conn:
            query = text(f"select * from quarterback_predictions_{year} order by predicted_stats desc")
            res = conn.execute(query)
            for row in res.all():
                predictions.append(Prediction(row.player_name, row.predicted_stats))
        return predictions

    def get_linear_regression_predictions_k(self, year):
        predictions = []
        with self.engine.connect() as conn:
            query = text(f"select * from kicker_predictions_{year} order by predicted_stats desc")
            res = conn.execute(query)
            for row in res.all():
                predictions.append(Prediction(row.player_name, row.predicted_stats))
        return predictions

    def get_linear_regression_predictions_dst(self, year):
        predictions = []
        with self.engine.connect() as conn:
            query = text(f"select * from defense_predictions_{year} order by predicted_stats desc")
            res = conn.execute(query)
            for row in res.all():
                predictions.append(Prediction(row.player_name, row.predicted_stats))
        return predictions

    def get_linear_regression_predictions_rb(self, year):
        predictions = []
        with self.engine.connect() as conn:
            query = text(f"select * from running_back_predictions_{year} order by predicted_stats desc")
            res = conn.execute(query)
            for row in res.all():
                predictions.append(Prediction(row.player_name, row.predicted_stats))
        return predictions

    def get_linear_regression_predictions_wr(self, year):
        predictions = []
        with self.engine.connect() as conn:
            query = text(f"select * from wide_receiver_predictions_{year} order by predicted_stats desc")
            res = conn.execute(query)
            for row in res.all():
                predictions.append(Prediction(row.player_name, row.predicted_stats))
        return predictions

    def get_linear_regression_predictions_te(self, year):
        predictions = []
        with self.engine.connect() as conn:
            query = text(f"select * from tight_end_predictions_{year} order by predicted_stats desc")
            res = conn.execute(query)
            for row in res.all():
                predictions.append(Prediction(row.player_name, row.predicted_stats))
        return predictions

    def select_nonqb_with_total_points(self, year):
        players = []
        with self.engine.connect() as conn:
            query = text(
                f"select first_name, last_name, position, s.player_id as id, year, sum(total_points) as points from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} AND position != 'Quarterback' "
                f"group by s.player_id, last_name, position, year, first_name "
                f"order by points desc")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player.with_points_and_id(row.first_name, row.last_name, row.position, points=row.points,
                                                         id=row.id))
        return players

    def select_all_with_total_points(self, year):
        players = []
        with self.engine.connect() as conn:
            query = text(
                f"select first_name, last_name, position, s.player_id as id, year, sum(total_points) as points from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} "
                f"group by s.player_id, last_name, position, year, first_name "
                f"order by points desc")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player.with_points_and_id(row.first_name, row.last_name, row.position, points=row.points,
                                                         id=row.id))
        return players

    def get_linear_regression_predictions_all(self, year):
        predictions = []
        with self.engine.connect() as conn:
            query = text(f"select * from tight_end_predictions_{year} union "
                    f"select * from wide_receiver_predictions_{year} union "
                    f"select * from running_back_predictions_{year} union "
                    f"select * from quarterback_predictions_{year} "
                    f"order by predicted_stats desc")
            res = conn.execute(query)
            for row in res.all():
                predictions.append(Prediction(row.player_name, row.predicted_stats))
        return predictions

    def get_linear_regression_predictions_nonqb(self, year):
        predictions = []
        with self.engine.connect() as conn:
            query = text(f"select * from tight_end_predictions_{year} union "
                    f"select * from wide_receiver_predictions_{year} union "
                    f"select * from running_back_predictions_{year} "
                    f"order by predicted_stats desc")
            res = conn.execute(query)
            for row in res.all():
                predictions.append(Prediction(row.player_name, row.predicted_stats))
        return predictions

    def select_players(self):
        players = []
        with self.engine.connect() as conn:
            query = text("SELECT * FROM player")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player(row.first_name, row.last_name, row.position, row.team_id, row.id))
        return players

    def select_current_qbs(self):
        players = []
        qb = 'Quarterback'
        with self.engine.connect() as conn:
            query = text(
                f"select first_name, last_name, player.id as player_id, g.season_id as season_id, name from player "
                f"INNER JOIN stats s on player.id = s.player_id "
                f"INNER JOIN game g on g.game_id = s.game_id "
                f"INNER JOIN team t on player.team_id = t.id "
                f"where g.season_id=14 and position = \'{qb}\' "
                f"GROUP BY first_name, last_name, player.id, g.season_id, name "
                f"ORDER BY first_name ASC")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player(row.first_name, row.last_name, position=qb, id=row.player_id, team=row.name))
        return players

    def select_current_rbs(self):
        players = []
        rb = 'Running Back'
        with self.engine.connect() as conn:
            query = text(
                f"select first_name, last_name, player.id as player_id, g.season_id as season_id, name from player "
                f"INNER JOIN stats s on player.id = s.player_id "
                f"INNER JOIN game g on g.game_id = s.game_id "
                f"INNER JOIN team t on player.team_id = t.id "
                f"where g.season_id=14 and position = \'{rb}\' "
                f"GROUP BY first_name, last_name, player.id, g.season_id, name "
                f"ORDER BY first_name ASC")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player(row.first_name, row.last_name, position=rb, id=row.player_id, team=row.name))
        return players

    def select_current_wrs(self):
        players = []
        wr = 'Wide Receiver'
        with self.engine.connect() as conn:
            query = text(
                f"select first_name, last_name, player.id as player_id, g.season_id as season_id, name from player "
                f"INNER JOIN stats s on player.id = s.player_id "
                f"INNER JOIN game g on g.game_id = s.game_id "
                f"INNER JOIN team t on player.team_id = t.id "
                f"where g.season_id=14 and position = \'{wr}\' "
                f"GROUP BY first_name, last_name, player.id, g.season_id, name "
                f"ORDER BY first_name ASC")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player(row.first_name, row.last_name, position=wr, id=row.player_id, team=row.name))
        return players

    def player_dropdown(self, year):
        players = []
        with self.engine.connect() as conn:
            query = text(
                f"select DISTINCT(concat(first_name,' ', last_name)) as player_name, first_name, last_name, position, year from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year}")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player(row.first_name, row.last_name, row.position))
        return players

    def select_current_tes(self):
        players = []
        te = 'Tight End'
        with self.engine.connect() as conn:
            query = text(
                f"select first_name, last_name, player.id as player_id, g.season_id as season_id, name from player "
                f"INNER JOIN stats s on player.id = s.player_id "
                f"INNER JOIN game g on g.game_id = s.game_id "
                f"where g.season_id=14 and position = \'{te}\' "
                f"GROUP BY first_name, last_name, player.id, g.season_id, name "
                f"ORDER BY first_name ASC")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player(row.first_name, row.last_name, position=te, id=row.player_id, team=row.name))
        return players

    def qb_dropdown(self, year):
        players = []
        with self.engine.connect() as conn:
            qb = 'Quarterback'
            query = text(
                f"select DISTINCT(concat(first_name,' ', last_name)) as player_name, first_name, last_name, position, year from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{qb}\'")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player(row.first_name, row.last_name, row.position))
        return players

    def select_players_with_total_points(self, year):
        players = []
        with self.engine.connect() as conn:
            query = text(f"select first_name, last_name, position, year, sum(total_points) as points from player "
                         f"inner join stats s on player.id = s.player_id "
                         f"inner join game g on g.game_id = s.game_id "
                         f"inner join season s2 on s2.season_id = g.season_id "
                         f"where g.season_id = {year} "
                         f"group by last_name, position, year, first_name "
                         f"order by points desc")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player.with_points(row.first_name, row.last_name, row.position, points=row.points))
        return players

    def select_all_teams(self):
        teams = []
        with self.engine.connect() as conn:
            query = text(f"select name, id from team")
            res = conn.execute(query)
            for row in res.all():
                teams.append(Team(row.name, row.id))
        return teams

    def select_qbs_with_total_points(self, year):
        players = []
        with self.engine.connect() as conn:
            qb = 'Quarterback'
            query = text(
                f"select player.id as id, first_name, last_name, position, year, sum(total_points) as points from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{qb}\' "
                f"group by player.id, last_name, position, year, first_name "
                f"order by points desc")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player.with_points_and_id(row.first_name, row.last_name, row.position, points=row.points,
                                                         id=row.id))
        return players

    def select_k_with_total_points(self, year):
        players = []
        with self.engine.connect() as conn:
            qb = 'Kicker'
            query = text(
                f"select player.id as id, first_name, last_name, position, year, sum(total_points) as points from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{qb}\' "
                f"group by player.id, last_name, position, year, first_name "
                f"order by points desc")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player.with_points_and_id(row.first_name, row.last_name, row.position, points=row.points,
                                                         id=row.id))
        return players

    def select_dst_with_total_points(self, year):
        players = []
        with self.engine.connect() as conn:
            qb = 'Defense/Special Teams'
            query = text(
                f"select player.id as id, first_name, last_name, position, year, sum(total_points) as points from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{qb}\' "
                f"group by player.id, last_name, position, year, first_name "
                f"order by points desc")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player.with_points_and_id(row.first_name, row.last_name, row.position, points=row.points,
                                                         id=row.id))
        return players

    def get_qb_stats(self, player_id, season_id):
        players = []
        with self.engine.connect() as conn:
            qb = 'Quarterback'
            query = text(
                f"select first_name, last_name, total_points, fumbles, passing_yards, passing_touchdowns, "
                f"passing_attempts, passing_completions, passing_touchdowns, rushing_yards, rushing_attempts, rushing_touchdowns, "
                f"receiving_touchdowns, receptions, receiving_yards, interceptions, t.name as away_team, t2.name as home_team "
                f"from player inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join team t on g.away_team_id = t.id "
                f"inner join team t2 on t2.id = g.home_team_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"inner join receiving r on r.reception_id= s.reception_id "
                f"inner join rushing r2 on r2.rush_id = s.rush_id "
                f"inner join passing p on p.pass_id = s.pass_id "
                f"where g.season_id = {season_id} and position = \'{qb}\' and s.player_id = {player_id}")
            res = conn.execute(query)
            for row in res.all():
                players.append(
                    Player.qb_with_all_stats(row.first_name, row.last_name, qb, row.total_points, row.fumbles,
                                             row.passing_yards, row.passing_touchdowns,
                                             row.passing_attempts, row.passing_completions, row.rushing_yards, row.rushing_attempts,
                                             row.rushing_touchdowns,
                                             row.receiving_touchdowns, row.receptions, row.receiving_yards,
                                             row.interceptions, row.away_team, row.home_team))
        return players

    def get_wrs_stats(self, player_id, season_id):
        players = []
        with self.engine.connect() as conn:
            qb = 'Wide Receiver'
            query = text(
                f"select first_name, last_name, total_points, fumbles, passing_yards, passing_touchdowns, "
                f"passing_attempts, passing_completions, passing_touchdowns, rushing_yards, rushing_attempts, rushing_touchdowns, "
                f"receiving_touchdowns, receptions, receiving_yards, interceptions, t.name as away_team, t2.name as home_team "
                f"from player inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join team t on g.away_team_id = t.id "
                f"inner join team t2 on t2.id = g.home_team_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"inner join receiving r on r.reception_id= s.reception_id "
                f"inner join rushing r2 on r2.rush_id = s.rush_id "
                f"inner join passing p on p.pass_id = s.pass_id "
                f"where g.season_id = {season_id} and position = \'{qb}\' and s.player_id = {player_id}")
            res = conn.execute(query)
            for row in res.all():
                players.append(
                    Player.qb_with_all_stats(row.first_name, row.last_name, qb, row.total_points, row.fumbles,
                                             row.passing_yards, row.passing_touchdowns,
                                             row.passing_attempts, row.passing_completions, row.rushing_yards, row.rushing_attempts,
                                             row.rushing_touchdowns,
                                             row.receiving_touchdowns, row.receptions, row.receiving_yards,
                                             row.interceptions, row.away_team, row.home_team))
        return players

    def get_te_stats(self, player_id, season_id):
        players = []
        with self.engine.connect() as conn:
            qb = 'Tight End'
            query = text(
                f"select first_name, last_name, total_points, fumbles, passing_yards, passing_touchdowns, "
                f"passing_attempts, passing_completions, passing_touchdowns, rushing_yards, rushing_attempts, rushing_touchdowns, "
                f"receiving_touchdowns, receptions, receiving_yards, interceptions, t.name as away_team, t2.name as home_team "
                f"from player inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join team t on g.away_team_id = t.id "
                f"inner join team t2 on t2.id = g.home_team_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"inner join receiving r on r.reception_id= s.reception_id "
                f"inner join rushing r2 on r2.rush_id = s.rush_id "
                f"inner join passing p on p.pass_id = s.pass_id "
                f"where g.season_id = {season_id} and position = \'{qb}\' and s.player_id = {player_id}")
            res = conn.execute(query)
            for row in res.all():
                players.append(
                    Player.qb_with_all_stats(row.first_name, row.last_name, qb, row.total_points, row.fumbles,
                                             row.passing_yards, row.passing_touchdowns,
                                             row.passing_attempts, row.passing_completions, row.rushing_yards, row.rushing_attempts,
                                             row.rushing_touchdowns,
                                             row.receiving_touchdowns, row.receptions, row.receiving_yards,
                                             row.interceptions, row.away_team, row.home_team))
        return players

    def get_te_stats(self, player_id, season_id):
        players = []
        with self.engine.connect() as conn:
            qb = 'Tight End'
            query = text(
                f"select first_name, last_name, total_points, fumbles, passing_yards, passing_touchdowns, "
                f"passing_attempts, passing_completions, passing_touchdowns, rushing_yards, rushing_attempts, rushing_touchdowns, "
                f"receiving_touchdowns, receptions, receiving_yards, interceptions, t.name as away_team, t2.name as home_team "
                f"from player inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join team t on g.away_team_id = t.id "
                f"inner join team t2 on t2.id = g.home_team_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"inner join receiving r on r.reception_id= s.reception_id "
                f"inner join rushing r2 on r2.rush_id = s.rush_id "
                f"inner join passing p on p.pass_id = s.pass_id "
                f"where g.season_id = {season_id} and position = \'{qb}\' and s.player_id = {player_id}")
            res = conn.execute(query)
            for row in res.all():
                players.append(
                    Player.qb_with_all_stats(row.first_name, row.last_name, qb, row.total_points, row.fumbles,
                                             row.passing_yards, row.passing_touchdowns,
                                             row.passing_attempts, row.passing_completions, row.rushing_yards, row.rushing_attempts,
                                             row.rushing_touchdowns,
                                             row.receiving_touchdowns, row.receptions, row.receiving_yards,
                                             row.interceptions, row.away_team, row.home_team))
        return players

    def get_rb_stats(self, player_id, season_id):
        players = []
        with self.engine.connect() as conn:
            qb = 'Running Back'
            query = text(
                f"select first_name, last_name, total_points, fumbles, passing_yards, passing_touchdowns, "
                f"passing_attempts, passing_completions, passing_touchdowns, rushing_yards, rushing_attempts, rushing_touchdowns, "
                f"receiving_touchdowns, receptions, receiving_yards, interceptions, t.name as away_team, t2.name as home_team "
                f"from player inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join team t on g.away_team_id = t.id "
                f"inner join team t2 on t2.id = g.home_team_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"inner join receiving r on r.reception_id= s.reception_id "
                f"inner join rushing r2 on r2.rush_id = s.rush_id "
                f"inner join passing p on p.pass_id = s.pass_id "
                f"where g.season_id = {season_id} and position = \'{qb}\' and s.player_id = {player_id}")
            res = conn.execute(query)
            for row in res.all():
                players.append(
                    Player.qb_with_all_stats(row.first_name, row.last_name, qb, row.total_points, row.fumbles,
                                             row.passing_yards, row.passing_touchdowns,
                                             row.passing_attempts, row.passing_completions, row.rushing_yards, row.rushing_attempts,
                                             row.rushing_touchdowns,
                                             row.receiving_touchdowns, row.receptions, row.receiving_yards,
                                             row.interceptions, row.away_team, row.home_team))
        return players

    def update_team(self, name, new_name):
        with self.engine.connect() as conn:
            query = text(f"UPDATE team SET name = \'{new_name}\' WHERE name = \'{name}\'")
            res = conn.execute(query)

    def update_player_last_name(self, id, last_name):
        with self.engine.connect() as conn:
            query = text(f"UPDATE player SET last_name = :last_name WHERE id = :id")
            conn.execute(query, {'last_name': last_name, 'id': id})
            conn.commit()

    def update_player_first_name(self, id, first_name):
        with self.engine.connect() as conn:
            query = text(f"UPDATE player SET first_name = :first_name WHERE id = :id")
            conn.execute(query, {'first_name': first_name, 'id': id})
            conn.commit()

    def update_player_team(self, id, team_name):
        with self.engine.connect() as conn:
            query = text(
                f"UPDATE player SET team_id=(select id from team where name = :team_name) FROM team WHERE player.id = :id")
            conn.execute(query, {'team_name': team_name, 'id': id})
            conn.commit()

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

    def select_rbs_with_total_points(self, year):
        players = []
        with self.engine.connect() as conn:
            rb = 'Running Back'
            query = text(
                f"select first_name, last_name, position, year, sum(total_points) as points, s.player_id as id from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{rb}\' "
                f"group by s.player_id, last_name, position, year, first_name "
                f"order by points desc")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player.with_points_and_id(row.first_name, row.last_name, row.position, points=row.points,
                                                         id=row.id))
        return players

    def rb_dropdown(self, year):
        players = []
        with self.engine.connect() as conn:
            rb = 'Running Back'
            query = text(
                f"select DISTINCT(concat(first_name,' ', last_name)) as player_name, "
                f"first_name, last_name, position, year from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{rb}\'")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player(row.first_name, row.last_name, row.position))
        return players

    def select_wrs_with_total_points(self, year):
        players = []
        with self.engine.connect() as conn:
            wr = 'Wide Receiver'
            query = text(
                f"select first_name, last_name, position, year, s.player_id as id, sum(total_points) as points from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{wr}\' "
                f"group by s.player_id, last_name, position, year, first_name "
                f"order by points desc")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player.with_points_and_id(row.first_name, row.last_name, row.position, points=row.points,
                                                         id=row.id))
        return players

    def wr_dropdown(self, year):
        players = []
        with self.engine.connect() as conn:
            wr = 'Wide Receiver'
            query = text(
                f"select DISTINCT(concat(first_name,' ', last_name)) as player_name, first_name, last_name, position, year from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{wr}\'")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player(row.first_name, row.last_name, row.position))
        return players

    def select_tes_with_total_points(self, year):
        players = []
        with self.engine.connect() as conn:
            te = 'Tight End'
            query = text(
                f"select first_name, last_name, position, s.player_id as id, year, sum(total_points) as points from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{te}\' "
                f"group by s.player_id, last_name, position, year, first_name "
                f"order by points desc")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player.with_points_and_id(row.first_name, row.last_name, row.position, points=row.points,
                                                         id=row.id))
        return players

    def select_k_with_total_points(self, year):
        players = []
        with self.engine.connect() as conn:
            k = 'Kicker'
            query = text(
                f"select first_name, last_name, position, s.player_id as id, year, sum(total_points) as points from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{k}\' "
                f"group by s.player_id, last_name, position, year, first_name "
                f"order by points desc")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player.with_points_and_id(row.first_name, row.last_name, row.position, points=row.points,
                                                         id=row.id))
        return players

    def select_dst_with_total_points(self, year):
        players = []
        with self.engine.connect() as conn:
            dst = 'Defense/Special Teams'
            query = text(
                f"select first_name, last_name, position, s.player_id as id, year, sum(total_points) as points from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{dst}\' "
                f"group by s.player_id, last_name, position, year, first_name "
                f"order by points desc")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player.with_points_and_id(row.first_name, row.last_name, row.position, points=row.points,
                                                         id=row.id))
        return players

    def te_dropdown(self, year):
        players = []
        with self.engine.connect() as conn:
            te = 'Tight End'
            query = text(
                f"select DISTINCT(concat(first_name,' ', last_name)) as player_name, first_name, last_name, position, year from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{te}\'")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player(row.first_name, row.last_name, row.position))
        return players

    def k_dropdown(self, year):
        players = []
        with self.engine.connect() as conn:
            k = 'Kicker'
            query = text(
                f"select DISTINCT(concat(first_name,' ', last_name)) as player_name, first_name, last_name, position, year from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{k}\'")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player(row.first_name, row.last_name, row.position))
        return players

    def dst_dropdown(self, year):
        players = []
        with self.engine.connect() as conn:
            dst = 'Defense/Special Teams'
            query = text(
                f"select DISTINCT(concat(first_name,' ', last_name)) as player_name, first_name, last_name, position, year from player "
                f"inner join stats s on player.id = s.player_id "
                f"inner join game g on g.game_id = s.game_id "
                f"inner join season s2 on s2.season_id = g.season_id "
                f"where g.season_id = {year} and position = \'{dst}\'")
            res = conn.execute(query)
            for row in res.all():
                players.append(Player(row.first_name, row.last_name, row.position))
        return players

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
                seasons.append(Season(row.season_id, row.year))
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
            result = conn.execute(query, {'first_name': first_name, 'last_name': last_name, 'position': position,
                                          'team_id': team_id})
            conn.commit()

    def insert_game(self, home_team_id, away_team_id, season_id, week):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO game (home_team_id, away_team_id, season_id, week) 
                         SELECT :home_team_id,:away_team_id,:season_id,:week 
                         WHERE NOT EXISTS (SELECT 1 FROM game WHERE home_team_id=:home_team_id AND away_team_id=:away_team_id AND season_id=:season_id AND week=:week)""")
            result = conn.execute(query,
                                  {'home_team_id': home_team_id, 'away_team_id': away_team_id, 'season_id': season_id,
                                   'week': week})
            conn.commit()

    def select_player_id(self, first_name, last_name):
        with self.engine.connect() as conn:
            query = text("""SELECT id FROM player WHERE first_name = :first_name AND last_name = :last_name""")
            result = conn.execute(query, {'first_name': first_name, 'last_name': last_name})
            id = result.scalar()
            return id

    def select_position(self, first_name, last_name):
        with self.engine.connect() as conn:
            query = text("""SELECT position FROM player WHERE first_name = :first_name AND last_name = :last_name""")
            result = conn.execute(query, {'first_name': first_name, 'last_name': last_name})
            position = result.scalar()
            return position

    def select_game_id(self, home_team_id, away_team_id, year):
        with self.engine.connect() as conn:
            query = text(
                """SELECT game_id FROM game WHERE home_team_id = :home_team_id AND away_team_id = :away_team_id AND season_id = :year""")
            result = conn.execute(query, {'home_team_id': home_team_id, 'away_team_id': away_team_id, 'year': year})
            id = result.scalar()
            return id

    def select_game_id_based_on_players_team(self, team, season_id, week):
        with self.engine.connect() as conn:
            query = text(
                """SELECT game_id FROM game 
                WHERE season_id = :season_id AND week = :week and (home_team_id=:team or away_team_id=:team)""")
            result = conn.execute(query, {'team': team, 'season_id': season_id, 'week': week})
            id = result.scalar()
            return id

    def select_team_id_from_team_name(self, team_name):
        with self.engine.connect() as conn:
            query = text(
                """SELECT id FROM team WHERE name=:team_name""")
            result = conn.execute(query, {'team_name': team_name})
            id = result.scalar()
            return id
    def insert_stats(self, pass_id, rush_id, reception_id, total_points, fumbles, game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO stats (pass_id, rush_id, reception_id, total_points, fumbles, game_id, player_id) 
                         SELECT :pass_id, :rush_id, :reception_id, :total_points, :fumbles, :game_id, :player_id 
                         WHERE NOT EXISTS (SELECT 1 FROM stats WHERE game_id=:game_id AND player_id=:player_id AND pass_id=:pass_id AND rush_id=:rush_id AND reception_id=:reception_id)""")
            result = conn.execute(query, {'fumbles': int(fumbles), 'game_id': game_id, 'player_id': player_id,
                                          'pass_id': pass_id, 'rush_id': rush_id, 'reception_id': reception_id,
                                          'total_points': float(total_points)})
            conn.commit()

    def insert_stats_for_kicker(self, kicker_id, total_points, game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO stats (kicker_id, total_points, game_id, player_id) 
                         SELECT :kicker_id, :total_points, :game_id, :player_id 
                         WHERE NOT EXISTS (SELECT 1 FROM stats WHERE game_id=:game_id AND player_id=:player_id AND kicker_id=:kicker_id)""")
            result = conn.execute(query, {'game_id': game_id, 'player_id': player_id,
                                          'kicker_id': kicker_id, 'total_points': float(total_points)})
            conn.commit()

    def insert_stats_for_dst(self, defense_id, total_points, game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO stats (defense_id, total_points, game_id, player_id) 
                         SELECT :defense_id, :total_points, :game_id, :player_id 
                         WHERE NOT EXISTS (SELECT 1 FROM stats WHERE game_id=:game_id AND player_id=:player_id AND defense_id=:defense_id)""")
            result = conn.execute(query, {'game_id': game_id, 'player_id': player_id,
                                          'defense_id': defense_id, 'total_points': float(total_points)})
            conn.commit()

    # def insert_stats(self, stats):
    #     with self.engine.connect() as conn:
    #         query = text("""INSERT INTO stats (pass_id, rush_id, reception_id, total_points, fumbles, game_id, player_id)
    #                         SELECT :pass_id, :rush_id, :reception_id, :total_points, :fumbles, :game_id, :player_id
    #                         WHERE NOT EXISTS (
    #                             SELECT 1 FROM stats
    #                             WHERE game_id = :game_id
    #                             AND player_id = :player_id
    #                             AND pass_id = :pass_id
    #                             AND rush_id = :rush_id
    #                             AND reception_id = :reception_id
    #                         )""")
    #         result = conn.execute(query, {
    #             'fumbles': int(stats.fumbles),
    #             'game_id': stats.game_id,
    #             'player_id': stats.player_id,
    #             'pass_id': stats.pass_id,
    #             'rush_id': stats.rush_id,
    #             'reception_id': stats.reception_id,
    #             'total_points': float(stats.total_points)
    #         })
    #         conn.commit()
    def select_pass_id(self, game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""SELECT pass_id FROM passing WHERE game_id = :game_id AND player_id = :player_id""")
            result = conn.execute(query, {'game_id': game_id, 'player_id': player_id})
            id = result.scalar()
            return id

    def select_kicking_id(self, game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""SELECT kicker_id FROM kicking WHERE game_id = :game_id AND player_id = :player_id""")
            result = conn.execute(query, {'game_id': game_id, 'player_id': player_id})
            id = result.scalar()
            return id

    def select_dst_id(self, game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""SELECT defense_id FROM defense WHERE game_id = :game_id AND player_id = :player_id""")
            result = conn.execute(query, {'game_id': game_id, 'player_id': player_id})
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
            result = conn.execute(query, {'game_id': game_id, 'player_id': player_id})
            id = result.scalar()
            return id

    def insert_season(self, year):
        with self.engine.connect() as conn:
            stmt = insert("season").values(year=year)
            result = conn.execute(stmt)
            conn.commit()

    def insert_passing(self, passing_attempts, passing_completions, passing_yards, passing_touchdowns, interceptions,
                       passing_two_point_conversions, game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO passing (passing_attempts, passing_completions, passing_yards, passing_touchdowns, interceptions, passing_two_point_conversions, game_id, player_id) 
                                SELECT :passing_attempts, :passing_completions, :passing_yards, :passing_touchdowns, :interceptions, :passing_two_point_conversions, :game_id, :player_id
                                WHERE NOT EXISTS (SELECT 1 FROM passing WHERE game_id=:game_id AND player_id=:player_id)""")
            result = conn.execute(query,
                                  {'passing_attempts': int(passing_attempts),
                                   'passing_completions': int(passing_completions),
                                   'passing_yards': int(passing_yards),
                                   'passing_touchdowns': int(passing_touchdowns), 'interceptions': int(interceptions),
                                   'passing_two_point_conversions': int(passing_two_point_conversions),
                                   'game_id': game_id, 'player_id': player_id})
            conn.commit()

    def insert_rushing(self, rushing_attempts, rushing_yards, rushing_touchdowns, rushing_two_point_conversions,
                       game_id, player_id):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO rushing (rushing_attempts, rushing_yards, rushing_touchdowns, rushing_two_point_conversions, game_id, player_id) 
                                SELECT :rushing_attempts, :rushing_yards, :rushing_touchdowns, :rushing_two_point_conversions, :game_id, :player_id
                                WHERE NOT EXISTS (SELECT 1 FROM rushing WHERE game_id=:game_id AND player_id=:player_id)""")
            result = conn.execute(query,
                                  {'rushing_attempts': int(rushing_attempts), 'rushing_yards': int(rushing_yards),
                                   'rushing_touchdowns': int(rushing_touchdowns),
                                   'rushing_two_point_conversions': int(rushing_two_point_conversions),
                                   'game_id': game_id, 'player_id': player_id})
            conn.commit()

    def insert_kicking(self, extra_point_attempts, extra_points_made, field_goal_attempts, field_goals_made, fifty_yard_field_goals_made, player_id, game_id):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO kicking (extra_point_attempts, extra_points_made, field_goal_attempts, field_goals_made, fifty_yard_field_goals_made, player_id, game_id) 
                                SELECT :extra_point_attempts, :extra_points_made, :field_goal_attempts, :field_goals_made, :fifty_yard_field_goals_made, :player_id, :game_id
                                WHERE NOT EXISTS (SELECT 1 FROM kicking WHERE game_id=:game_id AND player_id=:player_id)""")
            result = conn.execute(query,
                                  {'extra_point_attempts': int(extra_point_attempts), 'extra_points_made': int(extra_points_made),
                                   'field_goal_attempts': int(field_goal_attempts),
                                   'field_goals_made': int(field_goals_made),
                                   'fifty_yard_field_goals_made': int(fifty_yard_field_goals_made),
                                   'game_id': game_id, 'player_id': player_id})
            conn.commit()

    def insert_defense_special_teams(self, sacks, interceptions, safeties, fumble_recoveries, blocked_kicks, touchdowns, points_allowed, pass_yards_allowed, rush_yards_allowed, total_yards_allowed, player_id, game_id):
        with self.engine.connect() as conn:
            query = text("""INSERT INTO defense (sacks, interceptions, safeties, fumble_recoveries, blocked_kicks, touchdowns, points_allowed, pass_yards_allowed, rush_yards_allowed, total_yards_allowed, player_id, game_id) 
                            SELECT :sacks, :interceptions, :safeties, :fumble_recoveries, :blocked_kicks, :touchdowns, :points_allowed, :pass_yards_allowed, :rush_yards_allowed, :total_yards_allowed, :player_id, :game_id
                            WHERE NOT EXISTS (SELECT 1 FROM defense WHERE game_id=:game_id AND player_id=:player_id)""")
            result = conn.execute(query,
                                  {'sacks' : float(sacks), 'interceptions' : int(interceptions), 'safeties' : int(safeties),
                                   'fumble_recoveries' : int(fumble_recoveries), 'blocked_kicks' : int(blocked_kicks), 'touchdowns' : int(touchdowns),
                                   'points_allowed' : int(points_allowed), 'pass_yards_allowed' : int(pass_yards_allowed), 'rush_yards_allowed' : int(rush_yards_allowed),
                                   'total_yards_allowed' : int(total_yards_allowed), 'game_id' : game_id, 'player_id' : player_id})
            conn.commit()
    # def insert_rushing(self, rushing):
    #     with self.engine.connect() as conn:
    #         query = text("""INSERT INTO rushing (rushing_attempts, rushing_yards, rushing_touchdowns, rushing_two_point_conversions, game_id, player_id)
    #                             SELECT :attempts, :yards, :touchdowns, :two_pt_conv, :game_id, :player_id
    #                             WHERE NOT EXISTS (SELECT 1 FROM rushing WHERE game_id=:game_id AND player_id=:player_id)""")
    #         result = conn.execute(query, {
    #             'attempts': int(rushing.attempts),
    #             'yards': int(rushing.yards),
    #             'touchdowns': int(rushing.touchdowns),
    #             'two_pt_conv': int(rushing.two_pt_conv),
    #             'game_id': rushing.game_id,
    #             'player_id': rushing.player_id
    #         })
    #         conn.commit()

    def insert_receiving(self, receptions, receiving_yards, receiving_touchdowns, receiving_two_point_conversions,
                         game_id, player_id):
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

    # def insert_receiving(self, receiving):
    #     with self.engine.connect() as conn:
    #         query = text("""INSERT INTO receiving (receptions, receiving_yards, receiving_touchdowns, receiving_two_point_conversions, game_id, player_id)
    #                                         SELECT :receptions, :yards, :touchdowns, :two_pt_conv, :game_id, :player_id
    #                                         WHERE NOT EXISTS (SELECT 1 FROM receiving WHERE game_id=:game_id AND player_id=:player_id)""")
    #         result = conn.execute(query, {
    #             'receptions': int(receiving.receptions),
    #             'yards': int(receiving.yards),
    #             'touchdowns': int(receiving.touchdowns),
    #             'two_pt_conv': int(receiving.two_pt_conv),
    #             'game_id': receiving.game_id,
    #             'player_id': receiving.player_id
    #         })
    #         conn.commit()

    # def insert_passing(self, passing):
    #     with self.engine.connect() as conn:
    #         query = text("""INSERT INTO passing (passing_attempts, passing_completions, passing_yards, passing_touchdowns, passing_two_point_conversions, interceptions, game_id, player_id)
    #                                         SELECT :attempts, :completions, :yards, :touchdowns, :two_pt_conv, :interceptions, :game_id, :player_id
    #                                         WHERE NOT EXISTS (SELECT 1 FROM passing WHERE game_id=:game_id AND player_id=:player_id)""")
    #         result = conn.execute(query, {
    #             'attempts': passing.attempts,
    #             'completions': passing.completions,
    #             'yards': passing.yards,
    #             'touchdowns': passing.touchdowns,
    #             'two_pt_conv': passing.two_pt_conv,
    #             'interceptions': passing.interceptions,
    #             'game_id': passing.game_id,
    #             'player_id': passing.player_id
    #         })
    #         conn.commit()
