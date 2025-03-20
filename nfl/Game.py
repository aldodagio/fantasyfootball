from psycopg2 import IntegrityError
from nfl.Team import Team
class Game:
    def __init__(self, home_team, away_team, season_id, game_id, week):
        self.home_team = home_team
        self.away_team = away_team
        self.season_id = season_id
        self.game_id = game_id
        self.week = week

    def insert_games_and_players(row, year, pos, db, week):
        # INSERT GAMES
        game = row[2]
        home_team_id = Team.get_home_team_id(game)
        away_team_id = Team.get_away_team_id(game)
        season_id = db.select_season_id(year)
        try:
            db.insert_game(home_team_id, away_team_id, season_id, week)
        except IntegrityError as e:
            # Handle the exception gracefully (e.g., log an error message)
            print(f"Failed to insert game: {e}")
        # INSERT PLAYERS
        player = row[0].split(' ', 1)
        first_name = player[0]
        last_name = player[1]
        team_id = db.select_team_id(row[1])
        try:
            db.insert_player(first_name, last_name, pos, team_id)
        except IntegrityError as e:
            # Handle the exception gracefully (e.g., log an error message)
            print(f"Failed to insert player: {e}")