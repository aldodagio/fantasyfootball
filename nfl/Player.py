from psycopg2 import IntegrityError
class Player:
    def __init__(self, first_name, last_name, position, team_id=None, id=None, points=None, fumbles=None, passing_touchdowns=None, passing_yards=None,
                 passing_attempts=None, rushing_yards=None, rushing_touchdowns=None, rushing_attempts=None, receiving_touchdowns=None,
                 receptions=None, receiving_yards=None, interceptions=None, team=None):
        self.first_name = first_name
        self.last_name = last_name
        self.position = position
        self.team_id = team_id
        self.id = id
        self.points = points
        self.fumbles = fumbles
        self.passing_touchdowns = passing_touchdowns
        self.passing_attempts = passing_attempts
        self.passing_yards = passing_yards
        self.rushing_yards = rushing_yards
        self.rushing_attempts = rushing_attempts
        self.rushing_touchdowns = rushing_touchdowns
        self.receiving_touchdowns = receiving_touchdowns
        self.receptions = receptions
        self.receiving_yards = receiving_yards
        self.interceptions = interceptions
        self.team = team

    @classmethod
    def with_team_id(cls, first_name, last_name, position, team_id):
        return cls(first_name, last_name, position, team_id=team_id)

    @classmethod
    def with_id(cls, first_name, last_name, position, id):
        return cls(first_name, last_name, position, id=id)

    @classmethod
    def with_points(cls, first_name, last_name, position, points):
        return cls(first_name, last_name, position, points=points)

    @classmethod
    def with_points_and_id(cls, first_name, last_name, position, points, id):
        return cls(first_name, last_name, position, points=points, id=id)

    @classmethod
    def qb_with_all_stats(cls,first_name, last_name, position, total_points, fumbles, passing_yards,
                          passing_attempts, passing_touchdowns, rushing_yards, rushing_attempts, rushing_touchdowns,
                          receiving_touchdowns, receptions, receiving_yards, interceptions):
        return cls(first_name, last_name, position, points=total_points, fumbles=fumbles, passing_yards=passing_yards, passing_touchdowns=passing_touchdowns,
                    passing_attempts=passing_attempts, rushing_yards=rushing_yards, rushing_attempts=rushing_attempts, rushing_touchdowns=rushing_touchdowns,
                          receiving_touchdowns=receiving_touchdowns, receptions=receptions, receiving_yards=receiving_yards, interceptions=interceptions)

    def insert_players(self, row, pos, db):
        player = row[0].split(' ', 1)
        first_name = player[0]
        last_name = player[1]
        team_id = db.select_team_id(row[1])
        try:
            db.insert_player(first_name, last_name, pos, team_id)
        except IntegrityError as e:
            # Handle the exception gracefully (e.g., log an error message)
            print(f"Failed to insert data: {e}")

    # def search_player(self):
    #
    # def insert_player(self):
    #
    # def update_player(self):
    #
    # def delete_player(self):