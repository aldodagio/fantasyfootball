class Team:
    def __init__(self, name, id):
        self.name = name
        self.id = id

    def get_home_team_id(self, game, db):
        teams = game.split('@', 1)
        team = teams[0]
        return db.select_team_id(team)

    def get_away_team_id(self, game, db):
        teams = game.split('@', 1)
        team = teams[1]
        return db.select_team_id(team)

    @classmethod
    def team_constructor(cls, first_name, last_name, position, points):
        return cls(first_name, last_name, position, points=points)