class Team:

    def get_home_team_id(self, game, db):
        if '@' in game:
            teams = game.split('@', 1)
            team = teams[1].strip()
        elif 'vs' in game:
            teams = game.split('vs', 1)
            team = teams[0].strip()
        else:
            raise ValueError(f"Unsupported game format: {game}")
        return db.select_team_id(team)

    def get_away_team_id(self, game, db):
        if '@' in game:
            teams = game.split('@', 1)
            team = teams[0].strip()
        elif 'vs' in game:
            teams = game.split('vs', 1)
            team = teams[1].strip()
        else:
            raise ValueError(f"Unsupported game format: {game}")
        return db.select_team_id(team)
