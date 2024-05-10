class Passing:
    def __init__(self, attempts, completions, yards, touchdowns, two_pt_conv, interceptions, id, game_id, player_id):
        self.attempts = attempts
        self.completions = completions
        self.yards = yards
        self.touchdowns = touchdowns
        self.two_pt_conv = two_pt_conv
        self.interceptions = interceptions
        self.id = id
        self.game_id = game_id
        self.player_id = player_id
    def __init__(self, attempts, completions, yards, touchdowns, two_pt_conv, interceptions, game_id, player_id):
        self.attempts = attempts
        self.completions = completions
        self.yards = yards
        self.touchdowns = touchdowns
        self.two_pt_conv = two_pt_conv
        self.interceptions = interceptions
        self.game_id = game_id
        self.player_id = player_id