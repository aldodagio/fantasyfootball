class Rushing:
    def __init__(self, attempts, yards, touchdowns, two_pt_conv, id, game_id, player_id):
        self.attempts = attempts
        self.yards = yards
        self.touchdowns = touchdowns
        self.two_pt_conv = two_pt_conv
        self.id = id
        self.game_id = game_id
        self.player_id = player_id
    def __init__(self, attempts, yards, touchdowns, two_pt_conv, game_id, player_id):
        self.attempts = attempts
        self.yards = yards
        self.touchdowns = touchdowns
        self.two_pt_conv = two_pt_conv
        self.game_id = game_id
        self.player_id = player_id