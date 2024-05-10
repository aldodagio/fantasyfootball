import pandas as pd
class Intersection:
    def __init__(self, set_one, set_two):
        self.set_one = set_one
        self.set_two = set_two
    def merge(self,output_csv):
        df1 = pd.read_csv(self.set_one)
        df2 = pd.read_csv(self.set_two)
        common_players = pd.merge(df2,df1[['Player']],on='Player',how='inner')
        common_players.to_csv(output_csv, index=False)
