class Season:
    def __init__(self, id=None, year=None):
        self.year = year
        self.id = id
    def toYear(self, season_id):
        if season_id == 1:
            self.year = 2010
        elif season_id == 2:
            self.year = 2011
        elif season_id == 2:
            self.year = 2012
        elif season_id == 3:
            self.year = 2013
        elif season_id == 4:
            self.year = 2014
        elif season_id == 5:
            self.year = 2015
        elif season_id == 6:
            self.year = 2016
        elif season_id == 7:
            self.year = 2017
        elif season_id == 8:
            self.year = 2018
        elif season_id == 9:
            self.year = 2019
        elif season_id == 10:
            self.year = 2020
        elif season_id == 11:
            self.year = 2021
        elif season_id == 12:
            self.year = 2022
        elif season_id == 13:
            self.year = 2023
            return self.year
    def toID(self, year):
        if year == '2010':
            self.season_id = 1
        elif year == '2011':
            self.season_id = 2
        elif year == '2012':
            self.season_id = 3
        elif year == '2013':
            self.season_id = 4
        elif year == '2014':
            self.season_id = 5
        elif year == '2015':
            self.season_id = 6
        elif year == '2016':
            self.season_id = 7
        elif year == '2017':
            self.season_id = 8
        elif year == '2018':
            self.season_id = 9
        elif year == '2019':
            self.season_id = 10
        elif year == '2020':
            self.season_id = 11
        elif year == '2021':
            self.season_id = 12
        elif year == '2022':
            self.season_id = 13
        elif year == '2023':
            self.season_id = 14
            return self.season_id