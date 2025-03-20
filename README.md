# How to use the scraper object to collect our fantasy data for our postgres db
# 1. Call the set_up_scraper function inside the main function.
#    The function takes 4 arguments - fantasy_year, week, key, pos.
#    The fantasy_year parameter is simply an integer, ex. 2024.
#    The week parameter is the fantasy week you want to scrape, ex. 1 for week 1.
#    The key parameter is the api key or the session key.
#    The pos paramter is the position you are interested in scraping, ex. WR.
#    The function will return a scraper object.
#    Example URL: https://www.footballdb.com/fantasy-football/index.html?yr=2024&pos=QB&wk=18&key=48ca46aa7d721af4d58dccc0c249a1c4
# 2. Use the scrape function from the Scraper object.
#    You will have to loop the function and iterate through weeks 1-18.
#    Example: while week < end_week:
#              scraper.setWeek(week)
#              scraper.scrape()
#              week = week + 1
# 3. The raw data will now be in the \data\raw_data\{year} folder(s). The next step
#    is to clean the data.