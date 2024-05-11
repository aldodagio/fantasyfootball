import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import csv
class Scraper:
    def __init__(self, year, week, key, pos):
        self.year = year
        self.week = week
        self.key = key
        self.pos = pos
        self.url = "https://www.footballdb.com/fantasy-football/index.html?pos=" + pos + "&yr=" + str(year) + "&wk=" + str(week) + "&key=" + str(key)
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36"}
    def setYear(self,year):
        self.year = year
    def getYear(self):
        return self.year
    def setWeek(self,week):
        self.week = week
    def getWeek(self):
        return self.week
    def setKey(self, key):
        self.key = key
    def getKey(self):
        return self.key
    def setURL(self):
        self.url = "https://www.footballdb.com/fantasy-football/index.html?pos=OFF&yr=" + str(self.year) + "&wk=" + str(self.week) + "&key=" + str(self.key)
    def getURL(self):
        return self.url
    def setHeaders(self, headers):
        self.headers = headers
    def getHeaders(self):
        return self.headers
    def scrape(self):
        response = requests.get(self.url, headers=self.headers)
        # Check if the page is accessible
        if response.status_code == 200:
          chrome_options = Options()
          chrome_options.add_argument('--headless')  # Run Chrome in headless mode (without opening a window)
          driver = webdriver.Chrome(options=chrome_options)
          driver.get(self.url)
          page_source = driver.page_source
          driver.quit()
          # Parse the HTML content
          soup = BeautifulSoup(response.text, 'html.parser')
          # Find the table with the desired data
          table = soup.find('table', {'class': 'statistics scrollable'})
          if table:
              # Create a CSV file for writing
              with open('C:/Users/aldod/PycharmProjects/fantasyfootball/data/raw_data/' + str(self.year) + '/fantasyfootballdata_' + self.pos + str(
                      self.year) + '_week' + str(self.week) + '.csv', 'w+', newline='') as csv_file:
                  csv_writer = csv.writer(csv_file)

                  # Extract and write the header rows
                  headers_row2 = ['Player', 'Team', 'Game', 'Points', 'Passing Attempts', 'Passing Completions',
                                  'Passing Yards', 'Passing TDs', 'Interceptions', 'Passing 2Pt Conv',
                                  'Rushing Attempts', 'Rushing Yards', 'Rushing TDs', 'Rushing 2Pt Conv',
                                  'Receptions', 'Receiving Yards', 'Receiving TDs', 'Receiving 2Pt Conv',
                                  'Fumbles', 'Fumble TDs']
                  csv_writer.writerow(headers_row2)

                  # Extract and write the data rows
                  for row in table.find_all('tr')[2:]:  # Skip the first two header rows
                      data_row = []
                      cells = row.find_all(['th', 'td'])
                      for index, cell in enumerate(cells):
                          if index == 1:  # Game column
                              game_text = cell.find('b').get_text(strip=True) if cell.find('b') else cell.get_text(
                                  strip=True)
                              data_row.append(game_text)
                              data_row.append(cell.get_text(strip=True))
                          else:
                              data_row.append(cell.get_text(strip=True))
                      csv_writer.writerow(data_row)
              print("Scraping and writing to CSV completed.")
          else:
              print("Table with class 'statistics' not found.")