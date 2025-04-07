import csv
import re
import pandas as pd

team_mapping = {
    'ARI': 'Arizona Cardinals',
    'ATL': 'Atlanta Falcons',
    'BAL': 'Baltimore Ravens',
    'BUF': 'Buffalo Bills',
    'CAR': 'Carolina Panthers',
    'CHI': 'Chicago Bears',
    'CIN': 'Cincinnati Bengals',
    'CLE': 'Cleveland Browns',
    'DAL': 'Dallas Cowboys',
    'DEN': 'Denver Broncos',
    'DET': 'Detroit Lions',
    'GB': 'Green Bay Packers',
    'HOU': 'Houston Texans',
    'IND': 'Indianapolis Colts',
    'JAX': 'Jacksonville Jaguars',
    'KC': 'Kansas City Chiefs',
    'OAK': 'Las Vegas Raiders',
    'LV': 'Las Vegas Raiders',
    'SD': 'Los Angeles Chargers',
    'LAC': 'Los Angeles Chargers',
    'STL': 'Los Angeles Rams',
    'LAR': 'Los Angeles Rams',
    'LA': 'Los Angeles Rams',
    'MIA': 'Miami Dolphins',
    'MIN': 'Minnesota Vikings',
    'NE': 'New England Patriots',
    'NO': 'New Orleans Saints',
    'NYG': 'New York Giants',
    'NYJ': 'New York Jets',
    'PHI': 'Philadelphia Eagles',
    'PIT': 'Pittsburgh Steelers',
    'SF': 'San Francisco 49ers',
    'SEA': 'Seattle Seahawks',
    'TB': 'Tampa Bay Buccaneers',
    'TEN': 'Tennessee Titans',
    'WAS': 'Washington Commanders'
}


class Cleaner:
    def __init__(self, path_to_csv, output_csv):
        self.input_csv = path_to_csv
        self.output_csv = output_csv

    def setInputCSV(self, input_csv):
        self.input_csv = input_csv

    def setOutputCSV(self, output_csv):
        self.output_csv = output_csv

    def clean_player_column(self):
        with open(self.input_csv, 'r', newline='', encoding='latin1') as infile, open(self.output_csv, 'w', newline='',
                                                                                      encoding='latin1') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            # Read the header and write it unchanged
            header = next(reader)
            writer.writerow(header)

            # Process each row
            for row in reader:
                if row:  # Ensure row isn't empty
                    row[0] = self.extract_player_name(row[0])  # Assuming 'Player' column is at index 0
                writer.writerow(row)

        print("Player column cleaned successfully.")

    def clean_non_numeric_values(self):
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(self.input_csv, encoding='latin1')  # You may need to adjust the encoding

        # Identify non-numeric columns (excluding the first column)
        non_numeric_columns = df.columns[1:].difference(df.select_dtypes(include='number').columns)

        # Convert non-numeric columns to numeric (excluding the first column)
        df[non_numeric_columns] = df[non_numeric_columns].apply(
            lambda x: pd.to_numeric(x.astype(str).str.replace(',', ''), errors='coerce'))

        # Save the updated DataFrame to a new CSV file
        df.to_csv(self.output_csv, index=False)


    def clean_team_names(self):
        with open(self.input_csv, 'r', newline='', encoding='latin1') as infile, open(self.output_csv, 'w', newline='', encoding='latin1') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            # Write the header row unchanged
            header = next(reader)
            writer.writerow(header)

            # Iterate through each row in the CSV file
            for row in reader:
                # Extract the actual team abbreviation from the 'Player' column
                if row:  # Ensure the row isn't empty
                    extracted_team = self.extract_team_abbreviation(row[0])  # Assuming 'Player' column is at index 0
                    if extracted_team and extracted_team in team_mapping:
                        row[1] = team_mapping[extracted_team]  # Update 'Team' column at index 1

                # Update the 'Game' column if needed
                if len(row) > 2:  # Ensure there is a 'Game' column at index 2
                    game_teams = row[2].split('@')
                    for index, team in enumerate(game_teams):
                        if team in team_mapping:
                            game_teams[index] = team_mapping[team]
                    row[2] = '@'.join(game_teams)

                # Write the updated row to the output CSV file
                writer.writerow(row)

        print("Team column updated successfully.")


    def extract_team_abbreviation(self,s):
        # Extract using regex that finds capital letters at the end
        match = re.search(r'[A-Z]{2,3}(?=[A-Z]\.)', s)
        return match.group() if match else None

    def extract_player_name(self, s):
        # Find where the last name ends using lowercase to uppercase transition, allowing apostrophes
        match = re.match(r"([A-Za-z\-\'\.\s]+?)(?=[A-Z]{2,3}[A-Z]\.)", s)
        if match:
            return match.group(1).strip()  # Extract and clean spaces
        return s

    def clean_game_column(self):
        with open(self.input_csv, 'r', newline='', encoding='latin1') as infile, open(self.output_csv, 'w', newline='',
                                                                                      encoding='latin1') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            header = next(reader)
            writer.writerow(header)

            for row in reader:
                if len(row) < 3:
                    writer.writerow(row)
                    continue

                team_name = row[1]  # Full team name (e.g., Green Bay Packers)
                game_val = row[2]  # Original game string (e.g., '@ PHI' or 'vs PHI')

                # Match either "@ PHI" or "vs PHI"
                match = re.search(r'(vs|@)[\s\u00a0]*([A-Z]{2,3})', game_val)
                if match:
                    symbol = match.group(1)  # 'vs' or '@'
                    opp_abbrev = match.group(2)  # e.g., 'PHI'
                    opp_full = team_mapping.get(opp_abbrev, opp_abbrev)

                    # Build cleaned game string
                    row[2] = f"{team_name} {symbol} {opp_full}"

                writer.writerow(row)

        print("Game column cleaned successfully.")
