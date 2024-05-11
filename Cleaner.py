import csv

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
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(self.input_csv, encoding='latin1')
        # Check if 'Player' is in the columns
        if 'Player' not in df.columns:
            print("Error: 'Player' column not found in the DataFrame. Check the CSV file.")
            return

        # Function to clean values in 'Player' column
        def clean_value(value):
            if '.' in value:
                # Find the position of the last '.' and remove everything before and after it
                period_index = value.rfind('.')
                value = value[:period_index - 1]

            return value.strip()  # Remove leading and trailing spaces

        # Apply the cleaning function to 'Player' column (excluding the first row)
        df['Player'] = df['Player'].apply(lambda x: clean_value(x) if pd.notna(x) else x)
        # Save the cleaned DataFrame to a new CSV file
        df.to_csv(self.output_csv, index=False)

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
        with open(self.input_csv, 'r', newline='') as infile, open(self.output_csv, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            # Write the header row unchanged
            header = next(reader)
            writer.writerow(header)
            # Iterate through each row in the CSV file
            for row in reader:
                # Update the 'Team' column if needed
                team_abbreviation = row[1]
                if team_abbreviation in team_mapping:
                    row[1] = team_mapping[team_abbreviation]
                # Update the 'Game' column if needed
                game_teams = row[2].split('@')
                updated_game = ''
                for index, team in enumerate(game_teams):
                    if team in team_mapping:
                        game_teams[index] = team_mapping[team]
                updated_game = '@'.join(game_teams)
                row[2] = updated_game
                # Write the updated row to the output CSV file
                writer.writerow(row)

        print("Team column updated successfully.")
