import requests
import pandas as pd
import io
# Load the track list from the Excel file
track_list = pd.read_excel("track_list.xlsx")

# Display the first few rows
# print(track_list.head())

# Fetch a random row from the track list
random_row = track_list.sample(n=1)

# Display the random row
track_id = random_row['tid'].values[0]

response = requests.get(f"https://api.vitaldb.net/{track_id}")
data = response.content.decode('utf-8')
track_data = pd.read_csv(io.StringIO(data))

# Display the first few rows
print(track_data.head())