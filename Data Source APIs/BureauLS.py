import requests
import json
import pandas as pd

# Set headers and data for the request
headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": ['WPSFD4'], "startyear": "2021", "endyear": "2024"})

# Send the request
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)

# Load the response into JSON
json_data = json.loads(p.text)

# Initialize a list to hold the data
data_list = []

# Process each series in the response
for series in json_data['Results']['series']:
    seriesId = series['seriesID']
    for item in series['data']:
        year = item['year']
        period = item['period']
        value = item['value']
        footnotes = ""
        for footnote in item['footnotes']:
            if footnote:
                footnotes += footnote['text'] + ','
        # Filter for valid months
        if 'M01' <= period <= 'M12':
            data_list.append([seriesId, year, period, value, footnotes.rstrip(',')])

# Convert the list to a DataFrame
df = pd.DataFrame(data_list, columns=["series id", "year", "period", "value", "footnotes"])

print(df)

# Save the DataFrame to a CSV file
df.to_csv(f'{seriesId}.csv', index=False)