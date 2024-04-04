import requests, json, os

url = '''
https://apps.bea.gov/api/data?
&UserID=CEF9E43E-271E-4057-BDB5-EE207198AD9A
&method=Getdata
&DataSetName=NIPA
&TableName=T10101
&Frequency=Q
&Year=2023
&ResultFormat=json
'''


# &Industry=ALL
# &tableID=ALL


# &Parametername=TableID




print(url.replace("\n", ""))
response = requests.get(url.replace("\n", ""))
json_data = response.json()

# Write data to the JSON file
script_directory = os.path.dirname(os.path.realpath(__file__))
json_file_path = script_directory + "\gdp2.json"
with open(json_file_path, "w") as json_file:
    json.dump(json_data, json_file, indent=4)