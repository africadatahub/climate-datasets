import requests
import json
import pandas as pd
import csv
import os


# Define CKAN API endpoint and your API key]
CKAN_API_URL = os.getenv('CKAN_API_URL')
API_KEY = os.getenv('API_KEY')
resource_id = os.getenv('resource_id')


# CSV file path and delimiter
CSV_FILE_PATH = "TAVG-climatology.csv"
CSV_DELIMITER = ","

# Read data from CSV file
with open(CSV_FILE_PATH) as csvfile:
    reader = csv.DictReader(csvfile, delimiter=CSV_DELIMITER)
    data = [row for row in reader]

# Update resource data in CKAN
for row in data:
    # Set API endpoint and headers
    api_endpoint = f"{CKAN_API_URL}/api/3/action/datastore_upsert"
    headers = {"Authorization": API_KEY}

    # Set payload with updated data
    payload = {
        "id": resource_id,
        "month_number": row["month_number"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "time": row["time"],
        "climatology": row["climatology"],
        "force":True

    }
    # Make API call to update resource
    response = requests.post(api_endpoint, headers=headers, json=payload)


#create data store

print('Creating new datastore for ' + resource_id)

u = CKAN_API_URL + "/api/action/datastore_create"
r = requests.post(u, headers={
    "X-CKAN-API-Key": CKAN_API_URL,
    "Accept": "application/json",
    'Content-Type': 'application/json',
}, data=json.dumps({
    "resource_id": resource_id,
    "force": "true"
}))


# Create API call payload with resource ID and updated fields
payload = {
    'id': resource_id,
    #**new_fields
}

# Make API call to update dataset - delete the datastore
headers = {
    'Authorization': API_KEY
}
response = requests.put(CKAN_API_URL, headers=headers, json=payload)
print('Deleting datastore for ' + resource_id)
u = CKAN_API_URL+'/api/3/action/datastore_upsert'

response = requests.post(u, headers={
"X-CKAN-API-Key": API_KEY,
"Accept": "application/json",
 'Content-Type': 'application/json',
  }, data=json.dumps({
      "resource_id": resource_id,
      "force":True

   }))


if response.status_code == 200:
    print('Dataset updated successfully!')
else:
    print('Error updating dataset:', response.content)


    #return json.loads(response.text)
    