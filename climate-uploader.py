from dotenv import load_dotenv
import requests
import json
import pandas as pd
import csv
import os

load_dotenv()

# Define CKAN API endpoint and your API key]
ckan_api_url = os.getenv('ckan_url')
api_key =  os.getenv('api_keys')
resource_id =  os.getenv('resource_id')

# CSV file path and delimiter
CSV_FILE_PATH = "TAVG-climatology.csv"
CSV_DELIMITER = ","

# Read data from CSV file
with open(CSV_FILE_PATH) as csvfile:
    reader = csv.DictReader(csvfile, delimiter=CSV_DELIMITER)
    data = [row for row in reader]


#create data store

print('Creating new datastore for ' + resource_id)

u = ckan_api_url + "/api/action/datastore_create"
r = requests.post(u, headers={
    "X-CKAN-API-Key": ckan_api_url,
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
    'Authorization': api_key
}
response = requests.put(ckan_api_url, headers=headers, json=payload)
print('Deleting datastore for ' + resource_id)
u = ckan_api_url+'/api/3/action/datastore_upsert'

response = requests.post(u, headers={
"X-CKAN-API-Key": api_key,
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
    
# Update resource data in CKAN
for row in data:
    # Set API endpoint and headers
    api_endpoint = f"{ckan_api_url}/api/3/action/datastore_upsert"
    headers = {"Authorization": api_key}        

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

