from dotenv import load_dotenv
import requests
import json
import pandas as pd
import numpy as np
import argparse
import os
from functools import partial

load_dotenv()

# Define CKAN API endpoint and your API key]
csv_path =os.getenv('filename')
csv_delimiter = ","
ckan_api_url = os.getenv('ckan_url')
api_key =  os.getenv('api_keys')
resource_id = os.getenv('resource_id')

def datastore_delete(ckan_api_url, resource_id, api_key):
    print('Deleting datastore for ' + resource_id)

    u = ckan_api_url + "/api/3/action/datastore_delete"
    r = requests.post(u, headers={
      "X-CKAN-API-Key": api_key,
      "Accept": "application/json",
      'Content-Type': 'application/json',
    }, data=json.dumps({
        "resource_id": resource_id,
        "force": "true"
    }))

    return json.loads(r.text)

def datastore_create(records, fields, ckan_api_url, resource_id, api_key):
    print('Creating new datastore for ' + resource_id)

    u = ckan_api_url + "/api/3/action/datastore_create"
    r = requests.post(u, headers={
      "X-CKAN-API-Key": api_key,
      "Accept": "application/json",
      'Content-Type': 'application/json',
    }, data=json.dumps({
        "resource_id": resource_id,
        "records": records,
        "fields": fields,
        "force": "true"
    }))
    return json.loads(r.text)

def update_resource(df, csv_path, ckan_api_url, resource_id, api_key):
    
    df.to_csv(csv_path, index=False)
    u = ckan_api_url + "/api/3/action/resource_patch"
    r = requests.post(u, headers={
      "X-CKAN-API-Key": api_key
    }, data={
        "id": resource_id
    }, files=[('upload', open('./' + csv_path, 'r'))])
    data_output = json.loads(r.content)
    print(data_output)

print('Fetching Data')

climatology_df = pd.read_csv('TAVG-climatology.csv')
print(climatology_df)

df = climatology_df

def preparing_climate_data():

    print('Preparing data...')
    climate_df = pd.DataFrame()
    records = []

    update_resource(df, 'TAVG-climatology.csv', ckan_api_url,resource_id, api_key)

    for row in climate_df:

        records.append({
            "month_number": row[0],
            "latitude": row[1],
            "longitude": row[2],
            "time": row[3],
            "climatology": row[4],
           
        })

        fields = [
            { 
                "id": "month_number",
                "type": "numeric"
            },
            { 
                "id": "latitude",
                "type": "numeric"
            },
            { 
                "id": "longitude",
                "type": "numeric"
            },
            { 
                "id": "time",
                "type": "numeric"
            },
            { 
                "id": "climatology",
                "type": "numeric"
            }
            
        ]

    response = datastore_create(records, fields, ckan_api_url, resource_id, api_key)
    print(response) 

datastore_delete(ckan_api_url, resource_id, api_key)
preparing_climate_data()
