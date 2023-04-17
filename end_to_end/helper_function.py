import requests
import json
import pandas as pd

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

def update_resource(df, climate_csv_file, ckan_api_url, resource_id, api_key):
    
    df.to_csv(climate_csv_file, index=False)
    u = ckan_api_url + "/api/3/action/resource_patch"
    r = requests.post(u, headers={
    "X-CKAN-API-Key": api_key
    }, data={
        "id": resource_id
    }, files=[('upload', open(climate_csv_file, 'r'))])
    data_output = json.loads(r.content)
    print(data_output)

def preparing_climate_data():
    print('Preparing data...')
    climate_df = pd.DataFrame()
    records = []
   
    for row in climate_df:

        records.append({
            "month_number": row[0],
            "latitude": row[1],
            "longitude": row[2],
            "time": row[3],
            "climatology": row[4],
                
            })
    return records


def preparing_temp_data():
    print('Preparing data...')
    temp_df = pd.DataFrame()
    records = []
    
    for row in temp_df:

        records.append({
            "time": row[0],
            "latitude": row[1],
            "longitude": row[2],
            "temperature": row[3],
                
            })            
    return records

def preparing_precip_data():
    print('Preparing data...')
    climate_df = pd.DataFrame()
    records = []
   
    for row in climate_df:

        records.append({
            "time": row[0],
            "level": row[1],
            "latitude": row[2],
            "longitude": row[3],
            "precip": row[4],
            "month_number": row[5],
                
            })
    return records        
   