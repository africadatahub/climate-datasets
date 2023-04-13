from dotenv import load_dotenv
import os
import requests
import json
import sys
import netCDF4
from netCDF4 import Dataset
import xarray as xr
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
np.set_printoptions(threshold=np.inf)
load_dotenv()

csv_path =os.getenv('precip_filename')
csv_delimiter = ","
ckan_api_url = os.getenv('ckan_url')
api_key =  os.getenv('api_keys')
resource_id = os.getenv('precip_resource_id')

url = "http://climexp.knmi.nl/GPCCData/gpcc_10_combined.nc"
filename ="gpcc_10_combined.nc"
filepath = os.path.expanduser("~/dev/climate-datasets/Berkeley/" + filename)

response = requests.get(url)
with open(filepath, "wb") as file:
    file.write(response.content)

print("Done")

file='~/dev/climate-datasets/Berkeley/gpcc_10_combined.nc'
nc = xr.open_dataset(file, decode_times=False)
nc

dc = nc

dc = dc.where(dc['lat'] > -37, drop=True)
dc = dc.where(dc['lat'] < 37, drop=True)
dc = dc.where(dc['lon'] > -20, drop=True)
dc = dc.where(dc['lon'] < 60, drop=True)

# Convert time column months since January 1891 to date
dc['date'] = dc['time'] * np.timedelta64(1, 'M') + np.datetime64('1891-01-01')

dc = dc.drop('time')

dc['month_number'] = dc['date'].dt.month

dc['time'] = dc['date'].dt.year

dc = dc.drop('date')

dc = dc.where(dc['time'] > 1992, drop=True)

# rename column
dc = dc.rename({'lat': 'latitude', 'lon': 'longitude'})

df = dc.to_dataframe()

df = df.dropna(subset=['precip'])

df.to_csv('~/dev/climate-datasets/files/GPCC-Precipitation-1992-2022.csv')

print(resource_id)

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
    }, files=[('upload', open(csv_path, 'r'))])
    data_output = json.loads(r.content)
    print(data_output)

print('Fetching Data')

climatology_df = pd.read_csv(csv_path)
print(climatology_df)

df = climatology_df

def preparing_climate_data():

    print('Preparing data...')
    climate_df = pd.DataFrame()
    records = []
    fields = [
            { 
                "id": "time",
                "type": "numeric"
            },
            { 
                "id": "level",
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
                "id": "precip",
                "type": "numeric"
            },
            { 
                "id": "month_number",
                "type": "numeric"
            }
            
        ]

    update_resource(df, csv_path, ckan_api_url,resource_id, api_key)

    for row in climate_df:

        records.append({
            "time": row[0],
            "level": row[1],
            "latitude": row[2],
            "longitude": row[3],
            "precip": row[4],
            "month_number": row[5],
           
        })

        
    response = datastore_create(records, fields, ckan_api_url, resource_id, api_key)
    print(response) 

datastore_delete(ckan_api_url, resource_id, api_key)
preparing_climate_data()
