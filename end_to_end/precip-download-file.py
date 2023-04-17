from dotenv import load_dotenv
import os
import requests
from netCDF4 import Dataset
import xarray as xr
import numpy as np
import pandas as pd
np.set_printoptions(threshold=np.inf)
load_dotenv()
from helper_function import datastore_delete, datastore_create,update_resource,preparing_precip_data

precip_fields = [
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

dc = nc
dc = dc.where(dc['lat'] > -37, drop=True)
dc = dc.where(dc['lat'] < 37, drop=True)
dc = dc.where(dc['lon'] > -20, drop=True)
dc = dc.where(dc['lon'] < 60, drop=True)

dc['date'] = dc['time'] * np.timedelta64(1, 'M') + np.datetime64('1891-01-01')
dc = dc.drop('time')
dc['month_number'] = dc['date'].dt.month
dc['time'] = dc['date'].dt.year
dc = dc.drop('date')
dc = dc.where(dc['time'] > 1992, drop=True)
dc = dc.rename({'lat': 'latitude', 'lon': 'longitude'})
df = dc.to_dataframe()
df = df.dropna(subset=['precip'])
df.to_csv('~/dev/climate-datasets/files/GPCC-Precipitation-1992-2022.csv')

print(resource_id)

climatology_df = pd.read_csv(os.path.join('files',(csv_path)))
print(climatology_df)

precip_df = climatology_df
datastore_delete(ckan_api_url, resource_id, api_key)
preparing_precip_data()
datastore_delete(ckan_api_url, resource_id, api_key)
records = preparing_precip_data()
datastore_create(records, precip_fields, ckan_api_url, resource_id, api_key)
update_resource(df,csv_path, ckan_api_url, resource_id, api_key)