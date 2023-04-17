import os
import requests
import concurrent.futures
from dotenv import load_dotenv
import requests
import json
import pandas as pd
import numpy as np
import os
from netCDF4 import Dataset
import xarray as xr
import glob
np.set_printoptions(threshold=np.inf)
from helper_function import datastore_delete, datastore_create,update_resource,preparing_climate_data,preparing_temp_data, climate_fields, temp_fields

load_dotenv()

climate_csv_file =os.getenv('climate_filename')
temp_csv_file = os.getenv('temp_filename')
csv_delimiter = ","
ckan_api_url = os.getenv('ckan_url')
api_key =  os.getenv('api_keys')

file_info =[{
    "filename": "TAVG-climatology.csv",
    "resource_id": os.getenv('TAVG_climate_resource_id')
},
{
  "filename": "TAVG-temperature.csv",
    "resource_id": os.getenv('TAVG_temp_resource_id')

},
{
  "filename": "TMAX-climatology.csv",
    "resource_id": os.getenv('TMAX_climate_resource_id')  

},
{
  "filename": 'TMAX-temperature.csv',
    "resource_id": os.getenv('TMAX_temp_resource_id')

},
{
  "filename": "TAVG-climatology.csv",
    "resource_id": os.getenv('TMIN_climate_resource_id') 

},
{
  "filename": 'TMIN-temperature.csv',
    "resource_id": os.getenv('TMIN_temp_resource_id')

}]
urls = [

    os.getenv('download_Tavg'),
    os.getenv('download_Tmin'),
    os.getenv('download_Tmax')
]
   

filenames = [
    "Complete_TAVG_LatLong1.nc",
    "Complete_TMIN_LatLong1.nc",
    "Complete_TMAX_LatLong1.nc"
]

def download_file(url, filename):
    filepath = os.path.expanduser("~/dev/climate-datasets/Berkeley/" + filename)
    response = requests.get(url)
    with open(filepath, "wb") as file:
        file.write(response.content)
    print(f"Downloaded {filename}")

with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = []
    for url, filename in zip(urls, filenames):
        futures.append(executor.submit(download_file, url, filename))
    concurrent.futures.wait(futures)

print("All downloads complete")

for type in ['TAVG','TMIN','TMAX']:

    file = '~/dev/climate-datasets/Berkeley/Complete_' + type + '_LatLong1.nc'
    nc = xr.open_dataset(file)

    for metric in ['climatology','temperature']:

        years = np.arange(1993, 2023, 1)

        for year in years:

            dc = nc

            dc = dc.drop_vars('land_mask')
            if(metric == 'climatology'):
                dc = dc.drop_vars('temperature')
            else:
                dc = dc.drop_vars('climatology')

            # filter data to year
            dc = dc.where(dc['time'] > year, drop=True)
            dc = dc.where(dc['time'] < year + 1, drop=True)

            # filter data to latitude and longitude in africa
            dc = dc.where(dc['latitude'] > -37, drop=True)
            dc = dc.where(dc['latitude'] < 37, drop=True)
            dc = dc.where(dc['longitude'] > -20, drop=True)
            dc = dc.where(dc['longitude'] < 60, drop=True)

            df1 = dc.to_dataframe()

            df1.to_csv('~/dev/climate-datasets/files/' + type + '-' + metric + '-' + str(year) + '.csv')

        for year in years:

            df2 = pd.read_csv('~/dev/climate-datasets/files/' + type + '-' + metric + '-' + str(year) + '.csv')

            if(metric == 'climatology'):
                df2['time'] = df2['time'].astype(str).str[:4]
                df2 = df2.dropna(subset=['climatology']) 
            else:
                df2 = df2.dropna(subset=['temperature'])

            df2 = df2.drop_duplicates()

            df2.to_csv('~/dev/climate-datasets/files/' + type + '-' + metric + '-' + str(year) + '-filtered.csv', index=False)

        df3 = pd.concat([pd.read_csv(f) for f in glob.glob('/Users/mahlatsemoloto/dev/climate-datasets/files/' + type + '-' + metric + '-' + '*-filtered.csv')], ignore_index = True)

        df3 = df3.sort_values(by=['longitude', 'latitude'])

        df3.to_csv('/Users/mahlatsemoloto/dev/climate-datasets/files/' + type + '-' + metric + '.csv', index=False)

        for year in years:
            os.remove('/Users/mahlatsemoloto/dev/climate-datasets/files/' + type + '-' + metric + '-' + str(year) + '.csv')
            os.remove('/Users/mahlatsemoloto/dev/climate-datasets/files/' + type + '-' + metric + '-' + str(year) + '-filtered.csv')

for fileinfo in file_info:
    filename = fileinfo.get('filename').split('-')[-1]

    if filename == "temperature.csv":
        print(fileinfo)
        temperature_df = pd.read_csv(os.path.join('files',fileinfo.get('filename')))
        print(temperature_df)
        df_temp = temperature_df
        
        datastore_delete(ckan_api_url, fileinfo.get("resource_id"), api_key)
        records = preparing_temp_data()
        datastore_create(records, temp_fields, ckan_api_url, fileinfo.get("resource_id"), api_key)
        update_resource(df_temp, os.path.join('files',fileinfo.get("filename")), ckan_api_url,fileinfo.get("resource_id"), api_key)
        

    if filename == "climatology.csv":
        climatology_df = pd.read_csv(os.path.join('files',fileinfo.get('filename')))
        print(climatology_df)
        df_climate = climatology_df
        
        print(fileinfo)
        datastore_delete(ckan_api_url, fileinfo.get("resource_id"), api_key)
        records = preparing_climate_data()
        datastore_create(records, climate_fields, ckan_api_url, fileinfo.get("resource_id"), api_key)
        update_resource(df_climate, os.path.join('files',fileinfo.get("filename")), ckan_api_url,fileinfo.get("resource_id"), api_key)
       
