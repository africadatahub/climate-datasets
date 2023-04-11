import sys
import netCDF4
from netCDF4 import Dataset
import xarray as xr
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
np.set_printoptions(threshold=np.inf)

places_df = pd.read_json('./files/places.json')
countries_df = pd.read_json('./files/countries.json')

places_df = places_df.drop(['Rank'], axis=1)

for index, row in places_df.iterrows():
    
    country = row['country']
    
    country_row = countries_df.loc[countries_df['location'] == country]

    if not country_row.empty:
        iso_code = country_row['iso_code'].values[0]
        places_df.at[index, 'iso_code'] = iso_code

    else:
        print('Country not found: ' + country)

places_df.to_json('./files/places2.json', orient='records')