import sys
import netCDF4
from netCDF4 import Dataset
import xarray as xr
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
np.set_printoptions(threshold=np.inf)

places_df = pd.read_json('./files/places2.json')

for index, row in places_df.iterrows():

    filtered_df = pd.DataFrame()

    lat = row['lat']
    lon = row['lon']

    if(lat != "" and lon != ""):

        for metric in ['tmax', 'tmin', 'tavg']:

            if(metric == 'tmax'):
                df = pd.read_csv('./files/TMAX-temperature.csv')
            elif(metric == 'tmin'):
                df = pd.read_csv('./files/TMIN-temperature.csv')
            else:
                df = pd.read_csv('./files/TAVG-temperature.csv')

            for index2, year in enumerate([2017, 2012, 2007, 2002, 1997, 1993]):

                filtered_df = df[df['time'] >= year]

                filtered_df = filtered_df[(filtered_df['latitude'] >= lat - 0.5) & (filtered_df['latitude'] <= lat + 0.5) & (filtered_df['longitude'] >= lon - 0.5) & (filtered_df['longitude'] <= lon + 0.5)]

                avg_temp = filtered_df['temperature'].mean()

                avg_temp = round(avg_temp, 2)

                if(avg_temp == avg_temp):
                    if(metric == 'tmax'):
                        places_df.at[index, 'avg_tmax_' + str(((index2 + 1) * 5))] = avg_temp
                    elif(metric == 'tmin'):
                        places_df.at[index, 'avg_tmin_' + str(((index2 + 1) * 5))] = avg_temp
                    else:
                        places_df.at[index, 'avg_avg_' + str(((index2 + 1) * 5))] = avg_temp

# places_df to csv
places_df.to_csv('./files/AVG_Temperature_Anomaly.csv', index=False)                        