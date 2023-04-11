import sys
import os
import netCDF4
from netCDF4 import Dataset
import xarray as xr
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import glob
np.set_printoptions(threshold=np.inf)

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