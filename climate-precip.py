import sys
import netCDF4
from netCDF4 import Dataset
import xarray as xr
import numpy as np
import pandas as pds
from matplotlib import pyplot as plt
np.set_printoptions(threshold=np.inf)

file = 'files/Berkeley/gpcc_10_combined.nc'
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