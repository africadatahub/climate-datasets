from netCDF4 import Dataset
import xarray as xr
import numpy as np
from matplotlib import pyplot as plt
np.set_printoptions(threshold=np.inf)

file = '~/dev/climate-datasets/Berkeley/gpcc_10_combined.nc'
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
dc = dc.where(dc['time'] > 1949, drop=True)
dc = dc.where(dc['time'] < 1981, drop=True)
dc = dc.rename({'lat': 'latitude', 'lon': 'longitude'})
df = dc.to_dataframe()
df = df.dropna(subset=['precip'])