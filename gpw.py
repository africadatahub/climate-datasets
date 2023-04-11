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

file = 'files/gpw/gpw_v4_population_density_adjusted_rev11_1_deg.nc'
nc = xr.open_dataset(file)
nc

df = nc.to_dataframe()
df.to_csv('files/gpw-population.csv')