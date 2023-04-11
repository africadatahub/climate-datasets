import pandas as pd
import numpy as np

df = pd.read_csv('files/adh/Berkeley/TMIN/TMIN-Climatology-2010-2022.csv')
df

#  drop month_number, time and climatology columns
df = df.drop(['month_number', 'time', 'climatology'], axis=1)

df = df.drop_duplicates()

df.to_csv('files/adh/Berkeley/locations.csv', index=False)
