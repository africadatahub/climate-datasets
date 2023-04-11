import pandas as pd
import numpy as np

df = pd.read_csv('~/dev/climate-datasets/files/emdat_africa_1992-2023_natural.csv', encoding = 'unicode_escape', engine ='python')

# List all the Disaster Subgroups without duplication
df['Disaster Group'].unique()

# Drop rows where Disaster Group is not Natural
df = df[df['Disaster Group'] == 'Natural']
df['Disaster Subgroup'].unique()


# save to csv
# df.to_csv('files/emdat_africa_1992-2023_natural.csv', index=False)
