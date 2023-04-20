import pandas as pd

df = pd.read_csv('~/dev/climate-datasets/files/emdat_africa_1992-2023_natural.csv', encoding = 'unicode_escape', engine ='python')
df['Disaster Group'].unique()
df = df[df['Disaster Group'] == 'Natural']
df['Disaster Subgroup'].unique()