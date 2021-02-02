import pandas as pd
import numpy as np
import datetime

# Cleanup

data = pd.read_csv("data/DataSample.csv")
data.sort_values(by= ['Latitude', ' TimeSt'], inplace=True, ignore_index=True)
filtered = data.copy()

for index, row in data.iterrows():
    try:
        next_row = data.iloc[index+1]
    except:
        break
    if next_row['Latitude'] == row['Latitude'] and next_row['Longitude'] == row['Longitude']:
        date = datetime.datetime.strptime(row[' TimeSt'], '%Y-%m-%d %H:%M:%S.%f')
        next_date = datetime.datetime.strptime(next_row[' TimeSt'], '%Y-%m-%d %H:%M:%S.%f')
        if date == next_date:
            filtered.drop(index=index, inplace=True)
            filtered.drop(index=index+1, inplace=True)

# Label

