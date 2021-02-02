import pandas as pd
import numpy as np
import math
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

poi = pd.read_csv("data/POIList.csv")
filtered['ClosestPOI'] = ""
filtered['DistancePOI'] = np.nan

for index, row in filtered.iterrows():
    latitude_1 = row['Latitude']
    longitude_1 = row['Longitude']

    for poi_index, poi_row in poi.iterrows():
        latitude_2 = poi_row[' Latitude']
        longitude_2 = poi_row['Longitude']
        distance = math.hypot(latitude_2 - latitude_1, longitude_2 - longitude_1)
        if poi_index == 0:
            min_distance = distance
            min_poi = poi_row['POIID']
        if distance < min_distance:
            min_distance = distance
            min_poi = poi_row['POIID']

    filtered.at[index, 'ClosestPOI'] = min_poi
    filtered.at[index, 'DistancePOI'] = min_distance

