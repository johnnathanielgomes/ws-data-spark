import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
import datetime

# Cleanup
print("Cleanup")

data = pd.read_csv("data/DataSample.csv")
data.sort_values(by=['Latitude', ' TimeSt'], inplace=True, ignore_index=True)
filtered = data.copy()

for index, row in data.iterrows():
    try:
        next_row = data.iloc[index + 1]
    except:
        break
    if next_row['Latitude'] == row['Latitude'] and next_row['Longitude'] == row['Longitude']:
        date = datetime.datetime.strptime(row[' TimeSt'], '%Y-%m-%d %H:%M:%S.%f')
        next_date = datetime.datetime.strptime(next_row[' TimeSt'], '%Y-%m-%d %H:%M:%S.%f')
        if date == next_date:
            filtered.drop(index=index, inplace=True)
            filtered.drop(index=index + 1, inplace=True)

print("Number of requests: " + str(len(data.index)))
print("Filtered number of requests: " + str(len(filtered.index)) + "\n")

# Label
print("Adding Labels")

poi = pd.read_csv("data/POIList.csv")
filtered['ClosestPOI'] = ""
filtered['DistancePOI'] = np.nan

for index, row in filtered.iterrows():
    latitude_1 = row['Latitude']
    longitude_1 = row['Longitude']

    for poi_index, poi_row in poi.iterrows():
        latitude_2 = poi_row[' Latitude']
        longitude_2 = poi_row['Longitude']
        distance = math.hypot(longitude_2 - longitude_1, latitude_2 - latitude_1)
        if poi_index == 0:
            min_distance = distance
            min_poi = poi_row['POIID']
        if distance < min_distance:
            min_distance = distance
            min_poi = poi_row['POIID']

    filtered.at[index, 'ClosestPOI'] = min_poi
    filtered.at[index, 'DistancePOI'] = min_distance

print("Labels added\n")

# Analysis Part 1
print("Analysis Part 1")

poi['Total'] = 0.0
poi['Average'] = 0.0
poi['Std'] = 0.0

for index, row in filtered.iterrows():
    poi_index = poi[poi['POIID'] == row['ClosestPOI']].index.values.astype(int)[0]
    poi.at[poi_index, 'Total'] = poi.at[poi_index, 'Total'] + 1
    poi.at[poi_index, 'Average'] = poi.at[poi_index, 'Average'] + row['DistancePOI']

for index, row in poi.iterrows():
    if poi.at[index, 'Total'] == 0:
        continue
    else:
        poi.at[index, 'Average'] = poi.at[index, 'Average'] / poi.at[index, 'Total']

for index, row in filtered.iterrows():
    poi_index = poi[poi['POIID'] == row['ClosestPOI']].index.values.astype(int)[0]
    poi.at[poi_index, 'Std'] = poi.at[poi_index, 'Std'] + ((row['DistancePOI'] - poi.at[poi_index, 'Average']) ** 2)

for index, row in poi.iterrows():
    if poi.at[index, 'Total'] == 0:
        continue
    else:
        poi.at[index, 'Std'] = poi.at[index, 'Std'] / (poi.at[index, 'Total'] - 1)
        poi.at[index, 'Std'] = math.sqrt(poi.at[index, 'Std'])

for index, row in poi.iterrows():
    print(str(row['POIID']) + " average distance: " + str(row['Average']))
    print(str(row['POIID']) + " standard deviation: " + str(row['Std']))

# Analysis Part 2
print("\nAnalysis Part 2")

for index, row in poi.iterrows():
    if poi.at[index, 'Total'] == 0:
        continue
    latitude = row[' Latitude']
    longitude = row['Longitude']
    radius = 0
    fig, ax = plt.subplots()
    lat_arr = []
    long_arr = []
    for filtered_index, filtered_row in filtered.iterrows():
        if filtered_row['ClosestPOI'] == row['POIID']:
            ax.plot((filtered_row['Longitude'] - longitude), (filtered_row['Latitude'] - latitude), 'o', color='blue')
            if filtered_row['DistancePOI'] > radius:
                radius = filtered_row['DistancePOI']
    circle = plt.Circle((0, 0), radius, color='r', fill=False)
    ax.add_patch(circle)
    plt.title(row['POIID'])
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.axhline(y=0, color='black', linestyle='-')
    plt.axvline(x=0, color='black', linestyle='-')
    plt.show()
    area = math.pi * radius * radius
    density = row['Total'] / area
    print(str(row['POIID']) + " radius: " + str(radius))
    print(str(row['POIID']) + " density: " + str(density))

# Pipeline Dependency

f = open('text/question.txt', 'r')
start = f.readline().split()
start = int(start[len(start) - 1])
print("\nStart task: " + str(start))
end = f.readline().split()
end = int(end[len(end) - 1])
print("Goal task: " + str(end))
f.close()

f = open('text/task_ids.txt', 'r')
ids = f.readline().split(',')
f.close()

f = open('text/relations.txt', 'r')
arr = []
with open('text/relations.txt', 'r') as f:
    while True:
        line = f.readline()
        if not line:
            break
        line = line.replace('->', ' ').rstrip().split()
        arr.append(line)

outputs = []


def lookup(current, end_task, array, tasks):
    if current == end_task:
        tasks = tasks + "," + str(current)
        outputs.append(tasks)
    else:
        for match in array:
            if int(match[0]) == current:
                lookup(int(match[1]), end_task, array, tasks + "," + str(current))


lookup(start, end, arr, "")


for i in range(len(outputs)):
    lines = outputs[i][1:len(outputs[i])]
    lines = lines.split(",")
    if i == 0:
        min_output = len(lines)
        min_index = i
    elif len(lines) < min_output:
        min_output = len(lines)
        min_index = i

best_output = lines = outputs[min_index][1:len(outputs[min_index])]
print("The best output for the pipeline dependency is " + best_output)
