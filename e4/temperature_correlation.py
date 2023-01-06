import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
#from math import cos, asin, sqrt, pi

'''
def haversine(lat1, lon1, lat2, lon2):
    p = np.pi/180
    a = 0.5 - np.cos((lat2-lat1)*p)/2 + np.cos(lat1*p) * np.cos(lat2*p) * (1-np.cos((lon2-lon1)*p))/2
    return 12742 * np.arcsin(np.sqrt(a))
# ref: https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/21623206


#dis_f = np.vectorize(haversine)
def distance(city, stations):
    stations['distance'] = haversine(city['latitude'],city['longitude'],stations['latitude'],stations['longitude'])
    return stations['distance']
    #d = stations.apply(haversine,city['latitude'], city['longitude'], stations['latitude'], stations['longitude'])
    #print(d)'''


def distance(city, stations):
    R = 6371; 
    dLat = np.radians(np.subtract(city['latitude'], stations['latitude']))
    dLon = np.radians(np.subtract(city['longitude'], stations['longitude']))
    a = np.sin(dLat/2) * np.sin(dLat/2) + np.cos(np.radians(stations['latitude'])) * np.cos(np.radians(city['latitude'])) * np.sin(dLon/2) * np.sin(dLon/2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a)) 
    d = R * c 
    return d
# ref: https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/21623206

def best_tmax(city, stations):
    d = distance(city, stations)
    return stations['avg_tmax'].loc[d.idxmin()]


# get data
stations_file = sys.argv[1]
city_file = sys.argv[2]
output_file = sys.argv[3]

stations = pd.read_json(stations_file, lines=True)
city = pd.read_csv(city_file)

# standard the data
stations['avg_tmax'] = stations['avg_tmax'] / 10
city = city.dropna()
city['area'] = city['area'] / 1000000
city = city[city['area'] < 10000]
city['density'] = city['population'] / city['area']

city['avg_tmax'] = city.apply(best_tmax, axis=1,stations=stations)
#print(distance(city.iloc[0], stations))
#print(city)

plt.plot(city['avg_tmax'],city['density'], 'b.')
plt.title('Temperature vs Population Density')
plt.xlabel('Avg Max Temperature (\u00b0C)')
plt.ylabel('Population Density (people/km\u00b2)')
plt.savefig(output_file)
#plt.show()
