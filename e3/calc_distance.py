import sys
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
from math import cos, asin, sqrt, pi
from pykalman import KalmanFilter

def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation
    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.7f' % (pt['lat']))
        trkpt.setAttribute('lon', '%.7f' % (pt['lon']))
        trkseg.appendChild(trkpt)
    
    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)
    
    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)
    
    with open(output_filename, 'w') as fh:
        doc.writexml(fh, indent=' ')


def get_data(file):
    parse_result = ET.parse(file)
    df = pd.DataFrame(columns = ['lat', 'lon', 'datetime'])
    for child in parse_result.iter('{http://www.topografix.com/GPX/1/0}trkpt'):
        df1 = pd.DataFrame([[float(child.attrib['lat']), float(child.attrib['lon']), child[0].text]], 
                            columns=['lat', 'lon', 'datetime'])
        df = pd.concat([df, df1], ignore_index = True)
        #print(df)
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
    return df

def haversine(lat1, lon1, lat2, lon2):
    p = pi/180
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1-cos((lon2-lon1)*p))/2
    return 12742 * asin(sqrt(a))
# ref: https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/21623206

def distance(points):
    df = points.copy()
    df['lat2'] = df['lat'].shift()
    df['lon2'] = df['lon'].shift()
    #df['haversine'] = haversine(df['lat'], df['lon'], df['lat2'], df['lon2'])
    df['haversine'] = df.apply(lambda x: haversine( x['lat'], x['lon'], x['lat2'], x['lon2']), axis=1)
    #print(df)
    return df['haversine'].sum() * 1000

def smooth(points):
    initial_state = points.iloc[0]
    observation_covariance = np.diag([0.00005, 0.00005, 0.00005, 0.00005]) ** 2
    transition_covariance = np.diag([0.0001, 0.0001, 10000, 10000]) ** 2
    transition = [[1, 0, 6E-7, 29E-7], [0, 1, -43E-7, 12E-7], [0, 0, 1, 0], [0, 0, 0, 1]]

    kf = KalmanFilter(
        initial_state_mean=initial_state, 
        initial_state_covariance=observation_covariance,
        observation_covariance=observation_covariance, 
        transition_covariance=transition_covariance,
        transition_matrices=transition)

    kalman_smoothed, _ = kf.smooth(points)
    smoothed_data = pd.DataFrame(kalman_smoothed)
    smoothed_data.columns = ['lat', 'lon', 'Bx', 'By']
    #smoothed_data.columns = ['lat', 'lon']
    return smoothed_data

def main():
    input_gpx = sys.argv[1]
    input_csv = sys.argv[2]
    
    points = get_data(input_gpx).set_index('datetime')

    sensor_data = pd.read_csv(input_csv, parse_dates=['datetime']).set_index('datetime')
    points['Bx'] = sensor_data['Bx']
    points['By'] = sensor_data['By']

    #print(haversine(points['lat'][0], points['lon'][0], points['lat'][1], points['lon'][1]))
    dist = distance(points)
    print(f'Unfiltered distance: {dist:.2f}')
    #print(points)
    smoothed_points = smooth(points)
    smoothed_dist = distance(smoothed_points)
    print(f'Filtered distance: {smoothed_dist:.2f}')

    output_gpx(smoothed_points, 'out.gpx')


if __name__ == '__main__':
    main()
