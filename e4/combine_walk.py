import os
import pathlib
import sys
import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET


def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation, parse
    xmlns = 'http://www.topografix.com/GPX/1/0'
    
    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.10f' % (pt['lat']))
        trkpt.setAttribute('lon', '%.10f' % (pt['lon']))
        time = doc.createElement('time')
        time.appendChild(doc.createTextNode(pt['datetime'].strftime("%Y-%m-%dT%H:%M:%SZ")))
        trkpt.appendChild(time)
        trkseg.appendChild(trkpt)

    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)

    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)

    doc.documentElement.setAttribute('xmlns', xmlns)

    with open(output_filename, 'w') as fh:
        fh.write(doc.toprettyxml(indent='  '))


def get_data(input_gpx):
    parse_result = ET.parse(input_gpx)
    df = pd.DataFrame(columns = ['lat', 'lon', 'datetime'])
    for child in parse_result.iter('{http://www.topografix.com/GPX/1/0}trkpt'):
        df1 = pd.DataFrame([[float(child.attrib['lat']), float(child.attrib['lon']), child[1].text]], 
                            columns=['lat', 'lon', 'datetime'])
        df = pd.concat([df, df1], ignore_index = True)
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
    #print(df)
    return df



def main():
    input_directory = pathlib.Path(sys.argv[1])
    output_directory = pathlib.Path(sys.argv[2])
    
    accl = pd.read_json(input_directory / 'accl.ndjson.gz', lines=True, convert_dates=['timestamp'])[['timestamp', 'x']]
    gps = get_data(input_directory / 'gopro.gpx')
    phone = pd.read_csv(input_directory / 'phone.csv.gz')[['time', 'gFx', 'Bx', 'By']]

    first_time = accl['timestamp'].min()
    phone['timestamp'] = first_time + pd.to_timedelta(phone['time'], unit='sec')
    
    # Combining the Data
    accl['timestamp'] = accl['timestamp'].dt.round('4s')
    accl_comb = accl.groupby(['timestamp']).mean()

    gps['datetime'] = gps['datetime'].dt.round('4s')
    gps_comb = gps.groupby(['datetime']).mean()
    #print(gps_comb)
    #phone['timestamp'] = phone['timestamp'].dt.round('4s')
    #phone_comb = phone.groupby(['timestamp'], as_index=False).mean()

    # Correlating Data Sets
    best_offset = 0
    best_correlation = 0
    for offset in np.linspace(-5.0, 5.0, 101):
        phone_offset = phone.copy()
        phone_offset['timestamp'] = phone_offset['timestamp'] + pd.Timedelta(offset, "s")

        phone_offset['timestamp'] = phone_offset['timestamp'].dt.round('4s')
        phone_offset_comb = phone_offset.groupby(['timestamp']).mean()

        phone_offset_comb = phone_offset_comb.join(accl_comb, how='outer')
        phone_offset_comb = phone_offset_comb.dropna()
        
        correlation = phone_offset_comb['x'].dot(phone_offset_comb['gFx']).sum()
        if correlation > best_correlation:
            best_correlation = correlation
            best_offset = offset

    print(f'Best time offset: {best_offset:.1f}')
    os.makedirs(output_directory, exist_ok=True)

    # constructed combined
    phone['timestamp'] = phone['timestamp'] + pd.Timedelta(best_offset, "s")
    phone['timestamp'] = phone['timestamp'].dt.round('4s')
    combined = phone.groupby(['timestamp']).mean()
    combined = combined.join(accl_comb, how='outer')
    combined = combined.join(gps_comb, how='outer')
    combined = combined.dropna()
    combined.reset_index(inplace=True)
    combined['datetime'] = combined['timestamp']

    output_gpx(combined[['datetime', 'lat', 'lon']], output_directory / 'walk.gpx')
    combined[['datetime', 'Bx', 'By']].to_csv(output_directory / 'walk.csv', index=False)
    
    


main()
