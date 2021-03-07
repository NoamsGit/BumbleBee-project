import datetime

def parse_time(time):
    if 'system_utc' in time:
        _, time = time.split(':')
    y_m_d, h_m_s = time.split('__')
    year, month, day = y_m_d.split('-')
    hours, minutes, seconds, sub_seconds = h_m_s.split('-')
    year, month, day, hours, minutes, seconds, sub_seconds = [int(num) for num in [year, month, day, hours, minutes, seconds, sub_seconds]]
    time_stamp = datetime.datetime(year, month, day, hours, minutes, seconds, sub_seconds)
    return time_stamp

def parse_coord(lat, long):
    GeoJSON = {"type": "Point", "coordinates": [lat, long]}
    return GeoJSON

def parse_coord_record(line):
    line = line.split(',')
    time_stamp = parse_time(line[0])
    GeoJSON = parse_coord(line[1],line[2])
    coord_record = {
        "timeStamp":time_stamp,
        "point": GeoJSON,
        "numberOfSatellites": line[3]
    }
    return coord_record

def parse_humidity_record(line):
    line = line.split(',')
    time_stamp = parse_time(line[0])
    humidity_record = {
        "timeStamp":time_stamp,
        "humidity": line[1],
        "temperature": line[2]
    }
    return humidity_record

# if __name__ == '__main__':
#     coord_record = parse_coord_record('system_utc:2021-01-04__10-31-52-116141,35.80265421666667,32.10561791666667,7')
#     print('end!')
