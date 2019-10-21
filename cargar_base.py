from pymongo import MongoClient
import pandas

nombre_de_database = 'test2'
nombre_de_coleccion = 'flightInfo'

client = MongoClient("mongodb+srv://admin:admin@dronecluster-azazs.mongodb.net/test?retryWrites=true&w=majority")
db = client[nombre_de_database]
collection = db[nombre_de_coleccion]

numero_documentos = 0

flight_number = 1

archivos = ['./files/Oct-10th-2019-01-50PM-Flight-Airdata.csv', './files/Oct-10th-2019-02-00PM-Flight-Airdata.csv',
            './files/Oct-10th-2019-02-06PM-Flight-Airdata.csv']

for archivo in archivos:
    df = pandas.read_csv(archivo, header=0)

    for j in df.iterrows():
        document = {"time(millisecond)": j[1]['time(millisecond)'], "datetime(utc)": j[1]['datetime(utc)'],
                    "latitude": j[1]['latitude'], "longitude": j[1]['longitude'],
                    "height_above_takeoff(feet)": j[1]['height_above_takeoff(feet)'],
                    "height_above_ground_at_drone_location(feet)": j[1]['height_above_ground_at_drone_location(feet)'],
                    "ground_elevation_at_drone_location(feet)": j[1]['ground_elevation_at_drone_location(feet)'],
                    "altitude_above_seaLevel(feet)": j[1]['altitude_above_seaLevel(feet)'],
                    "speed(mph)": j[1]['speed(mph)'], "distance(feet)": j[1]['distance(feet)'],
                    "satellites": j[1]['satellites'], "gpslevel": j[1]['gpslevel'], "voltage(v)": j[1]['voltage(v)'],
                    "max_altitude(feet)": j[1]['max_altitude(feet)'], "max_ascent(feet)": j[1]['max_ascent(feet)'],
                    "max_speed(mph)": j[1]['max_speed(mph)'], "max_distance(feet)": j[1]['max_distance(feet)'],
                    "xSpeed(mph)": j[1][' xSpeed(mph)'], "ySpeed(mph)": j[1][' ySpeed(mph)'],
                    "zSpeed(mph)": j[1][' zSpeed(mph)'], "compass_heading(degrees)": j[1][' compass_heading(degrees)'],
                    "pitch(degrees)": j[1][' pitch(degrees)'], "roll(degrees)": j[1][' roll(degrees)'],
                    "isPhoto": j[1]['isPhoto'], "isVideo": j[1]['isVideo'], "rc_elevator": j[1]['rc_elevator'],
                    "rc_aileron": j[1]['rc_aileron'], "rc_throttle": j[1]['rc_throttle'],
                    "rc_rudder": j[1]['rc_rudder'],
                    "gimbal_heading(degrees)": j[1]['gimbal_heading(degrees)'],
                    "gimbal_pitch(degrees)": j[1]['gimbal_pitch(degrees)'],
                    "battery_percent": j[1]['battery_percent'], "voltageCell1": j[1]['voltageCell1'],
                    "voltageCell2": j[1]['voltageCell2'], "voltageCell3": j[1]['voltageCell3'],
                    "voltageCell4": j[1]['voltageCell4'], "voltageCell5": j[1]['voltageCell5'],
                    "voltageCell6": j[1]['voltageCell6'], "current(A)": j[1]['current(A)'],
                    "battery_temperature(f)": j[1]['battery_temperature(f)'], "altitude(feet)": j[1]['altitude(feet)'],
                    "ascent(feet)": j[1]['ascent(feet)'], "flycStateRaw": j[1]['flycStateRaw'],
                    "flycState": j[1]['flycState'], "message": j[1]['message'], "flightNumber": flight_number}
        print(document)
        post_id = collection.insert_one(document).inserted_id
        print(post_id)
        numero_documentos = numero_documentos + 1
    flight_number = flight_number + 1

print("El numero total de documentos ingresados fue: {}".format(numero_documentos))
