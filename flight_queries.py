from pymongo import MongoClient
import pandas as pa
import pprint

client = MongoClient("mongodb+srv://admin:admin@dronecluster-azazs.mongodb.net/test?retryWrites=true&w=majority")

db = client.test

collection = db["dronDataFlight1"]

records = collection.aggregate([
    {
        '$group': {
            '_id': "$flightNumber",
            'record_number': { '$sum': 1},
            'initial_time': { '$min': '$datetime(utc)'},
            'final_time': { '$max': '$datetime(utc)'},
            'average_height': {'$avg': '$max_ascent(feet)'},
            'max_speed_x': { '$max': '$xSpeed(mph)'},
            'max_speed_y': { '$max': '$ySpeed(mph)'},
            'max_speed_z': { '$max': '$zSpeed(mph)'},
            'max_latitude': { '$max': '$latitude'},
            'min_latitude': { '$min': '$latitude'},
            'max_longitude': { '$max': '$longitude'},
            'min_longitude': { '$min': '$longitude'}
        }
    }
])

for record in records:
    pprint.pprint(record)