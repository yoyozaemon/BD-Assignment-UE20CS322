#!/usr/bin/env python3
import json
import sys
from math import dist

distance, latitude, longitude = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])


def euclideanDistance(lat1, lat2, lon1, lon2):
    return dist((lat1, lon1), (lat2, lon2))


def validate(line):
    current_json = json.loads(line)
    if 48 < current_json["humidity"] < 54 and 20 < current_json["temperature"] < 24:
        if euclideanDistance((float(current_json["lat"])), latitude, (float(current_json['lon'])), longitude) < distance:
            return True


for i in sys.stdin:
    i = i.strip()
    if validate(i):
        print(i)
