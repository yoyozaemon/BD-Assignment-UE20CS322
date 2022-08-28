#!/usr/bin/env python3

import json
import sys


def validate(obj):
    if obj["sensor_id"] < 5000 and (2500 > obj["location"] > 1700) and obj["pressure"] >= 93500.00 and \
            obj["humidity"] >= 72.00 and obj["temperature"] >= 12.00:
        return True


for i in sys.stdin:
    i = i.strip()
    temp = json.loads(i)
    if validate(temp):
        print(i)
