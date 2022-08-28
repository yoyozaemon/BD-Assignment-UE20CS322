#!/usr/bin/env python3

import json
import sys
import collections

res = {}


def collect(json_obj):
    if json_obj["timestamp"] in res.keys():
        res.update({json_obj["timestamp"]: res[json_obj["timestamp"]] + 1})
    else:
        res.update({(json_obj["timestamp"]): 1})


def printResult():
    temp = collections.OrderedDict(sorted(res.items()))
    for i in temp:
        print(str(i) + ' ' + str(temp.get(i)))


for i in sys.stdin:
    if i is not None:
        obj = json.loads(i.strip())
        collect(obj)

printResult()
