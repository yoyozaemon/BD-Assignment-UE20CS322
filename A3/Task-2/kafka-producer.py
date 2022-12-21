#!/usr/bin/env python3

from kafka import KafkaProducer
import sys
import csv 

bootstrap_servers = ['localhost:9092']
topicName = sys.argv[1]
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

for line in sys.stdin:
    if line == "EOF\n" or line == "EOF":
        producer.send(topicName, value=line.encode())
        continue
    line_csv =  csv.reader([line.strip()])
    line_split = list()
    for row in line_csv :  # Only way to get line outof csv iterator
        line_split = row 
    val = line_split[6]+","+line_split[7]
    print(val)
    producer.send(topicName, key=line_split[0].encode(), value=val.encode())
producer.flush()
producer.close()

