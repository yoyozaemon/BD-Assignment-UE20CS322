from kafka import KafkaConsumer
from json import dumps, loads

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

D = dict()

for message in consumer:
    msg = message.value
    D[msg['State']] = {}
    D[msg['State']]['Max'] = msg['Max']
    D[msg['State']]['Min'] = msg['Min']
json_obj=dumps(D)
print(json_obj)
