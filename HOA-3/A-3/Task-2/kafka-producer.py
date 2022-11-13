import sys
from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
   
D = dict()               
for line in sys.stdin:
    #data = {'number' : e}
    if(len(line.split(',')) == 1):
    	break
    line = line.split(',')
    state = line[0]
    mini = int(float(line[-2]))
    maxi = int(float(line[-3]))
    if state in D:
    	D[state]['TotMax'] = D[state]['TotMax'] + maxi
    	D[state]['TotMin'] = D[state]['TotMin'] + mini
    	D[state]['Count'] = D[state]['Count'] + 1
    else:
    	D[state] = {}
    	D[state]['TotMax'] = maxi
    	D[state]['TotMin'] = mini
    	D[state]['Count'] = 1
    #data = {"number": 1}
    #producer.send('numtest', value=data)
    #sleep(5)
#print(D)

for i in D:
	#print(i, D[i]['TotMin'])
	data = {"State": i, "Min": round(D[i]['TotMin']/D[i]['Count'], 2), "Max": round(D[i]['TotMax']/D[i]['Count'], 2)}
	producer.send('numtest', value=data)
	print("message sent")
	#print(data)

