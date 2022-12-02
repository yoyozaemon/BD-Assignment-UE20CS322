import socket as soc
from _thread import *
import json as jsn
import os as o

PORT = 4001
try:
    server1 = soc.socket(soc.AF_INET, soc.SOCK_STREAM)
    name_host = soc.gethostname()
    address_ip = soc.gethostbyname(name_host)
    server1.bind((address_ip, PORT))
    server1.listen(5)
    print("socket is listening!!")

except soc.error as e:
    print(e)

msgList = {}    

p = o.path.join(o.getcwd(),'Topics')
numberPart = 3
part = 1
def thread_product(client, address):
    global msgList,numberPart,part
    
    msgTopic = jsn.loads(client.recv(1024).decode('utf-8'))
    if(not o.path.exists(o.path.join(p,msgTopic['topic']))):
        o.mkdir(o.path.join(p,msgTopic['topic']))
    f = open(o.path.join(p,msgTopic['topic'],f'partition{part}.txt'),'a')
    f.write(msgTopic['message']+'\n')
    port_rep = 4002
    try:
        rep = soc.socket(soc.AF_INET, soc.SOCK_STREAM)
        rep.connect((address_ip,port_rep))
        rep.send("broker00".encode('utf-8'))
        rep.send((msgTopic['topic'] + ',' + f'{part}').encode('utf-8'))
        ack = rep.recv(1024).decode('utf-8')
        rep.send(msgTopic['message'].encode('utf-8'))
        rep.close()

    except soc.error as e:
        print(f"Broker with port {port_rep} Failed")
    
    try:
        rep = soc.socket(soc.AF_INET, soc.SOCK_STREAM)
        port_rep = 4003
        rep.connect((address_ip,port_rep))
        rep.send("broker00".encode('utf-8'))
        rep.send((msgTopic['topic'] + ',' + f'{part}').encode('utf-8'))
        ack = rep.recv(1024).decode('utf-8')
        rep.send(msgTopic['message'].encode('utf-8'))
        rep.close()

    except soc.error as e:
        print(f"Broker with port {port_rep} Failed")
    
    if msgTopic['topic'] in msgList:
        msgList[msgTopic['topic']].append(msgTopic['message'])
    else:
        msgList[msgTopic['topic']] = [msgTopic['message']]
    print(msgList)
    f.close()
    part += 1
    if(part>numberPart):
        part = 1
    client.close()
    
def consumerThread(client, address):
    global msgList,numberPart
    part1 = 1
    offsetTopic = jsn.loads(client.recv(1024).decode('utf-8'))
    Topic = offsetTopic['topic']
    off_set = offsetTopic['offset']
    if(o.path.exists(o.path.join(p,Topic))):
        print('Continue')
    else:
        o.mkdir(o.path.join(p,Topic))
    
    temp_msg_list = []
    while part1 <= numberPart:
        if(o.path.isfile(o.path.join(p,Topic,f'partition{part1}.txt')) == False):
            f = open(o.path.join(p,Topic,f'partition{part1}.txt'),'w')
            f.close()
        f = open(o.path.join(p,Topic,f'partition{part1}.txt'),'r')
        lineMsg =  f.readline()
        while lineMsg != '':
            temp_msg_list.append(lineMsg)
            
            lineMsg =  f.readline()
        f.close()
        part1 += 1
    msgList[Topic] = temp_msg_list
    ind = len(msgList[Topic])
    # print(msgList[topic])
    if(off_set == '1'):
        ind = 0
    while True:
        
        # print(msgList[topic])
        if len(msgList[Topic]) == ind:
            continue
        d = msgList[Topic][ind]
        # print(data)
        ind += 1
        client.send(d.encode('utf-8'))

def topic_rep(client,address):
    global p
    partitionTopic = client.recv(1024).decode('utf-8')
    client.send("OK".encode('utf-8'))
    Topic,part1 = partitionTopic.split(',')
    message = client.recv(1024).decode('utf-8')
    # print(topic_partition)
    if(not o.path.exists(o.path.join(p,Topic))):
        o.mkdir(o.path.join(p,Topic))
    f = open(o.path.join(p,Topic,f'partition{part1}.txt'),'a')
    f.write(message+'\n')
    client.close()


if __name__ == '__main__':
    while True:
        client, address = server1.accept()
        clientType = client.recv(9).decode('utf-8')
        if clientType == "zookeeper":
            client.close()
        elif clientType == "producers":
            start_new_thread(thread_product, (client, address))
        elif clientType == "consumers":
            start_new_thread(consumerThread, (client, address))
        elif clientType == "broker00":
            start_new_thread(topic_rep, (client,address))
