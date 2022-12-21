#implementation mini-kafka zookeeper
from _thread import *
import threading as thread
import socket as soc
import time as t
import signal as sig
import sys as s

PORT = 2181
port_master = 4001

def broker_connect(port_broker):
    global port_master
    while True:
        try:
            client = soc.socket(soc.AF_INET,soc.SOCK_STREAM)
            client.connect((address_ip,port_broker))
            client.send("zookeeper".encode())
            print("broker alive on port: ",port_broker,port_master)
            client.close()
        except soc.error as err:
            if port_master == port_broker:
                if port_broker == 4001:
                    port_master = 4002
                elif port_broker == 4002:
                    port_master = 4003
                elif port_broker == 4003:
                    port_master = 4001
            print("Broker dead on port: ",port_broker,port_master)
        t.sleep(5)


if __name__ == '__main__':
    try:
        server1 = soc.socket(soc.AF_INET,soc.SOCK_STREAM)
        name_host = soc.gethostname()
        address_ip = soc.gethostbyname(name_host)
        server1.bind((address_ip,PORT))
        server1.listen(5)
        print("socket is listening!!")

    except soc.error as err:
        print(err)
    start_new_thread(broker_connect,(4001,))
    start_new_thread(broker_connect,(4002,))
    start_new_thread(broker_connect,(4003,))
    while True:
        client,address = server1.accept()
        clientType = client.recv(1024).decode('utf-8')
        if(clientType == "producer"):
            client.send(str(port_master).encode('utf-8'))
        elif(clientType == "consumer"):
            client.send(str(port_master).encode('utf-8'))
            # pass
        