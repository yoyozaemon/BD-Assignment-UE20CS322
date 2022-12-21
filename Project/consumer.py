import sys as s
import socket as soc
import argparse as ap
import time as t

parse = ap.ArgumentParser()
parse.add_argument('--fromBeginning', required=True)

arguments = parse.parse_args()
off_set = arguments.fromBeginning


def brokerRequest(cons,address_ip,port_master):
	PORT = int(port_master)
	cons.connect((address_ip,PORT))
	cons.send("conss".encode('utf-8'))
	cons.send(('{"topic":"test1","offset":"' + off_set + '"}').encode('utf-8'))
	while True:
		result = cons.recv(1024).decode('utf-8')
		if result:
			print(result.strip())
		else:
			break

def zookeeperReq(cons,address_ip):
	PORT = 2181
	cons.connect((address_ip, PORT))
	cons.send("cons".encode('utf-8'))
	port_master = cons.recv(1024).decode('utf-8') 
	print(port_master)
	cons.close()
	return port_master

if __name__ == '__main__':
	while True:
		try:
			cons = soc.socket(soc.AF_INET,soc.SOCK_STREAM)
			print("Socket successfully created!!")
			name_host = soc.gethostname()
			address_ip = soc.gethostbyname(name_host)
			port_master = zookeeperReq(cons,address_ip)
		except soc.error as e:
			continue
			
		try:
			cons = soc.socket(soc.AF_INET,soc.SOCK_STREAM)
			print("Socket successfully created!!")
			brokerRequest(cons,address_ip,port_master)
			cons.close()
		except soc.error as e:
			continue

		t.sleep(6)