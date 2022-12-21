import sys as s
import socket as soc


def zookeeperReq(prod,address_ip):
	PORT = 2181
	prod.connect((address_ip, PORT))
	prod.send("prod".encode('utf-8'))
	port_master = prod.recv(1024).decode('utf-8')
	print(port_master)
	prod.close()
	return port_master

def brokerRequest(prod,address_ip,port_master):
	PORT = int(port_master)
	prod.connect((address_ip,PORT))
	prod.send("prods".encode('utf-8'))
	prod.send('{"topic":"test1","message":"PES"}'.encode('utf-8'))
	prod.close()

if __name__ == '__main__':
	try:
		prod = soc.socket(soc.AF_INET,soc.SOCK_STREAM)
		print("socket successfully created!!")
		name_host = soc.gethostname()
		address_ip = soc.gethostbyname(name_host)
		port_master = zookeeperReq(prod,address_ip)
	except soc.error as e:
		print(e)
		
	try:
		prod = soc.socket(soc.AF_INET,soc.SOCK_STREAM)
		print("socket successfully created!!")
		brokerRequest(prod,address_ip,port_master)
	except soc.error as er:
		print(er)