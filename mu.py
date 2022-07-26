from opcua import Client
from opcua import ua
import socket
from socket import *
import binascii
import _thread
import time
from datetime import datetime
import pandas as pd


df = pd.read_csv('substations.csv')

select_sub = df[df['subs']=='S01']

# count = 0
# for ind in select_sub.index:
#     print(select_sub['id'][ind])
#     globals()[f"var{ind}"]= select_sub['id'][ind]
#     count = ind
#
# test =  globals()[f"var{count}"]
# print(test)

url = "opc.tcp://localhost:8899/freeopcua/server/"
client = Client(url)

count = 0
for ind in select_sub.index:
    print(select_sub['id'][ind])
    id_val = select_sub['id'][ind]
    con_str = "ns=2;i=" + str(id_val)
    globals()[f"var{ind}"]= client.get_node(con_str)
    count = ind

# value2 = globals()[f"var{count}"].get_value()

def getValue():
    dt = datetime.now()
    for ind in select_sub.index:
        globals()[f"value{ind}"] = globals()[f"var{ind}"].get_value()
        print(dt, select_sub['id'][ind], globals()[f"value{ind}"] )

    print("============")


def connectToUAserver():
    # serverPort = 12000
    while(True):
        cUAstatus = False
        while cUAstatus == False:
            try:
                client.connect()
                print(client.connect())
                print("connected to OPC UA Server")
                cUAstatus = True
            except:
                print("cannot connect to OPC UA Server")
                time.sleep(2)

        while cUAstatus == True:
            try:
                getValue()
                time.sleep(2)
            except:
                #client.disconnect()
                print("disconnected from OPC UA Server")
                time.sleep(2)
                cUAstatus = False



    # while True:
    #     # 1. Configure server socket
    #     serverSocket = socket(AF_INET, SOCK_STREAM)
    #     serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    #     serverSocket.bind(('127.0.0.1', serverPort))
    #     serverSocket.listen(1)
    #     print("waiting for client connecting...")
    #     connectionSocket, addr = serverSocket.accept()
    #     connectionSocket.setsockopt(SOL_SOCKET, SO_KEEPALIVE,1)
    #     print(connectionSocket.getsockopt(SOL_SOCKET,SO_KEEPALIVE))
    #     print("...connected.")
    #     serverSocket.close() # Destroy the server socket; we don't need it anymore since we are not accepting any connections beyond this point.
    #
    #     # 2. communication routine
    #     while True:
    #         try:
    #             sentence = connectionSocket.recv(512).decode()
    #         except ConnectionResetError as e:
    #             print("Client connection closed")
    #             break
    #         if(len(sentence)==0): # close if client closed connection
    #             break
    #         else:
    #             print("recv: "+str(sentence))
    #
    #     # 3. proper closure
    #     connectionSocket.shutdown(SHUT_RDWR)
    #     connectionSocket.close()
    #     print("connection closed.")

try:
   #_thread.start_new_thread( serverOne, ( ) )
   #_thread.start_new_thread( serverOneCC, ( ) )
   #_thread.start_new_thread( serverTwo, ( ) )
   #_thread.start_new_thread( serverTwoCC, ( ) )
   _thread.start_new_thread( connectToUAserver, ( ) )
except:
   print ("Error: unable to start thread")

while 1:
   pass