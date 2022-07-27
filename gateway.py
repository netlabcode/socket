import pickle

from opcua import Client
from opcua import ua
import socket
from socket import *
import binascii
import _thread
import time
from datetime import datetime
import socket
from time import sleep


def test():
    print('test client')

    # configure socket and connect to server
    clientSocket = socket.socket()
    host = '127.0.0.1'
    port = 8777
    clientSocket.connect((host, port))

    # keep track of connection status
    connected = True
    print("connected to server")

    while True:
        # attempt to send and receive wave, otherwise reconnect
        try:
            #message = clientSocket.recv(1024).decode("UTF-8")
            message = clientSocket.recv(1024)
            #clientSocket.send(bytes("Client wave", "UTF-8"))
            data_arr =  pickle.loads(message)
            print(data_arr[1])
            print(type(data_arr))
        except socket.error:
            # set connection status and recreate socket
            connected = False
            clientSocket = socket.socket()
            print("connection lost... reconnecting")
            while not connected:
                # attempt to reconnect, otherwise sleep for 2 seconds
                try:
                    clientSocket.connect((host, port))
                    connected = True
                    print("re-connection successful")
                except socket.error:
                    time.sleep(2)

    clientSocket.close()


#test()

try:
   #_thread.start_new_thread( serverOne, ( ) )
   #_thread.start_new_thread( serverOneCC, ( ) )
   #_thread.start_new_thread( serverTwo, ( ) )
   #_thread.start_new_thread( serverTwoCC, ( ) )
   _thread.start_new_thread( test(), ( ) )
except:
   print ("Error: unable to start thread")

while 1:
   pass