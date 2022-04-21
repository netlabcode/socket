#!/user/bin/env python3
# https://stackoverflow.com/questions/62149284/robust-continuous-tcp-connection-python-socket
# https://instructobit.com/tutorial/101/Reconnect-a-Python-socket-after-it-has-lost-its-connection
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
    port = 12000  
    clientSocket.connect( ( host, port ) )  
    
    # keep track of connection status  
    connected = True  
    print( "connected to server" )  
    
    while True:  
        # attempt to send and receive wave, otherwise reconnect  
        try:  
            message = clientSocket.recv( 1024 ).decode( "UTF-8" )  
            clientSocket.send( bytes( "Client wave", "UTF-8" ) )  
            print( message )  
        except socket.error:  
            # set connection status and recreate socket  
            connected = False  
            clientSocket = socket.socket()  
            print( "connection lost... reconnecting" )  
            while not connected:  
                # attempt to reconnect, otherwise sleep for 2 seconds  
                try:  
                    clientSocket.connect( ( host, port ) )  
                    connected = True  
                    print( "re-connection successful" )  
                except socket.error:  
                    sleep( 2 )  
    
    clientSocket.close()





# Define a function for the thread
def serverOne():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s1:
        s1.bind(('',PORT1))
        s1.listen()
        conn1, addr = s1.accept()
        value=0
        with conn1:
            print('Server 1 from:',addr)
            while True:
                a = 1
                while a < 6:

                    try:
                        #Update OPC value
                        value1 = val1.get_value()
                        value2 = val2.get_value()
                        value3 = val3.get_value()
                        value4 = val4.get_value()
                        value5 = val5.get_value()
                        value6 = val6.get_value()
                        dt = datetime.now()


                        #covert inetger to string
                        #stringd = str(value)

                        stringd =  str(dt)+"+"+str(value1)+"+"+str(value2)+"+"+str(value3)+"+"+str(value4)+"+"+str(value5)+"+"+str(value6)

                        #convert string to bytes data
                        data1 = stringd.encode()

                        #send data back to client
                        conn1.sendall(data1)

                        #print('S1:',data1)
                        time.sleep(1)

                    except Exception:
                        print("One")
                        pass


def serverOneCC():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s1:
        s1.bind(('',PORT3))
        s1.listen()
        conn1, addr = s1.accept()
        value=0
        with conn1:
            print('Server 1 from:',addr)
            while True:
                a = 1
                while a < 6:

                    try: 
                        #Update OPC value
                        value1 = val1.get_value()
                        value2 = val2.get_value()
                        value3 = val3.get_value()
                        value4 = val4.get_value()
                        value5 = val5.get_value()
                        value6 = val6.get_value()
                        dt = datetime.now()

                        #covert inetger to string
                        #stringd = str(value)

                        stringd = str(dt)+"+"+str(value1)+"+"+str(value2)+"+"+str(value3)+"+"+str(value4)+"+"+str(value5)+"+"+str(value6)

                        #convert string to bytes data
                        data1 = stringd.encode()

                        #send data back to client
                        conn1.sendall(data1)

                        #print('S1:',data1)
                        time.sleep(1)

                    except Exception:
                        print("OneCC")
                        pass


# Define a function for the thread
def serverTwo():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s2:
        s2.bind(('',PORT2))
        s2.listen()
        conn2, addr = s2.accept()
        valueb=0
        with conn2:
            print('Server 2 from:',addr)
            while True:
                b = 1
                value = 2

                data2 = conn2.recv(1024)
                data3 = data2.decode("utf-8")

                try: 
                    a,b = data3.split("+")

                    if '.' in b:
                        value = float(b)
                    else:
                        value = int(b)
                    check = int(a)
                    if check == 367:
                        val1.set_value(value, ua.VariantType.Int16)
                        print('Value 367 set to:',value)
                    if check == 373:
                        val2.set_value(value, ua.VariantType.Int16)
                        print('Value 373 set to:',value)
                    elif check == 379:
                        val3.set_value(value, ua.VariantType.Int16)
                        print('Value 379 set to:',value)
                    elif check == 412:
                        val4.set_value(value, ua.VariantType.Float)
                        print('Value 412 set to:',value)
                    elif check == 259:
                        val5.set_value(value, ua.VariantType.Float)
                        print('Value 259 set to:',value)
                    elif check == 260:
                        val6.set_value(value, ua.VariantType.Float)
                        print('Value 260 set to:',value)
                    elif check == 532:
                        val7.set_value(value, ua.VariantType.Int16)
                        print('Value 532 set to:',value)
                    elif check == 538:
                        val8.set_value(value, ua.VariantType.Int16)
                        print('Value 538 set to:',value)
                    elif check == 544:
                        val9.set_value(value, ua.VariantType.Int16)
                        print('Value 544 set to:',value)
                    elif check == 679:
                        val10.set_value(value, ua.VariantType.Int16)
                        print('Value 679 set to:',value)
                    elif check == 685:
                        val11.set_value(value, ua.VariantType.Int16)
                        print('Value 685 set to:',value)
                    else:
                            print(".")

                except Exception:
                        print("Two")
                        pass

def serverTwoCC():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s2:
        s2.bind(('',PORT4))
        s2.listen()
        conn2, addr = s2.accept()
        valueb=0
        with conn2:
            print('Server 2 from:',addr)
            while True:
                b = 1
                value = 2

                data2 = conn2.recv(1024)
                data3 = data2.decode("utf-8")

                try:
                    a,b = data3.split("+")

                    if '.' in b:
                        value = float(b)
                    else:
                        value = int(b)
                    check = int(a)
                    if check == 367:
                        val1.set_value(value, ua.VariantType.Int16)
                        print('Value 367 set to:', value)
                    if check == 373:
                        val2.set_value(value, ua.VariantType.Int16)
                        print('Value 373 set to:', value)
                    elif check == 379:
                        val3.set_value(value, ua.VariantType.Int16)
                        print('Value 379 set to:', value)
                    elif check == 412:
                        val4.set_value(value, ua.VariantType.Float)
                        print('Value 412 set to:', value)
                    elif check == 259:
                        val5.set_value(value, ua.VariantType.Float)
                        print('Value 259 set to:',value)
                    elif check == 260:
                        val6.set_value(value, ua.VariantType.Float)
                        print('Value 260 set to:',value)
                    elif check == 532:
                        val7.set_value(value, ua.VariantType.Int16)
                        print('Value 532 set to:',value)
                    elif check == 538:
                        val8.set_value(value, ua.VariantType.Int16)
                        print('Value 538 set to:',value)
                    elif check == 544:
                        val9.set_value(value, ua.VariantType.Int16)
                        print('Value 544 set to:',value)
                    elif check == 679:
                        val10.set_value(value, ua.VariantType.Int16)
                        print('Value 679 set to:',value)
                    elif check == 685:
                        val11.set_value(value, ua.VariantType.Int16)
                        print('Value 685 set to:',value)
                    else:
                            print(".")

                except Exception:
                        print("TwoCC")
                        pass


# Create two threads as follows
try:
   #_thread.start_new_thread( serverOne, ( ) )
   #_thread.start_new_thread( serverOneCC, ( ) )
   #_thread.start_new_thread( serverTwo, ( ) )
   #_thread.start_new_thread( serverTwoCC, ( ) )
   _thread.start_new_thread( test, ( ) )
except:
   print ("Error: unable to start thread")

while 1:
   pass
