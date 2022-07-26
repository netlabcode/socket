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
client.connect()
print("connected to OPC UA Server")

count = 0
for ind in select_sub.index:
    print(select_sub['id'][ind])
    id_val = select_sub['id'][ind]
    con_str = "ns=2;i=" + str(id_val)
    globals()[f"var{ind}"]= client.get_node(con_str)
    count = ind

value2 = globals()[f"var{count}"].get_value()

def getValue():
    for ind in select_sub.index:
        globals()[f"value{ind}"] = globals()[f"var{ind}"].get_value()
        dt = datetime.now()
        print(dt, select_sub['id'][ind], globals()[f"value{ind}"] )

    print("============")

a = 1
while a < 6:
    getValue()
    time.sleep(1)