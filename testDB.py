import socket
import sys
import psycopg2
import threading
from _thread import *
import world_amazon_pb2
import amazon_ups_pb2
# from worldConn import WorldConnector
from upsConn import UPSConnector
from webConn import WebConnector
from databaseConn import DatabaseConnector
from google.protobuf.internal.decoder import _DecodeVarint32
from google.protobuf.internal.encoder import _EncodeVarint

WORLD_HOST = "vcm-8003.vm.duke.edu"
WORLD_PORT = 23456


def conn1():
    command = world_amazon_pb2.AConnect()
    init = command.initwh.add()
    init.id = 1
    init.x = 0
    init.y = 0
    command.isAmazon = True
    return command


def conn2():
    command = world_amazon_pb2.AConnect()
    command.worldid = 2
    command.isAmazon = True
    # init = command.initwh.add()
    # init.id = 1
    # init.x = 0
    # init.y = 0
    return command


def init_wh(seqnum):
    command = world_amazon_pb2.ACommands()
    buy = command.buy.add()
    product = buy.things.add()
    product.id = 1
    product.description = "test product"
    product.count = 10
    buy.whnum = 1
    buy.seqnum = seqnum
    pack = command.topack.add()
    pack.whnum = 1
    pack.shipid = seqnum + 100
    pack.seqnum = seqnum + 100
    things = pack.things.add()
    things.id = 1
    things.description = "test product"
    things.count = 5
    return command


def recv_data(sock):
    var_int_buff = []
    while True:
        buf = sock.recv(1)
        var_int_buff += buf
        msg_len, new_pos = _DecodeVarint32(var_int_buff, 0)
        if new_pos != 0:
            break
    whole_message = sock.recv(msg_len)
    return whole_message
"""
db = DatabaseConnector()
conn = psycopg2.connect("dbname = 'Amazon' user = 'postgres' password = 'passw0rd'")
cur = conn.cursor()
cur.execute("SELECT packageid, des, upsid, description, quatity FROM orders WHERE truckid = -1;")
rs = cur.fetchall()
if rs:
    for result in rs:
        print(result)
        print(result[1][0])
        # create.description = result[3]
        # create.quatity = result[4]
cur.close()
conn.close()
"""
try:
    print("to connecte")
    world_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    world_sock.connect((WORLD_HOST, WORLD_PORT))
    print("connected")
    msg = conn1().SerializeToString()
    hdr = []
    _EncodeVarint(hdr.append, len(msg))
    world_sock.sendall(hdr[0])
    world_sock.sendall(msg)

    whole_message = recv_data(world_sock)

    recv = world_amazon_pb2.AConnected()
    recv.ParseFromString(whole_message)
    print(recv)

    seqnum = 1
    while seqnum <= 100:
        msg = init_wh(seqnum).SerializeToString()
        hdr = []
        _EncodeVarint(hdr.append, len(msg))
        world_sock.sendall(hdr[0])
        world_sock.sendall(msg)

        whole_message = recv_data(world_sock)

        recv = world_amazon_pb2.AResponses()
        recv.ParseFromString(whole_message)
        print(recv)
        seqnum = seqnum + 1
    while True:
        a = 1
except socket.error as msg:
    print(msg)

finally:
    world_sock.close()
"""
"""