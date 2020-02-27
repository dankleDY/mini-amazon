import socket
import string
import world_amazon_pb2
import psycopg2
import sys

import gl
from sender import sender
from receiver import receiver

from google.protobuf.internal.decoder import _DecodeVarint32
from google.protobuf.internal.encoder import _EncodeVarint

WORLD_HOST = "vcm-8209.vm.duke.edu"
WORLD_PORT = 23456

# ===========TODO==========
# ==========build the pack new order request string =======


def conn1():
    command = world_amazon_pb2.AConnect()
    #command.worldid=1
    init = command.initwh.add()
    init.id = 1
    init.x = 0
    init.y = 0
    command.isAmazon = True
    return command

class WorldConnector:
    def __init__(self, conn):
        self.conn = conn
        self.sock = None
        self.sender = sender()
        self.receiver = receiver()

    def sender_receiver_setup(self):
        self.sender.setup(self.conn, self.sock)
        self.receiver.setup(self.conn, self.sock)

    def init_wh(self):
        try:
            pur = world_amazon_pb2.APurchaseMore()
            pur.whnum = 1
            with gl.lock:
                pur.seqnum = gl.seq
                gl.seq += 1
            things = pur.things.add()
            things.id = 1
            things.description = "C++"
            things.count = 10000
            a = pur.seqnum  
            msg = pur.SerializeToString()
            #msg = pur.SerializeToString()
            print("In init wh:",msg)
            #msg.decode(encoding="utf-8", errors="ignore")
            self.sender.db_insert_request(a, msg , "'APurchaseMore'")


            pur = world_amazon_pb2.APurchaseMore()
            pur.whnum = 1
            with gl.lock:
                pur.seqnum = gl.seq
                gl.seq += 1
            things = pur.things.add()
            things.id = 2
            things.description = 'JAVA'
            things.count = 10000
            print("In init wh 2:",pur)
            a = pur.seqnum 
            msg = (pur.SerializeToString())
            self.sender.db_insert_request(pur.seqnum, msg , "'APurchaseMore'")
            print("Init world warehouse done")
        except UnboundLocalError as e:
            print("======== IN worldConn Init_wh=======")
            print(e)

    def handle(self):
        self.init_wh()
        while(1):
            a=1
        while (1):
            try:
                self.pack_new_order()
                self.load_unloaded()
                self.check_reconnect()
            except:
                print("==========worldConn handle error==========")

    # TABLE orders : packageid  itemid  quantity   upsid  des  pack  truckid   deliver 
    def pack_new_order(self):
        try:
            # ==== select new order(unpacked) ====
            sql = """SELECT * FROM orders WHERE pack='unpacked';"""
            cur = self.conn.cursor()
            cur.execute(sql)
            results = cur.fetchall()
            if results:
                # put a request in the sender queue
                for result in results:
                    packageid = result[0]
                    # make a request to the world with package id, quantity, upsid, put it in the sender
                    purchase, pack = self.make_pack_request(result)
                    with gl.lock:
                        if (purchase.HasField('things')):
                            pur = purchase.SerializeToString()
                            self.sender.db_insert_request(gl.seq, pur, "APurchaseMore")
                            gl.seq += 1
                        self.sender.db_insert_request(gl.seq, pack, "APack")
                        gl.seq += 1
                    sql = ("""UPDATE orders SET status = packing WHERE packageid = %s;"""%(packageid,))
                    one = self.conn.cursor()
                    one.execute(sql)
                    one.commit()
                    one.close()
            cur.close()
            print("pack order placed")
        except(Exception, psycopg2.DatabaseError) as error:
            print("=======pack order error ============")
            print(error)

    def load_unloaded(self):
        try:
            # ==== select unloaded order ====
            sql = """SELECT * FROM orders WHERE pack='packed' AND truckid>0 AND deliver='unloaded';"""
            cur = self.conn.cursor()
            cur.execute(sql)
            results = cur.fetchall()
            if results:
                # put a request in the sender queue
                for result in results:
                    load = self.make_load_request(result)
                    self.sender.db_insert_request(gl.seq, load, "APutOnTruck")
                    gl.seq += 1
            cur.close()
        except(Exception, psycopg2.DatabaseError) as error:
            print("==== load unloaded error====")
            print(error)

    # ============put on truck==========
    def make_load_request(self, result):
        load = world_amazon_pb2.APutOnTruck()
        load.whnum = 1
        load.truckid = result[6]
        load.shipid = result[0]
        load.seqnum = gl.seq
        msg = load.SerializeToString()
        return msg

    # ============pack new order==========
    def make_pack_request(self, result):
        product = self.make_product(result[1], result[2])
        # check local inventory
        pur = self.check_local_inventory(result[1], result[2])
        pack = world_amazon_pb2.APack()
        pack.whnum = 1
        product = pack.things.add()
        pack.shipid = result[0]
        pack.seqnum = gl.seq
        msg = pack.SerializeToString()
        return pur, msg

    def make_product(self, id, count):
        product = world_amazon_pb2.AProduct()
        product.id = id
        product.description = self.get_description(id)
        print("==== get description====")
        product.count = count
        return product

    def check_local_inventory(self, id, count):
        try:
            sql = ("""SELECT quantity FROM warehouse WHERE itemid = %s;""" % (id,))
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            quantity = result[0]
            pur = world_amazon_pb2.APurchaseMore()
            pur.whnum = 1
            pur.seqnum = gl.seq
            if (quantity <= count):
                product = self.make_product(id, count)
                product = pur.things.add()
            elif ((quantity - count) < 2000):
                q = 5000 - (quantity - count)
                product = self.make_product(id, q)
                product = pur.things.add()
            return pur  # NOT SERIALIZED
        except(Exception, psycopg2.DatabaseError) as error:
            print("======== check local inven error ==========")
            print(error)

    def get_description(self, id):
        try:
            sql = ("""SELECT description FROM warehouse WHERE itemid = %s ;""" % (id,))
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            return result[0]
        except(Exception, psycopg2.DatabaseError) as error:
            print("======== get description error ==========")
            print(error)

    def recv_data(self, mysock):
        var_int_buff = []
        while True:
            buf = mysock.recv(1)
            var_int_buff += buf
            msg_len, new_pos = _DecodeVarint32(var_int_buff, 0)
            if new_pos != 0:
                break
        whole_message = mysock.recv(msg_len)
        return whole_message

    def connect_world(self):
        try:
            world_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            world_sock.connect((WORLD_HOST, WORLD_PORT))
            print("world sock connected")
            # send AConnect
            msg = conn1().SerializeToString()
            hdr = []
            _EncodeVarint(hdr.append, len(msg))
            world_sock.sendall(hdr[0])
            world_sock.sendall(msg)
            print("world connect msg sent")
            # recv response
            whole_message = self.recv_data(world_sock)
            recv = world_amazon_pb2.AConnected()
            recv.ParseFromString(whole_message)
            print(recv)
            self.sock = world_sock
            #print("=======world connected=========")
            print("world sock is:",self.sock)
        except IndexError as err:
            print(err)
            exit()
        except BaseException:
            print("=======world connect error=========")
            print("Unexpected worldConn_socket error: ", sys.exc_info()[0])
        except:
            exit()

    def check_reconnect(self):
        if (self.sender.reconnect == True):
            self.sock = self.sender.sock
            self.receiver.sock = self.sock
