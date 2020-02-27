import psycopg2
import world_amazon_pb2
import time
import sys
import socket
from google.protobuf.internal.decoder import _DecodeVarint32
from google.protobuf.internal.encoder import _EncodeVarint

#=========TODO==========
#    update status in orders

#  Request table :  request_id   request   status

WORLD_HOST = "vcm-8209.vm.duke.edu"
WORLD_PORT = 23456

class sender():
    def __init__(self):
        self.conn = None
        self.sock = None
        self.reconnect = False
        self.ACommands = world_amazon_pb2.ACommands()
        
    def setup(self,conn,sock):
        self.conn = conn
        self.sock = sock
    
    def handle(self):
        # sql = """SELECT * FROM requests"""    DELETE ACKED requests
        sql1 = """SELECT seq_num, type FROM requests WHERE status = 'unacked' OR type = 'ACK';"""  # keep acked requests for debug
        sql2 = """SELECT request FROM requests WHERE status = 'unacked' OR type = 'ACK';"""
        cur = self.conn.cursor()
        while(1):
            try:
                self.ACommands = world_amazon_pb2.ACommands()
                cur.execute(sql1)
                results = cur.fetchall()
                cur.execute(sql2)
                if results:
                    print("in sender")
                    for result in results:
                        request = cur.fetchone()[0]
                        self.append_request(result,request)
                print("about to send sender")
                print(self.ACommands)
                msg = self.ACommands.SerializeToString()
                self.send_ACommands(msg)
                #======= sleep 5 for now ==========
                time.sleep(5)
            except:
                print("==========sender handle error==========")
                
            
    def append_request(self,result,request): #res is byte string    
        try:
            type = result[1]
            string = str(request, encoding='utf-8', errors='ignore')
            print("string is :",string)
            if(type=="APack"):
                pack = (self.ACommands).topack.add()
                pack.ParseFromString(string)
            elif(type == "APutOnTruck"):
                load = (self.ACommands).load.add()
                load.ParseFromString(string)
            elif (type == "APurchaseMore"):
                purchase = (self.ACommands).buy.add()  
                purchase.ParseFromString(string)
                print("the purchase is", purchase)
            elif(type == "AQuery"):
                query = (self.ACommands).queries.add()
                query.ParseFromString(string)
            elif(type == "ACK"): # the ack for world's response
                ack = (self.ACommands).acks.add()
                ack.ParseFromString(string)
            else:
                print("error type when appending request:",type)
        except BaseException as error:
            print("===== append request error ========")
            print(error)

    def send_ACommands(self,msg):
        try:
            hdr = []
            _EncodeVarint(hdr.append,len(msg))
            self.sock.sendall(hdr[0])
            self.sock.sendall(msg)
            print("sender  to world sent")
        except BaseException:
            print("======= send ACommand error=========")
            print("Unexpected Sender_socket error: ", sys.exc_info()[0])
            #print ("Caught exception socket.error : %s" % exc)
            self.sock.close()
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((WORLD_HOST, WORLD_PORT))
            print("reconnected")
            self.sock = sock
            self.reconnect = True

    def db_insert_request(self,seq,request,type):
        try:
            sql = ("""INSERT INTO requests (seq_num,request,status,type) VALUES(%s,%s,%s,%s);"""%(seq, psycopg2.Binary(request),"'unacked'",type))
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
            cur.close()
        except(Exception, psycopg2.DatabaseError) as error:
            print("=========insert request error====")
            print(error)



    
