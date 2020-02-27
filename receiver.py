import psycopg2
import world_amazon_pb2
import time
from socket import socket
#import server
import gl 
#from worldConn import WorldConnector
from google.protobuf.internal.decoder import _DecodeVarint32
from google.protobuf.internal.encoder import _EncodeVarint

#======== recv from the world and update orders database===========

class receiver:
    def __init__(self):
        self.conn = None
        self.sock = None
    
    def setup(self,conn,sock):
        self.conn = conn
        self.sock = sock

    def none_select_exec(self,sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()
        cur.close()

    def recv_data(self):
        var_int_buff = []
        while True:
            buf = self.sock.recv(1)
            var_int_buff += buf
            msg_len, new_pos = _DecodeVarint32(var_int_buff, 0)
            if new_pos != 0:
                break
        whole_message = self.sock.recv(msg_len)
        return whole_message

    def handle(self):
        print("receiver handling")
        while(1):
            try:
                msg = self.recv_data()
                print("receiver:",msg)
                continue
                response = world_amazon_pb2.AResponse()
                response.ParseFromString(msg)
                if(response.HasField('acks')):
                    for ack in response.acks:
                        self.remove_request(ack)

                if(response.HasField('arrived')):
                    for arr in response.arrived: #APurchaseMore
                        self.handle_arrived(arr)

                if(response.HasField('ready')):
                    for packed in response.ready: #APacked
                        self.handle_ready_loaded(packed,'ready')
                            
                if(response.HasField('loaded')):
                    for loaded in response.loaded: #ALoaded
                        self.handle_ready_loaded(loaded,'loaded')

                if(response.HasField('error')):
                    for error in response.error:
                        print("======================")
                        print("ERROR MESSAGE:",error.err)
                        print("origin seq:",error.originseqnum)
                        print("======================")
            except:
                print("==========receiver handle error==========")
            
            #==============TODO? =============
            #  status

    def handle_arrived(self,arr):
        seq = arr.seqnum
        exists = self.check_response(seq)
        if(exists==False):
            # execute command
            if arr.HasField('things'):
                for thing in arr.things:
                    itemid = thing.id
                    count = thing.count 
                    #update warehouse
                    sql = ("""UPDATE warehouse SET quantity = quantity+ %s WHERE itemid = %s;"""%(itemid,count))
                    self.none_select_exec(sql)
                # record response &  make ACK
                self.record_response_and_make_ACK(seq)
    
    def handle_ready_loaded(self,res,type):
        id = res.shipid
        seq = res.seqnum
        exists = self.check_response(seq)
        if(exists==False):
            if(type == 'ready'):
                sql = ("""UPDATE orders SET pack = packed WHERE packageid = %s;"""%(id,))
            else:
                sql = ("""UPDATE orders SET deliver = loaded WHERE packageid = %s;"""%(id,))
            self.none_select_exec(sql)
            self.record_response_and_make_ACK(seq)

    def check_response(self,w_seq):
        sql = ("""SELECT * FROM responses WHERE w_seq_num = %s;"""%(w_seq,))
        cur = self.conn.cursor()
        cur.execute(sql)
        results = cur.fetchall()
        if results : 
            return True
        else:
            return False

    def record_response_and_make_ACK(self,w_seq):
        sql = ("""INSERT INTO responses (w_seq_num) VALUES(%s);"""%(w_seq,))
        self.none_select_exec(sql)
        with (gl.lock):
            sql = ("""INSERT INTO requests (seq_num,request,status,type) VALUES(%s,%s,acked,ACK);"""%(gl.seq,w_seq))
            gl.seq+=1

    # acked request can be removed
    def remove_request(self,ack):
        #sql = ("""DELETE FROM requests WHERE seq_num = %s;"""%(ack,))
        sql = ("""UPDATE requests SET status = acked WHERE seq_num = %s;"""%(ack,))
        self.none_select_exec(sql)
        
                    

