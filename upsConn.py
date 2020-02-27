import socket
import string
import psycopg2
import amazon_ups_pb2
from google.protobuf.internal.decoder import _DecodeVarint32
from google.protobuf.internal.encoder import _EncodeVarint

UPS_HOST = "vcm-8265.vm.duke.edu"
UPS_SEND_PORT = 34567

MY_HOST = socket.gethostbyname(socket.gethostname())
UPS_RECV_PORT = 45679


# =========TODO==========
#     

class UPSConnector:
    def __init__(self, conn):
        self.conn = conn
        self.recv_sock = None
        self.send_sock = None

    def handle(self):
        while (1):
            print("in the loop")
            self.ask_for_truck()
            self.let_truck_deliver()

    def ask_for_truck(self):
        print("asking for a truck")
        cur = self.conn.cursor()
        #============================ truckid -1=================
        cur.execute("SELECT packageid, des, upsid, description, quatity FROM orders WHERE truckid = 0;")
        rs = cur.fetchall()
        if rs:
            for result in rs:
                AMessage = amazon_ups_pb2.UAMessage()
                create = AMessage.creates.add()
                create.packageid = result[0]
                create.whid = 1
                create.desX = result[1][0]
                create.desY = result[1][1]
                create.wh_x = 0
                create.wh_y = 0
                create.upsid = result[2]
                # create.description = result[3]
                # create.quatity = result[4]
                cur.execute("UPDATE orders SET truckid = 0 WHERE packageid = %d;" % (result[0]))
                self.conn.commit()
                print(AMessage)
                self.send_data(AMessage.SerializeToString())
        cur.close()

    def let_truck_deliver(self):
        cur = self.conn.cursor()
        cur.execute("SELECT packageid, truckid FROM orders WHERE deliver = 'loaded';")
        rs = cur.fetchall()
        if rs:
            for result in rs:
                AMessage = amazon_ups_pb2.UAMessage()
                load = AMessage.loads.add()
                load.truckid = result[1]
                cur.execute("UPDATE orders SET deliver = 'delivering' WHERE pacageid = %d;"%(result[0]))
                self.conn.commit()
                self.send_data(AMessage.SerializeToString())
        cur.close()
           

    def send_data(self, msg):
        hdr = []
        print(msg)
        _EncodeVarint(hdr.append, len(msg))
        self.send_sock.sendall(hdr[0])
        self.send_sock.sendall(msg)

    def recv_handler(self):
        while True:
            UMessage = amazon_ups_pb2.UAMessage()
            UMessage = self.recv_data()
            print("received some data")
            UMessage.ParseFromString(message)
            print(UMessage)
            if UMessage.HasField("arrives"):
                self.truck_arrived(UMessage)
            if UMessage.HasField("delivered"):
                self.package_delivered(UMessage)

    def recv_data(self):
        var_int_buff = []
        while True:
            buf = self.recv_sock.recv(1)
            var_int_buff += buf
            msg_len, new_pos = _DecodeVarint32(var_int_buff, 0)
            if new_pos != 0:
                break
        whole_message = self.recv_sock.recv(msg_len)
        return whole_message

    def truck_arrived(self, UMessage):
        cur = self.conn.cursor()
        for truck in UMessage.arrives:
            cur.execute("UPDATE orders SET truckid = %d WHERE packageid = %d;"%(truck.truckid, truck.packageid))
            self.conn.commit()
        cur.close()

    def package_delivered(self, UMessage):
        cur = self.conn.cursor()
        for package in UMessage.delivered:
            cur.execute("UPDATE orders SET delivered = 'delivered' WHERE packageid = %d;"%(package.packageid))
            self.conn.commit()
        cur.close()

    def connect(self):
        try:
            ups_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ups_sock.connect((UPS_HOST, UPS_SEND_PORT))
            print("ups connect")
            self.recv_sock = ups_sock
        except socket.error as msg:
            print(msg)
            exit()

    def accept_ups(self):
        try:
            ups_recv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ups_recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            ups_recv_sock.bind((MY_HOST, UPS_RECV_PORT))
            print("listening")
            ups_recv_sock.listen()
            sock, addr = ups_recv_sock.accept()
            print("ups accepted")
            self.send_sock = sock
        except socket.error as msg:
            print(msg)
            exit()
