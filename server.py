import socket
import sys
import psycopg2
from  threading import Thread
from _thread import *
import world_amazon_pb2
import amazon_ups_pb2
from worldConn import WorldConnector
from upsConn import UPSConnector
from webConn import WebConnector
from databaseConn import DatabaseConnector
from google.protobuf.internal.decoder import _DecodeVarint32
from google.protobuf.internal.encoder import _EncodeVarint

conn = psycopg2.connect("dbname = 'Amazon' user = 'postgres' password = 'passw0rd'")
WORLD_HOST = "vcm-8003.vm.duke.edu"
WORLD_PORT = 23456


def world(conn):
    try:
        world = WorldConnector(conn)
        world.connect_world()
        world.sender_receiver_setup()
        t1 = Thread(target= world.handle, args=())
        t1.start()
        t2 = Thread(target = world.receiver.handle, args = ())
        t2.start()
        t3 = Thread(target = world.sender.handle, args = ())
        t3.start()
        #start_new_thread(world.start_receiver())
        #start_new_thread(world.start_sender())        
        t1.join()
        t2.join()
        t3.join()
        while(1):
            a=1
    except IndexError as e:
        print("Unexpected World error: ", e)
    except TypeError as e:
        print("Unexpected World error: ", e)
    except AttributeError as e:
        print("Unexpected World error: ", e)
    except BaseException:
        print("Unexpected World error: ", sys.exc_info()[0])


def ups(conn):
    try:
        ups = UPSConnector(conn)
        ups.connect()
        ups.accept_ups()
        start_new_thread(ups.handle())
        ups.recv_handler()
    except NameError as e:
        print(e)
        print("Unexpected UPS error: ", sys.exc_info()[0])

def init_local_db():
        try:
            conn = psycopg2.connect("dbname = 'Amazon' user = 'postgres' password = 'passw0rd'")
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS orders")
            cur.execute("DROP TABLE IF EXISTS warehouse")
            cur.execute("DROP TABLE IF EXISTS requests")
            cur.execute("DROP TABLE IF EXISTS responses")
            conn.commit()
            cur.execute("CREATE TABLE warehouse("
                        "itemid bigint PRIMARY KEY,"
                        "quatity integer,"
                        "location integer[2],"
                        "description text);")
            cur.execute("CREATE TABLE orders("
                        "packageid bigint PRIMARY KEY,"
                        "itemid bigint,"
                        "quatity integer,"
                        "description text,"
                        "upsid integer,"
                        "des integer[2] NOT NULL,"
                        "pack text DEFAULT 'unpacked',"
                        "truckid integer DEFAULT -1,"
                        "deliver text DEFAULT 'unloaded');")
            cur.execute("CREATE TABLE requests("
                        "seq_num bigint PRIMARY KEY,"
                        "request bytea,"
                        "status text DEFAULT 'unacked',"
                        "type text);")
            cur.execute("CREATE TABLE responses("
                        "seq_num bigint PRIMARY KEY,"
                        "response text,"
                        "type text);")
            cur.execute("INSERT INTO orders VALUES(1, 1, 20,'C++',666,array[1,2], 'unpacked', -1, 'unloaded')")
            cur.execute("INSERT INTO orders VALUES(2, 2, 30,'JAVA',666,array[2,3], 'unpacked', -1, 'unloaded')")
            cur.execute("INSERT INTO warehouse VALUES(1,10000,array[0,0],'C++')")
            cur.execute("INSERT INTO warehouse VALUES(2,10000,array[0,0],'JAVA')")
            conn.commit()
            #sql = ("""INSERT INTO orders(packageid, description, des, upsid) VALUES(%s, %s, %s, %s);"""%(2,'C++',[1,1],666))
            #cur.execute(sql)
            #conn.commit()
            cur.close()
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

if __name__ == "__main__":
    init_local_db()
    try:
        start_new_thread(world, (conn, ))
        #start_new_thread(ups, (conn,))
        while True:
            a = 1
    except:
        print("Unexpected main error: ", sys.exc_info()[0])

