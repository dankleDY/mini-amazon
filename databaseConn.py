from socket import socket
import psycopg2
import string
from array import array


class DatabaseConnector:
    def __init__(self):
        conn = None
        try:
            conn = psycopg2.connect("dbname = 'Amazon' user = 'postgres' password = 'passw0rd'")
            cur = conn.cursor()
            # cur.execute("DROP TABLE IF EXISTS orders")
            cur.execute("DROP TABLE IF EXISTS warehouse")
            cur.execute("DROP TABLE IF EXISTS requests")
            # cur.execute("DROP TABLE IF EXISTS responses")
            conn.commit()
            cur.execute("CREATE TABLE warehouse("
                        "itemid bigint PRIMARY KEY,"
                        "quatity integer,"
                        "location integer[2],"
                        "description text);")
            # cur.execute("CREATE TABLE orders("
            #             "packageid bigint PRIMARY KEY,"
            #             "itemid bigint,"
            #             "quatity integer,"
            #             "description text,"
            #             "upsid integer,"
            #             "des integer[2] NOT NULL,"
            #             "pack text DEFAULT 'unpacked',"
            #             "truckid integer DEFAULT -1,"
            #             "deliver text DEFAULT 'unloaded');")
            cur.execute("CREATE TABLE requests("
                        "seq_num bigint PRIMARY KEY,"
                        "request text,"
                        "status text DEFAULT 'unacked',"
                        "type text);")
            # cur.execute("CREATE TABLE responses("
            #             "seq_num bigint PRIMARY KEY,"
            #             "response text,"
            #             "type text);")
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

    def connect(self):
        pass

    def init_inventory(self, item_id, amount):
        conn = None
        sql = "INSERT INTO warehouse (item_id, amount) VALUES (%s, %s)";
        val = (item_id, amount)
        try:
            conn = psycopg2.connect("dbname = 'Amazon' user = 'postgres' password = 'passw0rd'")
            cur = conn.cursor()
            cur.execute(sql, val)
            conn.commit()
            cur.close()
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
