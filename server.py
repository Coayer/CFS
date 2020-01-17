import socket
import threading
import logging
import time
import traceback
import sqlite3
import os
import sys


class MasterNode:
    def __init__(self):
        logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO, datefmt="%H:%M:%S")
        
        self.DB_URI = "file:memdb?mode=memory&cache=shared"
        self.createDB()

        self.TIMEOUT = 0.5
        self.REFRESH = 5
        self.PORT = 5900
        self.MAX_PATH_LEN = 1024

        self.kill_threads = False

        logging.info("Server initialized...")


    def onConnect(self, connection, client):
        db_conn = sqlite3.connect(self.DB_URI, uri=True)
        cursor = db_conn.cursor()

        control_byte = connection.recv(1)
        logging.info("{0} sent {1}".format(client, control_byte))

        try:
            if control_byte == b"\xb0":    #upload file
                server_count = len(cursor.execute("SELECT node FROM nodes WHERE online = True").fetchall())
                
                connection.send(bytes.fromhex(f"{server_count:0{2}x}"))
                
                #client counts no of chunks, assigns chunk ids (via hashlib), sends them all back
                
                chunk_metadata = connection.recv(4096) #max val @ 255 servers with replication 3
                chunk_count = len(chunk_metadata)
                
                if (chunk_count % 32) != 0:
                    raise Exception("Invalid chunk metadata recieved")

                i = 0
                chunk_ids = []
                indexes = [x for x in range(0, chunk_count, 32)]

                for i in range(0, len(indexes) - 1):
                    chunk_ids.append(chunk_metadata[indexes[i]:indexes[i + 1]])

                online_servers = cursor.execute("SELECT ip FROM nodes WHERE online = True").fetchall()
                
                if len(online_servers) != server_count:
                    raise Exception("Server went offline during transaction")

                else:
                    connection.send((str(online_servers)[1:-1:]).encode()) #client should error check that all chunks recvd
                    
                    file_path = connection.recv(self.MAX_PATH_LEN).decode()

                    for (chunk, ip) in list(zip(chunk_ids, online_servers)):
                        id = cursor.execute("SELECT node FROM nodes WHERE ip = ?", (ip,)).fetchone()[0]
                        cursor.execute("INSERT INTO chunkNodes VALUES (?, ?)", (chunk, id))
                        cursor.execute("INSERT INTO chunks VALUES (?, ?)", (chunk, file_path))
                        cursor.execute("INSERT INTO files VALUES (?, False)", (file_path,))

            elif control_byte == b"\xb1":    #delete file
                file_path = connection.recv(self.MAX_PATH_LEN).decode()
                servers_with_file = self.serversWithFile(file_path)

                logging.info("Deleting file: {0}\nServers storing file: {1}".format(file_path, servers_with_file))
                cursor.execute("UPDATE files SET deleted = True WHERE file = ?", (file_path,))

                for server in servers_with_file:
                    threading.Thread(target=self.deleteFile, args=(server, file_path)).start()

            elif control_byte == b"\xb2":    #retrieve file
                file_path = connection.recv(self.MAX_PATH_LEN).decode()
                servers_with_file = self.serversWithFile(file_path)

                logging.info("Retrieving file: {0}\nServers storing file: {1}".format(file_path, servers_with_file))
                connection.send((str(servers_with_file)[1:-1:]).encode())

            else:
                raise Exception("Invalid control byte from connection {0}".format(client))

        except:
            logging.error(traceback.format_exc())

        finally:
            connection.close()

        db_conn.commit()
        db_conn.close()


    def createDB(self):
        db_conn = sqlite3.connect(self.DB_URI, uri=True)
        cursor = db_conn.cursor()
        
        cursor.execute("CREATE TABLE nodes (node STRING, ip STRING, online BOOL, PRIMARY KEY (node, ip))")
        cursor.execute("CREATE TABLE chunks (chunk STRING PRIMARY KEY, file STRING)")
        cursor.execute("CREATE TABLE files (file STRING PRIMARY KEY, deleted BOOL)")
        cursor.execute("CREATE TABLE chunkNodes (chunk STRING, node STRING, PRIMARY KEY (chunk, node))")
        
        db_conn.commit()
        db_conn.close()

        logging.info("Database created...")


    def listen(self):
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("localhost", self.PORT))
            server.listen()

            while True:
                connection, client = server.accept() 
                threading.Thread(target=self.onConnect, args=(connection, client)).start()

        except:
            logging.error(traceback.format_exc())
            logging.info("Server shutting down...")

            self.kill_threads = True
            server.close()
            sys.exit()



db = sqlite3.connect("file:memdb?mode=memory&cache=shared", uri=True)
master = MasterNode()
master.listen()
db.close()