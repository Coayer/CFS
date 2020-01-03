import socket
import threading
import logging
import time
import hashlib
import traceback
import sqlite3
import os
import sys

"""
Upload
Download
Delete

B1  B2  COMMAND
a   *   Question
b   *   Request

a   0   ID -- Should respond with x00 if has no ID(ea)

b   0   Upload
b   1   Delete
b   2   Retrieve
"""

class MasterNode:
    def __init__(self):
        logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO, datefmt="%H:%M:%S")
        
        self.DB_URI = "file:mem?mode=memory&cache=shared"
        self.createDB()

        self.TIMEOUT = 0.5
        self.PORT = 5900
        self.MAX_PATH_LEN = 1024

        self.kill_threads = False

        logging.info("Server initialized...")


    def checkOnlineNodes(self):
        db_conn = sqlite3.connect(self.DB_URI, uri=True)
        cursor = db_conn.cursor()

        while True:
            if self.kill_threads:
                db_conn.close()
                return

            for node in cursor.execute("SELECT ip FROM nodes").fetchall():
                threading.Thread(target=self.ping, args=(node)).start()

            time.sleep(0.1)    #polling rate for node checking


    def ping(self, server_ip):
        db_conn = sqlite3.connect(self.DB_URI, uri=True)
        cursor = db_conn.cursor()

        try:
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            temp_sock.settimeout(self.TIMEOUT)
            temp_sock.connect((server_ip, self.PORT))
            temp_sock.send(b"\xa0")
            id = temp_sock.recv(1)

            if id == b"\x00":
                used_ids = [int(used_id, 16) for used_id in cursor.execute("SELECT id FROM nodes").fetchall()]
                
                for val in range(1, 255):
                    if val not in used_ids:
                        id = f"{val:0{2}x}"

                temp_sock.send(bytes.fromhex(id))

                cursor.execute("INSERT INTO nodes VALUES (?, ?, ?)", (id, server_ip, "True"))

                logging.info("New server online: {0} @ {1}".format(id, server_ip))
        
        except TimeoutError:
            id = ""

        finally:
            temp_sock.close()

        if id == "":
            cursor.execute("UPDATE nodes SET online = True WHERE ip = ?", (server_ip,))
        
        elif id != cursor.execute("SELECT id FROM nodes WHERE ip = ?", (server_ip,)).fetchone():
            cursor.execute("DELETE FROM nodes WHERE id = ?", (id,))
            cursor.execute("DELETE FROM nodes WHERE ip = ?", (server_ip,))
            cursor.execute("INSERT INTO nodes VALUES (?, ?, ?)", (id, server_ip, "True"))
            
            logging.error("Server ID/IP mismatch found: {0} @ {1}".format(id, server_ip))

        else:
            if cursor.execute("SELECT online FROM nodes WHERE id = ?", (id,)).fetchone() == "False":
                cursor.execute("UPDATE nodes SET online = True WHERE id = ?", (id,)) 
            
            logging.info("Server is online: {0}".format(server_ip))

        db_conn.commit()
        db_conn.close()


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
                        id = cursor.execute("SELECT node FROM nodes WHERE ip = ?", (ip,)).fetchone()
                        cursor.execute("INSERT INTO chunkNodes VALUES (?, ?)", (chunk, id))
                        cursor.execute("INSERT INTO chunks VALUES (?, ?)", (chunk, file_path))

            elif control_byte == b"\xb1":    #delete file
                file_path = connection.recv(self.MAX_PATH_LEN).decode()
                servers_with_file = self.serversWithFile(file_path)

                logging.info("Deleting file: {0}\nServers storing file: {1}".format(file_path, servers_with_file))

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


    def serversWithFile(self, file_path):
        db_conn = sqlite3.connect(self.DB_URI, uri=True)
        cursor = db_conn.cursor()

        servers = cursor.execute("""SELECT ip FROM nodes WHERE chunks.file = ? 
        AND chunks.chunk = chunkNodes.chunk AND chunkNodes.node = nodes.node 
        AND nodes.online = True HAVING COUNT(chunks.chunk) = 1""", (file_path,)).fetchall()
        
        db_conn.close()
        return servers


    def deleteFile(self, server_ip, file_path):
        db_conn = sqlite3.connect(self.DB_URI, uri=True)
        cursor = db_conn.cursor()

        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp_sock.settimeout(self.TIMEOUT)

        try:
            temp_sock.connect((server_ip, self.PORT))
            temp_sock.send(b"\xb1")
            temp_sock.send(file_path.encode())
            
            chunk = cursor.execute("SELECT chunk FROM chunkNodes WHERE chunks.file = ? AND chunks.chunk = chunkNodes.chunk AND chunkNodes.node = ?", (file_path, server_ip)).fetchone()
            cursor.execute("DELETE FROM chunks WHERE chunk = ?", (chunk,))
            db_conn.commit()

        except:
            logging.error(traceback.format_exc())

        finally:
            temp_sock.close()

        db_conn.close()


    def createDB(self):
        db_conn = sqlite3.connect(self.DB_URI, uri=True)
        cursor = db_conn.cursor()
        
        cursor.execute("CREATE TABLE nodes (node STRING, ip STRING, online BOOL, PRIMARY KEY (node, ip))")
        cursor.execute("CREATE TABLE chunks (chunk STRING PRIMARY KEY, file STRING)")
        cursor.execute("CREATE TABLE chunkNodes (chunk STRING, node STRING, PRIMARY KEY (chunk, node))")
        
        db_conn.commit()
        db_conn.close()


        db_ = sqlite3.connect(self.DB_URI, uri=True)
        cu = db_.cursor()
        cu.execute("SELECT ip FROM nodes").fetchall()
        db_.close()

        logging.info("Database created...")


    def listen(self):
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("localhost", self.PORT))
            server.listen()

            threading.Thread(target=self.checkOnlineNodes).start() #also add routine check for files not deleted by offline nodes

            while True:
                connection, client = server.accept() 
                threading.Thread(target=self.onConnect, args=(connection, client)).start()

        except:
            logging.error(traceback.format_exc())
            logging.info("Server shutting down...")
            self.kill_threads = True
            server.close()
            sys.exit()


master = MasterNode()
master.listen()