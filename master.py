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


    def checkOnlineNodes(self):
        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp_sock.connect(("1.1.1.1", 53))
        
        self.IP_ADDRESS = temp_sock.getsockname()[0]
        self.SUBNET = (".").join(self.IP_ADDRESS.split(".")[:3]) + "."

        temp_sock.close()

        while True:
            if self.kill_threads:
                return

            for x in range(1, 255):
                ip = self.SUBNET + str(x)
                
                if ip != self.IP_ADDRESS:
                    threading.Thread(target=self.ping, args=(ip)).start()

            time.sleep(self.REFRESH)


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
            cursor.execute("UPDATE nodes SET online = False WHERE ip = ?", (server_ip,))
        
        elif id != cursor.execute("SELECT id FROM nodes WHERE ip = ?", (server_ip,)).fetchone()[0]:
            cursor.execute("DELETE FROM nodes WHERE id = ?", (id,))
            cursor.execute("DELETE FROM nodes WHERE ip = ?", (server_ip,))
            cursor.execute("INSERT INTO nodes VALUES (?, ?, ?)", (id, server_ip, "True"))
            
            logging.error("Server ID/IP mismatch found: {0} @ {1}".format(id, server_ip))

        else:
            if cursor.execute("SELECT online FROM nodes WHERE id = ?", (id,)).fetchone()[0] == "False":
                cursor.execute("UPDATE nodes SET online = True WHERE id = ?", (id,)) 
            
            logging.info("Server is online: {0}".format(server_ip))

        db_conn.commit()
        db_conn.close()


    def garbageCollection(self):
        db_conn = sqlite3.connect(self.DB_URI, uri=True)
        cursor = db_conn.cursor()

        while True:
            if self.kill_threads:
                db_conn.close()
                return
            
            deleted_files = cursor.execute("SELECT file FROM files WHERE deleted = True").fetchall()

            for file_path in deleted_files:
                if len(cursor.execute("SELECT chunk FROM chunks WHERE file = ?", (file_path,)).fetchall()) == 0:
                    cursor.execute("DELETE FROM files WHERE file = ?", (file_path,))
                
                else:
                    for server in self.serversWithFile(file_path):
                        threading.Thread(target=self.deleteFile, args=(server, file_path)).start()

            time.sleep(self.REFRESH)


    def onConnect(self, connection, client):
        db_conn = sqlite3.connect(self.DB_URI, uri=True)
        cursor = db_conn.cursor()

        control_byte = connection.recv(1)
        logging.info("{0} sent {1}".format(client, control_byte.decode()))

        try:
            if control_byte == b"\xb0":    #upload file
                server_count = len(cursor.execute("SELECT node FROM nodes WHERE online = True").fetchall())
                
                connection.send(bytes.fromhex(f"{server_count:0{2}x}"))
                
                #client counts no of chunks, assigns chunk ids (via hashlib), sends them all back
                
                chunk_metadata = connection.recv(4096).decode() #max val @ 255 servers with replication 3
                chunk_count = len(chunk_metadata)
                
                if (chunk_count % 32) != 0:
                    raise Exception("Invalid chunk metadata recieved")

                chunk_ids = self.parseChunkIDs(chunk_metadata)
                server_ips = cursor.execute("SELECT ip FROM nodes WHERE online = True").fetchall()
                
                if len(server_ips) != server_count:
                    raise Exception("Server went offline during transaction")

                else:
                    connection.send((",").join(server_ips).encode())    #client should error check that all chunks recvd
                    
                    file_path = connection.recv(self.MAX_PATH_LEN).decode()

                    cursor.execute("INSERT INTO files VALUES (?, ?, False)", (file_path, list(dict.fromkeys(chunk_ids))))    #dict thing dedupes

                    for (chunk, ip) in list(zip(chunk_ids, server_ips)):
                        node_id = cursor.execute("SELECT node FROM nodes WHERE ip = ?", (ip,)).fetchone()[0]
                        cursor.execute("INSERT INTO chunkNodes VALUES (?, ?)", (chunk, node_id))

                        if len(cursor.execute("SELECT file FROM chunks WHERE chunk = ?", (chunk,)).fetchone[0]) == 0:
                            cursor.execute("INSERT INTO chunks VALUES (?, ?)", (chunk, file_path))

            elif control_byte == b"\xb1":    #delete file
                file_path = connection.recv(self.MAX_PATH_LEN).decode()
                servers_with_file = self.serversWithFile(file_path)

                logging.info("Deleting file: {0}\nServers storing file: {1}".format(file_path, servers_with_file))
                cursor.execute("UPDATE files SET deleted = True WHERE file = ?", (file_path,))

                for server in servers_with_file:
                    threading.Thread(target=self.deleteFile, args=(server, file_path)).start()

            elif control_byte == b"\xb2":    #retrieve file
                file_path = connection.recv(self.MAX_PATH_LEN).decode()
                chunk_ids = self.parseChunkIDs(cursor.execute("SELECT chunkOrder FROM files WHERE file = ?", (file_path,)).fetchone()[0])
                
                server_ips = []

                for chunk in chunk_ids:
                    server_ip = cursor.execute("SELECT ip FROM nodes WHERE chunkNodes.chunk = ? AND chunkNodes.node = nodes.node AND nodes.online = True").fetchone()[0]

                    if server_ip == "":
                        connection.send(b"\x00")
                        raise Exception("Server not available, cancelling retrieve transaction")

                    server_ips.append(server_ip)

                logging.info("Retrieving file: {0}\nServers storing file: {1}".format(file_path, servers_with_file))
                
                connection.send(chunk_ids.encode())
                connection.send((",").join(server_ips).encode()) 
                
            else:
                raise Exception("Invalid control byte from connection {0}".format(client))

        except:
            logging.error(traceback.format_exc())

        finally:
            connection.close()

        db_conn.commit()
        db_conn.close()


    def parseChunkIDs(self, chunk_metadata):
        i = 0
        chunk_ids = []
        indexes = [x for x in range(0, chunk_count, 32)]

        for i in range(0, len(indexes) - 1):
            chunk_ids.append(chunk_metadata[indexes[i]:indexes[i + 1]])

        return chunk_ids


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
            chunk_id = cursor.execute("""SELECT chunk FROM chunkNodes WHERE chunks.file = ? 
            AND chunks.chunk = chunkNodes.chunk AND chunkNodes.node = ?""", (file_path, server_ip)).fetchone()[0]

            temp_sock.connect((server_ip, self.PORT))
            temp_sock.send(b"\xb1")
            temp_sock.send(chunk_id.encode())

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
        cursor.execute("CREATE TABLE files (file STRING PRIMARY KEY, chunkOrder STRING, deleted BOOL)")    #chunkOrder stores chunk IDs so file can be reconstructed
        cursor.execute("CREATE TABLE chunkNodes (chunk STRING, node STRING, PRIMARY KEY (chunk, node))")
        
        db_conn.commit()
        db_conn.close()

        logging.info("Database created...")


    def listen(self):
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("0.0.0.0", self.PORT))
            server.listen()

            threading.Thread(target=self.checkOnlineNodes).start()
            threading.Thread(target=self.garbageCollection).start()

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
