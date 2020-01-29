import pickle
import socket
import threading
import logging
import time
import traceback
import os
import sys


class MasterNode:
    def __init__(self):
        logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO, datefmt="%H:%M:%S")
        
        self.TIMEOUT = 0.5
        self.REFRESH = 5
        self.PORT = 5900
        self.PACKET_SIZE = 1024
        self.ID = b"\x00"

        self.kill_threads = False

        logging.info("Server initialized...")


    def onConnect(self, connection, client):
        control_byte = connection.recv(1)
        logging.info("{0} sent {1}".format(client, control_byte.decode()))

        try:
            if control_byte == b"\xb0":    #store chunk
                chunk_id = connection.recv(20)
                chunk_data = self.recieveData(connection)

                with open(chunk_id, "wb") as byte_file:
                    pickle.dump(chunk_data, byte_file)

            elif control_byte == b"\xb1":    #delete chunk
                chunk_id = connection.recv(20)
                os.remove(chunk_id.decode())

            elif control_byte == b"\xb2":    #retrieve chunk
                chunk_id = connection.recv(20)

                with open(chunk_id, "rb") as byte_file:
                    chunk_data = pickle.load(byte_file)

                connection.send(chunk_data) # no no no
            
            elif control_byte == b"\xa0":
                connection.send(self.ID)
                self.ID = connection.recv(1)

            else:
                raise Exception("Invalid control byte from connection {0}".format(client))

        except:
            logging.error(traceback.format_exc())

        finally:
            connection.close()


    def recieveData(self, connection):
        data = []

        while True:
            packet_data = connection.recv(self.PACKET_SIZE)

            if packet_data != b"":
                data.append(packet_data)
            else:
                break

        return b"".join(data)


    def listen(self):
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("0.0.0.0", self.PORT))
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



master = MasterNode()
master.listen()