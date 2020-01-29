import socket
import time
import threading
import os
import sys
import hashlib


class client:
    def __init__(self):
        self.TIMEOUT = 0.5
        self.PORT = 5900
        self.PACKET_SIZE = 1024


    def main(self):
        command = sys.argv[0]
        path = sys.argv[1]
        master = sys.argv[2]

        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.settimeout(self.TIMEOUT)
        master_socket.connect((master, self.PORT))
            
        if command == "upload":
            self.upload(master_socket, path)

        elif command == "delete":
            master_socket.send(b"\xb1")
            master_socket.send(path.encode())

        elif command == "download":
            self.download(master_socket, path)


    def download(self, master_socket, path):
        master_socket.send(b"\xb2")
        master_socket.send(path.encode())
        

    def upload(self, master_socket, path):
        master_socket.send(b"\xb0")

        server_count = int(bytes.hex(master_socket.recv(1)), 16)
        chunks = self.splitFile(path, server_count)
        chunk_metadata = []

        for chunk in chunks:
            chunk_id = hashlib.sha1().update(chunk).digest()
            chunk_metadata.append(chunk_id)

        chunk_metadata = b"".join(chunk_metadata)

        for i in range(0, len(chunk_metadata), self.PACKET_SIZE):
            master_socket.send(chunk_metadata[i : i + self.PACKET_SIZE])

        server_ips = self.recieve(master_socket)

        master_socket.send(path.encode())

        for server in server_ips:
            #send chunk to each


    def recieve(self, master_socket):
        data = []

        while True:
            packet_data = master_socket.recv(self.PACKET_SIZE)

            if packet_data != b"":
                data.append(packet_data.decode())
            else:
                return data


    def splitFile(self, path, node_count, replication_level=3):
        with open(path, "rb") as file:
            data = file.read()

        dataLength = len(data)
        splitLevel = dataLength // (node_count // replication_level)

        i = 0
        chunks = []
        indexes = [x for x in range(0, dataLength, splitLevel)]

        for i in range(0, len(indexes) - 1):
            chunks.append(data[indexes[i] : indexes[i + 1]])

        return chunks
