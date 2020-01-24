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
        self.MAX_PATH_LEN = 1024
        self.PACKET_SIZE = 1024


    def main(self):
        self.MASTER = input("Enter master server IP address:\n")
        
        while True:
            transaction = input("Select file operation (upload, delete, retrieve) followed by file path:\n").split(" ")
            
            master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_socket.settimeout(self.TIMEOUT)
            master_socket.connect((self.MASTER, self.PORT))
            
            if transaction[0] == "upload":
                self.upload(master_socket, transaction[1])

    
    def upload(self, master_socket, path):
        master_socket.send(b"\xb0")
        server_count = master_socket.recv(1)

        chunks = self.splitFile(path, server_count)
        chunk_metadata = []

        for chunk in chunks:
            chunk_id = hashlib.sha1().update(chunk).digest()
            chunk_metadata.append(chunk_id)

        chunk_metadata = b"".join(chunk_metadata)

        for i in range(0, len(chunk_metadata), self.PACKET_SIZE):
            master_socket.send(chunk_metadata[i:i+self.PACKET_SIZE])


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
