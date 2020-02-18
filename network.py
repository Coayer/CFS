PACKET_SIZE = 1024
TIMEOUT = 0.5
PORT = 5900

def sendData(connection, data):
    for i in range(0, len(data), PACKET_SIZE):
        connection.send(data[i : i + PACKET_SIZE])


def recieveData(connection):
    data = []

    while True:
        packet_data = connection.recv(PACKET_SIZE)

        if packet_data != b"":
            data.append(packet_data)
        else:
            break

    return b"".join(data)