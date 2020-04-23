"""Utils file.

This file is to house code common between the Master and the Worker

"""
import socket
import json
import logging


# Configure logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('sh').setLevel(logging.ERROR)


def tcp_recv(sock):
    """Recieve message from adr:port via TCP connection."""
    # Listen for a connection for 1s.  The socket library avoids consuming
    # CPU while waiting for a connection.
    try:
        clientsocket, address = sock.accept()
    except socket.timeout:
        return None

    logging.info("TCP recv connection from %s", address)

    # Receive data, one chunk at a time.  If recv() times out before we can
    # read a chunk, then go back to the top of the loop and try again.
    # When the client closes the connection, recv() returns empty data,
    # which breaks out of the loop.  We make a simplifying assumption that
    # the client will always cleanly close the connection.
    message_chunks = []
    while True:
        try:
            data = clientsocket.recv(4096)
        except socket.timeout:
            return None
        if not data:
            break
        message_chunks.append(data)
    clientsocket.close()

    # Decode list-of-byte-strings to UTF8 and parse JSON data
    message_bytes = b''.join(message_chunks)
    message_str = message_bytes.decode("utf-8")
    try:
        message_dict = json.loads(message_str)
    except json.JSONDecodeError:
        return None

    return message_dict


def tcp_send(message, port, adr="localhost"):
    """Send message to adr:port via TCP connection."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.connect((adr, port))
    sock.sendall(json.dumps(message).encode('UTF-8'))
    logging.info("TCP send connection to %s:%s", adr, port)
    logging.debug("%s", message)
    sock.close()


def write_to_debug(message):
    """Write to debug."""
    open_file = open("debug.txt", 'w')
    open_file.write(message)
    open_file.close()
