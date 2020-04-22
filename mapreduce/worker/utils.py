"""Utils file.

This file is to house helper code for the Worker

"""
import time
import logging
# import socket
from mapreduce.utils import tcp_send, write_to_debug


def register(self):
    """Register worker with master."""
    # send register to master
    register_message = {
        "message_type": "register",
        "worker_host": "localhost",
        "worker_port": self.worker_port,
        "worker_pid": self.pid
    }
    tcp_send(register_message, self.master_port)

    # wait to receive register_ack
    # settin up the TCP connection for listening
    # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # sock.bind(("localhost", self.worker_port))
    # sock.listen(5)
    # sock.settimeout(1)
    # while not self.signals["shutdown"]:
    #     time.sleep(0.1)
    #     message = tcp_recv(sock)
    #     if message and message["message_type"] == "register_ack":
    #         break
    # sock.close()
    logging.info("Worker:%s recieved register ack", self.worker_port)
    # returns to __main__ to start other threads


def wait_for_register(self):
    """Wait for register."""
    write_to_debug(("worker %s waiting for ack", self.pid))
    while not self.signals["registered"]:
        time.sleep(0.2)
        if self.signals["shutdown"]:
            return
