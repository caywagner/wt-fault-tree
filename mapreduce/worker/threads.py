"""All thread loops for Worker."""
import time
import logging
import socket
from mapreduce.utils import tcp_recv, udp_send
from mapreduce.worker.responses import distribute


def listen(self, port):
    """Listen on port for TCP communications and fills listen_queue."""
    # settin up the TCP connection for listening
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("localhost", port))
    sock.listen(5)
    sock.settimeout(1)

    while not self.signals["shutdown"]:
        time.sleep(0.1)
        message = tcp_recv(sock)

        if message:
            self.listen_queue.append(message)

    sock.close()
    logging.info("WORKER:%s listen shutting down", self.worker_port)


def heartbeat(self):
    """Send heartbeat to master via UDP every 2 seconds."""
    while not self.signals["shutdown"]:
        time.sleep(2)

        heartbeat_message = {
            "message_type": "heartbeat",
            "worker_pid": self.pid
        }
        udp_send(heartbeat_message, self.heartbeat_port)

    logging.info("WORKER:%s heartbeat shutting down", self.worker_port)


def main_loop(self):
    """Pops messages off the listen queue and distributes."""
    while not self.signals["shutdown"]:
        time.sleep(0.01)

        if len(self.listen_queue) != 0:
            message = self.listen_queue.pop()
            distribute(self, message)
