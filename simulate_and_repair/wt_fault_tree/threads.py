"""All thread loops for Worker."""
import time
import logging
import socket
from simulate_and_repair.utils import tcp_recv, tcp_send


def listen(self):
    """Listen on port for TCP comms and fills message_recv_queue."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("localhost", self.port))
    sock.listen(5)
    sock.settimeout(1)

    while not self.signals["shutdown"]:
        time.sleep(0.1)

        message = tcp_recv(sock)

        if message:
            logging.debug("Maintenance recieved message\n%s", message)
            self.message_recv_queue.append(message)

    sock.close()
    logging.info("Fault tree listen shutting down")


def send(self):
    """Send TCP messages from the message_send_queue."""
    while not self.signals["shutdown"]:
        time.sleep(0.1)

        if len(self.message_send_queue):
            message = self.message_send_queue.pop()

            tcp_send(message, self.master_port)

    logging.info("Fault tree send shutting down")
