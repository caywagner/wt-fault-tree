"""All thread loops for Master."""
import time
import logging
import socket
from mapreduce.utils import tcp_recv, udp_recv
from mapreduce.master.utils import query_workers, redistribute
from mapreduce.master.utils import get_map_tasks, send_map_jobs, send_sort_jobs
from mapreduce.master.utils import merge_sorted, send_reduce_tasks, move_output
from mapreduce.master.responses import distribute


def listen(self, port):
    """Listen on port for TCP communications and fills listen_queue."""
    # settin up the TCP connection for listening
    socky = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socky.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    socky.bind(("localhost", port))
    socky.listen(5)
    socky.settimeout(1)

    while not self.signals["shutdown"]:
        time.sleep(0.1)

        message = tcp_recv(socky)

        if message:
            self.listen_queue.append(message)

    socky.close()
    logging.info("MASTER listen shutting down")


def heartbeat(self, port):
    """Listen on port for UDP heartbeats and distributes."""
    # Sets up UDP socket
    # Sets up UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("localhost", port))
    sock.settimeout(1)

    while not self.signals["shutdown"]:
        time.sleep(0.1)
        message = udp_recv(sock)
        for pid in self.registered_workers:
            self.registered_workers[pid]["missed_heartbeats"] += 1
            if self.registered_workers[pid]["missed_heartbeats"] > 5:
                logging.debug("worker {pid} is dead")
                self.registered_workers[pid]["status"] = "redistribute"

        if message:
            distribute(self, message)

        # logging.info("MASTER heartbeat")

    sock.close()
    logging.info("MASTER heartbeat shutting down")


def main_loop(self, port):
    """Pop messages off the listen queue and distributes."""
    logging.info("Starting master port %s main loop.", port)
    while not self.signals["shutdown"]:
        time.sleep(0.1)

        if len(self.listen_queue) != 0:
            message = self.listen_queue.pop()
            distribute(self, message)

        # logging.info("MASTER main loop")


def handle_jobs(self):
    """Pop message off the job queue and assigns them to workers."""
    logging.debug("Starting handle jobs.")
    while not self.signals["shutdown"]:
        time.sleep(0.1)

        # get new job
        if len(self.master_job_queue) == 0:
            continue

        if len(query_workers(self, ["alive"])) == 0:
            continue

        job_msg = self.master_job_queue.pop()

        # mapping
        logging.debug("Starting map tasks")
        map_tasks = get_map_tasks(job_msg)
        logging.debug("MAP TASKS: %s", map_tasks)
        send_map_jobs(self, job_msg, map_tasks)

        # wait for mapping jobs to finish
        while len(self.pending_jobs) != 0:
            time.sleep(0.1)
        logging.info("Master recieved all mapping jobs.")

        # assign sort tasks
        send_sort_jobs(self, job_msg)

        # wait for sorting jobs to finish
        while len(self.pending_jobs) != 0:
            time.sleep(0.1)
        logging.info("Master recieved all sorting jobs.")

        # # merge sorted files
        merge_sorted(self, job_msg)

        # send reduce tasks
        send_reduce_tasks(self, job_msg)

        # wait for reducing jobs to finish
        while len(self.pending_jobs) != 0 and not self.signals["shutdown"]:
            logging.debug("PENDING NON ZERO")
            time.sleep(0.1)
        logging.info("Master recieved all reducing jobs.")

        # move to output directory
        if not self.signals["shutdown"]:
            move_output(job_msg)


def fault_tolerance(self):
    """Let worker die responsibly."""
    # little bit worried about what happens ...
    # if a worker dies while this loop is running?
    # it'll take a half second pause before starting stuff it'll be fine
    while not self.signals["shutdown"]:
        time.sleep(0.5)
        pids = [a[0] for a in query_workers(self, ["redistribute"])]
        if pids:
            redistribute(self, pids)
