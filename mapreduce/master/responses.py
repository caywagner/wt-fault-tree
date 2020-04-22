"""All message responses for Master."""
import logging
import json
from mapreduce.utils import tcp_send, write_to_debug
from mapreduce.master.utils import setup_job_files


def distribute(self, message):
    """Send master messages where they need to go."""
    # function dict to distrbiute
    function = {
        "shutdown": shutdown,
        "new_master_job": new_master_job,
        "status": status,
        "register": register,
        "heartbeat": handle_heartbeat
    }

    # attempt distributing
    msg_type = message["message_type"]
    try:
        function[msg_type](self, message)
    except KeyError as err:
        logging.debug(err)
        logging.info("Master unable to distribute message type %s", msg_type)

    logging.debug("Master received\n%s", json.dumps(message, indent=2))


def shutdown(self, message):
    """Shut self down then sends shut down message to all workers."""
    # shuts self down
    # only operational part of master is in __main__ after main_loop()
    logging.debug("%s", message)
    self.signals["shutdown"] = True

    shutdown_message = {
        "message_type": "shutdown"
    }
    write_to_debug("shutdown is true")
    # send shutdown message to each worker
    for worker in self.registered_workers.values():
        if worker["status"] != "dead":
            try:
                tcp_send(shutdown_message, worker["port"])
                logging.info("Master shutting down worker %s", worker["port"])
            except ConnectionRefusedError:
                logging.debug("Connection refused from worker %s"
                              "during shutdown", worker["port"])


def new_master_job(self, message):
    """Add incoming master job to job queue."""
    # set up workspace
    setup_job_files(self.job_cnt)
    message["job_number"] = self.job_cnt
    self.job_cnt += 1

    self.master_job_queue.append(message)
    logging.debug("master job queue: %s", self.master_job_queue)


def status(self, message):
    """Remove finished job from pending jobs."""
    # ugly as hell -- sorry
    if message.get("output_file"):
        inconsistent = "output_file"
    else:
        inconsistent = "output_files"

    logging.debug("PENDING JOBS: %s", self.pending_jobs)

    # remove from registered workers
    for worker_pid, worker in self.registered_workers.items():
        for task_message in worker["tasks"]:
            try:
                if task_message[inconsistent] == message[inconsistent]:

                    worker["tasks"].remove(task_message)
                    logging.debug("Removed task %s from registered worker %s.",
                                  message[inconsistent], worker_pid)
            except KeyError:
                # ignore -- just means wrong inconsistent used
                pass

    # remove from pending jobs
    if inconsistent == "output_file":
        try:
            ofile = message[inconsistent].split('/')[-1]
            self.pending_jobs.remove(ofile)
        except KeyError:
            logging.debug("Master tried to remove %s from pending jobs but"
                          "it did not exist.", message[inconsistent])
    else:
        for out_file in list(message[inconsistent]):
            try:
                ofile = out_file.split('/')[-1]
                self.pending_jobs.remove(ofile)
            except KeyError:
                logging.debug("Master tried to remove %s from pending jobs"
                              "but it did not exist.", out_file)


def register(self, message):
    """Register worker and sends register_ack."""
    # connect to the server
    # sock.connect(("localhost", port))
    write_to_debug("registering worker {0}".format(message["worker_pid"]))
    worker_port = message["worker_port"]
    worker_pid = message["worker_pid"]

    logging.info("Master sending register_ack to worker %s", worker_port)

    # register_ack message
    register_ack_message = {
        "message_type": "register_ack",
        "worker_host": "localhost",
        "worker_port": worker_port,
        "worker_pid": worker_pid
    }

    tcp_send(register_ack_message, worker_port)

    # add worker to registered workers
    worker = {
        "port": worker_port,
        "missed_heartbeats": -1,
        "status": "alive",
        "tasks": []
    }
    self.registered_workers[worker_pid] = worker


def handle_heartbeat(self, message):
    """Reset worker's missed pings and revives dead."""
    # reset missed pings to 0
    worker_pid = message["worker_pid"]
    self.registered_workers[worker_pid]["missed_heartbeats"] = 0

    # marks dead workers as aline
    # lets alive and dead workers persist
    if self.registered_workers[worker_pid]["status"] == "dead":
        self.registered_workers[worker_pid]["status"] = "alive"
