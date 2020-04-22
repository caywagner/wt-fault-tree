"""All message responses for Worker."""
import logging
import json
import os.path
import subprocess
from mapreduce.utils import tcp_send


def distribute(self, message):
    """Send master messages where they need to go."""
    # function dict to distrbiute
    function = {
        "new_worker_job": new_worker_job,
        "new_sort_job": new_sort_job,
        "shutdown": shutdown,
        "register_ack": register_ack
    }

    # attempt distributing
    msg_type = message["message_type"]
    if not self.signals["registered"]:
        # if msg_type in ["register_ack", "shutdown"]:
        if msg_type in ('register_ack', 'shutdown'):
            function[msg_type](self, message)
        else:
            return
    else:
        try:
            function[msg_type](self, message)
        except KeyError:
            logging.info("Wrkr:%s cant dist msg %s",
                         self.worker_port, msg_type)

        logging.debug("Wrkr:%s recv\n%s",
                      self.worker_port, json.dumps(message, indent=2))


def register_ack(self, message):
    """Send register ack message."""
    self.signals["registered"] = True
    logging.info("%s", message)


def new_worker_job(self, message):
    """Execute worker job and reports back to master."""
    # get name of executable
    executable = message["executable"]

    # create empty list to store output files
    output_files = []

    for infile in message["input_files"]:

        # construct path of output file
        outfile = os.path.join(message["output_directory"],
                               os.path.basename(infile))

        # open files
        opened_infile = open(infile, "r")
        opened_outfile = open(outfile, "w")

        # run executable on infile and write to outfile

        subprocess.run([executable], stdout=opened_outfile,
                       stdin=opened_infile, check=True)
        # text=True,

        # while s.returncode != 0:
        # wait()

        # close files
        opened_infile.close()
        opened_outfile.close()

        # add outfile to output_files
        output_files.append(outfile)

    status_message = {
        "message_type": "status",
        "output_files": output_files,
        "status": "finished",
        "worker_pid": self.pid
    }

    # worker tells master that the job is complete
    tcp_send(status_message, self.master_port)


def new_sort_job(self, message):
    """Execute sort job and reports back to master."""
    # Compile all files into one list
    sort_list = []
    for i in message["input_files"]:
        infile = open(i, "r")
        for line in infile:
            sort_list.append(line)
        infile.close()

    # perform the sort job
    sort_list.sort()

    # write sorted list to the output file
    outfile = open(message["output_file"], "a")
    for i in sort_list:
        outfile.write(i)
    outfile.close()

    status_message = {
        "message_type": "status",
        "output_file": message["output_file"],
        "status": "finished",
        "worker_pid": self.pid
    }

    # worker tells master that the job is complete
    tcp_send(status_message, self.master_port)


def shutdown(self, message):
    """Shuts down worker."""
    self.signals["shutdown"] = True
    logging.info("Worker:%s shutting down", self.worker_port)
    logging.info("%s", message)
