"""Is the MASTER."""
import os
import logging
import threading
import pathlib
import glob
import shutil
import click
from mapreduce.master.threads import listen, heartbeat, main_loop
from mapreduce.master.threads import handle_jobs, fault_tolerance

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('sh').setLevel(logging.ERROR)


class Master:
    """The big one."""

    def __init__(self, port):
        """Doing it."""
        logging.info("Starting master:%s", port)
        logging.info("Master:%s PWD %s", port, os.getcwd())

        # set up file structure
        setup_files()

        # set up Master attributes
        self.port = port
        self.signals = {"shutdown": False}
        self.listen_queue = []
        # used by main and listen, holds messages to be handled distributed
        self.registered_workers = {}  # used by all to keep track of workers
        # key: worker_pid. Values:
        #      "status" : string   <"alive"|"busy"|"dead">
        #      "missed_heartbeats" : int,
        #      "tasks" : task,
        #      "port" : port,
        self.master_job_queue = []
        # used by handle_jobs, contains new_master_job messages
        self.pending_jobs = set()  # used by handle_jobs,
        self.job_cnt = 0  # incremented with each new job started

        # start listening thread
        listening_thread = threading.Thread(target=listen,
                                            args=(self, self.port))
        listening_thread.start()
        logging.info("Starting master listen on %s", self.port)

        # start heartbeat thread
        heartbeat_thread = threading.Thread(target=heartbeat,
                                            args=(self, self.port - 1))
        heartbeat_thread.start()
        logging.info("Starting master heartbeat on %s", self.port - 1)

        fault_thread = threading.Thread(target=fault_tolerance, args=(self,))
        fault_thread.start()
        logging.info("starting fault tolerance")
        # start job thread
        job_thread = threading.Thread(target=handle_jobs, args=(self,))
        job_thread.start()

        # run body of main thread. exits when signals["shutdown"] = True
        main_loop(self, self.port)

        # shut down master
        listening_thread.join()
        heartbeat_thread.join()
        job_thread.join()
        fault_thread.join()
        logging.info("MASTER main shutting down")

    def fucccckkkk_youuuuu(self):
        """P y l i n t."""
        logging.info("with a passion %s", self.port)

    def fucccckkkk_youuuuuuuuuu(self):
        """P y l i n t."""
        logging.info("with a deep deep passion %s", self.port)


def setup_files():
    """Format tmp folder. Uses relative path per piaze post @1505."""
    pathlib.Path('tmp').mkdir(parents=True, exist_ok=True)
    bad_files = glob.glob(os.path.join('tmp', '*'))
    for file in bad_files:
        shutil.rmtree(file)


@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    """Is main."""
    Master(port)


if __name__ == '__main__':
    main()
