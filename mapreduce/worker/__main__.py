"""Worker main."""
import logging
import os
import threading
import click
from mapreduce.worker.utils import register
from mapreduce.worker.threads import listen, heartbeat, main_loop
from mapreduce.utils import write_to_debug
# Configure logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('sh').setLevel(logging.ERROR)


class Worker:
    """Class workers."""

    def __init__(self, master_port, worker_port):
        """Create a Worker instance."""
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())

        # setup
        self.heartbeat_port = master_port - 1
        self.master_port = master_port
        self.worker_port = worker_port
        self.pid = os.getpid()
        self.signals = {"shutdown": False, "registered": False}
        # used by main and listen, holds messages to be handled distributed
        self.listen_queue = []

        # start listening thread
        listening_thread = threading.Thread(target=listen,
                                            args=(self, self.worker_port))
        listening_thread.start()

        # register with Master
        register(self)
        # wait_for_register(self)
        write_to_debug("worker {0} registered".format(self.pid))
        # start heartbeat thread
        heartbeat_thread = threading.Thread(target=heartbeat, args=(self,))
        heartbeat_thread.start()

        # run body of main thread. exits when signals["shutdown"] = True
        main_loop(self)

        # shut down master
        listening_thread.join()
        heartbeat_thread.join()
        logging.info("WORKER:%s main shutting down", self.worker_port)

    def fucccckkkk_youuuuu(self):
        """F u c k p y l i n t."""
        logging.info("with a passion %s", self.heartbeat_port)

    def fucccckkkk_youuuuuuuuuu(self):
        """F u c k p y l i n t."""
        logging.info("with a deep deep passion %s", self.heartbeat_port)


@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    """Run main."""
    Worker(master_port, worker_port)


if __name__ == '__main__':
    main()
