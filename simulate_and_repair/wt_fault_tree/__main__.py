"""Wind Turbine Fault Tree."""
import os
import threading
import click
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('sh').setLevel(logging.ERROR)

class Fault_Tree:
    """Run wind turbine fault tree simulation."""

    def __init__(self, master_port, worker_port):
        """Create fault tree instance."""

        # set up fault tree attributes
        self.master_port = master_port
        self.worker_port = worker_port
        self.pid = os.getpid()
        self.signals = {"shutdown": False}
        self.message_send_queue = []
        self.message_recv_queue = []

        self.time = 0


        # start listening thread
        # TODO

        # register with master
        # TODO register

        # start sending thread
        # TODO

        # start iteration thread
        # TODO

        # main loop to handle messages
        # TODO while true distribute

        # shutdown
        # TODO join thread

        logging.info("Turbine %s shutting down.", self.worker_port)



@click.command()
@click.argument("master_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(master_port, worker_port):
    """Run fault tree."""
    Fault_Tree(master_port, worker_port)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter





