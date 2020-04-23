"""Maintenance."""
import os
import threading
import click
import logging
from simulate_and_repair.maintenance.threads import listen, send, handle_message

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('sh').setLevel(logging.ERROR)

class Maintenance:
    """Manage wt simulations and communicate repairs."""

    def __init__(self, port):
        """Create maintenance instance."""

        # set up Maintenance attributes
        self.port = port
        self.signals = {"shutdown": False}
        self.message_send_queue = []
        self.message_recv_queue = []
        self.registered_turbines = {}
        # key: turbine_pid, value:
        #       "basic_json_loc": string
        #       "intermediate_json_loc": string
        #       "messages": string
        #       "costs": [int]
        #       "cost_to_view": int

        # start listening thread
        listening_thread = threading.Thread(target=listen,
                                           args=(self,))
        listening_thread.start()                                           

        # start sending thread
        sending_thread = threading.Thread(target=send,
                                           args=(self,))
        sending_thread.start()   

        # main loop to handle messages
        # uses thread that __init__ is running on
        handle_message(self)

        # shutdown
        listening_thread.join()
        sending_thread.join()

        logging.info("Maintenance %s shutting down.", self.port)



@click.command()
@click.argument("port", nargs=1, type=int)
def main(port):
    """Run maintenance."""
    Maintenance(port)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter

