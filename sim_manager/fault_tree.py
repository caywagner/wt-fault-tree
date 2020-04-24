"""Fault tree."""

class Fault_Tree:
    """Manage repair messages."""

    def __init__(self, basic_json, intermediate_json):
        self.time = 0
        self.basic = []
        self.intermediate = []

        self.messages_send = []
        self.messages_recv = []

        # TODO fill from json files

    def send_messages(self, message):
        """Recieve messages from manager."""
        # TODO 
        pass 

    def get_messages(self):
        """Send messages to manager."""
        temp = self.messages_send
        self.messages_send.clear()

        return temp

    def iterate(self, n=1):
        """Iterate n timesteps."""

        # TODO handle repair messages (messages_recv)

        # TODO iterate basic events
        # should generate messages stored in messages_send

        # TODO iterate intermediate events
        # should generate messages stored in messages_send

