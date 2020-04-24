"""Fault tree."""

"""Simple simulation of fault tree."""
import json
import click
import random
import time
from copy import deepcopy
from math import exp
import copy



class Fault_Tree:
    """Manage repair messages."""

    def __init__(self, basic_json, intermediate_json):
        self.time = 0
        self.basic = {}
        self.intermediate = {}
        self.intermediate_keys = []


        self.messages_send = []
        self.messages_recv = []

        # read in events
        with open(basic_json, 'r') as f:  # FIXME param basic
            basic_list = json.load(f)
            for event in basic_list:
                self.basic[event['tag']] = event

        with open(intermediate_json, 'r') as f:  # FIXME param intermediate
            intermediate_list = json.load(f)
            for event in intermediate_list:
                self.intermediate[event['tag']] = event
                # intermediate[event['tag']]['tag_num'] = int(event['tag'][-3:])
        # give the prepare sequence for intermediate node to update.
        self.intermediate_keys = list(self.intermediate.keys())
        self.intermediate_keys = sorted(self.intermediate_keys, reverse=True)


    def recv_messages(self, message):
        """Recieve messages from manager."""
        self.messages_recv = deepcopy(message)


    def get_messages(self):
        """Send messages to manager."""
        temp = self.messages_send
        self.messages_send.clear()

        return temp

    def generate_error_message(self, event):
        """Make error message from event"""
        # add event to messages.
        event_new = copy.deepcopy(event)
        event_new['message'] = 'repair'


        self.messages_send.append(event_new)

        print(event_new)
        if __debug__:
            print(event_new)

    def get_status_list(self, children):
        state_list = []
        for child in children:
            if 'g' in child:
                state_list.append(self.intermediate[child]['state'])
            elif 'e' in child:
                state_list.append(self.basic[child]['state'])
            else:
                raise IndexError
        return state_list

    def cal_state(self, state_list, logic_type):
        result = False;
        if logic_type == 'AND':
            result = True
            for state in state_list:
                result = result and state

        elif logic_type == 'OR':
            result = False
            for state in state_list:
                result = result or state

        else:
            raise TypeError
        return result

    def roll_basic(self, event):
        """Handle different sorts of probabilies."""
        if event["prob_type"] == "constant":
            p = event["prob_args"][0]
            if random.random() < p:
                event["state"] = True
                self.generate_error_message(event)
        elif event["prob_type"] == "linear":
            # P(t) = m(t_curr-t_0)
            t0 = event["t0"]
            m = event["prob_args"][0]
            p = m * (self.time - t0)
            if random.random() < p:
                event["state"] = True
                self.generate_error_message(event)
        elif event["prob_type"] == "exponential":
            # TODO; exponential fomula?
            t0 = event["t0"]
            m = event["prob_args"][0]
            p = exp(m * (self.time - t0))
            if random.random() < p:
                event["state"] = True
                self.generate_error_message(event)

        else:
            # Wrong prob type;
            raise TypeError


    def check_intermediate(self, event):
        """Check and/or of children to update state."""
        state_list = self.get_status_list(event['children'])
        event_state = self.cal_state(state_list, event['gate_type'])
        event['state'] = event_state

        if event_state:
            self.generate_error_message(event)

    def iterate(self, freq_num = 1):
        """Iterate n timesteps."""
        for i in range(freq_num):
            # step basic events forward
            for tag, event in self.basic.items():
                self.roll_basic(event)

            # step intermediate events forward
            for event in self.intermediate_keys:
                self.check_intermediate(self.intermediate[event])
            # TODO handle repair messages (messages_recv)

            # TODO iterate basic events
            # should generate messages stored in messages_send

            # TODO iterate intermediate events
            # should generate messages stored in messages_send

