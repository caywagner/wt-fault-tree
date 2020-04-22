"""Simple simulation of fault tree."""
import json
import click
import random
import time
from copy import deepcopy
import copy

basic = {}
intermediate = {}



def generate_error_message(event, messages):
    """Make error message from event"""
    # add event to messages.
    event_new = copy.deepcopy(event)
    event_new['message'] = 'repair'
    messages.append(event_new)


def get_status_list(children):
    state_list = []
    for child in children:
        if 'g' in child:
            state_list.append(intermediate[child]['state'])
        elif 'e' in child:
            state_list.append(basic[child]['state'])
        else:
            raise IndexError
    return state_list

def cal_state(state_list, logic_type):
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






def roll_basic(event, messages):
    """Handle different sorts of probabilies."""
    if event["prob_type"] == "constant":
        if random.random() < event["prob_args"][0]:
            event["state"] = True
            # print(event)
            # TODO add to messages
            return generate_error_message(event, messages)
        else:
            return;


    # TODO handle prob_type exponential, etc

    # no fitting probability type
    raise RuntimeWarning


def check_intermediate(event, messages):
    """Check and/or of children to update state."""
    # TODO
    state_list = get_status_list(event['children'])
    event_state = cal_state(state_list, event['gate_type'])
    event['state'] = event_state

    if  event_state:
        # TODO add to messages
        generate_error_message(event, messages)
    # print(event)





def iterate(intermediate_keys, messages):
    # step basic events forward
    for tag, event in basic.items():
        roll_basic(event, messages)

    # step intermediate events forward
    for event in intermediate_keys:
        check_intermediate(intermediate[event] , messages)

    # TODO generate profit message
    


def compare_event(event1, event2):
    return int(event1['tag'][-3:]) - int(event2['tag'][-3:])


def send_message(messages):
    print(len(messages))
    for mes in messages:
        print(mes['tag'])


def main():

    # read in events
    with open('trees/fig3/fig3-basic.json', 'r') as f:  # FIXME param basic
        basic_list = json.load(f)
        for event in basic_list:
            basic[event['tag']] = event

    with open('trees/fig3/fig3-intermediate.json', 'r') as f:  # FIXME param intermediate
        intermediate_list = json.load(f)
        for event in intermediate_list:
            intermediate[event['tag']] = event
            intermediate[event['tag']]['tag_num'] = int(event['tag'][-3:])
    intermediate_keys = list(intermediate.keys())
    # sorted(intermediate_list, key=lambda event: event["tag_num"], reverse=True)
    intermediate_keys = sorted(intermediate_keys, reverse=True)
    global time
    global messages
    time = 0
    messages = []
    while time < 1000: # FIXME param time max
        iterate(intermediate_keys, messages)
        send_message(messages)
        messages = []

        # TODO send the messages to the repair model
        # clear messages;
        # sleep
        # TODO handle repair messages -- generate cost messages

        time += 1



if __name__ == '__main__':

    main()