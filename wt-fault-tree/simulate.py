"""Simple simulation of fault tree."""
import json
import click
import random
from copy import deepcopy

def generate_error_message(event):
    """Make error message from event"""
    


def roll_basic(event):
    """Handle different sorts of probabilies."""
    if event["prob_type"] == "constant":
        if random.random() < event["prob_args"][0]:
            event["state"] = True
            return generate_error_message(event)

    # TODO handle prob_type exponential, etc

    # no fitting probability type
    raise RuntimeWarning


def check_intermediate(event, basic, intermediate):
    """Check and/or of children to update state."""
    # TODO


def iterate(basic, intermediate):
    # step basic events forward
    for tag, event in basic.items():
        roll_basic(event)

    # step intermediate events forward
    for tag, event in intermediate.items():
        check_intermediate(event, basic, intermediate)

    # TODO generate profit message
    



def main():
    # read in events 
    with open('trees/fig3/fig3-basic.csv', 'r') as f:  # FIXME param basic
        basic_list = json.load(f)
        basic = {}
        for event in basic_list:
            basic[event['tag']] = event

    with open('trees/fig3/fig3-intermediate.csv', 'r') as f:  # FIXME param intermediate
        intermediate_list = json.load(f)
        intermediate = {}
        for event in intermediate_list: 
            intermediate[event['tag']] = event

    global time
    global messages
    time = 0
    messages = []
    while time < 1000: # FIXME param time max
        iterate(basic, intermediate)
        
        # TODO handle repair messages -- generate cost messages
        time += 1



if __name__ == '__main__':
    main()