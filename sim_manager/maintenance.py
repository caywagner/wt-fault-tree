
from sim_manager.fault_tree import Fault_Tree
"""
class Maintenance:

    def __init__(self, recv_freq, scope):
        pass

        self.ft = Fault_Tree()

        for i in range(1000):

            ft.iterate(n=recv_freq)

            msg = ft.get_messages()

            # process messages 
            # generate repair messages 

            ft.send_messages(msg)
"""




tree = Fault_Tree('tree_json/fig3-basic.json', 'tree_json/fig3-intermediate.json')


tree.iterate(100)


