
from sim_manager.fault_tree import Fault_Tree
import json




class Maintenance:

    def __init__(self, recv_freq, scope, basic_path, intermediate_path, cost_map_path  ):

        self.ft = Fault_Tree('tree_json/fig3-basic.json', 'tree_json/fig3-intermediate.json')
        self.recv_freq = recv_freq
        self.scope= scope
        self.cost_map = {}
        self.init_cost_map(cost_map_path)


    def init_cost_map(self, cost_map_path):
        with open(cost_map_path, 'r') as f:  # FIXME param basic
            cost_map_file = json.load(f)
            for cost_item in cost_map_file:
                self.cost_map[cost_item['tag']] = cost_item['cost']


    def start(self):
        for i in range(2):
            self.ft.iterate(self.recv_freq)

            msg = self.ft.get_messages()  # the
            print("Broken Msg", msg)

            repair_msg = self.process(msg)
            print("Repair Msg", repair_msg)

            # process messages
            # generate repair messages

            self.ft.recv_messages(repair_msg)  # the fault tree will
            self.ft.repair()

    def process(self, msgs):
        repair_msg = []


        # TODO process messages
        for broken_item in msgs:
            broken_tag = broken_item["tag"]
            repair_msg.append({"tag": broken_tag, "cost": self.cost_map[broken_tag] })

        return repair_msg



def main():
    basic_path = 'tree_json/fig3-basic.json'
    intermediate_path = 'tree_json/fig3-intermediate.json'
    cost_map_path = 'tree_json/cost-map.json'
    maintenance_model = Maintenance(5, [1], basic_path, intermediate_path, cost_map_path )
    maintenance_model.start()




if __name__ == '__main__':
    main()

