
from sim_manager.fault_tree import Fault_Tree
import json
import matplotlib.pyplot as plt

"""
1. cost only basic
2. fix all

"""


class Maintenance:

    def __init__(self, run_time, recv_freq, scope, basic_path, intermediate_path, cost_map_path  ):

        self.ft = Fault_Tree(basic_path, intermediate_path)
        self.recv_freq = recv_freq
        self.run_time = run_time
        self.scope= scope
        # self.cost_map = {}
        self.cost_log = []
        self.cost_sum = [0]
        # self.init_cost_map(cost_map_path)

    """
    def init_cost_map(self, cost_map_path):
        with open(cost_map_path, 'r') as f:  # FIXME param basic
            cost_map_file = json.load(f)
            for cost_item in cost_map_file:
                self.cost_map[cost_item['tag']] = cost_item['cost']
    """

    def start(self):
        cycle_time = int(self.run_time / self.recv_freq)
        for i in range(cycle_time):
            # print("cycle ", i)
            self.ft.iterate(self.recv_freq)




            msg = self.ft.get_messages()  # the
            # print("Broken Msg", msg)

            repair_msg = self.process(msg)
            # print("Repair Msg", repair_msg)
            # if repair_msg:
            #   print("cycle : ", i, ":", repair_msg)


            # process messages
            # generate repair messages

            self.ft.recv_messages(repair_msg)  # the fault tree will
            self.ft.repair()
        a=  list(range(0, len(self.cost_sum) * self.recv_freq, self.recv_freq ))
        print(len(self.cost_sum))
        plt.plot(a , self.cost_sum, label="check freq: " + str(self.recv_freq))
        plt.legend()



    def process(self, msgs):
        repair_msg = []

        cost_local = 0
        # TODO process messages
        for broken_item in msgs:
            broken_tag = broken_item["tag"]
            if 'e' in broken_tag:
                cost_local = cost_local + broken_item['cost']
            repair_msg.append({"tag": broken_tag })
        self.cost_log.append(cost_local)
        self.cost_sum.append(self.cost_sum[-1] + cost_local)
        return repair_msg



def main():
    basic_path = 'tree_json/tower-garcia.json'
    intermediate_path = 'tree_json/tower-intermediate.json'
    cost_map_path = 'tree_json/cost-map.json'
    for freq in [50, 100, 200, 500]:
        maintenance_model = Maintenance(500000, freq, [1], basic_path, intermediate_path, cost_map_path )
        maintenance_model.start()


    # plt.ylim(0, 40000)
    # plt.xlim(0, 20000)
    plt.xlabel("Fault Tree Run Time")
    plt.ylabel("Cost")
    plt.title("Maintenance Cost of Tower Fault Tree with Garcia Probability")
    plt.show()

if __name__ == '__main__':
    main()

