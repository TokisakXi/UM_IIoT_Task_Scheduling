import json


class Monitor(object):
    """
    Monitor类被用于监控整个模拟过程，并将集群的状态记录写到json文件中

    """
    def __init__(self, simulation):
        self.simulation = simulation
        self.env = simulation.env
        self.event_file = simulation.event_file
        self.events = []

    def run(self):
        """
        Monitor核心方法
        在模拟过程中不断检查集群的状态，并记录每个时间步的状态
        时间 + 集群状态
        :return:
        """
        while not self.simulation.finished:
            state = {
                'timestamp': self.env.now,
                'cluster_state': self.simulation.cluster.state
            }
            self.events.append(state)
            yield self.env.timeout(1)

        state = {
            'timestamp': self.env.now,
            'cluster_state': self.simulation.cluster.state
        }
        self.events.append(state)

        self.write_to_file()

    def write_to_file(self):
        with open(self.event_file, 'w') as f:
            json.dump(self.events, f, indent=4)
