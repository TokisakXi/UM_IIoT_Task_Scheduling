class Scheduler(object):
    """

    env - 调度器与模拟环境交互
    algorithm - 调度算法
    simulation - 调度器所依附的仿真对象
    cluster - 调度器所管理的计算集群，包含所有机器和作业信息
    destroyed - 标记调度器是否已被销毁
    valid_pairs - 字典。存储有效的机器和任务对

    """

    def __init__(self, env, algorithm):
        self.env = env
        self.algorithm = algorithm
        self.simulation = None
        self.cluster = None
        self.destroyed = False
        self.valid_pairs = {}

    def attach(self, simulation):
        """
        将调度器与仿真对象关联。通常会绑定到集群及其所有作业和机器
        :param simulation:
        :return:
        """
        self.simulation = simulation
        self.cluster = simulation.cluster

    def make_decision(self):
        while True:
            machine, task = self.algorithm(self.cluster, self.env.now)
            if machine is None or task is None:
                break
            else:
                task.start_task_instance(machine)

    def run(self):
        while not self.simulation.finished:
            self.make_decision()
            yield self.env.timeout(1)
        self.destroyed = True
