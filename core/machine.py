from enum import Enum

"""

id - 机器的唯一标识符
cpu_capacity - 机器的CPU总容量
memory_capacity - 机器的内存总容量
disk_capacity - 机器的磁盘总容量
cpu - 当前剩余的cpu容量
memory - 当前剩余的内存容量
disk - 当前剩余的磁盘容量
cluster - 机器所在的集群
task_instances - 机器上正在运行的所有任务实例，每个任务实例都会消耗一定的机器资源
machine_door - 机器的状态

"""


class MachineDoor(Enum):
    TASK_IN = 0  # 机器正在接收任务
    TASK_OUT = 1  # 机器正在完成任务
    NULL = 3  # 机器没有任务正在进行


class Machine(object):
    def __init__(self, machine_config):
        self.id = machine_config.id
        self.group = machine_config.group
        self.cpu_capacity = machine_config.cpu_capacity
        self.memory_capacity = machine_config.memory_capacity
        self.disk_capacity = machine_config.disk_capacity
        self.cpu = machine_config.cpu
        self.memory = machine_config.memory
        self.disk = machine_config.disk

        self.cluster = None  # 表示机器所在的集群
        self.task_instances = []  # 用于存储该机器上正在运行的任务实例
        self.machine_door = MachineDoor.NULL  # 刚创建好的机器上没有任务正在运行

    def run_task_instance(self, task_instance):
        """
        将一个任务实例运行在当前机器上
        :param task_instance:
        :return:
        """

        # 减少机器的资源来模拟任务实例的资源消耗
        self.cpu -= task_instance.cpu
        self.memory -= task_instance.memory
        self.disk -= task_instance.disk
        # 将任务实例添加到task_instance中
        self.task_instances.append(task_instance)
        # 接收到任务实例的机器改变了机器状态
        self.machine_door = MachineDoor.TASK_IN

    def stop_task_instance(self, task_instance):
        """
        当任务实例执行完成时，释放被占用的资源
        :param task_instance:
        :return:
        """
        self.cpu += task_instance.cpu
        self.memory += task_instance.memory
        self.disk += task_instance.disk
        self.machine_door = MachineDoor.TASK_OUT  # 将机器的状态设置为Task_Out表示机器任务完成

    @property
    def running_task_instances(self):
        """
        返回机器上所有正在运行的任务实例，即已经启动但尚未完成的任务实例
        :return: 正在运行的任务实例（list）
        """
        ls = []
        for task_instance in self.task_instances:
            if task_instance.started and not task_instance.finished:
                ls.append(task_instance)
        return ls

    @property
    def finished_task_instances(self):
        """
        返回机器上所有已完成的任务实例 (list)
        :return:
        """
        ls = []
        for task_instance in self.task_instances:
            if task_instance.finished:
                ls.append(task_instance)
        return ls

    def attach(self, cluster):
        """
        将当前机器附加到一个集群中
        :param cluster:
        :return:
        """
        self.cluster = cluster

    def accommodate(self, task):
        """
        检查当前机器是否能够容纳一个给定的任务。
        :param task:
        :return:
        """
        return self.cpu >= task.task_config.cpu and \
            self.memory >= task.task_config.memory and \
            self.disk >= task.task_config.disk

    @property
    def feature(self):
        """
        返回机器当前的剩余资源
        :return:
        """
        return [self.cpu, self.memory, self.disk]

    @property
    def capacity(self):
        """
        返回机器的最大资源量
        :return:
        """
        return [self.cpu_capacity, self.memory_capacity, self.disk_capacity]

    @property
    def state(self):
        """
        返回机器的状态数据
        :return:
        """
        return {
            'id': self.id,
            'group': self.group,
            'cpu_capacity': self.cpu_capacity,
            'memory_capacity': self.memory_capacity,
            'disk_capacity': self.disk_capacity,
            'cpu': self.cpu / self.cpu_capacity,
            'memory': self.memory / self.memory_capacity,
            'disk': self.disk / self.disk_capacity,
            'running_task_instances': len(self.running_task_instances),
            'finished_task_instances': len(self.finished_task_instances)
        }

    def __eq__(self, other):
        """
        重载==运算符，判断两个机器对象是否相等，通过机器id
        :param other:
        :return:
        """
        return isinstance(other, Machine) and other.id == self.id
