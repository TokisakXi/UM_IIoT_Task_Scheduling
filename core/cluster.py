from core.machine import Machine

"""

定义了一个Cluster类

主要职责是管理一组机器（machine）和作业(jobs)
并提供了多种属性和方法来查询集群的状态、添加机器和作业。

"""
class Cluster(object):
    """

    machines - 一个列表，用于存储集群中的所有机器。每个机器都包含自身的资源和任务实例
    jobs - 一个列表，用于存储集群中的所有作业(job对象)。每个作业可能包含多个任务，作业会在集群中运行

    unfinished_jobs - 集群中所有尚未完成的作业列表
    ready_unfinished_tasks - 集群中所有尚未完成且已准备好的任务列表
    tasks_which_has_waiting_instance - 所有包含等待任务实例的任务列表
    ready_tasks_which_has_waiting_instance - 所有已经准备好且包含等待任务实例的任务列表
    finished_jobs - 集群中所有已经完成的作业列表
    finished_tasks - 集群中所有已经完成的任务列表
    running_task_instances - 集群中所有正在运行的任务实例列表
    cpu - 集群中所有机器的当前 CPU 剩余量的总和
    memory - 集群中所有机器的当前内存剩余量的总和
    disk - 集群中所有机器的当前磁盘剩余量的总和
    cpu_capacity - 集群中所有机器的 CPU 总容量
    memory_capacity - 集群中所有机器的内存总容量
    disk_capacity - 集群中所有机器的磁盘总容量
    state - 集群的所有状态信息

    """
    def __init__(self):
        self.machines = []
        self.jobs = []

    @property
    def unfinished_jobs(self):
        """
        集群中所有尚未完成的作业job列表
        :return: 集群中所有尚未完成的作业列表（list）
        """
        ls = []
        for job in self.jobs:
            if not job.finished:
                ls.append(job)
        return ls

    @property
    def unfinished_tasks(self):
        """
        集群中所有尚未完成的任务task列表
        :return:
        """
        ls = []
        for job in self.jobs:
            ls.extend(job.unfinished_tasks)
        return ls

    @property
    def ready_unfinished_tasks(self):
        """
        集群中所有尚未完成且已准备好的任务列表
        :return:
        """
        ls = []
        for job in self.jobs:
            ls.extend(job.ready_unfinished_tasks)
        return ls

    @property
    def tasks_which_has_waiting_instance(self):
        """
        集群中所有包含等待任务实例的任务列表
        :return:
        """
        ls = []
        for job in self.jobs:
            ls.extend(job.tasks_which_has_waiting_instance)
        return ls

    @property
    def ready_tasks_which_has_waiting_instance(self):
        """
        返回所有已经准备好且包含等待任务实例的任务列表
        :return:
        """
        ls = []
        for job in self.jobs:
            ls.extend(job.ready_tasks_which_has_waiting_instance)
        return ls

    @property
    def finished_jobs(self):
        """
        返回集群中所有已经完成的作业列表
        :return:
        """
        ls = []
        for job in self.jobs:
            if job.finished:
                ls.append(job)
        return ls

    @property
    def finished_tasks(self):
        """
        返回集群中所有已经完成的任务列表
        :return:
        """
        ls = []
        for job in self.jobs:
            ls.extend(job.finished_tasks)
        return ls

    @property
    def running_task_instances(self):
        """
        返回集群中所有正在运行的任务实例列表
        :return:
        """
        task_instances = []
        for machine in self.machines:
            task_instances.extend(machine.running_task_instances)
        return task_instances

    def add_machines(self, machine_configs):
        for machine_config in machine_configs:
            machine = Machine(machine_config)
            self.machines.append(machine)
            machine.attach(self)

    def add_job(self, job):
        self.jobs.append(job)

    @property
    def cpu(self):
        """
        返回集群中所有机器的当前CPU剩余量的总和
        :return:
        """
        return sum([machine.cpu for machine in self.machines])

    @property
    def memory(self):
        """
        返回集群中所有机器的当前memo剩余量的总和
        :return:
        """
        return sum([machine.memory for machine in self.machines])

    @property
    def disk(self):
        """
        返回集群中所有机器的当前disk剩余量的总和
        :return:
        """
        return sum([machine.disk for machine in self.machines])

    @property
    def cpu_capacity(self):
        """
        返回集群中所有机器的CPU总容量
        :return:
        """
        return sum([machine.cpu_capacity for machine in self.machines])

    @property
    def memory_capacity(self):
        """
        返回集群中所有机器的memo总容量
        :return:
        """
        return sum([machine.memory_capacity for machine in self.machines])

    @property
    def disk_capacity(self):
        """
        返回集群中所有机器的disk总容量
        :return:
        """
        return sum([machine.disk_capacity for machine in self.machines])

    @property
    def state(self):
        """
        返回本集群的状态信息

        到达的作业数
        未完成的作业数
        已完成的作业数
        未完成的任务数
        已完成的任务数
        正在运行的任务实例数
        各机器的状态
        当前CPU、内存、磁盘的使用比例
        :return:
        """
        return {
            'arrived_jobs': len(self.jobs),
            'unfinished_jobs': len(self.unfinished_jobs),
            'finished_jobs': len(self.finished_jobs),
            'unfinished_tasks': len(self.unfinished_tasks),
            'finished_tasks': len(self.finished_tasks),
            'running_task_instances': len(self.running_task_instances),
            'machine_states': [machine.state for machine in self.machines],
            'cpu': self.cpu / self.cpu_capacity,
            'memory': self.memory / self.memory_capacity,
            'disk': self.disk / self.disk_capacity,
        }
