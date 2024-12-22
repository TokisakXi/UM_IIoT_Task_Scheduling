class TaskInstanceConfig(object):
    """

    最底层的任务实例配置，由TaskConfig的配置构成

    """

    def __init__(self, task_config):
        self.cpu = task_config.cpu
        self.memory = task_config.memory
        self.disk = task_config.disk
        self.duration = task_config.duration


class TaskConfig(object):
    """

    次顶层的配置类，描述了一个任务的配置信息，涵盖了任务的索引、实例数量、资源需求

    """

    def __init__(self, task_index, instances_number, cpu, memory, disk, duration, parent_indices=None):
        self.task_index = task_index
        self.instances_number = instances_number
        self.cpu = cpu
        self.memory = memory
        self.disk = disk
        self.duration = duration
        self.parent_indices = parent_indices


class JobConfig(object):
    """
    最顶层的配置类，定义了一个作业整体的配置信息
    """

    def __init__(self, idx, submit_time, task_configs):
        self.submit_time = submit_time
        self.task_configs = task_configs
        self.id = idx


class MachineConfig(object):
    """

    machine配置类，用于定义每个机器的配置，包括cpu容量、内存容量、磁盘容量

    """
    idx = 0  # 类变量，用于给每个MachineConfig实例分配一个唯一id，会自动递增

    def __init__(self, cpu_capacity, memory_capacity, disk_capacity, group=None, cpu=None, memory=None, disk=None):
        # self.group = group
        self.cpu_capacity = cpu_capacity
        self.memory_capacity = memory_capacity
        self.disk_capacity = disk_capacity

        # 如果没有传入cpu, memory, disk，则使用默认的cpu_capacity, memory_capacity, disk_capacity
        self.group = group if group is not None else 0
        self.cpu = cpu_capacity if cpu is None else cpu
        self.memory = memory_capacity if memory is None else memory
        self.disk = disk_capacity if disk is None else disk

        self.id = MachineConfig.idx
        MachineConfig.idx += 1
