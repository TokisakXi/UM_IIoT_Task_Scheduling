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
