from core.config import *


class Task(object):
    """

    env - 任务所在的仿真环境 (env)
    job - 任务所属的作业 (job)
    task_index - 任务在作业中的索引 (int)
    task_config - 任务的配置对象，包括了Task的具体参数 (TaskConfig)
    _ready - 标记任务是否准备好执行 （bool）
    task_instances - 存储任务实例的列表 (list)
    next_instance_pointer - 指向下一个待调度任务实例的指针 (int)

    id - 任务的唯一标识符 (str)
    ready - 任务是否已经准备好执行 (bool)
    running_task_instances - 正在运行的任务实例列表(list)
    finished_task_instances - 已完成的任务实例列表 (list)
    started - 返回任务是否已经启动 （bool）
    waiting_task_instances_number - 任务实例中尚未调度的数量 (int)
    has_waiting_task_instances - 是否还有未调度的任务实例 (bool)
    finished - 判断任务是否完成 (bool)
    started_timestamp - 任务开始执行的时间戳 (float)
    finished_timestamp - 任务完成的时间戳 (float)

    函数：
    start_task_instance(machine) - 将下一个任务实例分配到指定的机器上执行

    """
    def __init__(self, env, job, task_config):
        self.env = env  # Task所在的仿真环境，用于执行事件
        self.job = job  # 任务所属的作业实例
        self.task_index = task_config.task_index  # 任务索引，用于标识一个任务在作业中的位置
        self.task_config = task_config  # 任务配置对象，包括任务的配置细节，如资源需求、持续时间
        self._ready = False  # 用于标记任务是否准备好执行（初始为False）
        self._parents = None # 用于存储任务的父任务列表

        self.task_instances = []  # 任务的实例列表

        '''
        根据task_config中的配置信息，为Task创建多个TaskInstance实例，并将它们存储在task_instances列表中
        '''

        # 通过TaskInstanceConfig类（配置类）
        task_instance_config = TaskInstanceConfig(task_config)
        for task_instance_index in range(int(self.task_config.instances_number)):
            self.task_instances.append(TaskInstance(self.env, self, task_instance_index, task_instance_config))
        self.next_instance_pointer = 0

    @property  # 使用@property装饰器将此方法作为只读属性暴露
    def id(self):
        return str(self.job.id) + '-' + str(self.task_index)

    @property
    def parents(self):
        if self._parents is None:
            if self.task_config.parent_indices is None:
                raise ValueError("Task_config's parent_indices should not be None.")
            self._parents = []
            for parent_index in self.task_config.parent_indices:
                self._parents.append(self.job.tasks_map[parent_index])
        return self._parents

    @property
    def ready(self):
        if not self._ready:
            for p in self.parents:
                if not p.finished:
                    return False
            self._ready = True
        return self._ready

    @property
    def running_task_instances(self):
        ls = []
        for task_instance in self.task_instances:
            if task_instance.started and not task_instance.finished:
                ls.append(task_instance)
        return ls

    @property
    def finished_task_instances(self):
        """
        用于获取当前任务Task中所有正在运行的任务实例
        :return: 返回所有已经开始但尚未完成的任务实例
        """
        ls = []
        for task_instance in self.task_instances:
            if task_instance.finished:
                ls.append(task_instance)
        return ls

    # the most heavy
    def start_task_instance(self, machine):
        """
        启动任务实例
        将下一个任务实例分配到指定的机器上进行执行
        :param machine:
        :return:
        """
        # schedule()方法将任务实例分配给指定的machine
        self.task_instances[self.next_instance_pointer].schedule(machine)
        # 将任务实例指针推一格
        self.next_instance_pointer += 1


    @property
    def started(self):
        """
        遍历任务的所有实例
        如果某个任务实例的started属性为True，表示该任务实例已经开始执行
        如果没有一个任务实例started属性为True，返回False
        :return:
        """
        for task_instance in self.task_instances:
            if task_instance.started:
                return True
        return False

    @property
    def waiting_task_instances_number(self):
        """
        获取当前任务中等待执行的任务实例数量
        :return: 等待执行的任务实例数量
        """
        return self.task_config.instances_number - self.next_instance_pointer

    @property
    def has_waiting_task_instances(self):
        """
        判断当前任务是否还有待调度的任务实例
        :return:
        """
        return self.task_config.instances_number > self.next_instance_pointer

    @property
    def finished(self):
        """
        任务只有在 没有待执行的任务实例&&没有正在运行的任务实例 -> 任务完成
        A task is finished only if it has no waiting task instances and no running task instances.
        :return: bool
        """
        if self.has_waiting_task_instances:
            return False
        if len(self.running_task_instances) != 0:
            return False
        return True

    @property
    def started_timestamp(self):
        """
        用来获取任务中所有任务实例最早开始时间
        :return:
        """
        t = None
        for task_instance in self.task_instances:
            if task_instance.started_timestamp is not None:
                if (t is None) or (t > task_instance.started_timestamp):
                    t = task_instance.started_timestamp
        return t

    @property
    def finished_timestamp(self):
        """
        用来获取任务完成的时间,也就是任务实例中所有任务实例的最晚完成时间
        :return:
        """
        if not self.finished:
            return None
        t = None
        for task_instance in self.task_instances:
            if (t is None) or (t < task_instance.finished_timestamp):
                t = task_instance.finished_timestamp
        return t


class Job(object):
    """

    env - job所在的仿真环境(env)
    job_config - job的配置信息，包含作业/任务的详细配置 (JobConfig)
    id - 作业的唯一标识符 (str)
    tasks_map - 存储作业中所有任务的字典（key - task_index val - Task） (dict)
    tasks - tasks_map中所有任务实例的val (list)
    unfinished_tasks - 作业中所有尚未完成的任务实例 (list)
    ready_unfinished_tasks - 作业中所有未完成且准备好执行的任务实例 (list)
    tasks_which_has_waiting_instance - 所有还存在等待中的任务实例的任务 （list）
    ready_tasks_which_has_waiting_instance - 准备好执行的任务&&有等待任务实例 （list）
    running_tasks - 作业中所有正在运行的任务实例 (list)
    finished_tasks - 作业中所有已完成的任务实例 (list)
    started - 检查作业中的任务是否已经开始执行 (bool)
    finished - 检查作业中的所有任务是否已经完成 (bool)
    started_timestamp - 作业中所有任务中最早开始的时间戳 (float)
    finished_timestamp - 返回作业中所有任务中最晚完成的时间戳 (float)


    """


    # task_cls是一个类变量，保存了Task类
    task_cls = Task

    def __init__(self, env, job_config):
        self.env = env
        self.job_config = job_config # job的配置信息
        self.id = job_config.id

        # 字典，用来存储job中所有的任务。键是任务的索引(task_index)
        self.tasks_map = {}
        # 遍历job配置中的所有任务配置，然后逐个创建Task的实例
        for task_config in job_config.task_configs:
            task_index = task_config.task_index
            # 使用Job.task_cls来创建Task类的实例，并添加到tasks_map中
            self.tasks_map[task_index] = Job.task_cls(env, self, task_config)

    @property
    def tasks(self):
        """
        定义tasks属性
        :return: tasks_map中的所有value
        """
        return self.tasks_map.values()

    @property
    def unfinished_tasks(self):
        """
        返回job中所有尚未完成的Task实例
        :return:
        """
        ls = []
        for task in self.tasks:
            if not task.finished:
                ls.append(task)
        return ls

    @property
    def ready_unfinished_tasks(self):
        """
        返回job中所有 未完成&&准备好 的Task实例
        :return:
        """
        ls = []
        for task in self.tasks:
            if not task.finished and task.ready:
                ls.append(task)
        return ls

    @property
    def tasks_which_has_waiting_instance(self):
        """
        检查每个任务是否有“等待执行的任务实例”，如果有则将该任务添加到返回的列表中
        :return:
        """
        ls = []
        for task in self.tasks:
            if task.has_waiting_task_instances:
                ls.append(task)
        return ls

    @property
    def ready_tasks_which_has_waiting_instance(self):
        """
        遍历任务列表，返回 等待任务实例&&准备好执行 的任务列表
        :return:
        """
        ls = []
        for task in self.tasks:
            if task.has_waiting_task_instances and task.ready:
                ls.append(task)
        return ls

    @property
    def running_tasks(self):
        ls = []
        for task in self.tasks:
            if task.started and not task.finished:
                ls.append(task)
        return ls

    @property
    def finished_tasks(self):
        ls = []
        for task in self.tasks:
            if task.finished:
                ls.append(task)
        return ls

    @property
    def started(self):
        for task in self.tasks:
            if task.started:
                return True
        return False

    @property
    def finished(self):
        for task in self.tasks:
            if not task.finished:
                return False
        return True

    @property
    def started_timestamp(self):
        t = None
        for task in self.tasks:
            if task.started_timestamp is not None:
                if (t is None) or (t > task.started_timestamp):
                    t = task.started_timestamp
        return t

    @property
    def finished_timestamp(self):
        if not self.finished:
            return None
        t = None
        for task in self.tasks:
            if (t is None) or (t < task.finished_timestamp):
                t = task.finished_timestamp
        return t


class TaskInstance(object):
    """

    env - 任务实例所在的仿真环境 (env)
    task - 所属的任务 (Task)
    task_instance_index - 该任务实例的顺序 (int)
    config - 任务实例的配置 (TaskInstanceConfig)
    cpu - 任务实例所需要的cpu资源 （int）
    memory - 任务实例所需要的内存资源 (int)
    disk - 任务实例所需要的磁盘资源 (disk)
    duration - 任务实例的执行时间 (int)
    machine - 执行任务实例的机器 (Machine)
    process - 与任务实例执行相关的仿真进程，控制任务实例的生命周期
    new - 标记任务实例是否为新任务，刚创建时候为True (bool)
    started - 标记任务实例是否已经开始执行 （bool）
    finished - 标记任务实例是否已完成执行 （bool）
    started_timestamp - 任务实例开始执行的时间戳
    finished_timestamp - 任务实例完成执行的时间戳

    id - 任务实例的唯一标识符

    函数：


    """
    def __init__(self, env, task, task_instance_index, task_instance_config):
        self.env = env
        self.task = task
        self.task_instance_index = task_instance_index
        self.config = task_instance_config
        self.cpu = task_instance_config.cpu
        self.memory = task_instance_config.memory
        self.disk = task_instance_config.disk
        self.duration = task_instance_config.duration

        self.machine = None
        self.process = None
        self.new = True

        self.started = False
        self.finished = False
        self.started_timestamp = None
        self.finished_timestamp = None

    @property
    def id(self):
        return str(self.task.id) + '-' + str(self.task_instance_index)

    def do_work(self):
        """
        模拟了任务实例在特定机器上执行的过程
        :return:
        """
        # self.cluster.waiting_tasks.remove(self)
        # self.cluster.running_tasks.append(self)
        # self.machine.run(self)
        yield self.env.timeout(self.duration)

        self.finished = True
        self.finished_timestamp = self.env.now

        self.machine.stop_task_instance(self)

    def schedule(self, machine):
        """
        将任务实例调度到某台机器上进行执行
        该函数执行了任务实例的启动、时间记录、任务的实际执行过程
        :param machine:
        :return:
        """
        self.started = True
        self.started_timestamp = self.env.now

        self.machine = machine
        self.machine.run_task_instance(self)
        self.process = self.env.process(self.do_work())
