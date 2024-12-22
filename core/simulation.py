from core.monitor import Monitor


class Simulation(object):
    """
    整个调度系统的核心，负责管理集群、任务代理、调度器和监控的交互
    并协调各个组件在仿真环境中的运行

    env - 仿真环境
    cluster - 关联的计算机集群


    """
    def __init__(self, env, cluster, task_broker, scheduler, event_file):
        self.env = env
        self.cluster = cluster
        self.task_broker = task_broker
        self.scheduler = scheduler
        self.event_file = event_file
        if event_file is not None:
            self.monitor = Monitor(self)

        self.task_broker.attach(self)
        self.scheduler.attach(self)

    def run(self):
        # Starting monitor process before task_broker process
        # and scheduler process is necessary for log records integrity.
        if self.event_file is not None:
            self.env.process(self.monitor.run())
        self.env.process(self.task_broker.run())
        self.env.process(self.scheduler.run())

    @property
    def finished(self):
        return self.task_broker.destroyed \
               and len(self.cluster.unfinished_jobs) == 0
