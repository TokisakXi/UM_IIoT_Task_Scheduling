from core.job import Job


class Broker(object):
    job_cls = Job

    """
    Broker是任务代理类，负责从任务配置创建作业并将它们提交到集群中。
    它扮演了一个“任务生成器”的角色，按照预定的时间间隔向集群中添加作业
    
    env - 当前的仿真环境，simpy的环境对象
    simulation - 当前的仿真对象
    cluster - Broker会向集群中添加作业
    job_configs - 作业配置，决定了在仿真过程中创建哪些作业
    
    """

    def __init__(self, env, job_configs):
        self.env = env
        self.simulation = None
        self.cluster = None
        self.destroyed = False
        self.job_configs = job_configs

    def attach(self, simulation):
        """
        将Broker与Simulation相关联
        :param simulation:
        :return:
        """
        self.simulation = simulation
        self.cluster = simulation.cluster

    def run(self):
        """
        一个协程，负责根据job_configs中定义的提交时间，按顺序生成作业并提交到集群中
        :return:
        """
        for job_config in self.job_configs:
            assert job_config.submit_time >= self.env.now
            yield self.env.timeout(job_config.submit_time - self.env.now)
            job = Broker.job_cls(self.env, job_config)
            # print('a task arrived at time %f' % self.env.now)
            self.cluster.add_job(job)
        self.destroyed = True
