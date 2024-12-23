from operator import attrgetter
import pandas as pd
import numpy as np

from core.job import JobConfig, TaskConfig


class CSVReader(object):
    def __init__(self, filename):
        self.filename = filename
        df = pd.read_csv(self.filename)

        df.task_id = df.task_id.astype(dtype=int)
        df.job_id = df.job_id.astype(dtype=int)
        df.instances_num = df.instances_num.astype(dtype=int)

        job_task_map = {}  # 存储每个job_id对应的任务列表 key-job_id  val-TaskConfig
        job_submit_time_map = {}  # 存储每个job_id对应的提交时间
        for i in range(len(df)):
            series = df.iloc[i]
            job_id = series.job_id
            task_id = series.task_id
            cpu = series.cpu
            memory = series.memory
            disk = series.disk
            duration = series.duration
            submit_time = series.submit_time
            instances_num = series.instances_num

            # 如果job_id没有对应的值，则会创建一个空列表
            task_configs = job_task_map.setdefault(job_id, [])
            task_configs.append(TaskConfig(task_id, instances_num, cpu, memory, disk, duration))
            # 记录一下每个job_id的提交时间，创建JobConfig的时候用到
            job_submit_time_map[job_id] = submit_time

        job_configs = []
        for job_id, task_configs in job_task_map.items():
            job_configs.append(JobConfig(job_id, job_submit_time_map[job_id], task_configs))
        # 根据job提交时间对job_configs列表排序
        job_configs.sort(key=attrgetter('submit_time'))
        # 将job_configs结果保存到成员变量中，其他函数还会用到
        self.job_configs = job_configs

    def generate(self, offset, number):
        # 确保返回的作业数不超过总数
        number = number if offset + number < len(self.job_configs) else len(self.job_configs) - offset
        ret = self.job_configs[offset: offset + number]
        # 获取子集中的第一个作业，并用它的提交时间作为基准时间
        the_first_job_config = ret[0]
        submit_time_base = the_first_job_config.submit_time
        # 初始化变量来统计任务数量、任务实例数、任务持续时间、任务CPU和内存需求
        tasks_number = 0 # 统计ret中所有作业的任务数量
        task_instances_numbers = []
        task_instances_durations = []
        task_instances_cpu = []
        task_instances_memory = []
        for job_config in ret:
            job_config.submit_time -= submit_time_base
            tasks_number += len(job_config.task_configs)
            for task_config in job_config.task_configs:
                task_instances_numbers.append(task_config.instances_number)
                task_instances_durations.extend([task_config.duration] * int(task_config.instances_number))
                task_instances_cpu.extend([task_config.cpu] * int(task_config.instances_number))
                task_instances_memory.extend([task_config.memory] * int(task_config.instances_number))

        print('作业总数: ', len(ret))
        print('任务总数:', tasks_number)

        print('任务实例数量的平均值: ', np.mean(task_instances_numbers))
        print('任务实例数量的标准差: ', np.std(task_instances_numbers))

        print('任务实例CPU使用量的平均值: ', np.mean(task_instances_cpu))
        print('任务实例CPU使用量的标准差: ', np.std(task_instances_cpu))

        print('任务实例memo使用量的平均值: ', np.mean(task_instances_memory))
        print('任务实例memo使用量的标准差: ', np.std(task_instances_memory))

        print('任务实例持续时间的平均值: ', np.mean(task_instances_durations))
        print('任务实例持续时间的标准差: ', np.std(task_instances_durations))

        return ret