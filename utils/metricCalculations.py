import time
import numpy as np
# import tensorflow as tf


def average_completion(exp):
    """
    计算全部任务的平均完成时间
    :param exp:
    :return:
    """
    completion_time = 0
    number_task = 0
    for job in exp.simulation.cluster.jobs:
        for task in job.tasks:
            number_task += 1
            completion_time += (task.finished_timestamp - task.started_timestamp)
    return completion_time / number_task


def average_slowdown(exp):
    """
    计算全部任务的平均慢速度
    任务实际执行时间/任务理想执行时间
    :param exp:
    :return:
    """
    slowdown = 0
    number_task = 0
    for job in exp.simulation.cluster.jobs:
        for task in job.tasks:
            number_task += 1
            slowdown += (task.finished_timestamp - task.started_timestamp) / task.task_config.duration
    return slowdown / number_task


# def multiprocessing_run(episode, trajectories, makespans, average_completions, average_slowdowns):
#     """
#     并行化地运行多个仿真试验，并将每个试验的结果保存在列表中（轨迹、总时间、平均完成时间、慢速度）
#     :param episode:
#     :param trajectories:
#     :param makespans:
#     :param average_completions:
#     :param average_slowdowns:
#     :return:
#     """
#     np.random.seed(int(time.time()))
#     tf.random.set_random_seed(time.time())
#     episode.run()
#     trajectories.append(episode.simulation.scheduler.algorithm.current_trajectory)
#     makespans.append(episode.simulation.env.now)
#     # print(episode.simulation.env.now)
#     average_completions.append(average_completion(episode))
#     average_slowdowns.append(average_slowdown(episode))
