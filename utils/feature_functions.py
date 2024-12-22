import numpy as np


def features_extract_func(task):
    """
    提取任务的特征
    :param task:
    :return:
    """
    return [task.task_config.cpu, task.task_config.memory,
            task.task_config.duration, task.waiting_task_instances_number]


def features_extract_func_ac(task):
    """
    提取任务特征plus版
    任务实例的数量 + 当前运行任务实例数量 + 已完成任务实例的数量
    :param task:
    :return:
    """
    return features_extract_func(task) + [task.task_config.instances_number, len(task.running_task_instances),
                                          len(task.finished_task_instances)]


def features_normalize_func(x):
    """
    特征归一化函数
    :param x:
    :return:
    """
    y = (np.array(x) - np.array([0, 0, 0.65, 0.009, 74.0, 80.3])) / np.array([64, 1, 0.23, 0.005, 108.0, 643.5])
    return y


def features_normalize_func_ac(x):
    """
    特征归一化函数plus版
    :param x:
    :return:
    """
    y = (np.array(x) - np.array([0, 0, 0.65, 0.009, 74.0, 80.3, 80.3, 80.3, 80.3])) / np.array(
        [64, 1, 0.23, 0.005, 108.0, 643.5, 643.5, 643.5, 643.5])
    return y
