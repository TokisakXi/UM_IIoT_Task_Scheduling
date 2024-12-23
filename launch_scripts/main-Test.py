from algorithm.heuristic.DRF import DRF
from algorithm.heuristic.first_fit import FirstFitAlgorithm
from algorithm.heuristic.random_algorithm import RandomAlgorithm
from algorithm.heuristic.tetris import Tetris
from core.config import MachineConfig
from utils.Episode import Episode
from utils.csv_reader import CSVReader
import time

from utils.metricCalculations import average_completion, average_slowdown

machines_number = 3
jobs_len = 10
jobs_csv_path = '../jobs_files/jobs.csv'
event_file = '../monitor_files/monitorTest.json'

machine_configs = [MachineConfig(64, 1, 1) for i in range(machines_number)]
csv_reader = CSVReader(jobs_csv_path)
jobs_configs = csv_reader.generate(0, jobs_len)

# 随机调度算法
tic = time.time()
algorithm = RandomAlgorithm()
episode = Episode(machine_configs, jobs_configs, algorithm, event_file)
episode.run()
print(
    f"随机调度 \t结束时间：{episode.env.now}\t算法实际执行时间:{time.time() - tic} \t任务平均完成时间:{average_completion(episode)} \t任务平均慢速度:{average_slowdown(episode)}")

# 首适应算法
tic = time.time()
algorithm = FirstFitAlgorithm()
episode = Episode(machine_configs, jobs_configs, algorithm, event_file)
episode.run()
print(
    f"首适应调度\t结束时间：{episode.env.now}\t算法实际执行时间:{time.time() - tic} \t任务平均完成时间:{average_completion(episode)} \t任务平均慢速度:{average_slowdown(episode)}")

# Tetris调度算法
tic = time.time()
algorithm = Tetris()
episode = Episode(machine_configs, jobs_configs, algorithm, None)
episode.run()
print(
    f"Tetris调度\t结束时间：{episode.env.now}\t算法实际执行时间:{time.time() - tic} \t任务平均完成时间:{average_completion(episode)} \t任务平均慢速度:{average_slowdown(episode)}")