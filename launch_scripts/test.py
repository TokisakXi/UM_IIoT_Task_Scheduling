from core.cluster import Cluster
from core.config import MachineConfig
from core.machine import Machine

if __name__ == '__main__':
    machine_config = MachineConfig(100, 100, 100, 0, None, None, None)
    machine_configs=[machine_config]
    cluster = Cluster()
    cluster.add_machines(machine_configs)

    print(cluster.state)
