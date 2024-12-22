from abc import ABC, abstractmethod # abc模块 用于定义抽象父类(ABC)


"""

抽象类Algorithm 是所有算法的父类
所有算法都必须继承它，并且重写__call__()方法

"""
class Algorithm(ABC):
    @abstractmethod # 用于标记抽象方法
    def __call__(self, cluster, clock):
        pass
