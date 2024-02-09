import abc
import json


class AlgorithmInterface(metaclass=abc.ABCMeta):

    def __init__(self, parameters, infrastructure):
        self.__infrastructure = json.loads(infrastructure)
        self.__parameters = json.loads(parameters)

    def get_input_parameters(self):
        return self.__parameters

    def get_infrastructure_parameters(self):
        return self.__infrastructure

    @abc.abstractmethod
    def launch(self):
        pass

