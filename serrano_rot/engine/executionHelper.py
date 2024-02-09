import os
import logging

import serrano_rot.algorithms

from PyQt5.QtCore import QObject
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtCore import QProcess

logger = logging.getLogger("SERRANO.ROT.ExecutionHelper")


class ExecutionHelper(QObject):

    standardOutput = pyqtSignal(object)
    standardError = pyqtSignal(object)
    executionTerminated = pyqtSignal(object)

    def __init__(self, instance_id):

        super(QObject, self).__init__()

        self.__instance_id = instance_id
        self.__p = None
        self.__is_assigned = False
        self.__wrapper_script = "%s/rot_wrapper.py" % (os.path.dirname(serrano_rot.algorithms.__file__))

    def is_assigned(self):
        return self.__is_assigned

    def get_instance_id(self):
        return self.__instance_id

    def __process_finished(self):
        logger.debug("Helper_Instance_ID '%s' finished." % self.__instance_id)
        self.__p = None
        self.__is_assigned = False

    def __handle_stderr(self):
        data = self.__p.readAllStandardError()
        stderr = bytes(data).decode("utf8")
        logger.debug("Helper_Instance_ID '%s' - StandardError: '%s'" %(self.__instance_id, stderr))
        self.standardError.emit({"helper_instance_id": self.__instance_id, "standard_error": stderr})

    def __handle_stdout(self):
        data = self.__p.readAllStandardOutput()
        stdout = bytes(data).decode("utf8")
        logger.debug("Helper_Instance_ID '%s' - StandardOutput: '%s'" %(self.__instance_id, stdout))
        self.standardOutput.emit({"helper_instance_id": self.__instance_id, "standard_output": stdout})

    def clear(self):
        self.__p = None
        self.__is_assigned = False

    def start(self, execution_plugin, parameters, infrastructure):
        self.__is_assigned = True
        logger.debug("Setup execution parameters for Helper_Instance_ID '%s'" % self.__instance_id)
        self.__p = QProcess(self)
        self.__p.finished.connect(self.__process_finished)
        self.__p.readyReadStandardOutput.connect(self.__handle_stdout)
        self.__p.readyReadStandardError.connect(self.__handle_stderr)
        self.__p.start("python3", [self.__wrapper_script, execution_plugin, parameters, infrastructure])

    def terminate(self):
        logger.debug("Terminate assigned execution for Helper_Instance_ID '%s'" % self.__instance_id)
        self.__p.kill()
        self.clear()
        self.executionTerminated.emit({"helper_instance_id": self.__instance_id})
