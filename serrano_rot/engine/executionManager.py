import json
import time
import psutil
import logging

from PyQt5.QtCore import QObject
from PyQt5.QtCore import pyqtSignal

from serrano_rot.utils.enums import ResponseStatus
from serrano_rot.engine.executionHelper import ExecutionHelper

logger = logging.getLogger("SERRANO.ROT.ExecutionManager")


class ExecutionManager(QObject):
    forwardResponse = pyqtSignal(object)

    def __init__(self, engine_id):
        super(QObject, self).__init__()

        self.engine_id = engine_id

        self.__helper_instances = []
        self.__active_execution_ids = []
        self.__limit_of_instances = int(psutil.cpu_count())

        for i in range(self.__limit_of_instances):
            self.__helper_instances.append(ExecutionHelper(i))
            self.__active_execution_ids.append("")
            self.__helper_instances[i].standardError.connect(self.__handle_standardError)
            self.__helper_instances[i].standardOutput.connect(self.__handle_standardOutput)
            self.__helper_instances[i].executionTerminated.connect(self.__handle_executionTerminated)

    def __handle_standardError(self, data):

        exec_id = self.__active_execution_ids[data["helper_instance_id"]]

        self.forwardResponse.emit(json.dumps({"engine_id": self.engine_id, "type": "execution", "execution_id": exec_id,
                                              "status": ResponseStatus.FAILED, "timestamp": int(time.time()),
                                              "reason": data["standard_error"], "results": {}}))

        self.remove_active_execution_id(exec_id)

    def __handle_standardOutput(self, data):

        exec_id = self.__active_execution_ids[data["helper_instance_id"]]

        self.forwardResponse.emit(json.dumps({"engine_id": self.engine_id, "type": "execution", "execution_id": exec_id,
                                              "status": ResponseStatus.COMPLETED, "timestamp": int(time.time()),
                                              "reason": "", "results": data["standard_output"]}))
        self.remove_active_execution_id(exec_id)

    def __handle_executionTerminated(self, data):

        exec_id = self.__active_execution_ids[data["helper_instance_id"]]

        self.forwardResponse.emit(json.dumps({"engine_id": self.engine_id, "type": "execution", "execution_id": exec_id,
                                              "status": ResponseStatus.CANCELLED, "timestamp": int(time.time()),
                                              "reason": "Cancelled by the user", "results": {}}))

        self.remove_active_execution_id(exec_id)

    def __assign_instance_id(self, execution_id):

        for instance in self.__helper_instances:
            if not instance.is_assigned():
                self.__active_execution_ids[instance.get_instance_id()] = execution_id
                return instance.get_instance_id()

        return None

    def get_active_execution_ids(self):
        return self.__active_execution_ids

    def __get_instance_id_by_execution_id(self, execution_id):
        instance_id = None
        for iid, eid in enumerate(self.__active_execution_ids):
            if execution_id == eid:
                instance_id = iid
                break
        return instance_id

    def remove_active_execution_id(self, execution_id):
        # print(self.__active_execution_ids)
        self.__active_execution_ids = ["" if eid == execution_id else eid for eid in self.__active_execution_ids]
        # print(self.__active_execution_ids)

    # SLOT Methods
    def handle_dispatch_request(self, data):

        logger.info("Handle dispatcher request")
        logger.debug("Request parameters: %s" % data)

        req = data #json.loads(data)

        if req["action"] == "start" and req["engine_id"] == self.engine_id:

            if req["execution_id"] in self.get_active_execution_ids():
                self.forwardResponse.emit(json.dumps({"engine_id": self.engine_id, "execution_id": req["execution_id"],
                                                      "type": "execution", "status": ResponseStatus.REJECTED,
                                                      "timestamp": int(time.time()), "results": {},
                                                      "reason": "Already active execution"}))
                logger.debug(
                    "There is already active execution for the requested execution_id '%s'" % req["execution_id"])
                return

            instance_id = self.__assign_instance_id(req["execution_id"])

            if instance_id is None:
                self.forwardResponse.emit(json.dumps({"engine_id": self.engine_id, "execution_id": req["execution_id"],
                                                      "type": "execution", "status": ResponseStatus.REJECTED,
                                                      "timestamp": int(time.time()), "results": {},
                                                      "reason": "Max number of concurrent executions"}))
                logger.debug(
                    "Max number of concurrent executions - Discard requested execution_id '%s'" % req["execution_id"])
                return
            else:
                logger.debug("Start requested execution_id '%s'" % req["execution_id"])
                self.__helper_instances[instance_id].start(req["execution_plugin"],
                                                           json.dumps(req["parameters"]),
                                                           json.dumps(req["infrastructure"]))

        elif req["action"] == "cancel" and req["engine_id"] == self.engine_id:

            logger.info("Terminate execution '%s'" % req["execution_id"])

            instance_id = self.__get_instance_id_by_execution_id(req["execution_id"])

            if instance_id is None:
                logger.debug("Unable to find active instance for execution '%s'" % req["execution_id"])
                return

            self.__helper_instances[instance_id].terminate()

        else:
            logger.debug("Invalid request description, request is discarded")
            self.forwardResponse.emit(json.dumps({"engine_id": self.engine_id, "execution_id": req["execution_id"],
                                                  "type": "execution", "status": ResponseStatus.REJECTED,
                                                  "timestamp": int(time.time()), "results": {},
                                                  "reason": "Unknown action or wrong engine id"}))
