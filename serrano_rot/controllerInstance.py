import sys
import json
import time
import signal
import os.path
import logging

from PyQt5.QtCore import QCoreApplication

import controller.dispatcher
import controller.accessInterface
import controller.dataBrokerInterface

LOG_LEVEL = {"CRITICAL": 50, "ERROR": 40, "WARNING": 30, "INFO": 20, "DEBUG": 10}

CONF_FILE = "/etc/serrano/controller.json"


class ControllerInstance:

    def __init__(self, conf_params):
        self.dispatcher = None
        self.accessInterface = None
        self.dataBrokerInterface = None

        self.config = conf_params

        logging.basicConfig(filename="%s.log" % (int(time.time())), level=LOG_LEVEL[self.config["log_level"]])

        # Suppress logging messages from pika only to levels: WARNING,ERROR and CRITICAL
        logging.getLogger('pika').setLevel(logging.WARNING)

        self.logger = logging.getLogger("SERRANO.ROT.ControllerInstance")

    def boot(self):

        self.logger.info("Initialize services ... ")

        self.dispatcher = controller.dispatcher.Dispatcher(self.config)

        self.accessInterface = controller.accessInterface.AccessInterface(self.config["rest_interface"],
                                                                          self.config["sqlite_db"])
        self.accessInterface.restInterfaceMessage.connect(self.dispatcher.handle_access_request)
        self.accessInterface.start()

        self.dataBrokerInterface = controller.dataBrokerInterface.DataBrokerInterface(self.config["databroker_interface"])
        self.dataBrokerInterface.engineResponse.connect(self.dispatcher.handle_engine_response)
        self.dispatcher.dispatcherRequest.connect(self.dataBrokerInterface.forward_dispatcher_request)
        self.dispatcher.executionResponse.connect(self.dataBrokerInterface.forward_execution_request_response)
        self.dispatcher.engineEvent.connect(self.dataBrokerInterface.forward_dispatcher_evt_message)
        self.dataBrokerInterface.start()

        self.logger.info("ROT controller is ready ...")


if __name__ == "__main__":

    signal.signal(signal.SIGINT, signal.SIG_DFL)

    config_params = None

    if os.path.exists("%s/.rot/controller.json" % os.path.expanduser("~")):
        with open("%s/.rot/controller.json" % os.path.expanduser("~")) as f:
            config_params = json.load(f)

    if config_params is None and os.path.exists(CONF_FILE):
        with open(CONF_FILE) as f:
            config_params = json.load(f)

    if config_params is None or "sqlite_db" not in config_params:
        sys.exit(0)

    if os.path.exists(config_params["sqlite_db"]):

        app = QCoreApplication(sys.argv)

        instance = ControllerInstance(config_params)
        instance.boot()

        sys.exit(app.exec_())
