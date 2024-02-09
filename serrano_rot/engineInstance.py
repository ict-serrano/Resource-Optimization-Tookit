import sys
import uuid
import json
import time
import signal
import logging
import os.path
import platform

from PyQt5.QtCore import QTimer, QObject, QCoreApplication

import engine.engineInterface
import engine.executionManager
# import engine.dataBrokerInterface

LOG_LEVEL = {"CRITICAL": 50, "ERROR": 40, "WARNING": 30, "INFO": 20, "DEBUG": 10}

CONF_FILE = "/etc/serrano/engine.json"


class EngineInstance(QObject):

    def __init__(self, conf_params):

        super(QObject, self).__init__()

        self.engineInterface = None
        self.executionManager = None
        self.statusTimer = None
        self.engine_uuid = None

        self.hostname = platform.uname()[1]

        self.config = conf_params

        logging.basicConfig(filename="%s.log" % (int(time.time())), level=LOG_LEVEL[self.config["log_level"]])

        # Suppress logging messages from pika only to levels: WARNING,ERROR and CRITICAL
        logging.getLogger('pika').setLevel(logging.WARNING)

        self.logger = logging.getLogger("SERRANO.ROT.EngineInstance")

        self._set_engine_uuid(self.config)

    def _set_engine_uuid(self, config):

        if "engine_uuid" in config and len(config["engine_uuid"]) > 0:
            self.engine_uuid = config["engine_uuid"]
            self.logger.info("Engine uuid is loaded from configuration: '%s'" % self.engine_uuid)
        else:
            self.engine_uuid = str(uuid.uuid4())
            self.logger.info("Create new engine uuid for the instance. UUID: '%s'" % self.engine_uuid)

    def boot(self):

        self.logger.info("Initialize services ... ")

        self.executionManager = engine.executionManager.ExecutionManager(self.engine_uuid)

        self.engineInterface = engine.engineInterface.EngineInterface(self.engine_uuid, self.config["databroker_interface"])
        self.engineInterface.dispatcherRequest.connect(self.executionManager.handle_dispatch_request)
        self.engineInterface.start()

        self.executionManager.forwardResponse.connect(self.engineInterface.forward_response)

        self.engine_heartbeat()

        self.statusTimer = QTimer(self)
        self.statusTimer.timeout.connect(self.engine_heartbeat)
        self.statusTimer.start(int(self.config["heartbeat"])*1000)

        self.logger.info("ROT execution engine is ready ...")

    def engine_heartbeat(self):
        self.engineInterface.forward_response(json.dumps({"engine_id": self.engine_uuid, "type": "heartbeat",
                                                          "timestamp": int(time.time()), "hostname": self.hostname}))


if __name__ == "__main__":

    signal.signal(signal.SIGINT, signal.SIG_DFL)

    config_params = None

    if os.path.exists("%s/.rot/engine.json" % os.path.expanduser("~")):
        with open("%s/.rot/engine.json" % os.path.expanduser("~")) as f:
            config_params = json.load(f)

    if config_params is None and os.path.exists(CONF_FILE):
        with open(CONF_FILE) as f:
            config_params = json.load(f)

    if config_params is None:
        sys.exit(0)

    app = QCoreApplication(sys.argv)

    instance = EngineInstance(config_params)
    instance.boot()

    sys.exit(app.exec_())
