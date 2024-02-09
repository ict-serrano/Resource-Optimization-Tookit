import logging

import serrano_rot.engine.requestInterface as requestInterface
import serrano_rot.engine.responseInterface as responseInterface

from PyQt5.QtCore import QThread
from PyQt5.QtCore import pyqtSignal

logger = logging.getLogger("SERRANO.ROT.EngineInterface")


class EngineInterface(QThread):
    dispatcherRequest = pyqtSignal(object)

    def __init__(self, engine_id, config):
        QThread.__init__(self)

        self.engine_id = engine_id

        self.requestInterface = requestInterface.RequestInterface(engine_id, config)
        self.requestInterface.dispatcherRequest.connect(self.handle_incoming_request)
        self.responseInterface = responseInterface.ResponseInterface(config)

        logger.info("EngineInterface is read ...")

    def handle_incoming_request(self, data):
        self.dispatcherRequest.emit(data)

    def forward_response(self, data):
        self.responseInterface.forward_response_to_controller(data)

    def __del__(self):
        self.wait()

    def run(self):
        self.requestInterface.start()
        #self.responseInterface.start()
        logger.info("EngineInterface is running ...")

