import sys
import json
import signal
from PyQt5.QtCore import QCoreApplication

import api.clientEvents as clientEvents
import api.clientInstance as clientInstance


class Example:

    def __init__(self):
        self.client = None

    def start(self):
        self.client = clientInstance.ClientInstance()

        self.client.connect([clientEvents.EventExecutionCompleted,
                             clientEvents.EventExecutionError,
                             clientEvents.EventExecutionCancelled],
                            self.on_execution_response)
        self.client.connect([clientEvents.EventEnginesChanged], self.on_engines_changed)

    def on_engines_changed(self, evt):
        print("-------------------------------")
        print("Available engines changed")
        print(evt.engine_uuid)
        print(evt.event_id)
        print(evt.timestamp)
        print("-------------------------------")
        print()

    def on_execution_response(self, evt):
        print("-------------------------------")
        print("Event type: %s" % evt.evt_type)
        if evt.evt_type == "EventExecutionCompleted":
            print("Execution UUID: %s" % evt.execution_uuid)
            print("Results: %s" % evt.results)
            print()
            for log_msg in self.client.get_logs(evt.execution_uuid):
                print("\t %s " % json.dumps(log_msg))
            print()
        else:
            print("Execution UUID: %s" % evt.execution_uuid)
            print("Execution Status: %s" % evt.status)
            print("Execution Reason: %s" % evt.reason)
        print("-------------------------------")
        print()

    def play_with_rest(self):

        engines = self.client.get_engines()
        for engine_uuid in engines:
            print(self.client.get_engine(engine_uuid))
            print()

        print(self.client.get_executions())
        print()

        res = self.client.post_execution("Dummy", {"times":2, "length": 8})
        print(res)
        print()

        res = self.client.post_execution("Dummy2", {"times": 2, "length": 8})
        print(res)
        print()


if __name__ == "__main__":

    signal.signal(signal.SIGINT, signal.SIG_DFL)

    app = QCoreApplication(sys.argv)

    ex = Example()
    ex.start()

    ex.play_with_rest()

    sys.exit(app.exec_())
