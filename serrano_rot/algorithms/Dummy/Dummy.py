import time
import json
import string
import random
import serrano_rot.algorithms.algorithmInterface as algorithmInterface


class Dummy(algorithmInterface.AlgorithmInterface):

    def __init__(self, parameters, infrastructure):
        super().__init__(parameters, infrastructure)

    def launch(self):
        params = self.get_input_parameters()

        random.seed(int(time.time()))
        rand_letters = random.choices(string.ascii_lowercase, k=params["length"])
        text = "".join(rand_letters)

        results = {"random_text": text*params["times"]}

        return json.dumps(results)