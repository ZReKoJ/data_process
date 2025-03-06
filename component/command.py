#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time
import subprocess 

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from collections import OrderedDict
from data_process_lib import Component

class CommandComponent(Component):

    def __init__(self):
        super().__init__()

    def _read_input(self, input_list):
        return None
        
    def _read_config(self, node_info):
        config = super()._read_config(node_info)

        # Default Setting 
        config["variables"] = config.get("variables", {})
        config["commands"] = config.get("commands", {})

        return config

    def process(self):
        super().process()

        self.log_info("Start Process")

        variables = self._config["variables"]

        for variable, command in self._config["commands"].items():
            # Execute the command and capture the output
            self.log_info("Execute command: {}".format(command.format(**variables)))
            output = subprocess.check_output(
                list(map(lambda sentence : sentence.format(**variables), command.split(" ")))
            ).decode("utf-8")

            with open("{}/{}.result".format(self._OUTPUT_PATH, variable), 'a') as fw:
                fw.write("{}".format(output))

            variables[variable] = output.replace("\n", "")
            
        self.log_info("End Process")

if __name__ == "__main__":
    try:
        component = CommandComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)
