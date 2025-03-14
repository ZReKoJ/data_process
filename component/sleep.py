#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time 

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from data_process_lib import Component

class SleepComponent(Component):

    def __init__(self):
        super().__init__()

    # Abstract from parent
    def _read_input(self, input_list):
        return None
        
    # Abstract from parent
    def _read_config(self, node_info):
        config = super()._read_config(node_info)

        # Default Setting 
        config["seconds"] = config.get("seconds", 0)

        return config
        
    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")

        self.log_info("Sleep for {} seconds".format(self._config["seconds"]))
        time.sleep(self._config["seconds"])
        self.log_info("Awaken")

        self.log_info("End Process")

if __name__ == "__main__":
    try:
        component = SleepComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)