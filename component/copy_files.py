#!/usr/bin/python3
import sys
import os
import re

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from data_process_lib import Component

class CopyFilesComponent(Component):

    def __init__(self):
        super().__init__()

    def _read_input(self, input_list):
        pass

    # to be implemented
    def _read_config(self, node_info):
        config = super()._read_config(node_info)

        # Default Setting 
        config["aa"] = config.get("aa", "hha")

        return config

    # to be implemented
    def process(self):
        super().process()

        self.log_info("Start Process")



        self.log_info("End Process")

if __name__ == "__main__":
    try:
        component = CopyFilesComponent()
        component.init()
        config = component.get_config()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)