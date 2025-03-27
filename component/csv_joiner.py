#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time 
import re

import concurrent.futures

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from utils import FileWriter
from data_process_lib import AsyncComponent

class CSVJoinerComponent(AsyncComponent):

    def __init__(self):
        super().__init__()

    def init(self):
        # to create the tmp folder
        super().init(tmp=True)

    # Abstract from parent
    def _read_input(self, input_list):

        if len(input_list) > 1:
            data = list(map(
                    lambda path : [
                        os.path.join(root, filename) 
                        for root, dirs, files 
                        in os.walk(path) 
                        if len(files) > 0 
                            for filename in files 
                            if filename.endswith(".csv")
                    ], 
                    input_list
                ))

            for idx, files in enumerate(data):
                if len(files) == 0:
                    raise ImportError("No .csv files found for the input {}".format(idx))

            return data

        raise AssertionError("Component expects 1+ inputs and {} is provided".format(len(input_list)))
        
    # Abstract from parent
    def _read_config(self, node_info):
        config = super()._read_config(node_info)

        # Default Setting 
        config["delimiter"] = config.get("delimiter", "|")
        config["header"] = config.get("header", True)

        return config
        
    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")

        file_writer = FileWriter(mode="a" if self._config["header"] else "w")

        futures = []

        for input_idx, files in enumerate(self._data):
            print(input_idx, files)

        file_writer.shutdown()

        self.log_info("End Process")

if __name__ == "__main__":
    try:
        component = CSVJoinerComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)