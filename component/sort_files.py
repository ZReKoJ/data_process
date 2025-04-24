#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time 
import re
import heapq
import shutil

import concurrent.futures

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from utils import FileWriter, MakeItPicklableWrapper, flatten
from data_process_lib import SortComponent

class CSVJoinerComponent(SortComponent):

    def __init__(self):
        super().__init__()

    def init(self):
        # to create the tmp folder
        super().init(tmp=True)

    # Abstract from parent
    def _read_input(self, input_list):
        files = []

        for input_record in input_list:
            if os.path.isdir(input_record):
                files = files + [ os.path.join(root, filename) for root, dirs, files in os.walk(input_record) if len(files) > 0 for filename in files ]
            elif os.path.isfile(input_record):
                files.append(input_record)
            else:
                raise ImportError("Path {} is incorrect".format(input_record))

        files = [ filepath for filepath in files if filepath.endswith(".csv")]
        if len(files) == 0:
            raise ImportError("No .csv files found")

        return files
        
    # Abstract from parent
    def _read_config(self, node_info):
        config = super()._read_config(node_info)

        # Default Setting 
        config["input_delimiter"] = config.get("input_delimiter", ",")
        config["output_delimiter"] = config.get("output_delimiter", ",")
        config["header"] = config.get("header", True)

        return config
        
    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")
        
        for filepath in self._data:
            origin_file = self._sort_file(
                filepath, 
                has_header=self._config["header"],
                key=MakeItPicklableWrapper(self.get_key).add_args(
                    self._config["key"], 
                    self._config["input_delimiter"]
                )
            )
            shutil.copy2(origin_file, os.path.join(self._OUTPUT_PATH, os.path.basename(origin_file)))

        self.log_info("End Process")

    @classmethod
    def get_key(cls, line, index_list, delimiter):
        csv_line = line.split(delimiter)
        # idx - 1, starts with 0 the list, and passed variable starts with 1
        return delimiter.join([ csv_line[idx - 1] if idx > 0 else csv_line[idx] for idx in index_list])

if __name__ == "__main__":
    try:
        component = CSVJoinerComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)
