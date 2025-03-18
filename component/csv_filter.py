#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time 

import concurrent.futures

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from utils import read_file_line_by_line, FileWriter
from data_process_lib import AsyncComponent

class CSVFilterComponent(AsyncComponent):

    def __init__(self):
        super().__init__()

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
        config["delimiter"] = config.get("delimiter", "|")
        config["header"] = config.get("header", True)

        return config

    @classmethod
    def check_line(cls, line):
        return True
        
    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")

        file_writer = FileWriter(mode="a")

        futures = []

        for filepath in self._data:
            
            file_basename, file_extension = os.path.splitext(os.path.basename(os.path.normpath(filepath)))
            output_filepath = os.path.join(self._OUTPUT_PATH, "{}_filtered{}".format(file_basename, file_extension))
            read_header = False

            for line in read_file_line_by_line(filepath):
                if not read_header and self._config["header"]:
                    read_header = True
                    with open(output_filepath, "w") as fw:
                        fw.write("{}\n".format(line))
                else:
                    if self.check_line(line):
                        future = self._executor.submit(
                            file_writer.write,
                            output_filepath, 
                            line
                        )
                        futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            # Just to trigger exceptions if any
            future.result()

        file_writer.shutdown()

        self.log_info("End Process")

if __name__ == "__main__":
    try:
        component = CSVFilterComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)