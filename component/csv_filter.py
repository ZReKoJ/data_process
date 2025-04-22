#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time 
import re

import concurrent.futures
from collections import OrderedDict

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from utils import read_file_line_by_line, FileWriter, UtilityFunction
from data_process_lib import AsyncComponent

class CSVFilterComponent(AsyncComponent):

    __CONDITION_FUNCTION_REGEX = '(\\w+)\\(([^)]+)\\)'
    __CONDITION_PARAMETER_REGEX = '\\$(\\w+)'

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

        return files
        
    # Abstract from parent
    def _read_config(self, node_info):
        config = super()._read_config(node_info)

        # Default Setting 
        config["input_delimiter"] = config.get("input_delimiter", ",")
        config["output_delimiter"] = config.get("output_delimiter", ",")
        config["header"] = config.get("header", True)
        config["conditions"] = config.get("conditions", [])

        return config

    @classmethod
    def check_line(cls, line, conditions):
        # First loop is OR clause
        for or_condition in conditions:

            result = True 

            # Second loop is AND clause
            for and_condition in or_condition:
                function = re.match(cls.__CONDITION_FUNCTION_REGEX, and_condition)

                function_name = function.group(1)
                function_parameters = []

                for param in function.group(2).split(","):
                    parameter = param.strip()

                    # Check if has index
                    index_name = re.match(cls.__CONDITION_PARAMETER_REGEX, parameter)
                    if index_name:
                        index_value = index_name.group(1)

                        # list starts with index 0
                        index = list(line.keys())[int(index_value) - 1] if index_value.isdigit() else index_value

                        # list starts with index 0
                        function_parameters.append(line[index])
                        continue
                    
                    # Check if it is a string ('')
                    if parameter.startswith("'") and parameter.endswith("'"):
                        function_parameters.append(parameter.strip("'"))
                        continue

                    function_parameters.append(parameter)
                    
                result = result and UtilityFunction.predicate(function_name)(*function_parameters)

                # if after any iteration the result for one is False, it means in and clause it will be False so directly break the loop
                if not result:
                    break

            # if after any iteration the result for one is True, it means in or clause it will be True so directly return True
            if result:
                return True
        
    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")

        file_writer = FileWriter(mode="a" if self._config["header"] else "w")

        futures = []

        for filepath in self._data:
            
            file_basename, file_extension = os.path.splitext(os.path.basename(os.path.normpath(filepath)))
            output_filepath = os.path.join(self._OUTPUT_PATH, "{}_filtered{}".format(file_basename, file_extension))
            header = None

            for line in read_file_line_by_line(filepath):
                line = OrderedDict(
                    (str(idx) if not header else header[idx], field) 
                    for idx, field 
                    in enumerate(line.split(self._config["input_delimiter"]))
                )
                if not header and self._config["header"]:
                    header = list(line.values())
                    with open(output_filepath, "w") as fw:
                        fw.write("{}\n".format(self._config["output_delimiter"].join(header)))
                else:
                    future = self._executor.submit(self.check_line, line, self._config["conditions"])
                    # Check if __check_line returns true
                    if future.result():
                        future = self._executor.submit(file_writer.write, output_filepath, self._config["output_delimiter"].join(line.values()))
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
