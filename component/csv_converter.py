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

class CSVConverterComponent(AsyncComponent):

    __CONDITION_FUNCTION_REGEX = '(\\w+)\\(([^)]+)\\)'
    __CONDITION_PARAMETER_REGEX = '\\$(\\w+)'
    __CLASS_ID = "csv_converter"

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
        config["delimiter"] = config.get("delimiter", ",")
        config["header"] = config.get("header", True)
        config["conditions"] = config.get("conditions", [])

        return config

    @classmethod
    def convert_line(cls, line, conditions, is_header=False):
        for condition in conditions:
            function = re.match(cls.__CONDITION_FUNCTION_REGEX, condition)

            function_name = function.group(1)
            if cls.__CLASS_ID not in function_name:
                function_name = "_".join([cls.__CLASS_ID, function_name])
            function_parameters = []

            for param in function.group(2).split(","):
                parameter = param.strip()

                # Check if has index
                index_name = re.match(cls.__CONDITION_PARAMETER_REGEX, parameter)
                if index_name:
                    index_value = index_name.group(1)
                    # if line is digit or str

                    # list starts with index 0
                    index = list(line.keys())[int(index_value) - 1] if index_value.isdigit() else index_value

                    function_parameters.append(index)
                    continue
                
                # Check if it is a string ('')
                if parameter.startswith("'") and parameter.endswith("'"):
                    function_parameters.append(parameter.strip("'"))
                    continue

                function_parameters.append(parameter)
                
            line = UtilityFunction.generate_value(function_name)(line, *function_parameters, is_header=is_header)

        return [ str(item) for item in line.values() ]
        
    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")

        file_writer = FileWriter(mode="a" if self._config["header"] else "w")

        futures = []

        for filepath in self._data:
            
            file_basename, file_extension = os.path.splitext(os.path.basename(os.path.normpath(filepath)))
            output_filepath = os.path.join(self._OUTPUT_PATH, "{}_converted{}".format(file_basename, file_extension))
            header = None

            for line in read_file_line_by_line(filepath):
                line = OrderedDict(
                    (str(idx), field) 
                    for idx, field 
                    in enumerate(line.split(self._config["delimiter"]))
                )
                if not header and self._config["header"]:
                    header = self.convert_line(
                        line,
                        self._config["conditions"], 
                        is_header=True
                    )
                    with open(output_filepath, "w") as fw:
                        fw.write("{}\n".format(self._config["delimiter"].join(header)))
                else:
                    future = self._executor.submit(
                        self.convert_line, 
                        line,
                        self._config["conditions"]
                    )
                    # Convert the line
                    converted_line = future.result()
                    future = self._executor.submit(file_writer.write, output_filepath, self._config["delimiter"].join(converted_line))
                    futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            # Just to trigger exceptions if any
            future.result()

        file_writer.shutdown()

        self.log_info("End Process")

if __name__ == "__main__":
    try:
        component = CSVConverterComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)