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
    __CONDITION_PARAMETER_REGEX = '\\$([\\w-]+)'

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
                    
                        try:
                            index = int(index_value)
                            index = list(line.keys())[index - 1 if index > 0 else index]
                        except ValueError:
                            index = index_value
                            
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
                return line
        
    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")

        # self._data = ["/home/miguser/csscript/zihao/data_process-main/execution/grafana_info_20260923170529/M003_get_output_files/Online_NorthCGW_20260923.log"]

        futures = [ self._executor.submit(self.filter_file, filepath, self._OUTPUT_PATH, self._config) for filepath in self._data ]

        for future in concurrent.futures.as_completed(futures):
            # Just to trigger exceptions if any
            future.result()

        self.log_info("End Process")
       
    @classmethod 
    def filter_file(cls, filepath, out_path, config): 
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=config["WORKERS"])

        futures = []
        
        file_basename, file_extension = os.path.splitext(os.path.basename(os.path.normpath(filepath)))
        output_filepath = os.path.join(out_path, "{}_filtered{}".format(file_basename, file_extension))
        header = None
        fw = open(output_filepath, "w")

        for line in read_file_line_by_line(filepath):
            line = OrderedDict(
                (str(idx) if not header else header[idx], field) 
                for idx, field 
                in enumerate(line.split(config["input_delimiter"]))
            )
            if not header and config["header"]:
                header = list(line.values())
                fw.write("{}\n".format(config["output_delimiter"].join(header)))
            else:
                future = executor.submit(cls.check_line, line, config["conditions"])
                futures.append(future)
        
        for future in concurrent.futures.as_completed(futures):
            # Check if __check_line returns true
            line = future.result()
            if line:
                fw.write("{}\n".format(config["output_delimiter"].join(line.values())))
                    
        executor.shutdown(wait=True)
        

if __name__ == "__main__":
    try:
        component = CSVFilterComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)
