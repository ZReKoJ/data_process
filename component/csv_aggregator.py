#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time 
import re
import heapq

import concurrent.futures

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from utils import MakeItPicklableWrapper, UtilityFunction
from data_process_lib import SortComponent

class CSVAggregatorComponent(SortComponent):

    __CONDITION_FUNCTION_REGEX = '(\\w+)\\(([^)]+)\\)'
    __CONDITION_PARAMETER_REGEX = '\\$(\\w+)'
    __CLASS_ID = "csv_aggregator"

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
        config["delimiter"] = config.get("delimiter", ",")
        config["header"] = config.get("header", True)

        return config
        
    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")

        sorted_files = [ 
            self._sort_file(
                filepath, 
                has_header=self._config["header"],
                key=MakeItPicklableWrapper(self.get_key).add_args(
                    self._config["key"], 
                    self._config["delimiter"]
                )
            )
            for filepath 
            in self._data 
        ]
        
        output_filepath = os.path.join(self._OUTPUT_PATH, "{}_aggregated.csv".format("_".join([self.whoami(), self._node_info["name"]])))

        file_handlers = [ open(sorted_file, "r") for sorted_file in sorted_files ]

        fw = open(output_filepath, "w")

        # Read header
        if self._config["header"]:
            header = []
            # Write only once the header if many files for the same input
            written_header = False
            for fr in file_handlers:
                line = fr.readline().strip()
                if line and not written_header:
                    header += self.get_key(line, self._config["key"], self._config["delimiter"]).split(self._config["delimiter"])
                    written_header = True

            fw.write("{}\n".format(self._config["delimiter"].join(header + self._config["conditions"])))

        # Initialize heaps
        queue = []
        for idx, fr in enumerate(file_handlers):
            line = fr.readline().strip()
            if line:
                heapq.heappush(queue, (
                    self.get_key(line, self._config["key"], self._config["delimiter"]), 
                    idx, 
                    line
                ))
        
        agg_key = None 
        agg_records = []

        futures = []

        while len(queue) > 0:
            key, idx, line = heapq.heappop(queue)
            line = line.split(self._config["delimiter"])

            if key == agg_key:
                agg_records.append(line)
            else:
                if agg_key is not None:
                    futures.append(self._executor.submit(self.aggregate, agg_key.split(self._config["delimiter"]), agg_records, self._config["conditions"]))
                agg_key = key
                agg_records = [line]

            newline = file_handlers[idx].readline().strip()
            if newline:
                heapq.heappush(queue, (
                    self.get_key(newline, self._config["key"], self._config["delimiter"]), 
                    idx,
                    newline
                ))

        if agg_key is not None:
                futures.append(self._executor.submit(self.aggregate, agg_key.split(self._config["delimiter"]), agg_records, self._config["conditions"]))

        for future in concurrent.futures.as_completed(futures):
            line = future.result()
            fw.write("{}\n".format(self._config["delimiter"].join(line)))

        fw.close()

        for fr in file_handlers:
            fr.close()

        self.log_info("End Process")

    @classmethod
    def get_key(cls, line, index_list, delimiter):
        csv_line = line.split(delimiter)
        # idx - 1, starts with 0 the list, and passed variable starts with 1
        return delimiter.join([ csv_line[idx - 1] for idx in index_list])

    @classmethod
    def aggregate(cls, key, array_list, conditions):
        result = [] + key

        for condition in conditions:
            function = re.match(cls.__CONDITION_FUNCTION_REGEX, condition)

            function_name = function.group(1)
            if cls.__CLASS_ID not in function_name:
                function_name = "_".join([cls.__CLASS_ID, function_name])

            function_parameters = [ parameter.strip() for parameter in function.group(2).split(",")]

            converted_array_list = []

            for line in array_list:
                parameters = []
                for parameter in function_parameters:

                    # Check if has index
                    index_name = re.match(cls.__CONDITION_PARAMETER_REGEX, parameter)
                    if index_name:
                        index_value = index_name.group(1)
                        # if line is digit or dict
                        index = int(index_value) if index_value.isdigit() else index_value

                        # list starts with index 0
                        parameters.append(line[index - 1])
                        continue
                    
                    # Check if it is a string ('')
                    if parameter.startswith("'") and parameter.endswith("'"):
                        parameters.append(parameter.strip("'"))
                        continue

                    parameters.append(parameter)

                converted_array_list.append(parameters)
            
            result.append(UtilityFunction.generate_value(function_name)(converted_array_list))
        
        return [ str(element) for element in result ]
            
if __name__ == "__main__":
    try:
        component = CSVAggregatorComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)