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

from utils import FileWriter, MakeItPicklableWrapper
from data_process_lib import SortComponent

class CSVJoinerComponent(SortComponent):

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
        config["delimiter"] = config.get("delimiter", ",")
        config["header"] = config.get("header", True)

        return config
        
    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")

        sorted_files = []

        for input_idx, files in enumerate(self._data):
            sorted_files.append([ 
                self._sort_file(
                    filepath, 
                    has_header=self._config["header"],
                    key=MakeItPicklableWrapper(self.get_key).add_args(
                        self._config["key"][input_idx], 
                        self._config["delimiter"]
                    )
                )
                for filepath 
                in files 
            ])
        
        output_filepath = os.path.join(self._OUTPUT_PATH, "{}_joined.csv".format("_".join([self.whoami(), self._node_info["name"]])))

        file_handlers = [
            [ 
                open(sorted_file, "r")
                for sorted_file 
                in sorted_files_list
            ]
            for sorted_files_list 
            in sorted_files
        ]

        fw = open(output_filepath, "w")

        # Read header
        if self._config["header"]:
            # Write only once the header if many files for the same input
            header = []
            for file_handler_list in file_handlers:
                written_header = False
                for fr in file_handler_list:
                    line = fr.readline().strip()
                    if line and not written_header:
                        header.append(line)
                        written_header = True
            fw.write("{}\n".format(self._config["delimiter"].join(header)))

        # Initialize heaps
        queues = []
        for input_idx, file_handler_list in enumerate(file_handlers):
            queues.append([])
            for file_handler_idx, fr in enumerate(file_handler_list):
                line = fr.readline().strip()
                if line:
                    heapq.heappush(queues[input_idx], (
                        self.get_key(line, self._config["key"][input_idx], self._config["delimiter"]), 
                        input_idx,
                        file_handler_idx, 
                        line
                    ))

        # Join Loop
        join_line = []

        while sum([ len(queue) for queue in queues]) > 0:
            # cursor is the length of the array
            cursor = len(join_line)
            if cursor < len(sorted_files):
                # If not value in queue means the iteration ends
                if len(queues[cursor]) == 0:
                    join_line.pop()
                    continue

                key, input_idx, file_handler_idx, line = heapq.heappop(queues[cursor])
                # defines the first key
                if cursor == 0:
                    join_key = key

                if key == join_key:
                    join_line.append(line)

                    newline = file_handlers[input_idx][file_handler_idx].readline().strip()
                    if newline:
                        heapq.heappush(queues[cursor], (
                            self.get_key(newline, self._config["key"][input_idx], self._config["delimiter"]), 
                                input_idx,
                                file_handler_idx, 
                                newline
                            ))
                else:
                    # get last and return to queue
                    join_line.pop()
                    heapq.heappush(queues[cursor], (
                        self.get_key(newline, self._config["key"][input_idx], self._config["delimiter"]), 
                            input_idx,
                            file_handler_idx, 
                            line
                        ))
            else:
                fw.write("{}\n".format(self._config["delimiter"].join(join_line)))
                join_line.pop()

        fw.close()

        for file_handler_list in file_handlers:
            for fr in file_handler_list:
                fr.close()

        self.log_info("End Process")

    @classmethod
    def get_key(cls, line, index_list, delimiter):
        csv_line = line.split(delimiter)
        # idx - 1, starts with 0 the list, and passed variable starts with 1
        return delimiter.join([ csv_line[idx - 1] for idx in index_list])

if __name__ == "__main__":
    try:
        component = CSVJoinerComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)