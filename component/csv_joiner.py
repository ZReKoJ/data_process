#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time 
import re
import heapq
import itertools

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

        if len(input_list) > 1:
            data = list(map(
                    lambda path : [
                        os.path.join(root, filename) 
                        for root, dirs, files 
                        in os.walk(path) 
                        if len(files) > 0 
                            for filename in files 
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
        config["input_delimiter"] = config.get("input_delimiter", ",")
        config["output_delimiter"] = config.get("output_delimiter", ",")
        config["header"] = config.get("header", True)
        config["join_none"] = config.get("join_none", False)

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
                        self._config["input_delimiter"]
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
                    line = fr.readline().strip().split(self._config["input_delimiter"])
                    if line and not written_header:
                        header.append(line)
                        written_header = True
            # add key at the beginning and remove keys on the others
            # starts with 0 the list, and passed variable starts with 1
            header = [ 
                str(header[0][idx - 1]) 
                for idx 
                in self._config["key"][0] 
            ] + [ 
                [ 
                    item 
                    for i_idx, item 
                    in enumerate(head) 
                    if i_idx + 1 not in self._config["key"][j_idx]
                ]
                for j_idx, head 
                in enumerate(header) 
            ]
            fw.write("{}\n".format(self._config["output_delimiter"].join(list(flatten(header)))))

        # Initialize heaps
        num_fields_without_key = [None] * len(sorted_files)
        queues = []
        for input_idx, file_handler_list in enumerate(file_handlers):
            queues.append([])
            for file_handler_idx, fr in enumerate(file_handler_list):
                line = fr.readline().strip()
                if line:
                    line_without_key = [
                        item 
                        for idx, item in enumerate(line.split(self._config["input_delimiter"])) 
                        if idx + 1 not in self._config["key"][input_idx]
                    ]
                    if num_fields_without_key[input_idx] is None:
                        num_fields_without_key[input_idx] = len(line_without_key)
                    elif num_fields_without_key[input_idx] != len(line_without_key):
                        raise ImportError("Number of fields do not match!!!")
                    heapq.heappush(queues[input_idx], (
                        self.get_key(line, self._config["key"][input_idx], self._config["input_delimiter"]), 
                        input_idx,
                        file_handler_idx, 
                        self._config["input_delimiter"].join(line_without_key)
                    ))

        # Join Loop
        join_line = []

        while sum([ len(queue) for queue in queues]) > 0:
        
            min_key = min([ queue[0][0] for queue in queues if len(queue) > 0 ])
            
            records = []
            
            for queue in queues:
                records.append([])
                
                while len(queue) > 0 and queue[0][0] == min_key:
                    key, input_idx, file_handler_idx, line = heapq.heappop(queue)
                    records[input_idx].append(line)
                    
                    newline = file_handlers[input_idx][file_handler_idx].readline().strip()
                    if newline:
                        heapq.heappush(queue, (
                            self.get_key(newline, self._config["key"][input_idx], self._config["input_delimiter"]), 
                            input_idx,
                            file_handler_idx,
                            self._config["input_delimiter"].join([
                                item 
                                for idx, item in enumerate(newline.split(self._config["input_delimiter"])) 
                                if idx + 1 not in self._config["key"][input_idx]
                            ])
                        ))
            
            if self._config["join_none"]:
                # if empty one array, fill with one record_lines with empty values
                records = [ record_lines if len(record_lines) > 0 else [self._config["input_delimiter"] * (num_fields_without_key[record_idx] - 1)] for record_idx, record_lines in enumerate(records) ]
                
            for combo in itertools.product(*records):
                fw.write("{}\n".format(self._config["output_delimiter"].join(
                    [ min_key.replace(self._config["input_delimiter"], self._config["output_delimiter"]) ] + 
                    [ line_part.replace(self._config["input_delimiter"], self._config["output_delimiter"]) for line_part in combo ]
                )))

        fw.close()

        for file_handler_list in file_handlers:
            for fr in file_handler_list:
                fr.close()

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
