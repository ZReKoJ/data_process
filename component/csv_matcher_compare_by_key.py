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

class CSVMatcherCompareByKeyComponent(SortComponent):

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
        config["input_delimiter"] = config.get("input_delimiter", ",")
        config["output_delimiter"] = config.get("output_delimiter", "|")
        config["field_delimiter"] = config.get("field_delimiter", ",")
        config["show_equal"] = config.get("show_equal", False)
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
                    line = fr.readline().strip()
                    if line and not written_header:
                        header.append(line)
                        written_header = True
            fw.write("{}\n".format(self._config["input_delimiter"].join(header)))

        # Initialize heaps
        queues = []
        for input_idx, file_handler_list in enumerate(file_handlers):
            queues.append([])
            for file_handler_idx, fr in enumerate(file_handler_list):
                line = fr.readline().strip()
                if line:
                    heapq.heappush(queues[input_idx], (
                        self.get_key(line, self._config["key"][input_idx], self._config["input_delimiter"]), 
                        input_idx,
                        file_handler_idx, 
                        line
                    ))

        # Join Loop
        join_line = []
        futures = []

        while sum([ len(queue) for queue in queues]) > 0:

            selected = [ heapq.heappop(queue) if len(queue) > 0 else None for queue in queues ] 

            minimum = next(
                iter(sorted(
                    (line for line in selected if line is not None),
                    key=lambda line: line[0]
                )),
                None  # Default if the list is empty
            )

            to_compare_records = []

            for idx, selected_record in enumerate(selected):
                key, input_idx, file_handler_idx, line = selected_record

                # these has to be pushed back to queue
                if selected_record is not None and key != minimum[0]:
                    heapq.heappush(queues[input_idx], selected_record)
                    to_compare_records.append(None)
                # these are to be compared and new line should be added to queue
                elif selected_record is not None and key == minimum[0]:
                    to_compare_records.append(line.split(self._config["input_delimiter"]))
                    
                    line = file_handlers[input_idx][file_handler_idx].readline().strip()
                    if line:
                        heapq.heappush(queues[input_idx], (
                            self.get_key(line, self._config["key"][input_idx], self._config["input_delimiter"]), 
                            input_idx,
                            file_handler_idx, 
                            line
                        ))
                # if selected line is None (no more input)
                else:
                    to_compare_records.append(None)

            # key : minimum[0]
            futures.append(self._executor.submit(self.compare_record, minimum[0], to_compare_records))

        for future in concurrent.futures.as_completed(futures):
            key, status, compared_record, all_equal = future.result()

            fw.write("{}{}{}{}{}\n".format(
                key,
                self._config["output_delimiter"],
                self._config["output_delimiter"].join([
                    "" if not self._config["show_equal"] and all_equal[idx] else self._config["field_delimiter"].join(field) 
                    for idx, field
                    in enumerate(compared_record)
                ]),
                self._config["output_delimiter"],
                self._config["field_delimiter"].join(status)
            ))

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

    @classmethod
    def compare_record(self, key, record_to_compare):
        max_length = 0
        position = 0
        status = []
        compared_record = []
        all_equal = []

        for idx, record in enumerate(record_to_compare):
            if record is None:
                status.append("{}_MISSING".format(idx))
            else:
                max_length = max(max_length, len(record))

        while position < max_length:
            fields = list(map(
                lambda record: None if record is None else None if len(record) <= position else record[position], 
                record_to_compare
            ))
            all_equal.append(len(set(fields)) == 1)
            compared_record.append(list(map(
                lambda field : "" if field is None else field, fields
            )))
            position += 1

        status.append("EQUAL" if all(all_equal) else "DIFF")

        return key, status, compared_record, all_equal

if __name__ == "__main__":
    try:
        component = CSVMatcherCompareByKeyComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)