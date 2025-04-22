#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time 
import zipfile
import csv
import string

import xml.etree.ElementTree as ET
import concurrent.futures

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from utils import read_file_line_by_line
from data_process_lib import AsyncComponent

class CSV2HTMLComponent(AsyncComponent):

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
            raise ImportError("No .xlsx files found")

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

        futures = [
            self._executor.submit(
                self.csv_to_html_table, 
                filepath,
                self._OUTPUT_PATH,
                self._config["delimiter"],
                self._config["header"]
            )
            for filepath 
            in self._data
        ]
    
        for future in concurrent.futures.as_completed(futures):
            # Just to trigger exceptions if any
            future.result()

        self.log_info("End Process")

    @classmethod
    def csv_to_html_table(cls, input_file, output_directory, delimiter, has_header):
        file_basename, file_extension = os.path.splitext(os.path.basename(os.path.normpath(input_file)))
        output_filepath = os.path.join(output_directory, "{}.html".format(file_basename))

        fw.write("<table>\n")
        with open(output_filepath, "w") as fw:
            # Check if has to read header
            read_header = not has_header
            for line in read_file_line_by_line(input_file):
                fw.write("<tr>{}</tr>\n".format("".join([
                    "<{tag}>{value}</{tag}>".format(**{
                        "value" : item,
                        "tag" : "td" if read_header else "th"
                    }) 
                    for item 
                    in line.split(delimiter)
                ])))
                read_header = True
        fw.write("</table>\n")

if __name__ == "__main__":
    try:
        component = CSV2HTMLComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)
