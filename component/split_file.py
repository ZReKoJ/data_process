#!/usr/bin/python3
import csv
import numbers
import functools
import time
import datetime
import re
import sys
import os

import concurrent.futures

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from utils import get_subclasses
from data_process_lib import AsyncComponent

###################################
# FUNCTIONS
###################################
    
def load_csv_file(path, delimiter):
    with open(path, 'r') as f:
        reader = csv.reader(f, delimiter=delimiter)
        for row in reader:
            yield row
        
def split_file(path, filename, output_path, output_filename):
    factory = SplitRuleFactory()

    for rule in sorted(config["split_rule"], key=lambda attribute : attribute["order"]):
        if re.search(rule["regex"], filename) is not None:
            record = factory.get_rule(rule["rule"]).split(path, filename, output_path, output_filename, rule["params"], config["header"])
    
###################################
# CLASSES
###################################

class SplitRuleFactory(object):

    def __init__(self):
        self.__split_rule_classes = list(map(lambda subclass : subclass(), get_subclasses(SplitRule)))
        self.__instances = {}

    def get_rule(self, rulename):
        upper_rulename = rulename.upper()
        if upper_rulename not in self.__instances:
            split_rule_class = next((split_rule_class for split_rule_class in self.__split_rule_classes if split_rule_class.check_rule(upper_rulename)), None)
            if split_rule_class is None:
                raise AssertionError("{} rule not defined".format(rulename))
            else:
                self.__instances[upper_rulename] = split_rule_class
        return self.__instances[upper_rulename]

class SplitRule(object):

    def __init__(self, iden):
        self._id = iden.upper()
    
    def check_rule(self, rulename):
        return self._id == rulename.upper()
    
    def split(self, path, filename, output_path, output_filename, params, header=None):
        raise NotImplementedError("Error: Function %s not implemented" % ("split"))
        
class SplitByValueRule(SplitRule):
    
    def __init__(self):
        super().__init__("SplitByValue")
        
    def split(self, path, filename, output_path, output_filename, params, header=None):
    
        files = {}
    
        file_basename, file_extension = os.path.splitext(filename)
        for record in load_csv_file(os.path.join(path, filename), params["delimiter"]):
            if header is True:
                header = record
                continue
            value = str(record[params["value_position"] - 1])
            if value not in files:
                files[value] = open(os.path.join(output_path, output_filename.format(value)), 'w')
                if header is not True and header is not None:
                    files[value].write("{}\n".format(params["delimiter"].join(header)))
            files[value].write("{}\n".format(params["delimiter"].join(record)))
            
        for key in files:
            files[key].close()
        
class SplitByValueEndingRule(SplitRule):
    
    def __init__(self):
        super().__init__("SplitByValueEnding")
        
    def split(self, path, filename, output_path, output_filename, params, header=None):
    
        files = {}
    
        file_basename, file_extension = os.path.splitext(filename)
        for record in load_csv_file(os.path.join(path, filename), params["delimiter"]):
            if header is True:
                header = record
                continue
            value = str(record[params["value_position"] - 1])
            value = value[-params["ending_length"]:]
            if value not in files:
                files[value] = open(os.path.join(output_path, output_filename.format(value)), 'w')
                if header is not True and header is not None:
                    files[value].write("{}\n".format(params["delimiter"].join(header)))
            files[value].write("{}\n".format(params["delimiter"].join(record)))
            
        for key in files:
            files[key].close()
            
class TailRule(SplitRule):
    
    def __init__(self):
        super().__init__("Tail")
        
    def split(self, path, filename, output_path, output_filename, params, header=None):
        # output_filename not used here
        
        block_size = params.get("block_size", 4096)
        
        with open(os.path.join(path, filename), "rb") as fr:
            fr.seek(0, 2)
            position = fr.tell()
            buffer = b""

            while position > 0:
                read_size = min(block_size, position)
                position -= read_size

                fr.seek(position)
                buffer = fr.read(read_size) + buffer

                if buffer.count(b"\n") >= params["number_lines"]:
                    break
                    
            output = buffer.splitlines()[-params["number_lines"]:]
            
            with open(os.path.join(output_path, filename), "wb") as fw:
                fw.write(b'\n'.join(output) + (b'\n' if len(output) > 0 else b''))

class HeadRule(SplitRule):

    def __init__(self):
        super().__init__("Head")

    def split(self, path, filename, output_path, output_filename, params, header=None):
        # output_filename not used here

        block_size = params.get("block_size", 4096)

        with open(os.path.join(path, filename), "rb") as fr:
            buffer = b""

            while True:
                chunk = fr.read(block_size)

                if not chunk:
                    break

                buffer += chunk

                if buffer.count(b"\n") >= params["number_lines"]:
                    break

        output = buffer.splitlines()[:params["number_lines"]]

        with open(os.path.join(output_path, filename), "wb") as fw:
                fw.write(b'\n'.join(output) + (b'\n' if len(output) > 0 else b''))
            
class SplitFileComponent(AsyncComponent):

    def __init__(self):
        super().__init__()

    # This function must be implemented by subclasses of class Component
    def _read_input(self, input_list):
        # For this component it should have only 1 input
        if len(input_list) > 0:
            data = list(map(lambda path : { root : files for root, dirs, files in os.walk(path) if len(files) > 0 }, input_list))
            return data
        raise AssertionError("Component expects +0 inputs and {} is provided".format(len(input_list)))
    
    # This function must be implemented by subclasses of class Component
    def _read_config(self, node_info):
        config = super()._read_config(node_info)
        
        # Check config
        config["header"] = config.get("header", False)
        if "split_rule" not in config or len(config["split_rule"]) == 0:
            raise ImportError("Split_rule is not provided or not rule is found")
        
        return config

    # This function must be implemented by subclasses of class Component
    def process(self):
        super().process()
        
        self.log_info("Start Process")
        
        content = [[] for _ in range(len(self._data))]
        
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=self._config["WORKERS"])
        
        futures = []
        
        # Read files
        for index, folders in enumerate(self._data):
            for path, filenames in folders.items():
                for filename in filenames:
                    file_basename, file_extension = os.path.splitext(filename)
                    convert_task = executor.submit(split_file, path, filename, self._OUTPUT_PATH, "{}_{{}}.{}_splitted".format(file_basename, index))
                    futures.append(convert_task)
                    
        for future in concurrent.futures.as_completed(futures):
            # Just to trigger the exception if happens
            future.result()
            
        executor.shutdown(wait=True)
        
        self.log_info("End Process")
    
###################################
# EXECUTION
###################################

if __name__ == '__main__':
    
    try:
        component = SplitFileComponent()
        component.init()
        # Get Config
        config = component.get_config()
        component.process()
    except:
        component.log_exception("Exception Occurred !!!")
        exit(1)
