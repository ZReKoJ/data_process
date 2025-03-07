#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import re
import shutil

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from data_process_lib import Component

class CopyFilesComponent(Component):

    def __init__(self):
        super().__init__()

    # Abstract from parent
    def _read_input(self, input_list):
        # If copy out and path is not specified then error
        if self._config["in_out"] == "out" and "path" not in self._config:
            raise ImportError("path is not provided")

        if self._config["in_out"] == "in":
            if "path" in self._config:
                if isinstance(self._config["path"], list):
                    input_list.extend(self._config["path"])
                elif isinstance(self._config["path"], str):
                    input_list.append(self._config["path"])
                else:
                    raise ImportError("Unexpected path variable type")
        
        if len(input_list) == 0:
            raise ImportError("No input provided")

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
        config["in_out"] = config.get("in_out", "in")
        config["move"] = config.get("move", False)
        config["overwrite"] = config.get("overwrite", False)
        config["match"] = config.get("match", [".*"])

        return config

    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")

        destination_path = self._OUTPUT_PATH if self._config["in_out"] == "in" else self._config["path"]

        for origin_file in self._data:
            filename = os.path.basename(origin_file)
            if any([ re.search(regex, filename) for regex in self._config["match"]]):
                destination_file = os.path.join(destination_path, filename)
                if not self._config["overwrite"] and os.path.exists(destination_file):
                    raise FileExistsError("{} file already exists in the destination folder".format(filename))
                if self._config["move"]:
                    shutil.move(origin_file, destination_file)
                else:
                    shutil.copy(origin_file, destination_file)

        self.log_info("End Process")

if __name__ == "__main__":
    try:
        component = CopyFilesComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)