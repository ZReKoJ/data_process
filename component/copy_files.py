#!/usr/bin/python3
import sys
import os
import re

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from data_process_lib import Component

class CopyFilesComponent(Component):

    def __init__(self):
        super().__init__()

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

        return { root : files for path in input_list for root, dirs, files in os.walk(path) if len(files) > 0 }

    def _read_config(self, node_info):
        config = super()._read_config(node_info)

        # Default Setting 
        config["in_out"] = config.get("in_out", "in")
        config["move"] = config.get("move", False)
        config["overwrite"] = config.get("overwrite", False)
        config["match"] = config.get("match", [".*"])

        return config

    def process(self):
        super().process()

        self.log_info("Start Process")

        destination_path = self._OUTPUT_PATH if self._config["in_out"] == "in" else self._config["path"]

        for path, filenames in self._data.items():
            for filename in filenames:
                if any([ re.search(regex, filename) for regex in self._config["match"]]):
                    origin_file = os.path.join(path, filename)
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