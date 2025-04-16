#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time
import tarfile

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from data_process_lib import Component

class TarGzComponent(Component):

    def __init__(self):
        super().__init__()

    # Abstract from parent
    def _read_input(self, input_list):
        return input_list

    # Abstract from parent 
    def _read_config(self, node_info):
        config = super()._read_config(node_info)

        # Default Setting 

        return config

    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")

        for filepath in self._data:
            basename = os.path.basename(os.path.normpath(filepath))
            with tarfile.open(os.path.join(self._OUTPUT_PATH, basename + ".tar.gz"), "w:gz") as tar:
                tar.add(filepath, arcname=basename)
            
        self.log_info("End Process")

if __name__ == "__main__":
    try:
        component = TarGzComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)
