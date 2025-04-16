#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time
import tarfile

import concurrent.futures

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from data_process_lib import AsyncComponent

class TarGzComponent(AsyncComponent):

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

        futures = [ 
            self._executor.submit(self.create_tar_gz_from_folder, filepath, self._OUTPUT_PATH) 
            for filepath 
            in self._data
        ]
        
        for future in concurrent.futures.as_completed(futures):
            # Just to trigger the exception if happens
            future.result()
            
        self.log_info("End Process")

    @classmethod
    def create_tar_gz_from_folder(cls, source, destination):
        basename = os.path.basename(os.path.normpath(source))
        with tarfile.open(os.path.join(destination, basename + ".tar.gz"), "w:gz") as tar:
            tar.add(source, arcname=basename)

if __name__ == "__main__":
    try:
        component = TarGzComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)
