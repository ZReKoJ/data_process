#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time
import tarfile
import gzip
import shutil

import concurrent.futures

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from utils import *
from data_process_lib import AsyncComponent

class Packing(object):
    
    @staticmethod
    def check_method(method):
        raise NotImplementedError("Error: Function %s not implemented" % ("check_method"))
    
    def compress(self, source, destination):
        raise NotImplementedError("Error: Function %s not implemented" % ("compress"))
    
    def decompress(self, source, destination):
        raise NotImplementedError("Error: Function %s not implemented" % ("decompress"))

class TarGz(Packing):
    
    @staticmethod
    def check_method(method):
        return method.lower() in ["tar.gz", ".tar.gz", "targz"]
    
    def compress(self, source, destination):
        basename = os.path.basename(os.path.normpath(source))
        with tarfile.open(os.path.join(destination, basename + ".tar.gz"), "w:gz") as tar:
            tar.add(source, arcname=basename)
    
    def decompress(self, source, destination):
        with tarfile.open(source, 'r:gz') as tar:
            tar.extractall(path=destination)

class Gz(Packing):
    
    @staticmethod
    def check_method(method):
        return method.lower() in [".gz", "gz"]
    
    def decompress(self, source, destination):
        basename = os.path.basename(os.path.normpath(source))[:-3]
        with gzip.open(source, 'rb') as f_in:
            with open(os.path.join(destination, basename), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
class PackingComponent(AsyncComponent):

    def __init__(self):
        super().__init__()

    # Abstract from parent
    def _read_input(self, input_list):
        return input_list

    # Abstract from parent 
    def _read_config(self, node_info):
        config = super()._read_config(node_info)

        # Default Setting 
        config["compress_flag"] = config.get("compress_flag", True)
        config["compress_method"] = config.get("compress_method", "tar.gz")

        return config

    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")
        
        packing = next((subclass() for subclass in get_subclasses(Packing) if subclass.check_method(self._config["compress_method"])), None)
        
        futures = []
        
        if self._config["compress_flag"]:
            futures = [ 
                self._executor.submit(packing.compress, filepath, self._OUTPUT_PATH) 
                for filepath 
                in self._data
            ]
        else: 
            packing.decompress(
                "/home/miguser/csscript/zihao/data_process-main/execution/OSP2a2_OSPENT1_OSPENT2_OSPASEG_mastool_compare_sao_20250701165551/OSPENT2001_get_sao_files/cbs_pm_static_prop_inst_all_20250630_101_101_0_0.unl.gz",
                "/home/miguser/csscript/zihao/data_process-main/execution/OSP2a2_OSPENT1_OSPENT2_OSPASEG_mastool_compare_sao_20250701165551/OSPENT2002_decompress_tar_gz"
            )
            futures = [ 
                self._executor.submit(packing.decompress, os.path.join(root, filename), self._OUTPUT_PATH)
                for filepath
                in self._data
                    for root, dirs, files
                    in os.walk(filepath)
                        for filename
                        in files
            ] 
        
        for future in concurrent.futures.as_completed(futures):
            # Just to trigger the exception if happens
            future.result()
            
        self.log_info("End Process")

if __name__ == "__main__":
    try:
        component = PackingComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)
