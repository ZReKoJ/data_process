import os
import argparse
import logging
import json
import shutil
import re

from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
from collections import OrderedDict
from utils import json_raise_on_duplicates
from logging.config import fileConfig

def json_custom_process(key_value_pairs, mapping_values):

    mapped_key_value_pairs = [ 
        (key, re.sub(
            # {word} pattern
            "\\{(\\w+)\\}",
            lambda ocurrence : str(mapping_values.get(ocurrence.group(1), ocurrence.group(0))),
            value
        ) if isinstance(value, str) else value)
        for key, value 
        in key_value_pairs 
    ]

    registered_keys = json_raise_on_duplicates(mapped_key_value_pairs, [
        # Reservation for comments, not check, for example "__comment"
        lambda key : key.startswith("__")
    ], OrderedDict)

    return registered_keys

# Component Class
# Used to be the parents of all the scripts in component folder
# Data process is based on the rules defined in this parent class
class Component(object):

    _BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
    _loggers = {}

    def __init__(self):
        self._args = self.__parseArguments()

        self._execution_id = self._args["id"]
        self._component_id = self._args["component_id"]

        self._FLOW_CONFIG = self._args["flow"]
        self._LOG_PATH = os.path.join(self._BASE_PATH, "log")
        self._LOG_FILE = os.path.join(self._LOG_PATH, self._execution_id + ".log")

        if not os.path.exists(self._LOG_PATH):
            os.makedirs(self._LOG_PATH)

        # To have some dynamic values
        self._execution_variables = {
            "execution_id" : self._execution_id,
            "flow_path" : self._FLOW_CONFIG,
            "base_path" : self._BASE_PATH,
            "log_path" : self._LOG_PATH,
            "current_path" : os.getcwd()
        }

        logger = self._get_logger(self._LOG_FILE)
        # Default value, after init it will be overwritted
        logger.setLevel("INFO")
        for handler in logger.handlers:
            formatter = handler.formatter._fmt
            handler.setFormatter(logging.Formatter(re.sub("%\(message\)s", "{} ~> %(message)s".format(self.whoami()), formatter)))

    @classmethod
    def log_info(cls, message):
        # Message on all loggers
        for logger in cls._loggers.values():
            logger.info(message)

    @classmethod
    def log_error(cls, message):
        # Message on all loggers
        for logger in cls._loggers.values():
            logger.error(message)

    @classmethod
    def log_debug(cls, message):
        # Message on all loggers
        for logger in cls._loggers.values():
            logger.debug(message)

    @classmethod
    def log_exception(cls, message):
        # Message on all loggers
        for logger in cls._loggers.values():
            logger.exception(message)

    def init(self):
        self._node_info = self._read_flow(self._FLOW_CONFIG)
        self._config = self._read_config(self._node_info)

        self._get_logger(self._LOG_FILE).setLevel(self._config["LOGGING_LEVEL"])

        self._OUTPUT_PATH = os.path.join(self._BASE_PATH, "execution", self._execution_id, "_".join([self.whoami(), self._node_info["name"]]))
        self._INPUT_PATH = self._args["input"]

        self._data = self._read_input(self._INPUT_PATH)

        self.log_info("Component Initialized")

    @classmethod
    def _get_logger(cls, log_file):
        if log_file not in cls._loggers:
            logger_key = "component"
            fileConfig(
                os.path.join(cls._BASE_PATH, "config", "logging.ini"),
                defaults={"LOG_FILE" : log_file}
            )
            cls._loggers[log_file] = logging.getLogger(logger_key)
        
        return cls._loggers[log_file]
    
    # Abstract method. Must be implemented on all subclasses of this class
    def _read_input(self, input_list):
        raise NotImplementedError("Function {} not implemented".format("_read_input"))

    def _read_flow(self, config_file):
        if config_file is None or config_file == "":
            raise ImportError("No flow configuration is provided: {}".format(config_file))
        
        with open(config_file, "r") as fr:
            flow_config = json.load(fr, object_pairs_hook=lambda dictionary : json_custom_process(dictionary, self._execution_variables))
            if self.whoami() not in flow_config.get("nodes", {}):
                raise ImportError("Node with ID {} not found in flow {}".format(self._component_id, config_files))
            return flow_config.get("nodes", {}).get(self.whoami(), {})

    # Can be overwritted by subclass method
    def _read_config (self, node_info):
        config = node_info.get("config", {})

        # Default setting
        config["LOGGING_LEVEL"] = config.get("LOGGING_LEVEL", "INFO")

        return config

    # Can be overwritted by subclass method
    def process(self):
        self.__clean_output()

    def __clean_output(self):
        self.log_info("Output folder: {}".format(self._OUTPUT_PATH))
        if os.path.exists(self._OUTPUT_PATH):
            self.log_info("Output folder exists, proceed empty data")
            shutil.rmtree(self._OUTPUT_PATH)
        os.makedirs(self._OUTPUT_PATH)

    def get_args(self):
        return self._args

    def get_config(self):
        return self._config

    def whoami(self):
        return self._component_id

    def __parseArguments(self):
        # Create argument parser
        parser = argparse.ArgumentParser()

        # Mandatory arguments
        # Nothing 

        # Optional arguments
        parser.add_argument("--id", required=True, type=str, help="Execution ID")
        parser.add_argument("-z", "--component_id", required=True, type=str, help="Component ID")
        parser.add_argument("-f", "--flow", required=True, type=str, help="Flow File")
        parser.add_argument("-i", "--input", type=str, action='append', default=[], help="Input Data")

        # Version
        parser.add_argument("-v", "--version", action="version", help="Version", version="%(prog)s - Version 1.0")

        # Parse Arguments
        args = vars(parser.parse_args())

        return args
        
class AsyncComponent(Component):

    def __init__(self):
        super().__init__()

    # Abstract from parent
    def _read_input(self, input_list):
        raise NotImplementedError("Function {} not implemented".format("_read_input"))

    def _read_config(self, node_info):
        config = super()._read_config(node_info)

        # Default Setting 
        config["WORKERS"] = config.get("WORKERS", 10)

        return config

    def process(self):
        super().process()

        self._executor = ProcessPoolExecutor(max_workers=self._config["WORKERS"])
        self._data_manager = Manager()

    def __del__(self):
        self._data_manager.shutdown()
        self._executor.shutdown(wait=True)

    