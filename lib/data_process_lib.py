import os
import argparse
import logging
import json
import shutil
import re

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

    def __init__(self):
        self._args = self.__parseArguments()

        self._execution_id = self._args["id"]
        self._component_id = self._args["component_id"]

        self._FLOW_CONFIG = self._args["flow"]
        self._BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
        self._LOG_PATH = os.path.join(self._BASE_PATH, "log")

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

        self._logger = self._get_logger(os.path.join(self._LOG_PATH, self._execution_id + ".log"))
        # Default value, after init it will be overwritted
        self._logger.setLevel("INFO")

    def log_info(self, message):
        self._logger.info("{} ~> {}".format(self.whoami(), message))

    def log_error(self, message):
        self._logger.error("{} ~> {}".format(self.whoami(), message))

    def log_debug(self, message):
        self._logger.debug("{} ~> {}".format(self.whoami(), message))

    def log_exception(self, message):
        self._logger.exception("{} ~> {}".format(self.whoami(), message))

    def init(self):
        self._node_info = self._read_flow(self._FLOW_CONFIG)
        self._config = self._read_config(self._node_info)

        self._logger.setLevel(self._config["LOGGING_LEVEL"])

        self._OUTPUT_PATH = os.path.join(self._BASE_PATH, "execution", self._execution_id, "_".join([self.whoami(), self._node_info["name"]]))
        self._INPUT_PATH = self._args["input"]

        self._data = self._read_input(self._INPUT_PATH)

        self.log_info("Component Initialized")

    def _get_logger(self, log_file):
        logger_key = "component"
        fileConfig(
            os.path.join(self._BASE_PATH, "config", "logging.ini"),
            defaults={"LOG_FILE" : log_file}
        )
        logger = logging.getLogger(logger_key)
        return logger
    
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
        config["WORKERS"] = config.get("WORKERS", 10)
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
        