import os
import argparse
import json

from lib.data_process_lib import * 
from lib.directed_graph import DirectedGraph

class DataProcessExecutor:

    def __init__(self, args):
        self.__args = args

        file_basename, file_extension = os.path.splitext(os.path.basename(os.path.normpath(self.__args["file"])))
        self.__id = "_".join([file_basename, self.__args["id"]])

        self._BASE_PATH = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
        self._LOG_PATH = os.path.join(self._BASE_PATH, "log")

        create_if_not_exists_folders([
            self._LOG_PATH
        ])

        self.__config = self.__load_config(self.__args["file"])

        self._logger = self.__get_logger(os.path.join(self._LOG_PATH, self.__id + ".log"))
        self._logger.setLevel(self.__config["LOGGING_LEVEL"])

        self.__graph = self.__create_graph(self.__config)

        self.log_info("Flow Initialized")
        self.log_info(str(self.__graph))

    def log_info(self, message):
        self._logger.info("{} ~> {}".format(self.__id, message))

    def log_error(self, message):
        self._logger.error("{} ~> {}".format(self.__id, message))

    def log_debug(self, message):
        self._logger.debug("{} ~> {}".format(self.__id, message))

    def __get_logger(self, log_file):
        logger_key = "flow"
        fileConfig(
            os.path.join(self._BASE_PATH, "config", "logging.ini"),
            defaults={'LOG_FILE' : log_file}
        )
        return logging.getLogger(logger_key)

    def __load_config(self, config_file):
        with open(config_file, "r") as fread:
            return json.load(fread, object_pairs_hook=lambda dictionary : json_raise_on_duplicates(dictionary, [
                # Reservation for comments, not check, for example "__comment"
                lambda key : key.startswith("__")
            ]))

    def __create_graph(self, config):
        graph = DirectedGraph()

        for key, value in config["nodes"].items():
            graph.add_node(key, value)

        for destination, origins in config["dependencies"].items():
            for origin in origins:
                graph.add_edge(origin, destination)

        return graph


def parse_arguments():
    # Create argument parser
    parser = argparse.ArgumentParser()

    # Positional mandatory arguments
    parser.add_argument("file", help="Flow Configuration file", type=str)

    # Optional arguments with parameter
    parser.add_argument("-i", "--id", type=str, help="Execution ID", action="append", default=[get_time(dateformat="%Y%m%d%H%M%S")])
    
    # Optional arguments without parameter
    parser.add_argument("--show-command", help="Show the commands to execute the components", action="store_true")

    # Others
    parser.add_argument("--version", help="Check Version", action="version", version='%(prog)s - Version 1.0')

    # Parse arguments
    args = vars(parser.parse_args())
    args["id"] = "_".join(args.get("id"))

    return args

if __name__ == '__main__': 

    args = parse_arguments()
    data_process = DataProcessExecutor(args)

    print(args)





