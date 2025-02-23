#!/usr/bin/python3
import os
import json
import argparse
import subprocess
import functools

import concurrent.futures

from lib.utils import * 
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
        self.__component_paths = self.__check_component_paths()

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

    def log_exception(self, message):
        self._logger.exception("{} ~> {}".format(self.__id, message))

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

    def __check_component_paths(self):
        component_paths = {}

        for root, dirs, files in os.walk(os.path.join(self._BASE_PATH, "component")):
            for file in files:
                if file in component_paths:
                    raise ImportError("Files duplicated_\n    - {}\n    - {}".format(component_paths[file], os.path.join(root, file)))
                else:
                    component_paths[file] = os.path.join(root, file)

        return component_paths

    def __create_graph(self, config):
        graph = DirectedGraph()

        for key, value in config["nodes"].items():
            graph.add_node(key, value)

        for destination, origins in config["dependencies"].items():
            for origin in origins:
                graph.add_edge(origin, destination)

        return graph

    def __generate_commands(self):
        self.__component_commands = {}
        
        max_node_length = 0

        for node, parameters in self.__config["nodes"].items():
            command = []
            max_node_length = max(len(node), max_node_length)

            if "script" not in parameters:
                raise ImportError("Parameter {} not declared for the component {}".format("script", node))

            if parameters["script"] not in self.__component_paths:
                raise ImportError("{} not exists".format(parameters["script"]))

            command.append(self.__component_paths[parameters["script"]])

            if "name" not in parameters:
                raise ImportError("Parameter {} not declared for the component {}".format("name", node))

            command.append("-z")
            command.append(node)
            command.append("--id")
            command.append(self.__id)
            command.append("-f")
            command.append(self.__args["file"])

            for dependency in self.__graph.get_reversed_edge(node):
                dependency_parameters = self.__config["nodes"][dependency]

                if "name" not in dependency_parameters:
                    raise ImportError("Parameter {} not declared for the component {}".format("name", node))

                command.append("-i")
                command.append(os.path.join(self._BASE_PATH, "execution", self.__id, "_".join([dependency, dependency_parameters["name"]])))

            self.__component_commands[node] = " ".join(command)
    
        for node, command in self.__component_commands.items():
            self.log_info("Command --> {}: {}".format(node.ljust(max_node_length), command))

    def __check(self):
        # If has cycles, the flow will never end
        if self.__graph.has_cycle():
            raise ImportError("The flow has cycles so the process will never end")

        # No need check, the results can be executed in parallel without being one only flow
        # if self.__graph.is_connected_undirected():
        #    raise ImportError("There is a component that is not connected into the flow")

        return True

    def run(self):
        if not self.__args["show_command"]:
            if self.__check():
                self.__executed_nodes = { node : False for node in self.__config["nodes"] }
                self.__components_waiting_to_be_executed = {}

                self.log_info("Start Flow")
                self.__generate_commands()

                self.__executor = concurrent.futures.ProcessPoolExecutor(max_workers=self.__config["WORKERS"])

                for node in self.__config["nodes"]:
                    if len(self.__graph.get_reversed_edge(node)) == 0:
                        self.log_info("Component {} called".format(node))
                        component_task = self.__executor.submit(subprocess.call, self.__component_commands[node], shell=True)
                        component_task.add_done_callback(functools.partial(self._on_component_finished, node))
                    else:
                        self.__components_waiting_to_be_executed[node] = { edge : False for edge in self.__graph.get_reversed_edge(node) }
        else: 
            self.__generate_commands()

    def _on_component_finished(self, node, future):
        if future.exception() is not None:
            self.log_error("Component {} finished with result {}".format(node, future.result()))
            self.log_error("{}".format(future.exception()))
            raise RuntimeError("Component {} execution failed".format(node))
        
        result = future.result()
        self.log_info("Component {} finished with result {}".format(node, future.result()))
        if result != 0:
            raise RuntimeError("Component {} execution failed".format(node))
        self.__executed_nodes[node] = True

        for edge in self.__graph.get_edge(node):
            self.__components_waiting_to_be_executed[edge][node] = True
            if all(self.__components_waiting_to_be_executed[edge].values()):
                self.log_info("Component {} called".format(edge))
                component_task = self.__executor.submit(subprocess.call, self.__component_commands[edge], shell=True)
                component_task.add_done_callback(functools.partial(self._on_component_finished, edge))

            if all(self.__executed_nodes.values()):
                self.log_info("End Flow")
                self.__executor.shutdown(wait=True)

def parse_arguments():
    # Create argument parser
    parser = argparse.ArgumentParser()

    # Positional mandatory arguments
    parser.add_argument("file", help="Flow Configuration file", type=str)

    # Optional arguments with parameter
    parser.add_argument("--id", type=str, help="Execution ID", action="append", default=[get_time(dateformat="%Y%m%d%H%M%S")])
    
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
    data_process.run()





