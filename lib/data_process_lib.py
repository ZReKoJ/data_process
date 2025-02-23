import argparse

# Component Class
# Used to be the parents of all the scripts in component folder
# Data process is based on the rules defined in this parent class
class Component(object):

    def __init__(self):
        self.__args = self.__parseArguments()

    def __parseArguments(self):
        # Create argument parser
        parser = argparse.ArgumentParser()

        # Mandatory arguments
        # Nothing 

        # Optional arguments
        parser.add_argument("-i", "--id", required=True, type=str, help="Execution ID")
        parser.add_argument("-z", "--component_id", required=True, type=str, help="Component ID")
        parser.add_argument("-f", "--flow", required=True, type=str, help="Flow File")
        parser.add_argument("-i", "--input", type=str, action='append', default=[], help="Input Data")

        # Version
        parser.add_argument("-v", "--version", action="version", help="Version", version="%(prog)s - Version 1.0")

        # Parse Arguments
        args = vars(parser.parse_args())

        return args
        