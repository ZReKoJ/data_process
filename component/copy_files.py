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