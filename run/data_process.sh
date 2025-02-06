#!/bin/bash
# Not using #!/usr/bin/python because the python I configured can be changed

directory=$(dirname $(realpath $0))
python ${directory}/../main.py $@
