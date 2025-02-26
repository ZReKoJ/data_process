#!/bin/bash


directory=$(dirname $(realpath $0))

find ${directory}/../component -type f -exec dos2unix {} \;
find ${directory}/../component -type f -exec chmod +x {} \;

${directory}/../main.py $@
