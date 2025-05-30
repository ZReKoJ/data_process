#!/bin/bash

directory=$(dirname $(realpath $0))

find ${directory}/../component -type f -exec dos2unix {} \; >/dev/null 2>&1
find ${directory}/../component -type f -exec chmod +x {} \; >/dev/null 2>&1

if [ $# -eq 0 ]; then
    path=${directory}/../flow
    while ! [[ -f "$path" && "$path" == *.json ]]; do
        select option in $(find ${path} -mindepth 1 -maxdepth 1 \( -type f -name '*.json' -o -type d \) -exec basename {} \;);
        do
            path="${path}/${option}"
            break
        done
    done
    ${directory}/../main.py ${path}
else
    ${directory}/../main.py $@
