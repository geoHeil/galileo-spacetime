#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "Cleaning up the results on $line"
    ssh $line "rm -rf '${GALILEO_ROOT}'/.results;mkdir -p '${GALILEO_ROOT}'/.results"&
done < "$1"

