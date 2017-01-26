#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    echo $line
    ssh $line "mkdir -p $3"&
    scp -C -i ~/.ssh/id_rsa -rp $2 $line:$3 &
done < "$1"
