#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    ssh $line "echo 'export _JAVA_OPTIONS=-Xmx4g' >> ~/.bashrc; echo 'export _JAVA_OPTIONS=-Xmx4g' >> ~/.profile; source ~/.bashrc;"&
done < "$1"

