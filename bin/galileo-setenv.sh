#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    ssh $line "mkdir -p /tmp/galileo; echo 'export JAVA_HOME=$2' >> ~/.bashrc; echo 'export GALILEO_HOME=$3' >> ~/.bashrc; echo 'export GALILEO_CONF=$3/config' >> ~/.bashrc; echo 'export GALILEO_ROOT=/tmp/galileo' >> ~/.bashrc; echo 'export JAVA_HOME=$2' >> ~/.profile; echo 'export GALILEO_HOME=$3' >> ~/.profile; echo 'export GALILEO_CONF=$3/config' >> ~/.profile; echo 'export GALILEO_ROOT=/tmp/galileo' >> ~/.profile; source ~/.bashrc;"&
done < "$1"
