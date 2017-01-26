#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    ssh $line "mkdir -p /tmp/galileo; echo 'export JAVA_HOME=/usr/lib/jvm/java-8-oracle/' >> ~/.bashrc; echo 'export GALILEO_HOME=$2' >> ~/.bashrc; echo 'export GALILEO_CONF=$2/config' >> ~/.bashrc; echo 'export GALILEO_ROOT=/tmp/galileo' >> ~/.bashrc; echo 'export JAVA_HOME=/usr/lib/jvm/java-8-oracle/' >> ~/.profile; echo 'export GALILEO_HOME=$2' >> ~/.profile; echo 'export GALILEO_CONF=$2/config' >> ~/.profile; echo 'export GALILEO_ROOT=/tmp/galileo' >> ~/.profile; source ~/.bashrc;"&
done < "$1"
