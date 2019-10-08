#!/usr/bin/env bash
set -u

docker pull consul:1.6.0

sudo firewall-cmd --zone=public --add-port=8500/tcp
sudo firewall-cmd --zone=public --add-port=8600/udp

sudo docker run \
        -p 8500:8500 \
        -p 8600:8600/udp \
        --name=consul-datadist \
        consul:1.6.0 agent -server -ui -bootstrap -client=0.0.0.0
