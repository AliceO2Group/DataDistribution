#!/usr/bin/env bash
set -u

docker pull consul:1.10

sudo firewall-cmd --zone=public --add-port=8500/tcp
sudo firewall-cmd --zone=public --add-port=8600/udp

docker rm consul-datadist

sudo docker run \
        -p 8500:8500 \
        -p 8600:8600/udp \
        --name=consul-datadist \
        consul:1.10
