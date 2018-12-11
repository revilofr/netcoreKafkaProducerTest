#!/bin/bash

# Exporting default host ip as DOCKER_HOST_IP
echo 'Exporting default host ip as DOCKER_HOST_IP'
default_interface=$(ip ro sh | awk '/^default/{ print $5; exit }')
export DOCKER_HOST_IP=$(ip ad sh $default_interface | awk '/inet /{print $2}' | cut -d/ -f1)
[ -z "$DOCKER_HOST_IP" ] && (echo "No docker host ip address">&2; exit 1 )
echo "Default host IP is set to $DOCKER_HOST_IP"

#Lauching kafka containers 
docker-compose up