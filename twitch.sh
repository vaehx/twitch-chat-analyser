#!/bin/bash

export DOCKER_HOSTNAME=`hostname`
docker-compose -f bootstrap/docker-compose.yml $@
