#!/bin/bash

docker build -t metacat_server image
docker image prune -f
