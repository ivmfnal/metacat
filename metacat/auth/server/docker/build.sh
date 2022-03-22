#!/bin/bash

docker=podman

if [ "$1" == "docker" ]; then
	docker=docker
fi

$docker build -t auth_server docker
