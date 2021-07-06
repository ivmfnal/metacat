#!/bin/bash

docker=docker

if [ "$1" == "podman" ]; then
	docker=podman
fi

$docker build -t metacat_auth_server .
