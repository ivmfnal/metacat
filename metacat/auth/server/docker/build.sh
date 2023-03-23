#!/bin/bash

docker=podman
tag=auth_server

if [ "$1" != "" ]; then
	tag=${tag}:$1
fi

if [ "$1" == "docker" ]; then
	docker=docker
fi

$docker build -t $tag image
