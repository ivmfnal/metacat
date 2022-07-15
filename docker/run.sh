#!/bin/bash

tag=metacat-client:latest

if [ "$1" == "shell" ]; then
	docker run --rm \
		-ti \
		$tag /bin/bash

else
	docker run --rm  \
		-d \
		$tag
fi

