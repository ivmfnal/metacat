#!/bin/bash

docker=docker

if [ "$1" == "podman" ]; then
	docker=podman
	mount=`pwd`/config:/config:z
	#user="--user `id -u`"
	shift
else
	mount=`pwd`/config:/config
	user=""
fi

if [ "$1" == "-i" ]; then
	$docker run -ti --rm $user -v $mount -p 8143:8143 -p 8280:8280 --entrypoint /bin/bash metacat_auth_server 
else
	$docker run -d  --rm $user -v $mount -p 8143:8143 -p 8280:8280                        metacat_auth_server
fi
