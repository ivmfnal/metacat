#!/bin/bash

docker=docker

config=`pwd`/config
interactive="no"
port_in=8080
port_out=8088
ssl_port_in=443
ssl_port_out=9443
pod_name=metacat_server
image_name=metacat_server

while [ "$1" != "" ]; do
	a=$1
	case $a in
		-i)
			interactive="yes"
			;;
		-c)
			shift
			config=$1
			;;
		-d)
			# debug
			port_out=8142
			pod_name=auth_server_debug
			;;
		-p)
			shift
			port_out=$1
			;;
	esac
	shift
done

ports="-p ${port_out}:${port_in} -p ${ssl_port_out}:${ssl_port_in}"

if [ "$docker" == "podman" ]; then
	mount=${config}:/config:z
else
	mount=${config}:/config
fi

if [ "$interactive" == "yes" ]; then
	$docker run -ti --rm -v $mount ${ports} --name $pod_name --entrypoint /bin/bash $image_name
else
	$docker run -d  --rm -v $mount ${ports} --name $pod_name			   $image_name
fi
