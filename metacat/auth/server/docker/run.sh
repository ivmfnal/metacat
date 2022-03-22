#!/bin/bash



docker=podman

config=`pwd`/config
interactive="no"
port_in=8443
port_out=8143
ports=${port_out}:${port_in}

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
			docker=docker
			;;
		-p)
			shift
			port=$1
			;;
	esac
	shift
done

if [ "$docker" == "podman" ]; then
	mount=${config}:/config:z
else
	mount=${config}:/config
fi

if [ "$interactive" == "yes" ]; then
	$docker run -ti --rm -v $mount -p ${ports} --entrypoint /bin/bash auth_server 
else
	$docker run -d  --rm -v $mount -p ${ports}                        auth_server
fi
