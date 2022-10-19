#!/bin/bash



docker=podman

config=`pwd`/config
interactive="no"
port_in=8443
port_out=8143
pod_name=auth_server

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

ports=${port_out}:${port_in}

if [ "$docker" == "podman" ]; then
	mount=${config}:/config:z
else
	mount=${config}:/config
fi

if [ "$interactive" == "yes" ]; then
	$docker run -ti --rm -v $mount -p ${ports} --name $pod_name --entrypoint /bin/bash auth_server 
else
	$docker run -d  --rm -v $mount -p ${ports} --name $pod_name			   auth_server
fi
