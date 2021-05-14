#!/bin/bash

if [ "$1" == "-i" ]; then
	docker run -ti --rm -v `pwd`/config:/config -p 8243:8243 -p 8280:8280 --entrypoint /bin/bash metacat_auth_server 
else
	docker run     --rm -v `pwd`/config:/config -p 8243:8243 -p 8280:8280                        metacat_auth_server
fi
