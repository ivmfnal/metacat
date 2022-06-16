#!/bin/bash

cd /tmp/auth_server
cp -R /config .
chmod -R go+rx ./config
chmod -R go-rwx ./config/*.pem

cp config/auth_server.conf /etc/httpd/conf.d/
if [ -f /etc/httpd/conf.d/zgridsite.conf ]; then
	mv /etc/httpd/conf.d/zgridsite.conf /etc/httpd/conf.d/zgridsite.conf-hide
fi

export AUTH_SERVER_CFG=`pwd`/config/config.yaml
#export OPENSSL_ALLOW_PROXY_CERTS=1
httpd -D FOREGROUND

