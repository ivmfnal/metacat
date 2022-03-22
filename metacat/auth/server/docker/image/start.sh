#!/bin/bash

cd /tmp/auth_server
cp -R /config .
chmod -R go+rx ./config

export AUTH_SERVER_CFG=/tmp/auth_server/config/config.yaml
httpd

while true; do
	sleep 1
done

