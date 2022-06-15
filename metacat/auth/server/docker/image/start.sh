#!/bin/bash

cd /tmp/auth_server
cp -R /config .
chmod -R go+rx ./config

export AUTH_SERVER_CFG=/tmp/auth_server/config/config.yaml
export OPENSSL_ALLOW_PROXY_CERTS=1
httpd -D FOREGROUND


