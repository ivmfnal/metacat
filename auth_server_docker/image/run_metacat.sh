#!/bin/bash

cd /tmp

cp -R /config /tmp
chmod go-rwx /tmp/config/*.pem

export PYTHONPATH=`pwd`
python metacat/auth/server/auth_server.py -c /config/config.yaml
echo Runnig...
