#!/bin/sh

export PYTHONPATH=`pwd`:`pwd`/webpie

python dm_common/auth/auth_server.py -c config.yaml

