#!/bin/bash

cd /tmp

cp -R /config /tmp
chmod go-rwx /tmp/config/*.pem

export PYTHONPATH=`pwd`
python env.py
echo Runnig env.py ...
